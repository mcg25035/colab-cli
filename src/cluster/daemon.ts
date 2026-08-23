import fs from 'fs';
import net from 'net';
import path from 'path';
import readline from 'readline';
import { AuthManager } from '../auth/auth-manager.js';
import { listRegistryAccounts } from '../auth/accounts-registry.js';
import { ColabClient } from '../colab/client.js';
import { Variant } from '../colab/api.js';
import { COLAB_API_DOMAIN, COLAB_GAPI_DOMAIN } from '../config.js';
import { DaemonClient } from '../daemon/client.js';
import { RuntimeManager } from '../runtime/runtime-manager.js';
import {
  CLUSTER_DIR,
  CLUSTER_LOG_FILE,
  CLUSTER_PID_FILE,
  CLUSTER_SOCK,
  addJob,
  getJob,
  getVmSetup,
  jobLogFile,
  listJobs,
  markVmSetup,
  readClusterState,
  setupHash,
  updateJob,
  writeClusterState,
  type Job,
} from './state.js';
import { pickIdleVm, snapshotPool } from './pool.js';
import { clusterUpload } from './upload.js';

/**
 * Cluster scheduler daemon: one long-lived local process spanning ALL
 * accounts and ALL their runtimes. CLI talks to it over a Unix socket via
 * JSON-lines (same framing as the per-runtime daemon). It owns the job
 * queue: submit → pick idle VM (or provision one) → open a background
 * shell on that VM's per-runtime daemon → run the command → watch status.
 * Recovery from reclaimed runtimes is Phase 4 and explicitly out of scope.
 */

const DISPATCH_INTERVAL_MS = 10_000;
const MAX_PROVISION_ATTEMPTS_PER_TICK = 1;
/** Accounts whose auth failed recently — skip until this epoch-ms. */
const authDeadUntil = new Map<string, number>();
let provisionRound = 0;
/** Consecutive transient dispatch failures per job (resets on success). */
const dispatchAttempts = new Map<number, number>();
const MAX_DISPATCH_ATTEMPTS = 5;

if (process.env.COLAB_CLUSTER_DAEMON !== '1') {
  console.error('cluster daemon must be spawned by the CLI (sets COLAB_CLUSTER_DAEMON=1)');
  process.exit(1);
}

fs.mkdirSync(CLUSTER_DIR, { recursive: true });
const logStream = fs.createWriteStream(CLUSTER_LOG_FILE, { flags: 'a' });
const clog = (...args: unknown[]) => {
  const line = `[${new Date().toISOString()}] ${args.map(String).join(' ')}`;
  logStream.write(line + '\n');
  console.log(line);
};

// ── per-account lazily-built managers (cluster daemon spans all accounts) ──
const managers = new Map<string, RuntimeManager>();
const managerReady = new Map<string, Promise<RuntimeManager>>();
async function managerFor(accountId: string): Promise<RuntimeManager> {
  let pending = managerReady.get(accountId);
  if (!pending) {
    pending = (async () => {
      const authManager = new AuthManager(accountId);
      await authManager.initialize(); // without this, getAccessToken reports "not logged in"
      const colabClient = new ColabClient(
        new URL(COLAB_API_DOMAIN),
        new URL(COLAB_GAPI_DOMAIN),
        () => authManager.getAccessToken(),
        () => authManager.logout(),
      );
      const m = new RuntimeManager(colabClient, accountId);
      managers.set(accountId, m);
      return m;
    })();
    managerReady.set(accountId, pending);
  }
  return pending;
}

async function daemonClientFor(job: Job): Promise<DaemonClient> {
  if (!job.accountId || !job.serverId) throw new Error(`job ${job.id} has no assignment`);
  const client = new DaemonClient();
  await client.connect(job.accountId, job.serverId);
  return client;
}

/** Run python on the kernel and drain output to completion. */
async function drainExec(client: DaemonClient, code: string): Promise<string> {
  let out = '';
  for await (const o of client.exec(code)) {
    if (o.type === 'error') throw new Error(`kernel exec: ${o.ename}: ${o.evalue}`);
    if (o.type === 'stream' && o.text) out += o.text;
  }
  return out;
}

// ── job log mirroring ──
// A job's VM (and its shell output) can vanish at any moment, so we
// continuously mirror a rendered tail of each running job's shell into a
// local `logs/job-<id>.log`. On shell close we take one final, larger
// snapshot. Diagnosis after the fact never depends on the VM still living.

const MIRROR_TAIL_LINES = 500;
const FINAL_TAIL_LINES = 2000;

const SETUP_OK_MARK = '__CLUSTER_SETUP_OK__';
const JOB_OK_MARK = '__CLUSTER_JOB_OK__';

/** Overwrite the job's log mirror with the current shell snapshot. */
async function mirrorJobLog(job: Job, client: DaemonClient, tailLines: number): Promise<string> {
  const snap = await client.shellAttachSnapshot(job.shellId!, tailLines);
  fs.mkdirSync(CLUSTER_DIR + '/logs', { recursive: true });
  const header =
    `# job ${job.id}${job.name ? ` (${job.name})` : ''}` +
    ` on ${job.accountId} @ ${job.endpoint}` +
    `\n# mirrored ${new Date().toISOString()}  shell=${job.shellId} status=${snap.status}\n\n`;
  fs.writeFileSync(jobLogFile(job.id), header + snap.buffered);
  return snap.buffered;
}

// ── dispatcher ──

async function pollRunningJobs(): Promise<void> {
  for (const job of readClusterState().jobs.filter((j) => j.status === 'running')) {
    try {
      const client = await daemonClientFor(job);
      try {
        const entry = (await client.shellList()).find((s) => s.shellId === job.shellId);
        if (!entry || entry.status === 'closed') {
          // Shell gone/closed → capture the final, larger snapshot while the
          // 5-min retention window might still serve it, then classify by
          // marker lines: setup failure vs command failure vs success.
          let tail = '';
          try {
            tail = await mirrorJobLog(job, client, FINAL_TAIL_LINES);
          } catch { /* retention already evicted it — the periodic mirrors stand */ }
          if (job.endpoint && tail.includes(SETUP_OK_MARK)) {
            markVmSetup(job.endpoint, job.accountId!, setupHash(job.setupScript ?? ''));
          }
          const jobOk = tail.includes(JOB_OK_MARK);
          const rehearsal = tail.includes(SETUP_OK_MARK);
          const succeeded = jobOk || (job.rehearse && rehearsal) || (!job.setupScript && !job.rehearse && tail.includes(JOB_OK_MARK));
          updateJob(job.id, {
            status: succeeded ? 'done' : 'failed',
            error: succeeded
              ? undefined
              : tail.includes(SETUP_OK_MARK) || !job.setupScript
                ? 'command exited without the completion marker (see log)'
                : 'setup script failed (see log)',
            lastOutput: tail.slice(-4000),
            endedAt: new Date().toISOString(),
          });
          clog(`job ${job.id} ${succeeded ? 'done' : 'failed'} (shell ${job.shellId} closed)`);
        } else {
          await mirrorJobLog(job, client, MIRROR_TAIL_LINES).catch(() => {});
        }
      } finally {
        client.close();
      }
    } catch (err) {
      // Daemon unreachable — its runtime is most likely reclaimed. Phase 4
      // will add re-provisioning from checkpoint; for now, fail clearly but
      // leave the last mirror standing for diagnosis.
      updateJob(job.id, {
        status: 'failed',
        error: `runtime unreachable: ${err instanceof Error ? err.message : String(err)} (last mirror in ${jobLogFile(job.id)})`,
        endedAt: new Date().toISOString(),
      });
      clog(`job ${job.id} failed: runtime unreachable`);
    }
  }
}

async function dispatchQueuedJobs(): Promise<void> {
  const state = readClusterState();
  const queued = state.jobs.filter((j) => j.status === 'queued');
  if (queued.length === 0) return;

  const running = state.jobs.filter((j) => j.status === 'running' || j.status === 'provisioning');
  const pool = await snapshotPool(running);

  for (const job of queued) {
    let target = await pickIdleVm(pool);
    if (!target) {
      // No idle VM in the whole pool — provision one. Rotate the starting
      // account each tick (a stable sort would otherwise retry the same
      // first accounts forever) and skip accounts whose login recently
      // failed (their tokens are dead until re-auth).
      if (running.length === 0 && state.jobs.some((j) => j.status === 'provisioning')) return;
      const now = Date.now();
      const healthy = listRegistryAccounts().filter((a) => (authDeadUntil.get(a.email) ?? 0) < now);
      const sorted = healthy.sort(
        (a, b) =>
          (pool.accounts.find((p) => p.accountId === a.email)?.vms.length ?? 0) -
          (pool.accounts.find((p) => p.accountId === b.email)?.vms.length ?? 0),
      );
      // rotate: start at (provisionRound % len) so ticks don't all hammer
      // the same least-loaded account
      const accounts = sorted.map((_, i) => sorted[(i + provisionRound) % sorted.length]);
      provisionRound++;
      let provisioned = false;
      for (const acct of accounts.slice(0, MAX_PROVISION_ATTEMPTS_PER_TICK + 1)) {
        try {
          clog(`job ${job.id}: no idle VM; provisioning runtime on ${acct.email}`);
          const server = await (await managerFor(acct.email)).create({
            variant: Variant.DEFAULT,
            // undefined accelerator = resolveAccelerator picks CPU
            accelerator: job.accelerator,
          });
          clog(`job ${job.id}: provisioned ${server.endpoint} on ${acct.email}`);
          target = { accountId: acct.email, server };
          provisioned = true;
          break;
        } catch (err) {
          const emsg = err instanceof Error ? err.message : String(err);
          clog(`job ${job.id}: provision on ${acct.email} failed: ${emsg}`);
          if (/not logged in/i.test(emsg)) {
            // dead token: skip this account for 10 minutes
            authDeadUntil.set(acct.email, now + 10 * 60_000);
          }
        }
      }
      if (!provisioned) {
        updateJob(job.id, { error: 'waiting for an idle VM (all accounts at capacity)' });
        continue;
      }
    }
    if (!target) continue; // unreachable (continue above), keeps TS narrowing honest

    try {
      updateJob(job.id, { status: 'provisioning' });
      const client = new DaemonClient();
      await client.connect(target.accountId, target.server.id);
      try {
        // 1) uploads first — the training command never starts before its data
        if (job.uploads?.length) {
          for (const up of job.uploads) {
            clog(`job ${job.id}: uploading ${up.src} -> ${up.dest}`);
            await clusterUpload(client, target.server, up.src, up.dest);
          }
        }

        // 2) Write the whole job as a temp script and run it with a fresh
        //    NON-interactive `bash` (which honors `set -e`; the relay's own
        //    bash is interactive because stdin is a TTY, so `set -e` fed to
        //    it would be silently ignored). Markers in stdout let the poller
        //    classify setup-failure vs command-failure vs success.
        const needsSetup =
          !!job.setupScript && getVmSetup(target.server.endpoint)?.hash !== setupHash(job.setupScript);
        const scriptPath = `/tmp/.cluster_job_${job.id}.sh`;
        const script = [
          'set -e',
          ...(needsSetup && job.setupScript ? [job.setupScript] : []),
          `echo ${SETUP_OK_MARK}`,
          job.command,
          `echo ${JOB_OK_MARK}`,
        ].filter(Boolean);
        if (needsSetup && job.setupScript) {
          clog(`job ${job.id}: running setup script on ${target.server.endpoint}`);
        }
        // Write the script via the KERNEL exec channel (base64-bodied):
        // heredoc-feeding on the PTY would echo the marker lines into the
        // scrollback, making the poller see "__MARK__" even when the script
        // never ran them.
        const b64 = Buffer.from(script.join('\n') + '\n', 'utf8').toString('base64');
        await drainExec(client, `import base64; open(${JSON.stringify(scriptPath)},'w').write(base64.b64decode(${JSON.stringify(b64)}).decode())`);
        const shellId = await client.shellOpen(120, 30);
        client.shellInput(shellId, `bash ${scriptPath}\nexit\n`);
        updateJob(job.id, {
          status: 'running',
          accountId: target.accountId,
          serverId: target.server.id,
          endpoint: target.server.endpoint,
          shellId,
          startedAt: new Date().toISOString(),
          error: undefined,
        });
        clog(`job ${job.id} running on ${target.accountId} ${target.server.endpoint} shell ${shellId}`);
        dispatchAttempts.delete(job.id);
        // mark the VM busy within this tick's pool so a second queued job
        // doesn't land on the same machine
        for (const acct of pool.accounts) {
          const vm = acct.vms.find((v) => v.server.endpoint === target!.server.endpoint);
          if (vm) vm.jobIds.push(job.id);
        }
      } finally {
        client.close();
      }
    } catch (err) {
      // Transient (daemon/socket flapped, relay WS 404 right after spawn)
      // → requeue, up to MAX_DISPATCH_ATTEMPTS; content errors (bad setup
      // script, missing upload src) fail hard and let the agent read the log.
      const emsg = err instanceof Error ? err.message : String(err);
      const transient = /connect|ECONNREFUSED|socket|WS open|404/i.test(emsg);
      const attempts = (dispatchAttempts.get(job.id) ?? 0) + 1;
      if (transient && attempts < MAX_DISPATCH_ATTEMPTS) {
        dispatchAttempts.set(job.id, attempts);
        updateJob(job.id, { status: 'queued', error: `dispatch attempt ${attempts} failed: ${emsg}` });
        clog(`job ${job.id} dispatch attempt ${attempts} failed, requeued: ${emsg}`);
      } else {
        dispatchAttempts.delete(job.id);
        updateJob(job.id, {
          status: 'failed',
          error: `dispatch failed: ${emsg}`,
          endedAt: new Date().toISOString(),
        });
        clog(`job ${job.id} dispatch FAILED: ${emsg}`);
      }
    }
  }
}

// ── socket protocol ──

type Request =
  | { type: 'ping' }
  | { type: 'submit'; command: string; name?: string; accelerator?: string;
      setupScript?: string; uploads?: Array<{ src: string; dest: string }>; rehearse?: boolean }
  | { type: 'list' }
  | { type: 'logs'; jobId: number; tail?: number }
  | { type: 'cancel'; jobId: number }
  | { type: 'pool' }
  | { type: 'shutdown' };

type Response =
  | { type: 'pong' }
  | { type: 'submitted'; job: Job }
  | { type: 'job_list'; jobs: Job[] }
  | { type: 'job_logs'; jobId: number; output: string; status: string }
  | { type: 'cancelled'; jobId: number; ok: boolean; error?: string }
  | { type: 'pool_result'; pool: unknown }
  | { type: 'error'; message: string };

async function handleRequest(msg: Request): Promise<Response> {
  switch (msg.type) {
    case 'ping':
      return { type: 'pong' };

    case 'submit': {
      const job = addJob({
        command: msg.command,
        name: msg.name,
        accelerator: msg.accelerator,
        setupScript: msg.setupScript,
        uploads: msg.uploads,
        rehearse: msg.rehearse,
      });
      clog(`job ${job.id} submitted${job.rehearse ? ' (rehearse)' : ''}: ${msg.command.slice(0, 80)}`);
      void dispatchSoon();
      return { type: 'submitted', job };
    }

    case 'list':
      return { type: 'job_list', jobs: listJobs() };

    case 'logs': {
      const job = getJob(msg.jobId);
      if (!job) return { type: 'error', message: `job ${msg.jobId} not found` };
      if (job.status === 'running' && job.accountId && job.shellId !== undefined) {
        // live snapshot, and refresh the durable mirror while we're here
        try {
          const client = await daemonClientFor(job);
          try {
            const tail = await mirrorJobLog(job, client, msg.tail ?? MIRROR_TAIL_LINES);
            return { type: 'job_logs', jobId: job.id, output: tail, status: job.status };
          } finally {
            client.close();
          }
        } catch (err) {
          return { type: 'error', message: `logs unavailable: ${err instanceof Error ? err.message : String(err)}` };
        }
      }
      // terminated/disconnected — the mirror file is the durable record and
      // survives a reclaimed VM
      try {
        return { type: 'job_logs', jobId: job.id, output: fs.readFileSync(jobLogFile(job.id), 'utf8'), status: job.status };
      } catch {
        return { type: 'job_logs', jobId: job.id, output: job.lastOutput ?? '', status: job.status };
      }
    }

    case 'cancel': {
      const job = getJob(msg.jobId);
      if (!job) return { type: 'cancelled', jobId: msg.jobId, ok: false, error: 'not found' };
      if (job.status === 'queued') {
        updateJob(job.id, { status: 'cancelled', endedAt: new Date().toISOString() });
        return { type: 'cancelled', jobId: job.id, ok: true };
      }
      if (job.status === 'running' || job.status === 'provisioning') {
        try {
          // shell close kills the whole VM-side process tree (not just EOF)
          const client = await daemonClientFor(job);
          try {
            if (job.shellId !== undefined) await client.shellClose(job.shellId);
          } finally {
            client.close();
          }
        } catch (err) {
          clog(`job ${job.id} cancel: close failed (continuing as cancelled): ${err instanceof Error ? err.message : String(err)}`);
        }
        updateJob(job.id, { status: 'cancelled', endedAt: new Date().toISOString() });
        return { type: 'cancelled', jobId: job.id, ok: true };
      }
      return { type: 'cancelled', jobId: job.id, ok: false, error: `already ${job.status}` };
    }

    case 'pool': {
      const running = listJobs().filter((j) => j.status === 'running');
      return { type: 'pool_result', pool: await snapshotPool(running) };
    }

    case 'shutdown':
      setTimeout(() => process.exit(0), 100);
      return { type: 'pong' };

    default:
      return { type: 'error', message: `unknown request type` };
  }
}

// dispatchSoon lets submit trigger an immediate dispatch instead of waiting
// for the next interval tick.
let dispatchTimer: NodeJS.Timeout | undefined;
function dispatchSoon() {
  if (dispatchTimer) return;
  dispatchTimer = setTimeout(() => {
    dispatchTimer = undefined;
    void tick();
  }, 500);
}

let ticking = false;
async function tick() {
  if (ticking) return; // a slow pollRunningJobs must never overlap the next tick
  ticking = true;
  try {
    await pollRunningJobs();
    await dispatchQueuedJobs();
  } catch (err) {
    clog('tick error:', err instanceof Error ? err.message : String(err));
  } finally {
    ticking = false;
  }
}

async function main() {
  await new Promise<void>((resolve, reject) => {
    const server = net.createServer((socket) => {
      const rl = readline.createInterface({ input: socket });
      rl.on('error', () => {});
      rl.on('line', (line) => {
        let msg: Request;
        try {
          msg = JSON.parse(line);
        } catch {
          return;
        }
        void handleRequest(msg)
          .then((res) => {
            if (!socket.destroyed) socket.write(JSON.stringify(res) + '\n');
          })
          .catch((err) => {
            if (!socket.destroyed)
              socket.write(JSON.stringify({ type: 'error', message: String(err) }) + '\n');
          });
      });
    });
    server.on('error', reject);
    try { fs.unlinkSync(CLUSTER_SOCK); } catch {}
    server.listen(CLUSTER_SOCK, () => {
      fs.chmodSync(CLUSTER_SOCK, 0o600);
      resolve();
    });
  });
  fs.writeFileSync(CLUSTER_PID_FILE, String(process.pid));
  clog(`cluster daemon ready on ${CLUSTER_SOCK} (pid ${process.pid}, ${listRegistryAccounts().length} accounts)`);

  setInterval(() => void tick(), DISPATCH_INTERVAL_MS).unref();
  void tick();
}

main().catch((err) => {
  clog('fatal:', err instanceof Error ? err.message : String(err));
  process.exit(1);
});
