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
import { extractProgress } from './progress-watcher.js';
import { scanAndFetchCkpts, shouldScanNow } from './checkpoint-manager.js';
import { getStoredServer, listStoredServers, removeStoredServer } from '../runtime/storage.js';

/**
 * Cluster scheduler daemon: one long-lived local process spanning ALL
 * accounts and ALL their runtimes. CLI talks to it over a Unix socket via
 * JSON-lines (same framing as the per-runtime daemon). It owns the job
 * queue: submit → pick idle VM (or provision one) → open a background
 * shell on that VM's per-runtime daemon → run the command → watch status.
 * Reclaimed runtimes are auto-recovered (Phase 4): the job is requeued,
 * lastCkpt re-uploaded, and CLUSTER_RESUME env vars injected for the rerun.
 */

const DISPATCH_INTERVAL_MS = 10_000;
const MAX_PROVISION_ATTEMPTS_PER_TICK = 1;
/** Accounts whose auth failed recently — skip until this epoch-ms. */
const authDeadUntil = new Map<string, number>();
let provisionRound = 0;
/** Consecutive transient dispatch failures per job (resets on success). */
const dispatchAttempts = new Map<number, number>();
const MAX_DISPATCH_ATTEMPTS = 5;
/** VMs that just ate a dispatch failure — fenced off for a few minutes so the
 *  dispatcher falls back to another idle VM (or provisions a fresh one)
 *  instead of slamming the same dead end 5 times and failing the job (T35). */
const badVmUntil = new Map<string, number>();
const BAD_VM_MS = 5 * 60_000;
const vmKey = (accountId: string, endpoint: string) => `${accountId}:${endpoint}`;

/**
 * Map a job-spec accelerator string to a Colab variant + accelerator name.
 * Mirrors parseAcceleratorSelection() in commands/runtime.ts, but returns
 * errors instead of process.exit (daemon must not die on a bad job spec).
 */
function acceleratorToVariant(accel?: string): { variant: Variant; accelerator?: string } {
  if (!accel) return { variant: Variant.DEFAULT };
  const n = accel.trim().toUpperCase().replace(/\s+(GPU|TPU)$/, '').replace(/[-\s]/g, '');
  if (n === 'CPU') return { variant: Variant.DEFAULT };
  if (['H100', 'G4', 'A100', 'L4', 'T4'].includes(n)) return { variant: Variant.GPU, accelerator: n };
  if (n === 'V6E1' || n === 'V5E1') return { variant: Variant.TPU, accelerator: n };
  throw new Error(`unknown accelerator: ${accel}`);
}

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
const DEFAULT_MAX_RECOVERIES = 3;

/** Consecutive daemon-probe failures before a running job's runtime is
 *  declared dead. Guards recovery from our-side network flaps. */
const PROBE_FAIL_THRESHOLD = 3;
const jobProbeFails = new Map<number, number>();

// ── Phase 4: runtime reclaim recovery ──
// A reclaimed/lost runtime surfaces as a daemon-connect failure while the
// job is running. Instead of failing the job outright, requeue it: the
// dispatcher will pick (or provision) a fresh VM, re-run setup there
// (vmSetup cache is keyed by endpoint), re-upload lastCkpt and inject
// CLUSTER_RESUME / CLUSTER_RESUME_CKPT so the user's command can resume.
// Whether the command actually resumes is application-layer — we only
// deliver the ckpt and the signal.
function tryRecoverJob(job: Job, reason: string): void {
  const rec = job.recoveries ?? 0;
  const max = job.maxRecoveries ?? DEFAULT_MAX_RECOVERIES;
  const canRecover = job.allowRecover !== false && rec < max;
  if (canRecover) {
    updateJob(job.id, {
      status: 'queued',
      accountId: undefined,
      serverId: undefined,
      endpoint: undefined,
      shellId: undefined,
      recoveries: rec + 1,
      recoverPending: true,
      lastRecoveredAt: new Date().toISOString(),
      error: undefined,
      progress: undefined,
    });
    clog(`job ${job.id} RECOVERY ${rec + 1}/${max}: requeued (${reason})`);
  } else {
    const why =
      job.allowRecover === false ? 'allow_recover=false' : `recovery exhausted (${rec}/${max})`;
    updateJob(job.id, {
      status: 'failed',
      error: `${reason} — ${why} (last mirror in ${jobLogFile(job.id)})`,
      endedAt: new Date().toISOString(),
    });
    clog(`job ${job.id} failed: ${reason} — ${why}`);
  }
}

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

/** Run progress + checkpoint observation for a live job off an already-open
 *  daemon client. Failures here must never disturb the job — degrade quietly. */
async function observeJob(job: Job, client: DaemonClient, logTail: string, forceCkptScan = false): Promise<void> {
  if (job.progressPattern) {
    const p = extractProgress(logTail, job.progressPattern);
    if (p !== undefined && p !== job.progress) {
      updateJob(job.id, { progress: p });
      job.progress = p;
    }
  }
  if (job.ckptGlob && shouldScanNow(job.id, forceCkptScan)) {
    const server =
      job.accountId && job.serverId ? getStoredServer(job.accountId, job.serverId) : undefined;
    if (server) {
      try {
        const n = await scanAndFetchCkpts(job, client, server, clog);
        if (n > 0) clog(`job ${job.id}: ${n} ckpt file(s) mirrored locally`);
      } catch (err) {
        clog(`job ${job.id}: ckpt scan failed (will retry): ${err instanceof Error ? err.message : String(err)}`);
      }
    }
  }
}

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
          // One last forced ckpt pull so a late-written checkpoint isn't
          // stranded on a runtime that's about to be released.
          await observeJob(job, client, tail, true);
          // Ground truth for pass/fail is the rc file the runner writes —
          // markers in scrollback can be lost to tail truncation or bypassed
          // by an explicit `exit` in the user command (issue #9).
          let rc: number | undefined;
          try {
            const out = await drainExec(
              client,
              `import os; p=${JSON.stringify(`/tmp/.cluster_job_${job.id}.rc`)}; print(open(p).read().strip() if os.path.exists(p) else '')`,
            );
            const parsed = parseInt(out.trim().split('\n').pop() ?? '', 10);
            if (Number.isFinite(parsed)) rc = parsed;
          } catch { /* VM may already be unhealthy; fall back to markers */ }
          if (rc === 0 && job.endpoint) {
            markVmSetup(job.endpoint, job.accountId!, setupHash(job.setupScript ?? ''));
          } else if (job.endpoint && tail.includes(SETUP_OK_MARK)) {
            // runner may be older (pre-rc fix) — keep the marker path working
            markVmSetup(job.endpoint, job.accountId!, setupHash(job.setupScript ?? ''));
          }
          const jobOk = rc === 0 || tail.includes(JOB_OK_MARK);
          const rehearsal = tail.includes(SETUP_OK_MARK);
          const succeeded = jobOk || (job.rehearse && rehearsal);
          const errMsg =
            rc !== undefined
              ? rehearsal
                ? `command exited with code ${rc} (see log)`
                : `setup script failed with code ${rc} (see log)`
              : !job.setupScript || tail.includes(SETUP_OK_MARK)
                ? 'command exited without the completion marker (see log)'
                : 'setup script failed (see log)';
          updateJob(job.id, {
            status: succeeded ? 'done' : 'failed',
            error: succeeded ? undefined : errMsg,
            lastOutput: tail.slice(-4000),
            endedAt: new Date().toISOString(),
          });
          clog(`job ${job.id} ${succeeded ? 'done' : 'failed'} (shell ${job.shellId} closed)`);
        } else {
          const buf = await mirrorJobLog(job, client, MIRROR_TAIL_LINES).catch(() => '');
          await observeJob(job, client, buf);
        }
      } finally {
        client.close();
      }
    } catch (err) {
      // Daemon unreachable — maybe reclaimed, maybe just a transient network
      // flap on OUR side. Don't act on a single failed tick (a hiccup would
      // otherwise orphan the still-running VM-side process); require several
      // consecutive failures before declaring the runtime dead.
      const fails = (jobProbeFails.get(job.id) ?? 0) + 1;
      jobProbeFails.set(job.id, fails);
      if (fails < PROBE_FAIL_THRESHOLD) {
        clog(`job ${job.id}: daemon probe failed ${fails}/${PROBE_FAIL_THRESHOLD} (${err instanceof Error ? err.message : String(err)}) — retrying next tick`);
        continue;
      }
      jobProbeFails.delete(job.id);
      const reason = `runtime unreachable: ${err instanceof Error ? err.message : String(err)}`;
      tryRecoverJob(job, reason);
      continue;
    }
    jobProbeFails.delete(job.id);
  }
}

async function dispatchQueuedJobs(): Promise<void> {
  const state = readClusterState();
  const queued = state.jobs.filter((j) => j.status === 'queued');
  if (queued.length === 0) return;

  const running = state.jobs.filter((j) => j.status === 'running' || j.status === 'provisioning');
  const pool = await snapshotPool(running);

  for (const job of queued) {
    // Fence recently-dead VMs out of this tick's picks, then pick matching hardware.
    const now0 = Date.now();
    const poolView: typeof pool = {
      accounts: pool.accounts.map((a) => ({
        ...a,
        vms: a.vms.filter((vm) => (badVmUntil.get(vmKey(a.accountId, vm.server.endpoint)) ?? 0) <= now0),
      })),
    };
    let target = await pickIdleVm(poolView, job.accelerator);
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
          const sel = acceleratorToVariant(job.accelerator);
          const server = await (await managerFor(acct.email)).create({
            variant: sel.variant,
            accelerator: sel.accelerator,
          });
          clog(`job ${job.id}: provisioned ${server.endpoint} (${server.label}) on ${acct.email}`);
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

        // 1.5) Phase 4 recovery: re-upload the mirrored ckpt onto the fresh
        // VM so the re-run can resume. App-layer still owns the resume logic;
        // we only deliver the file and the CLUSTER_RESUME_* env vars.
        let resumeCkptRemote = '';
        if (job.recoverPending) {
          const local = job.lastCkpt;
          const entry = job.ckpts?.find((c) => c.localPath === local);
          if (local && entry && fs.existsSync(local)) {
            const dest = entry.remotePath;
            const dir = dest.slice(0, dest.lastIndexOf('/'));
            await drainExec(client, `import os; os.makedirs(${JSON.stringify(dir)}, exist_ok=True)`);
            clog(`job ${job.id}: recovery — re-uploading ${path.basename(local)} -> ${dest}`);
            await clusterUpload(client, target.server, local, dest);
            resumeCkptRemote = dest;
          } else {
            clog(`job ${job.id}: recovery — no usable lastCkpt, resuming without ckpt`);
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
        const runnerPath = `/tmp/.cluster_run_${job.id}.sh`;
        const rcPath = `/tmp/.cluster_job_${job.id}.rc`;
        const script = [
          'set -e',
          `export CLUSTER_RESUME=${job.recoverPending ? 1 : 0}`,
          `export CLUSTER_RESUME_CKPT=${resumeCkptRemote ? JSON.stringify(resumeCkptRemote) : "''"}`,
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
        //
        // The RUNNER wrapper (also base64-written, never echoed) records the
        // real exit status: a user command ending in `exit 0` would otherwise
        // kill the script before JOB_OK is echoed and the job would be
        // misclassified as failed (issue #9). rc is ground truth; markers are
        // just the fast human-readable path.
        const b64 = Buffer.from(script.join('\n') + '\n', 'utf8').toString('base64');
        await drainExec(client, `import base64; open(${JSON.stringify(scriptPath)},'w').write(base64.b64decode(${JSON.stringify(b64)}).decode())`);
        const runner = [
          `bash ${scriptPath}`,
          'rc=$?',
          `echo $rc > ${rcPath}`,
          `if [ "$rc" -eq 0 ]; then echo ${JOB_OK_MARK}; fi`,
          'exit',
        ].join('\n');
        const rb64 = Buffer.from(runner + '\n', 'utf8').toString('base64');
        await drainExec(client, `import base64; open(${JSON.stringify(runnerPath)},'w').write(base64.b64decode(${JSON.stringify(rb64)}).decode())`);
        const shellId = await client.shellOpen(120, 30);
        client.shellInput(shellId, `bash ${runnerPath}\nexit\n`);
        // Race guard (T8): the user may have cancelled while we were
        // uploading/deploying. State refuses the running resurrection via
        // updateJob's terminal-state guard; here we detect that refusal and
        // kill the just-opened shell so no orphaned process survives.
        const applied = updateJob(job.id, {
          status: 'running',
          accountId: target.accountId,
          serverId: target.server.id,
          endpoint: target.server.endpoint,
          shellId,
          startedAt: new Date().toISOString(),
          error: undefined,
        });
        if (applied && applied.status === 'cancelled') {
          clog(`job ${job.id} was cancelled mid-dispatch; killing shell ${shellId}`);
          await client.shellClose(shellId).catch(() => {});
          return;
        }
        if (job.recoverPending) {
          // Command has actually started on the fresh VM — the recovery is complete.
          updateJob(job.id, { recoverPending: false });
          clog(`job ${job.id}: recovery complete (resume ckpt: ${resumeCkptRemote || 'none'})`);
        }
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
        badVmUntil.set(vmKey(target.accountId, target.server.endpoint), Date.now() + BAD_VM_MS);
        dispatchAttempts.set(job.id, attempts);
        updateJob(job.id, { status: 'queued', error: `dispatch attempt ${attempts} failed: ${emsg}` });
        clog(`job ${job.id} dispatch attempt ${attempts} failed, requeued (fencing ${target.server.endpoint} for ${BAD_VM_MS / 60000}min): ${emsg}`);
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
      setupScript?: string; uploads?: Array<{ src: string; dest: string }>; rehearse?: boolean;
      progressPattern?: string; ckptGlob?: string; ckptKeep?: number;
      allowRecover?: boolean; maxRecoveries?: number }
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
        progressPattern: msg.progressPattern,
        ckptGlob: msg.ckptGlob,
        ckptKeep: msg.ckptKeep,
        allowRecover: msg.allowRecover,
        maxRecoveries: msg.maxRecoveries,
        recoveries: 0,
      });
      clog(`job ${job.id} submitted${job.rehearse ? ' (rehearse)' : ''}: ${msg.command.slice(0, 80)}` +
        `${msg.progressPattern ? ' progress=' + JSON.stringify(msg.progressPattern) : ''}` +
        `${msg.ckptGlob ? ' ckpts=' + msg.ckptGlob : ''}`);
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
let sweeping = false;
let tickCount = 0;
async function tick() {
  if (ticking) return; // a slow pollRunningJobs must never overlap the next tick
  ticking = true;
  try {
    tickCount++;
    await pollRunningJobs();
    await dispatchQueuedJobs();
    if (tickCount % 3 === 0 && !sweeping) {
      sweeping = true;
      void sweepIdleVms().finally(() => { sweeping = false; });
    }
  } catch (err) {
    clog('tick error:', err instanceof Error ? err.message : String(err));
  } finally {
    ticking = false;
  }
}

/** Knocks on every idle VM's per-runtime daemon. A VM reclaimed while idle
 *  otherwise lingers in the pool looking alive until a job trips over it
 *  (the post-host-sleep zombie case). 2 consecutive failures → forget the
 *  runtime locally; nothing is destroyed Colab-side, just trust decay. */
const VM_PROBE_TIMEOUT_MS = 10_000;
const vmProbeFails = new Map<string, number>();
async function sweepIdleVms(): Promise<void> {
  const running = readClusterState().jobs.filter((j) => j.status === 'running');
  for (const acct of listRegistryAccounts()) {
    for (const server of listStoredServers(acct.email)) {
      const key = `${acct.email}:${server.id}`;
      if (running.some((j) => j.accountId === acct.email && j.endpoint === server.endpoint)) {
        vmProbeFails.delete(key); // running jobs have their own probe path
        continue;
      }
      let alive = false;
      try {
        const client = new DaemonClient();
        const probe = (async () => {
          try {
            await client.connect(acct.email, server.id);
            await client.shellList();
            return true;
          } catch {
            return false;
          } finally {
            client.close();
          }
        })();
        const timeout = new Promise<boolean>((res) =>
          setTimeout(() => res(false), VM_PROBE_TIMEOUT_MS),
        );
        alive = await Promise.race([probe, timeout]);
        if (!alive) client.close();
      } catch { alive = false; }
      if (alive) {
        if (vmProbeFails.delete(key)) clog(`pool: ${acct.email}/${server.endpoint} healthy again`);
        continue;
      }
      const fails = (vmProbeFails.get(key) ?? 0) + 1;
      vmProbeFails.set(key, fails);
      if (fails >= 2) {
        vmProbeFails.delete(key);
        removeStoredServer(acct.email, server.id);
        clog(`pool: pruned dead runtime ${acct.email}/${server.endpoint} (daemon unreachable twice — likely reclaimed)`);
      }
    }
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
