import fs from 'fs';
import path from 'path';
import {
  cancelJob,
  clusterPool,
  jobLogs,
  listClusterJobs,
  shutdownClusterDaemon,
  submitJob,
} from '../cluster/client.js';
import type { Job } from '../cluster/state.js';
import type { UploadSpec } from '../cluster/state.js';

/** shape of `cluster submit -f <file>` spec */
interface JobSpecFile {
  name?: string;
  accelerator?: string;
  /** Path to a local setup script (bash). Read and inlined at submit time. */
  setup_file?: string;
  uploads?: Array<{ src: string; dest: string }>;
  command: string;
  /** rehearsal: only run setup + trivial noop, used to validate setup_file
   *  on one VM before deploying it cluster-wide. */
  rehearse?: boolean;
  /** Optional progress regex; last match in stdout shows in `cluster list`. */
  progress_pattern?: string;
  /** Optional VM-side glob for checkpoints to auto-download locally. */
  ckpt_glob?: string;
  /** How many local ckpt copies to keep (oldest pruned). Default 3. */
  ckpt_keep?: number;
  /** Phase 4: set false to fail instead of auto-recovering a reclaimed VM. */
  allow_recover?: boolean;
  /** Phase 4: max auto-recoveries before permanent failure. Default 3. */
  max_recoveries?: number;
}

export function loadJobSpec(file: string): JobSpecFile {
  const spec = JSON.parse(fs.readFileSync(file, 'utf8')) as JobSpecFile;
  if (!spec.command) throw new Error('job spec missing required field: command');
  const dir = path.dirname(path.resolve(file));
  if (spec.setup_file) {
    const p = path.resolve(dir, spec.setup_file);
    if (!fs.existsSync(p)) throw new Error(`setup file not found: ${p}`);
  }
  if (spec.uploads) {
    for (const u of spec.uploads) {
      const p = path.resolve(dir, u.src);
      if (!fs.existsSync(p)) throw new Error(`upload src not found: ${p}`);
    }
  }
  return spec;
}

export async function submitFromSpecCommand(specFile: string): Promise<void> {
  const spec = loadJobSpec(specFile);
  const dir = path.dirname(path.resolve(specFile));
  const setupScript = spec.setup_file
    ? fs.readFileSync(path.resolve(dir, spec.setup_file), 'utf8')
    : undefined;
  const uploads: UploadSpec[] | undefined = spec.uploads?.map((u) => ({
    src: path.resolve(dir, u.src),
    dest: u.dest,
  }));
  await clusterSubmitCommand(spec.command, {
    name: spec.name,
    accelerator: spec.accelerator,
    setupScript,
    uploads,
    rehearse: spec.rehearse,
    progressPattern: spec.progress_pattern,
    ckptGlob: spec.ckpt_glob,
    ckptKeep: spec.ckpt_keep,
    allowRecover: spec.allow_recover,
    maxRecoveries: spec.max_recoveries,
  });
}

export async function clusterRehearseCommand(setupFile: string, name?: string): Promise<void> {
  const p = path.resolve(setupFile);
  if (!fs.existsSync(p)) throw new Error(`setup file not found: ${p}`);
  await clusterSubmitCommand('echo rehearse-noop', {
    name: name ?? `rehearse-${path.basename(p)}`,
    setupScript: fs.readFileSync(p, 'utf8'),
    rehearse: true,
  });
}

interface PoolAccountView {
  accountId: string;
  name?: string;
  vms: Array<{
    daemonAlive: boolean;
    shellCount: number;
    jobIds: number[];
    server: { endpoint: string; label: string; accelerator: string };
  }>;
}

export async function clusterStatusCommand(): Promise<void> {
  const pool = (await clusterPool()) as { accounts: PoolAccountView[] };
  console.log('CLUSTER POOL');
  let idle = 0, busy = 0, down = 0;
  for (const acct of pool.accounts) {
    console.log(`\n${acct.accountId}`);
    if (acct.vms.length === 0) console.log('  (no stored runtimes)');
    for (const vm of acct.vms) {
      const state = !vm.daemonAlive ? 'DOWN' : vm.jobIds.length > 0 ? `BUSY job=${vm.jobIds.join(',')}` : 'idle';
      if (!vm.daemonAlive) down++;
      else if (vm.jobIds.length > 0) busy++;
      else idle++;
      console.log(`  ${vm.server.endpoint}  ${vm.server.label}  ${state}  shells=${vm.shellCount}`);
    }
  }
  console.log(`\nVMs: ${idle} idle / ${busy} busy / ${down} down`);

  const jobs = await listClusterJobs();
  const q = jobs.filter((j) => j.status === 'queued').length;
  const r = jobs.filter((j) => j.status === 'running').length;
  console.log(`jobs: ${r} running / ${q} queued. See 'colab cluster list'.`);
}

export async function clusterSubmitCommand(
  command: string,
  opts: {
    name?: string;
    accelerator?: string;
    setupScript?: string;
    uploads?: Array<{ src: string; dest: string }>;
    rehearse?: boolean;
    progressPattern?: string;
    ckptGlob?: string;
    ckptKeep?: number;
    allowRecover?: boolean;
    maxRecoveries?: number;
  },
): Promise<void> {
  const job = await submitJob(command, opts.name, opts.accelerator, {
    setupScript: opts.setupScript,
    uploads: opts.uploads,
    rehearse: opts.rehearse,
    progressPattern: opts.progressPattern,
    ckptGlob: opts.ckptGlob,
    ckptKeep: opts.ckptKeep,
    allowRecover: opts.allowRecover,
    maxRecoveries: opts.maxRecoveries,
  });
  console.log(
    `Queued job ${job.id}${job.name ? ` (${job.name})` : ''}${job.accelerator ? ` [${job.accelerator}]` : ''}` +
      `${opts.setupScript ? ' +setup' : ''}${opts.uploads?.length ? ` +${opts.uploads.length} upload(s)` : ''}${opts.rehearse ? ' REHEARSE' : ''}` +
      `${opts.progressPattern ? ' +progress' : ''}${opts.ckptGlob ? ' +ckpt-sync' : ''}`,
  );
  console.log('Watch: colab cluster list / colab cluster logs ' + job.id);
  console.log(`Log file (survives VM loss): ~/.config/colab-cli/cluster/logs/job-${job.id}.log`);
}

function fmtJob(j: Job): string {
  const where = j.endpoint ? `${j.accountId} @ ${j.endpoint} shell=${j.shellId}` : '-';
  const err = j.error ? `  [${j.error}]` : '';
  const prog = j.progress ?? '-';
  const rcv = (j.recoveries ?? 0) > 0 ? ` retry${j.recoveries}${j.recoverPending ? ' (recovering)' : ''}` : '';
  return `${j.id}\t${j.status}${rcv}\t${prog}\t${j.name ?? ''}\t${where}\t${j.command.slice(0, 60)}${err}`;
}

export async function clusterListCommand(): Promise<void> {
  const jobs = await listClusterJobs();
  if (jobs.length === 0) {
    console.log('No jobs.');
    return;
  }
  console.log('ID\tSTATUS\tPROGRESS\tNAME\tASSIGNMENT\tCOMMAND');
  for (const j of jobs) console.log(fmtJob(j));
}

/** List a job's mirrored checkpoints (local copies that survive VM loss). */
export async function clusterCkptsCommand(jobId: number): Promise<void> {
  const jobs = await listClusterJobs();
  const job = jobs.find((j) => j.id === jobId);
  if (!job) {
    console.error(`job ${jobId} not found`);
    process.exit(1);
  }
  const ckpts = job.ckpts ?? [];
  if (ckpts.length === 0) {
    console.log(`job ${jobId}: no checkpoints mirrored${job.ckptGlob ? ` (glob: ${job.ckptGlob})` : ''}`);
    return;
  }
  console.log(`job ${jobId} checkpoints (keep=${job.ckptKeep ?? 3}, glob=${job.ckptGlob ?? '-'}):`);
  for (const c of ckpts) {
    const exists = fs.existsSync(c.localPath) ? '' : '  [MISSING on disk]';
    console.log(`  ${c.localPath}  ${c.sizeBytes} B  fetched ${c.fetchedAt}  <- ${c.remotePath}${exists}`);
  }
  if (job.lastCkpt) console.log(`lastCkpt: ${job.lastCkpt}`);
}

export async function clusterLogsCommand(jobId: number, tail?: number): Promise<void> {
  const res = await jobLogs(jobId, tail);
  if (res.output) process.stdout.write(res.output);
  console.error(`\n[job ${jobId} status: ${res.status}]`);
}

export async function clusterCancelCommand(jobId: number): Promise<void> {
  await cancelJob(jobId);
  console.log(`Job ${jobId} cancelled`);
}

export async function clusterShutdownCommand(): Promise<void> {
  await shutdownClusterDaemon();
  console.log('Cluster daemon stopped (state persisted; next `colab cluster` command respawns it)');
}
