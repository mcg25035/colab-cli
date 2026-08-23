import fs from 'fs';
import path from 'path';
import type { UUID } from 'crypto';
import { CONFIG_DIR } from '../config.js';

/**
 * Cluster state persistence. A single JSON file, written atomically
 * (write-temp + rename). Only jobs and the id counter are persisted —
 * pool state (accounts/VMs) is derived live from the registry + per-account
 * daemon liveness, since Colab runtimes are ephemeral by nature and stale
 * pool entries would be worse than no entries.
 */

export const CLUSTER_DIR = path.join(CONFIG_DIR, 'cluster');
export const CLUSTER_STATE_FILE = path.join(CLUSTER_DIR, 'state.json');
export const CLUSTER_SOCK = path.join(CLUSTER_DIR, 'cluster.sock');
export const CLUSTER_PID_FILE = path.join(CLUSTER_DIR, 'cluster.pid');
export const CLUSTER_LOG_FILE = path.join(CLUSTER_DIR, 'daemon.log');
export const CLUSTER_LOGS_DIR = path.join(CLUSTER_DIR, 'logs');

/** Per-job log mirror file — survives even after the VM is reclaimed. */
export function jobLogFile(jobId: number): string {
  return path.join(CLUSTER_LOGS_DIR, `job-${jobId}.log`);
}

export interface UploadSpec {
  /** Local path (file); resolved at submit time to an absolute path. */
  src: string;
  /** Remote destination path (file or directory) on the VM. */
  dest: string;
}

export type JobStatus = 'queued' | 'provisioning' | 'running' | 'done' | 'failed' | 'cancelled';

export interface Job {
  id: number;
  name?: string;
  /** The shell command line executed on the assigned VM's shell. */
  command: string;
  /** Requested accelerator label (e.g. 'CPU', 'L4'); informational for now —
   *  creation of new runtimes uses it when no idle VM exists. */
  accelerator?: string;
  /** Full setup script text (read inline at submit time so later edits to
   *  the source file can't change what the job runs). Runs BEFORE command on
   *  any VM whose cached `vmSetup` hash doesn't match. */
  setupScript?: string;
  /** Setup-only rehearsal job: runs setupScript then finishes green. */
  rehearse?: boolean;
  /** Local→VM uploads performed before the shell starts. */
  uploads?: UploadSpec[];
  status: JobStatus;
  /** Assignment (set once running). */
  accountId?: string;
  serverId?: UUID;
  endpoint?: string;
  shellId?: number;
  /** Tail of the shell's rendered output captured at completion. */
  lastOutput?: string;
  error?: string;
  createdAt: string;
  startedAt?: string;
  endedAt?: string;
}

export interface VmSetupRecord {
  /** sha1-ish hash of the setup script that completed on this VM. */
  hash: string;
  doneAt: string;
  accountId: string;
}

interface ClusterState {
  nextJobId: number;
  jobs: Job[];
  /** endpoint → last verified setup (a VM that passed setup once skips it
   *  for later jobs, until the script changes). */
  vmSetup: Record<string, VmSetupRecord>;
}

const EMPTY: ClusterState = { nextJobId: 1, jobs: [], vmSetup: {} };

export function readClusterState(): ClusterState {
  try {
    const raw = JSON.parse(fs.readFileSync(CLUSTER_STATE_FILE, 'utf8'));
    return {
      nextJobId: typeof raw.nextJobId === 'number' ? raw.nextJobId : 1,
      jobs: Array.isArray(raw.jobs) ? raw.jobs : [],
      vmSetup: raw.vmSetup && typeof raw.vmSetup === 'object' ? raw.vmSetup : {},
    };
  } catch {
    return { ...EMPTY, vmSetup: {} };
  }
}

export function writeClusterState(state: ClusterState): void {
  fs.mkdirSync(CLUSTER_DIR, { recursive: true });
  const tmp = `${CLUSTER_STATE_FILE}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(state, null, 2), { mode: 0o600 });
  fs.renameSync(tmp, CLUSTER_STATE_FILE);
}

export function addJob(j: Omit<Job, 'id' | 'createdAt' | 'status'>): Job {
  const st = readClusterState();
  const job: Job = { ...j, id: st.nextJobId++, status: 'queued', createdAt: new Date().toISOString() };
  st.jobs.push(job);
  writeClusterState(st);
  return job;
}

export function updateJob(id: number, patch: Partial<Job>): Job | undefined {
  const st = readClusterState();
  const job = st.jobs.find((x) => x.id === id);
  if (!job) return undefined;
  Object.assign(job, patch);
  writeClusterState(st);
  return job;
}

export function getJob(id: number): Job | undefined {
  return readClusterState().jobs.find((x) => x.id === id);
}

export function listJobs(): Job[] {
  return readClusterState().jobs;
}

/** Hash used to decide whether a VM must re-run a job's setup script. */
export function setupHash(script: string): string {
  let h = 0;
  for (let i = 0; i < script.length; i++) h = (h * 31 + script.charCodeAt(i)) >>> 0;
  return h.toString(16);
}

export function markVmSetup(endpoint: string, accountId: string, hash: string): void {
  const st = readClusterState();
  st.vmSetup[endpoint] = { hash, accountId, doneAt: new Date().toISOString() };
  writeClusterState(st);
}

export function getVmSetup(endpoint: string): VmSetupRecord | undefined {
  return readClusterState().vmSetup[endpoint];
}
