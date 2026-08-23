import fs from 'fs';
import net from 'net';
import path from 'path';
import readline from 'readline';
import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import {
  CLUSTER_DIR,
  CLUSTER_PID_FILE,
  CLUSTER_SOCK,
  type Job,
} from './state.js';

/** CLI-side client for the cluster scheduler daemon (one request per connection). */

async function canConnect(): Promise<boolean> {
  return new Promise((resolve) => {
    const sock = net.connect(CLUSTER_SOCK);
    sock.once('connect', () => { sock.end(); resolve(true); });
    sock.once('error', () => resolve(false));
  });
}

async function spawnClusterDaemon(): Promise<void> {
  fs.mkdirSync(CLUSTER_DIR, { recursive: true });
  const entry = path.join(path.dirname(fileURLToPath(import.meta.url)), 'daemon.js');
  const child = spawn(process.execPath, [entry], {
    detached: true,
    stdio: 'ignore',
    env: { ...process.env, COLAB_CLUSTER_DAEMON: '1' },
  });
  child.unref();
  // Wait for the socket to come up (daemon bind is fast; module load dominates).
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    if (await canConnect()) return;
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error('cluster daemon did not start within 15s');
}

export async function ensureClusterDaemon(): Promise<void> {
  // Stale pid file from a crashed daemon shouldn't block respawn.
  if (!(await canConnect())) {
    try { fs.rmSync(CLUSTER_PID_FILE, { force: true }); } catch {}
    await spawnClusterDaemon();
  }
}

export async function clusterRequest<T = unknown>(req: Record<string, unknown>): Promise<T> {
  await ensureClusterDaemon();
  return new Promise<T>((resolve, reject) => {
    const sock = net.connect(CLUSTER_SOCK, () => {
      sock.write(JSON.stringify(req) + '\n');
    });
    const rl = readline.createInterface({ input: sock });
    rl.once('line', (line) => {
      sock.end();
      try {
        const res = JSON.parse(line);
        if (res.type === 'error') reject(new Error(res.message));
        else resolve(res as T);
      } catch (err) {
        reject(err);
      }
    });
    sock.once('error', reject);
    sock.setTimeout(120_000, () => {
      sock.destroy();
      reject(new Error('cluster daemon request timed out'));
    });
  });
}

// ── typed helpers used by commands/cluster.ts ──

export async function submitJob(
  command: string,
  name?: string,
  accelerator?: string,
  extra?: { setupScript?: string; uploads?: Array<{ src: string; dest: string }>; rehearse?: boolean },
): Promise<Job> {
  const res = await clusterRequest<{ job: Job }>({ type: 'submit', command, name, accelerator, ...extra });
  return res.job;
}

export async function listClusterJobs(): Promise<Job[]> {
  const res = await clusterRequest<{ jobs: Job[] }>({ type: 'list' });
  return res.jobs;
}

export async function jobLogs(jobId: number, tail?: number): Promise<{ output: string; status: string }> {
  return clusterRequest({ type: 'logs', jobId, tail });
}

export async function cancelJob(jobId: number): Promise<{ ok: boolean; error?: string }> {
  const res = await clusterRequest<{ jobId: number; ok: boolean; error?: string }>({ type: 'cancel', jobId });
  if (!res.ok) throw new Error(res.error ?? 'cancel failed');
  return res;
}

export async function clusterPool(): Promise<unknown> {
  const res = await clusterRequest<{ pool: unknown }>({ type: 'pool' });
  return res.pool;
}

export async function shutdownClusterDaemon(): Promise<void> {
  if (!(await canConnect())) return;
  await clusterRequest({ type: 'shutdown' }).catch(() => {});
}
