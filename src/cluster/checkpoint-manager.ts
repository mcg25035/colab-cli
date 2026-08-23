import fs from 'fs';
import path from 'path';
import type { StoredServer } from '../runtime/storage.js';
import { getStoredServer } from '../runtime/storage.js';
import type { DaemonClient } from '../daemon/client.js';
import type { ConnectionProvider } from '../transfer/common.js';
import { downloadFile } from '../transfer/download.js';
import { jobCkptDir, updateJob, type JobCkpt, type Job } from './state.js';

/**
 * Checkpoint manager: periodically globs a VM-side pattern through the
 * kernel exec channel and mirrors new (or rewritten) matches into
 * ~/.config/colab-cli/cluster/checkpoints/job-<id>/. Old local copies roll
 * off once ckptKeep (default 3) is exceeded — VM files are never touched.
 *
 * Side effects are limited to: local disk under jobCkptDir, and the job's
 * ckpts[]/lastCkpt fields in cluster state.
 */

/** Per-job scan throttle (ms). Training ckpts land every few minutes;
 *  scanning faster than this just burns kernel round-trips. */
export const CKPT_SCAN_INTERVAL_MS = 60_000;
const nextScanAt = new Map<number, number>();

/** True when it's time to scan this job again (and marks it scanned). */
export function shouldScanNow(jobId: number, force = false): boolean {
  const now = Date.now();
  if (!force && (nextScanAt.get(jobId) ?? 0) > now) return false;
  nextScanAt.set(jobId, now + CKPT_SCAN_INTERVAL_MS);
  return true;
}

function makeConnectionProvider(server: StoredServer): ConnectionProvider {
  return {
    getProxyUrl() {
      return getStoredServer(server.accountId!, server.id)?.proxyUrl ?? server.proxyUrl;
    },
    getToken() {
      return getStoredServer(server.accountId!, server.id)?.token ?? server.token;
    },
  };
}

function makeDaemonExec(client: DaemonClient): (code: string) => Promise<string> {
  return async (code: string): Promise<string> => {
    const textParts: string[] = [];
    for await (const output of client.exec(code)) {
      if (output.type === 'stream' && output.text) textParts.push(output.text);
      if (output.type === 'error') throw new Error(`kernel exec: ${output.ename}: ${output.evalue}`);
    }
    return textParts.join('');
  };
}

interface RemoteFile {
  remotePath: string;
  sizeBytes: number;
  mtimeNs: number;
}

function buildGlobCode(globPattern: string): string {
  // json [[path, size, mtime_ns], ...] for files matching the glob.
  return [
    'import glob, os, json',
    `pat = ${JSON.stringify(globPattern)}`,
    'rows = []',
    'for p in sorted(glob.glob(pat)):',
    '    if os.path.isfile(p):',
    '        st = os.stat(p)',
    '        rows.append([p, st.st_size, st.st_mtime_ns])',
    'print("CKPTS:" + json.dumps(rows))',
  ].join('\n');
}

async function listRemoteCkpts(client: DaemonClient, globPattern: string): Promise<RemoteFile[]> {
  const out = await makeDaemonExec(client)(buildGlobCode(globPattern));
  const line = out.split('\n').find((l) => l.startsWith('CKPTS:'));
  if (!line) return [];
  const rows = JSON.parse(line.slice('CKPTS:'.length)) as Array<[string, number, number]>;
  return rows.map(([remotePath, sizeBytes, mtimeNs]) => ({ remotePath, sizeBytes, mtimeNs }));
}

/**
 * One scan pass for a job: find new/changed remote ckpts, download them,
 * prune old locals to ckptKeep, and persist ckpts/lastCkpt into state.
 * Returns how many files got fetched this pass.
 */
export async function scanAndFetchCkpts(
  job: Job,
  client: DaemonClient,
  server: StoredServer,
  clog: (...args: unknown[]) => void,
): Promise<number> {
  if (!job.ckptGlob) return 0;
  const remote = await listRemoteCkpts(client, job.ckptGlob);
  if (remote.length === 0) return 0;

  const known = new Map((job.ckpts ?? []).map((c) => [c.remotePath, c]));
  const fresh = remote.filter((r) => known.get(r.remotePath)?.mtimeNs !== r.mtimeNs);
  if (fresh.length === 0) return 0;

  fs.mkdirSync(jobCkptDir(job.id), { recursive: true });
  const conn = makeConnectionProvider(server);
  const exec = makeDaemonExec(client);
  const ckpts = [...(job.ckpts ?? [])];
  let fetched = 0;

  for (const f of fresh) {
    const localPath = path.join(jobCkptDir(job.id), path.basename(f.remotePath));
    clog(`job ${job.id}: fetching ckpt ${f.remotePath} (${f.sizeBytes} B) -> ${localPath}`);
    const res = await downloadFile(conn, { remotePath: f.remotePath.replace(/^\/+/, ''), localPath }, exec);
    if (res.sizeBytes !== f.sizeBytes) {
      clog(`job ${job.id}: ckpt ${f.remotePath} size mismatch (remote moved mid-download); skipped, will retry next scan`);
      continue;
    }
    const entry: JobCkpt = {
      remotePath: f.remotePath,
      localPath,
      sizeBytes: f.sizeBytes,
      mtimeNs: f.mtimeNs,
      fetchedAt: new Date().toISOString(),
    };
    const existing = ckpts.findIndex((c) => c.remotePath === f.remotePath);
    if (existing >= 0) ckpts[existing] = entry; else ckpts.push(entry);
    fetched++;
  }

  // Retention: keep the N newest by fetchedAt; oldest local files roll off.
  const keep = job.ckptKeep && job.ckptKeep > 0 ? job.ckptKeep : 3;
  ckpts.sort((a, b) => a.fetchedAt.localeCompare(b.fetchedAt));
  while (ckpts.length > keep) {
    const old = ckpts.shift()!;
    try {
      fs.rmSync(old.localPath, { force: true });
      clog(`job ${job.id}: pruned old ckpt ${old.localPath} (keep=${keep})`);
    } catch (err) {
      clog(`job ${job.id}: prune failed for ${old.localPath}: ${err instanceof Error ? err.message : err}`);
    }
  }

  const last = ckpts[ckpts.length - 1];
  if (fetched > 0 || ckpts.length !== (job.ckpts ?? []).length) {
    updateJob(job.id, { ckpts, lastCkpt: last?.localPath });
  }
  return fetched;
}
