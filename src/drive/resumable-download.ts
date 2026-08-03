import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { Readable } from 'stream';
import { request as gaxiosRequest } from 'gaxios';
import type { DriveFileInfo } from './client.js';
import { log } from '../logging/index.js';

const MAX_RETRIES = 5;
const RETRY_BASE_MS = 1000; // exponential backoff: 1s, 2s, 4s, 8s, 16s
const IDLE_TIMEOUT_MS = 60_000; // no bytes for this long → tear down and retry

const DOWNLOAD_BASE = 'https://www.googleapis.com/drive/v3/files';

export type DriveDownloadProgressEvent =
  | { type: 'start'; fileName: string; totalBytes: number; resumedFrom: number }
  | { type: 'progress'; bytesDownloaded: number; totalBytes: number }
  | { type: 'retry'; attempt: number; delayMs: number; reason: string }
  | { type: 'verifying' };

export interface DriveDownloadResult {
  destPath: string;
  totalBytes: number;
  resumedFrom: number;
  md5Verified: boolean;
}

// --- Part file naming ---

/**
 * Identity of a specific *revision* of a remote file, folded into the part
 * file name. This is what makes the download resumable without a sidecar
 * state file: the name carries the identity, the size carries the progress.
 *
 * md5Checksum changes whenever Drive stores new content for the same file ID,
 * so a remote overwrite yields a different fingerprint and the stale part is
 * never appended to. Files without a checksum fall back to size + mtime.
 */
function revisionFingerprint(meta: DriveFileInfo): string {
  const identity = meta.md5Checksum
    ? `md5:${meta.md5Checksum}`
    : `st:${meta.size ?? '0'}:${meta.modifiedTime ?? ''}`;
  return crypto.createHash('sha256').update(`${meta.id}\0${identity}`).digest('hex').slice(0, 12);
}

function partPathFor(destPath: string, fingerprint: string): string {
  return `${destPath}.${fingerprint}.part`;
}

/** Remove part files left over from other revisions / other files at this destination. */
function cleanupStaleParts(destPath: string, keepPath: string): void {
  const dir = path.dirname(destPath);
  const prefix = `${path.basename(destPath)}.`;
  let entries: string[];
  try {
    entries = fs.readdirSync(dir);
  } catch {
    return;
  }
  for (const entry of entries) {
    if (!entry.startsWith(prefix) || !entry.endsWith('.part')) continue;
    const middle = entry.slice(prefix.length, -'.part'.length);
    if (!/^[0-9a-f]{12}$/.test(middle)) continue;
    const full = path.join(dir, entry);
    if (full === keepPath) continue;
    try {
      fs.unlinkSync(full);
      log.debug(`Removed stale part file ${full}`);
    } catch {
      /* ignore */
    }
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function fileSizeOrZero(filePath: string): number {
  try {
    return fs.statSync(filePath).size;
  } catch {
    return 0;
  }
}

function computeFileMd5(filePath: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('md5');
    const stream = fs.createReadStream(filePath);
    stream.on('data', (chunk) => hash.update(chunk));
    stream.on('end', () => resolve(hash.digest('hex')));
    stream.on('error', reject);
  });
}

// --- Single range attempt ---

interface RangeAttemptOutcome {
  /** Whether the server honored the Range header (false → part was rewritten from 0). */
  restarted: boolean;
  /** Server answered 416: the requested offset is at or past the end of the file. */
  rangeUnsatisfiable: boolean;
  bytesOnDisk: number;
}

class RetryableError extends Error {}

/**
 * Fetch `[offset, EOF)` and append it to the part file. Any error after some
 * bytes have landed on disk is still progress: the next attempt re-reads the
 * part size and asks for a new range starting there.
 */
async function fetchRange(
  token: string,
  fileId: string,
  partPath: string,
  offset: number,
  totalBytes: number,
  onBytes: (bytesOnDisk: number) => void,
): Promise<RangeAttemptOutcome> {
  let res;
  try {
    res = await gaxiosRequest<Readable>({
      url: `${DOWNLOAD_BASE}/${encodeURIComponent(fileId)}?alt=media`,
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${token}`,
        // Drive gzips some content on the fly. A gzipped body is decoded by
        // gaxios, so the bytes written would not line up with the byte offsets
        // the Range header speaks in — which silently corrupts a resume.
        'Accept-Encoding': 'identity',
        ...(offset > 0 ? { Range: `bytes=${offset}-` } : {}),
      },
      responseType: 'stream',
      validateStatus: () => true,
    });
  } catch (err) {
    throw new RetryableError(`network error: ${(err as Error).message}`);
  }

  const status = res.status;

  if (status === 416) {
    // Requested range beyond EOF — the part is already at or past the remote
    // size. Let the caller's size check decide what that means.
    (res.data as Readable | undefined)?.destroy();
    return { restarted: false, rangeUnsatisfiable: true, bytesOnDisk: fileSizeOrZero(partPath) };
  }

  if (status === 401 || status === 403 || status === 404) {
    (res.data as Readable | undefined)?.destroy();
    throw new Error(`Download failed (${status}) — check that the file exists and the Drive session is still valid`);
  }

  if (status === 429 || status >= 500) {
    (res.data as Readable | undefined)?.destroy();
    throw new RetryableError(`server returned ${status}`);
  }

  if (status !== 200 && status !== 206) {
    (res.data as Readable | undefined)?.destroy();
    throw new Error(`Download failed (${status})`);
  }

  // 200 with offset > 0 means the server ignored our Range and is sending the
  // whole file. Appending would duplicate data, so restart the part file.
  const restarted = status === 200 && offset > 0;
  const writeOffset = restarted ? 0 : offset;
  if (restarted) {
    log.debug('Server ignored Range header (200 instead of 206) — restarting download from 0');
  }

  const stream = res.data as Readable;
  const writeStream = fs.createWriteStream(partPath, { flags: restarted || offset === 0 ? 'w' : 'a' });

  let written = 0;
  let idleTimer: NodeJS.Timeout | undefined;

  await new Promise<void>((resolve, reject) => {
    const fail = (err: Error): void => {
      if (idleTimer) clearTimeout(idleTimer);
      stream.destroy();
      // Let the write stream flush what it already accepted, then report.
      writeStream.end(() => reject(err));
    };

    const armIdleTimer = (): void => {
      if (idleTimer) clearTimeout(idleTimer);
      idleTimer = setTimeout(() => {
        fail(new RetryableError(`no data received for ${IDLE_TIMEOUT_MS / 1000}s`));
      }, IDLE_TIMEOUT_MS);
    };

    stream.on('data', (chunk: Buffer) => {
      written += chunk.length;
      onBytes(writeOffset + written);
      armIdleTimer();
    });
    stream.on('error', (err: Error) => fail(new RetryableError(`stream error: ${err.message}`)));
    writeStream.on('error', (err: Error) => {
      if (idleTimer) clearTimeout(idleTimer);
      stream.destroy();
      reject(err);
    });
    writeStream.on('finish', () => {
      if (idleTimer) clearTimeout(idleTimer);
      resolve();
    });

    armIdleTimer();
    stream.pipe(writeStream);
  });

  const bytesOnDisk = fileSizeOrZero(partPath);
  if (bytesOnDisk < totalBytes) {
    throw new RetryableError(`connection closed early at ${bytesOnDisk}/${totalBytes} bytes`);
  }
  return { restarted, rangeUnsatisfiable: false, bytesOnDisk };
}

// --- Public API ---

/**
 * Download a Drive file to `destPath`, resuming an interrupted download when a
 * matching part file is present. No state file is involved: the resume offset
 * is the part file's size and the remote revision it belongs to is encoded in
 * its name.
 */
export async function resumableDownload(
  token: string,
  meta: DriveFileInfo,
  destPath: string,
  options: { onProgress?: (event: DriveDownloadProgressEvent) => void } = {},
): Promise<DriveDownloadResult> {
  const { onProgress } = options;
  const totalBytes = parseInt(meta.size ?? '0', 10) || 0;

  const fingerprint = revisionFingerprint(meta);
  const partPath = partPathFor(destPath, fingerprint);

  fs.mkdirSync(path.dirname(destPath), { recursive: true });
  cleanupStaleParts(destPath, partPath);

  let bytesOnDisk = fileSizeOrZero(partPath);
  if (totalBytes > 0 && bytesOnDisk > totalBytes) {
    // Longer than the remote file: the part cannot be a prefix of it.
    log.debug(`Part file is larger than the remote file (${bytesOnDisk} > ${totalBytes}) — discarding`);
    fs.rmSync(partPath, { force: true });
    bytesOnDisk = 0;
  }

  const resumedFrom = bytesOnDisk;
  onProgress?.({ type: 'start', fileName: meta.name, totalBytes, resumedFrom });
  onProgress?.({ type: 'progress', bytesDownloaded: bytesOnDisk, totalBytes });

  let attempt = 0;
  let discardedAfter416 = false;
  while (totalBytes === 0 || bytesOnDisk < totalBytes) {
    try {
      const outcome = await fetchRange(
        token,
        meta.id,
        partPath,
        bytesOnDisk,
        totalBytes,
        (progressBytes) => onProgress?.({ type: 'progress', bytesDownloaded: progressBytes, totalBytes }),
      );
      bytesOnDisk = outcome.bytesOnDisk;

      if (outcome.rangeUnsatisfiable && bytesOnDisk < totalBytes) {
        // The server says our offset is past EOF, yet the part is shorter than
        // the size reported in the metadata. The part cannot be trusted as a
        // prefix — start over once, and give up if it happens again.
        if (discardedAfter416) {
          throw new Error(
            `Server rejected range ${bytesOnDisk}- for a file reported as ${totalBytes} bytes. ` +
              'The file may have changed on Drive; re-run `colab drive list` and try again.',
          );
        }
        log.debug(`Got 416 at offset ${bytesOnDisk} for a ${totalBytes}-byte file — discarding part and restarting`);
        fs.rmSync(partPath, { force: true });
        bytesOnDisk = 0;
        discardedAfter416 = true;
        continue;
      }

      // A file with unknown size (totalBytes === 0) is complete once the
      // stream ends without error.
      if (totalBytes === 0) break;
      if (bytesOnDisk >= totalBytes) break;
      attempt = 0; // progress was made — reset the backoff ladder
    } catch (err) {
      const onDisk = fileSizeOrZero(partPath);
      const madeProgress = onDisk > bytesOnDisk;
      bytesOnDisk = onDisk;

      if (!(err instanceof RetryableError)) throw err;
      if (madeProgress) attempt = 0;
      if (attempt >= MAX_RETRIES) {
        throw new Error(
          `Download failed after ${MAX_RETRIES} retries (${err.message}). ` +
            `${bytesOnDisk} bytes are kept in ${partPath}; re-run the same command to resume.`,
        );
      }
      const delayMs = RETRY_BASE_MS * Math.pow(2, attempt);
      attempt += 1;
      onProgress?.({ type: 'retry', attempt, delayMs, reason: err.message });
      log.debug(`Download error (${err.message}), retrying in ${delayMs}ms (attempt ${attempt}/${MAX_RETRIES})...`);
      await sleep(delayMs);
    }
  }

  // Verify before publishing the final name.
  let md5Verified = false;
  if (meta.md5Checksum) {
    onProgress?.({ type: 'verifying' });
    const actual = await computeFileMd5(partPath);
    if (actual !== meta.md5Checksum) {
      fs.rmSync(partPath, { force: true });
      throw new Error(
        `Checksum mismatch (expected ${meta.md5Checksum}, got ${actual}). ` +
          'The partial file was discarded; re-run the command to download again.',
      );
    }
    md5Verified = true;
  }

  fs.renameSync(partPath, destPath);

  return { destPath, totalBytes: fileSizeOrZero(destPath), resumedFrom, md5Verified };
}
