import { spawn } from 'child_process';
import fs from 'fs';
import net from 'net';
import path from 'path';
import { fileURLToPath } from 'url';
import { UUID } from 'crypto';
import {
  CONFIG_DIR,
  accountDir,
  accountSocketPath,
  accountPidPath,
  accountLogPath,
  accountLockPath,
} from '../config.js';
import { log } from '../logging/index.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DAEMON_SCRIPT = path.join(__dirname, 'server.js');

export function getSocketPath(accountId: string, serverId: UUID): string {
  return accountSocketPath(accountId, serverId);
}

function getPidPath(accountId: string, serverId: UUID): string {
  return accountPidPath(accountId, serverId);
}

function getLogPath(accountId: string, serverId: UUID): string {
  return accountLogPath(accountId, serverId);
}

function getLockPath(accountId: string, serverId: UUID): string {
  return accountLockPath(accountId, serverId);
}

/**
 * A daemon is "running" iff its Unix socket accepts connections. The .pid file
 * is informational only — signal probing via `process.kill(pid, 0)` was
 * unreliable under macOS sandboxing and across PID reuse, and misclassifying a
 * live daemon as dead caused silent state loss (duplicate daemons, orphaned
 * port forwards, empty `port-forward list`). Socket reachability is what
 * clients actually care about, so use it as the single source of truth.
 */
export async function isDaemonRunning(accountId: string, serverId: UUID): Promise<boolean> {
  return canConnect(getSocketPath(accountId, serverId));
}

/** In-process dedup: coalesce concurrent startDaemon calls for the same (account, server). */
const pendingStarts = new Map<string, Promise<void>>();

export async function startDaemon(accountId: string, serverId: UUID): Promise<void> {
  const key = `${accountId}:${serverId}`;
  const pending = pendingStarts.get(key);
  if (pending) {
    await pending;
    return;
  }

  const promise = startDaemonWithLock(accountId, serverId);
  pendingStarts.set(key, promise);
  try {
    await promise;
  } finally {
    pendingStarts.delete(key);
  }
}

/**
 * Acquire a cross-process lock (atomic mkdir), then spawn the daemon if needed.
 * If another process holds the lock, wait until the daemon is reachable or the
 * lock is released.
 */
async function startDaemonWithLock(accountId: string, serverId: UUID): Promise<void> {
  if (await isDaemonRunning(accountId, serverId)) {
    log.debug('Daemon already running for', accountId, serverId);
    return;
  }

  const lockPath = getLockPath(accountId, serverId);
  const maxWait = 30_000;
  const start = Date.now();

  while (Date.now() - start < maxWait) {
    if (tryAcquireLock(lockPath)) {
      try {
        // Re-check under lock — another process may have started the daemon
        if (await isDaemonRunning(accountId, serverId)) {
          log.debug('Daemon already running for', accountId, serverId);
          return;
        }
        return await spawnAndWait(accountId, serverId);
      } finally {
        releaseLock(lockPath);
      }
    }

    // Lock held by another process — it's starting the daemon.
    // Wait for the daemon to become reachable or the lock to be released.
    if (await canConnect(getSocketPath(accountId, serverId))) return;
    await sleep(200);
  }

  throw new Error(
    'Timed out waiting to start daemon. Check logs at: ' +
      getLogPath(accountId, serverId),
  );
}

/**
 * Daemon startup is bounded by `spawnAndWait`'s 30s timeout; a lock older than
 * this cannot belong to a live starter. Using mtime instead of PID probing
 * avoids the same EPERM/reuse hazards that killed signal-based liveness.
 */
const LOCK_STALE_MS = 60_000;

function tryAcquireLock(lockPath: string): boolean {
  try {
    fs.mkdirSync(lockPath);
    fs.writeFileSync(path.join(lockPath, 'pid'), String(process.pid));
    return true;
  } catch (err: any) {
    if (err.code !== 'EEXIST') return false;

    let ageMs: number;
    try {
      ageMs = Date.now() - fs.statSync(lockPath).mtimeMs;
    } catch {
      return false; // can't stat — assume held
    }
    if (ageMs < LOCK_STALE_MS) return false; // fresh lock, holder active

    // Stale lock — try to steal it. If another process clears it first, we
    // lose and retry on the next iteration of the caller's wait loop.
    try {
      fs.rmSync(lockPath, { recursive: true });
      fs.mkdirSync(lockPath);
      fs.writeFileSync(path.join(lockPath, 'pid'), String(process.pid));
      return true;
    } catch {
      return false;
    }
  }
}

function releaseLock(lockPath: string): void {
  try { fs.rmSync(lockPath, { recursive: true }); } catch {}
}

/**
 * Whether the current Node binary accepts `--use-env-proxy` (Node ≥22
 * upstream; distro builds may vary). Probed once and cached.
 *
 * Post-test Bug #1: `runtime create` on a system Node v20 fails because
 * `spawn(process.execPath, ['--use-env-proxy', ...])` gets
 * `bad option: --use-env-proxy`. When unsupported we spawn without the flag;
 * undici/ws still honor HTTP(S)_PROXY from the environment wherever proxies
 * are actually used.
 */
let nodeSupportsEnvProxy: boolean | undefined;
function probeEnvProxySupport(): Promise<boolean> {
  if (nodeSupportsEnvProxy !== undefined) {
    return Promise.resolve(nodeSupportsEnvProxy);
  }
  return new Promise((resolve) => {
    const probe = spawn(process.execPath, ['--use-env-proxy', '--version'], {
      stdio: 'ignore',
    });
    probe.on('error', () => {
      nodeSupportsEnvProxy = false;
      resolve(false);
    });
    probe.on('exit', (code) => {
      nodeSupportsEnvProxy = code === 0;
      resolve(nodeSupportsEnvProxy);
    });
  });
}

async function spawnAndWait(accountId: string, serverId: UUID): Promise<void> {
  log.debug('Starting daemon for', accountId, serverId);
  fs.mkdirSync(accountDir(accountId), { recursive: true });
  // The daemon binds UNIX_SOCK_PATH_MAX-aware path; its parent dir may live
  // outside accountDir ($XDG_RUNTIME_DIR/colab-cli/ on Linux when the per-
  // account path overflows AF_UNIX sun_path[108]). Ensure it exists before
  // we spawn the daemon so `server.listen(SOCKET_PATH)` doesn't ENOENT.
  fs.mkdirSync(path.dirname(getSocketPath(accountId, serverId)), { recursive: true });

  const logPath = getLogPath(accountId, serverId);
  const logFd = fs.openSync(logPath, 'a');

  // Daemon argv now receives the account id so it can resolve the correct
  // auth.json, servers.json, and socket/pid/log paths from the per-account
  // tree at startup. The legacy single-argv `serverId` form is dropped.
  const useEnvProxy = await probeEnvProxySupport();
  const argv = ['--use-env-proxy', DAEMON_SCRIPT, accountId, serverId];
  if (!useEnvProxy) {
    // Bug #1: older Node rejects --use-env-proxy. Proxying still works via
    // HTTP(S)_PROXY env vars, so just drop the flag instead of failing.
    argv.shift();
  }
  const child = spawn(
    process.execPath,
    argv,
    {
      detached: true,
      stdio: ['ignore', logFd, logFd],
      env: process.env,
    },
  );
  child.unref();
  fs.closeSync(logFd);

  const socketPath = getSocketPath(accountId, serverId);
  await waitForSocket(accountId, serverId, socketPath, 30_000);
  log.debug('Daemon started for', accountId, serverId);
}

async function waitForSocket(
  accountId: string,
  serverId: UUID,
  socketPath: string,
  timeoutMs: number,
): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await canConnect(socketPath)) return;
    await sleep(200);
  }
  throw new Error(
    'Daemon failed to start within timeout. Check logs at: ' +
      getLogPath(accountId, serverId),
  );
}

function canConnect(socketPath: string): Promise<boolean> {
  return new Promise((resolve) => {
    const client = net.connect(socketPath, () => {
      client.destroy();
      resolve(true);
    });
    client.on('error', () => resolve(false));
  });
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * Ask the daemon to shut down. Preferred path is the in-protocol `shutdown`
 * message, which lets the daemon clean up shells, port forwards, the kernel,
 * and its own `.sock` / `.pid` files. SIGTERM is a fallback for the case
 * where the socket is unreachable (daemon stuck or already half-dead). File
 * cleanup is intentionally left to the daemon — signaling a stale PID that
 * was reused by an unrelated process must not trash active daemon state.
 */
export async function stopDaemon(accountId: string, serverId: UUID): Promise<void> {
  const socketPath = getSocketPath(accountId, serverId);

  const sent = await sendShutdown(socketPath);
  if (sent) {
    const deadline = Date.now() + 5_000;
    while (Date.now() < deadline) {
      if (!(await canConnect(socketPath))) return;
      await sleep(100);
    }
    // Daemon acknowledged but didn't exit in time — fall through to SIGTERM.
  }

  try {
    const pid = parseInt(fs.readFileSync(getPidPath(accountId, serverId), 'utf-8').trim(), 10);
    if (Number.isFinite(pid)) {
      process.kill(pid, 'SIGTERM');
      log.debug('Sent SIGTERM to daemon', pid, 'for', accountId, serverId);
    }
  } catch {
    // .pid missing / unreadable, or signal denied — nothing more we can do
    // safely. Daemon owns cleanup; next startup will reclaim the socket.
  }
}

function sendShutdown(socketPath: string): Promise<boolean> {
  return new Promise((resolve) => {
    const client = net.connect(socketPath, () => {
      client.write('{"type":"shutdown"}\n');
      client.end();
      resolve(true);
    });
    client.on('error', () => resolve(false));
  });
}
