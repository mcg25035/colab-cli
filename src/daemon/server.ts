#!/usr/bin/env node

import net from 'net';
import fs from 'fs';
import path from 'path';
import readline from 'readline';
import { randomUUID, UUID } from 'crypto';
import { AuthManager } from '../auth/auth-manager.js';
import { AuthType } from '../colab/api.js';
import { ColabClient, ColabRequestError } from '../colab/client.js';
import { KernelConnection } from '../jupyter/kernel-connection.js';
import { KeepAlive } from '../runtime/keep-alive.js';
import { ConnectionRefresher } from '../runtime/connection-refresher.js';
import { getStoredServer, removeStoredServer } from '../runtime/storage.js';
import { formatRuntimeReleasedMessage } from '../runtime/release-detection.js';
import { COLAB_API_DOMAIN, COLAB_GAPI_DOMAIN } from '../config.js';
import {
  accountSocketPath,
  accountPidPath,
  accountDir,
  accountExecLogsDir,
  accountOutputsDir,
} from '../config.js';
import type { ClientMessage, ServerMessage, ShellStatus } from './protocol.js';
import { encode } from './protocol.js';
import { ExecutionStore } from './execution-store.js';
import { ForwardSession } from '../port-forward/session.js';
import { getTlsCredentials } from '../port-forward/tls.js';
import {
  deployPtyRelay,
  closePtyRelay,
  killPtyRelay,
  reconnectPtyRelay,
  relaySendStdin,
  relaySendResize,
  sweepOrphanRelays,
  type PtyRelay,
} from './pty-relay.js';

const accountId = process.argv[2];
const serverId = process.argv[3] as UUID;
if (!accountId || !serverId) {
  console.error('Usage: server.js <account-id> <server-id>');
  process.exit(1);
}

const SOCKET_PATH = accountSocketPath(accountId, serverId);
const PID_FILE = accountPidPath(accountId, serverId);

function cleanupFiles() {
  // Only remove files we actually own. If another daemon has taken over the
  // socket (e.g. we lost a startup race and are exiting), leave its files
  // intact instead of deleting them out from under it.
  let ownedByUs = false;
  try {
    const storedPid = parseInt(fs.readFileSync(PID_FILE, 'utf-8').trim(), 10);
    ownedByUs = storedPid === process.pid;
  } catch {
    return;
  }
  if (!ownedByUs) return;
  try { fs.unlinkSync(SOCKET_PATH); } catch {}
  try { fs.unlinkSync(PID_FILE); } catch {}
}

function isSocketAlive(socketPath: string): Promise<boolean> {
  return new Promise((resolve) => {
    const client = net.connect(socketPath, () => {
      client.destroy();
      resolve(true);
    });
    client.on('error', () => resolve(false));
  });
}

/**
 * Claim the daemon's Unix socket. If another daemon is already listening, exit
 * cleanly. If the socket file is stale (nothing is listening), remove it and
 * retry. This atomic helper replaces the legacy isSocketAlive → unlink →
 * listen dance, which had a race window where two concurrent daemons could
 * both conclude the socket was stale and both try to listen.
 */
async function claimSocket(server: net.Server, socketPath: string): Promise<void> {
  const doListen = () => new Promise<void>((resolve, reject) => {
    const onError = (err: unknown) => reject(err);
    server.once('error', onError);
    server.listen(socketPath, () => {
      server.removeListener('error', onError);
      resolve();
    });
  });

  try {
    await doListen();
    return;
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== 'EADDRINUSE') throw err;
  }

  if (await isSocketAlive(socketPath)) {
    console.log('Another daemon is already serving, exiting');
    process.exit(0);
  }

  try { fs.unlinkSync(socketPath); } catch {}
  await doListen();
}

interface ActiveExecution {
  execId: number;
  attachedSocket?: net.Socket;
  pendingAuthRequests: Map<string, (error?: string) => void>;
  pendingStdinResolve?: (value: string | undefined) => void;
  /** Resolves when background auth polling is interrupted. */
  pendingAuthInterruptResolve?: () => void;
}

interface ActiveShell {
  shellId: number;
  relay: PtyRelay;
  attachedSocket?: net.Socket;
  startedAt: Date;
  status: ShellStatus;
  /** Human-readable note on the current state (close reason / reconnect progress). */
  lastEvent?: string;
  /** Last dimensions we advertised to the relay (also mirrored on `relay.buffer`).
   *  Snapshot rendering reads these so it knows what the bash side thinks it is. */
  cols: number;
  rows: number;
}

const DEFAULT_SHELL_COLS = 80;
const DEFAULT_SHELL_ROWS = 24;

const MAX_CONCURRENT_SHELLS = 40;
const MAX_CONCURRENT_PORT_FORWARDS = 20;

/**
 * Per-shell relays are spawned by the daemon via the kernel exec channel
 * after `shell_open`: one `pty_relay.py` process per shell, listening on
 * its own port (`SHELL_RELAY_PORT_BASE + shellId * 2`) on the VM. Each
 * relay owns one PTY + one bash + one websockets server (ttyd protocol),
 * and exits on bash exit — SSH-style 1:1 lifecycle, no shared state
 * between shells, so multiple agents can drive their own shell without
 * any mutex.
 */
const SHELL_RELAY_PORT_BASE = 19000;
const SHELL_CLOSED_RETENTION_MS = 5 * 60 * 1000;

const AUTH_POLL_INTERVAL_MS = 5_000;
const AUTH_POLL_TIMEOUT_MS = 120_000;

async function main() {
  fs.mkdirSync(accountDir(accountId), { recursive: true });
  // SOCK_PATH may relocate to $XDG_RUNTIME_DIR when the per-account config
  // dir overflows Linux's 108-byte AF_UNIX sun_path cap; ensure its parent
  // exists so `server.listen(SOCKET_PATH)` can bind. See config.ts
  // UNIX_SOCK_PATH_MAX / accountSocketPath.
  fs.mkdirSync(path.dirname(SOCKET_PATH), { recursive: true });

  // Fast exit if a peer daemon is already live — skip the rest of startup.
  // The authoritative claim happens below via claimSocket once the server
  // instance is built.
  if (await isSocketAlive(SOCKET_PATH)) {
    console.log('Another daemon is already serving, exiting');
    process.exit(0);
  }

  // Initialize auth (per-account)
  const authManager = new AuthManager(accountId);
  await authManager.initialize();
  if (!authManager.isLoggedIn()) {
    console.error('Not logged in as', accountId);
    process.exit(1);
  }

  // Load server info (per-account)
  const server = getStoredServer(accountId, serverId);
  if (!server) {
    console.error('Server not found:', serverId);
    process.exit(1);
  }

  // Create colab client
  const colabClient = new ColabClient(
    new URL(COLAB_API_DOMAIN),
    new URL(COLAB_GAPI_DOMAIN),
    () => authManager.getAccessToken(),
    () => authManager.logout(),
  );

  const store = new ExecutionStore(accountId, serverId);
  const execState: { activeExecution?: ActiveExecution } = {};
  const shellState: {
    shells: Map<number, ActiveShell>;
    nextShellId: number;
  } = { shells: new Map(), nextShellId: 1 };

  const forwardState: {
    sessions: Map<number, ForwardSession>;
    nextId: number;
  } = { sessions: new Map(), nextId: 1 };
  let shuttingDown = false;
  let runtimeReleaseHandled = false;
  let socketServer: net.Server | undefined;

  const shutdown = (runtimeReleasedMessage?: string) => {
    if (shuttingDown) return;
    shuttingDown = true;

    if (runtimeReleasedMessage) {
      removeStoredServer(accountId, server.id);

      const active = execState.activeExecution;
      if (active) {
        store.fail(active.execId, runtimeReleasedMessage);
        if (active.pendingStdinResolve) {
          active.pendingStdinResolve(undefined);
          active.pendingStdinResolve = undefined;
        }
        if (active.pendingAuthInterruptResolve) {
          active.pendingAuthInterruptResolve();
          active.pendingAuthInterruptResolve = undefined;
        }
        for (const resolve of active.pendingAuthRequests.values()) {
          resolve(runtimeReleasedMessage);
        }
        active.pendingAuthRequests.clear();
        if (active.attachedSocket && !active.attachedSocket.destroyed) {
          active.attachedSocket.end(encode({ type: 'exec_error', message: runtimeReleasedMessage }));
        }
        execState.activeExecution = undefined;
      }

      for (const shell of shellState.shells.values()) {
        shell.status = 'closed';
        if (shell.attachedSocket && !shell.attachedSocket.destroyed) {
          shell.attachedSocket.end(
            encode({
              type: 'shell_closed',
              shellId: shell.shellId,
              reason: runtimeReleasedMessage,
            }),
          );
        }
      }
    }

    console.log('Shutting down daemon');
    // Tear down every live shell relay (WS, port-forward, VM-side process).
    for (const shell of shellState.shells.values()) {
      closePtyRelay(shell.relay, kernel).catch((err) => {
        console.error(`Shell ${shell.shellId} relay teardown failed:`, err instanceof Error ? err.message : err);
      });
    }
    shellState.shells.clear();
    for (const session of forwardState.sessions.values()) {
      session.close().catch(() => {});
    }
    forwardState.sessions.clear();
    kernel.close();
    keepAlive.stop();
    refresher.stop();
    socketServer?.close();
    cleanupFiles();
    process.exit(0);
  };

  const handleRuntimeReleased = async (
    source: 'connection-refresh' | 'keep-alive',
    error: ColabRequestError,
  ): Promise<void> => {
    if (runtimeReleaseHandled || shuttingDown) return;
    runtimeReleaseHandled = true;
    console.error(
      `Runtime release detected: source=${source} endpoint=${server.endpoint} status=${error.status}`,
    );
    shutdown(formatRuntimeReleasedMessage(server.endpoint));
  };

  const keepAlive = new KeepAlive(
    colabClient,
    server.endpoint,
    (error) => handleRuntimeReleased('keep-alive', error),
  );

  const refresher = new ConnectionRefresher(
    colabClient,
    accountId,
    server.id,
    server.endpoint,
    server.token,
    server.proxyUrl,
    server.tokenExpiry,
    (error) => handleRuntimeReleased('connection-refresh', error),
  );

  const propagateCredentialsOrThrow = async (authType: AuthType): Promise<void> => {
    const result = await colabClient.propagateCredentials(server.endpoint, {
      authType,
      dryRun: false,
    });
    if (!result.success) {
      throw new Error(`[${authType}] Credentials propagation unsuccessful`);
    }
  };

  const tryPropagateCredentialsForPolling = async (authType: AuthType): Promise<boolean> => {
    try {
      const result = await colabClient.propagateCredentials(server.endpoint, {
        authType,
        dryRun: false,
      });
      return result.success;
    } catch (err) {
      // During polling, treat 4xx responses as "authorization not ready yet"
      // and keep waiting; surface transport and server failures immediately.
      if (err instanceof ColabRequestError && err.status >= 400 && err.status < 500) {
        return false;
      }
      throw err;
    }
  };

  const requestEphemeralAuth = async (authType: AuthType): Promise<void> => {
    const active = execState.activeExecution;
    if (!active) {
      throw new Error('No active execution for auth');
    }

    // Pre-compute auth state so we can (a) auto-propagate when credentials
    // are already available and (b) surface the auth URL to non-interactive
    // callers such as `exec attach --no-wait` and `exec list`.
    let authUrl: string | undefined;
    try {
      const dryRun = await colabClient.propagateCredentials(server.endpoint, {
        authType,
        dryRun: true,
      });
      if (dryRun.success) {
        // Credentials already available — propagate directly, no user action.
        await propagateCredentialsOrThrow(authType);
        return;
      }
      if (!dryRun.unauthorizedRedirectUri) {
        throw new Error(
          `[${authType}] Credentials propagation dry run returned unexpected results: ${JSON.stringify(dryRun)}`,
        );
      }
      authUrl = dryRun.unauthorizedRedirectUri;
    } catch (err) {
      console.error('dryRun pre-check failed:', err);
    }

    // No attached socket (background exec) — store the URL and poll every 5s
    // with dryRun=false until propagation succeeds.
    if (!active.attachedSocket || active.attachedSocket.destroyed) {
      if (!authUrl) {
        throw new Error(`[${authType}] No authorization URL available for background auth`);
      }
      store.setPendingAuth(active.execId, authType, authUrl);
      const interrupted = new Promise<'interrupted'>((resolve) => {
        active.pendingAuthInterruptResolve = () => resolve('interrupted');
      });
      try {
        const deadline = Date.now() + AUTH_POLL_TIMEOUT_MS;
        while (true) {
          const remaining = deadline - Date.now();
          if (remaining <= 0) {
            throw new Error('Authorization timed out');
          }

          const tick = new Promise<'tick'>((resolve) => {
            setTimeout(() => resolve('tick'), Math.min(AUTH_POLL_INTERVAL_MS, remaining));
          });
          const wakeReason = await Promise.race([tick, interrupted]);
          if (wakeReason === 'interrupted') {
            throw new Error('Authorization interrupted');
          }

          if (await tryPropagateCredentialsForPolling(authType)) {
            return;
          }
        }
      } finally {
        active.pendingAuthInterruptResolve = undefined;
        store.clearPendingAuth(active.execId);
      }
    }

    // Socket is now available — proceed with normal auth flow
    const requestId = randomUUID();
    const result = await new Promise<string | undefined>((resolve) => {
      active.pendingAuthRequests.set(requestId, resolve);
      if (active.attachedSocket!.destroyed) {
        active.pendingAuthRequests.delete(requestId);
        resolve('CLI session closed before authorization completed');
        return;
      }
      active.attachedSocket!.write(
        encode({ type: 'auth_required', requestId, authType }),
      );
    });

    if (result) {
      throw new Error(result);
    }
  };

  // Create kernel (connect later, after socket is listening)
  const kernel = new KernelConnection(
    () => refresher.proxyUrl,
    () => refresher.token,
    colabClient,
    server.endpoint,
    server.kernelName ?? 'python3',
    requestEphemeralAuth,
  );

  // Begin kernel connection (may be slow on cold-start GPU runtimes). The
  // promise is stored so exec handlers can await it instead of failing early.
  // Kernel failure does NOT take down the daemon — shell, port-forward, and
  // future kernel restarts must remain available even when the initial
  // Jupyter WebSocket times out. The .catch below observes the rejection so
  // Node doesn't log unhandledRejection; later `await kernelReady` calls in
  // exec handlers still see the rejected state and fail the exec cleanly.
  console.log('Connecting to kernel...');
  const kernelReady = kernel.connect().then(() => {
    console.log('Kernel connected');
    // Startup sweep: this daemon just (re)attached to the runtime with an
    // empty shell table, so any pty_relay process still on the VM is an
    // orphan from a crashed daemon or a cleanup that failed under network
    // disturbance. Kill them + remove their /tmp files (B2).
    void sweepOrphanRelays(kernel).catch(() => {});
  });
  kernelReady.catch((err) => {
    console.error('Kernel connection failed:', err instanceof Error ? err.message : err);
  });

  /** Run execution and route outputs to store + attached socket. */
  async function runExecution(execId: number, code: string): Promise<void> {
    try {
      await kernelReady;
      if (!kernel.isConnected) {
        const message = kernel.isReconnecting
          ? 'Kernel WebSocket is reconnecting, retry in a few seconds.'
          : 'Kernel WebSocket disconnected and auto-reconnect failed. Run `colab runtime restart` to restore exec (resets kernel state; shell and port-forward unaffected).';
        store.fail(execId, message);
        const active = execState.activeExecution;
        if (active?.execId === execId && active.attachedSocket && !active.attachedSocket.destroyed) {
          active.attachedSocket.write(encode({ type: 'exec_error', message }));
        }
        return;
      }
      const outputs = await kernel.execute(code);
      for await (const output of outputs) {
        const active = execState.activeExecution;
        if (output.type === 'input_request') {
          store.setPendingInput(execId, output.prompt, output.password);
          if (active?.execId === execId && active.attachedSocket && !active.attachedSocket.destroyed) {
            // Forward stdin request to attached client
            active.attachedSocket.write(
              encode({ type: 'input_request', prompt: output.prompt, password: output.password }),
            );
          }
          // Wait for stdin via exec_send, attached client, or interrupt
          const value = await new Promise<string | undefined>((resolve) => {
            if (active?.execId === execId) {
              active.pendingStdinResolve = resolve;
            } else {
              resolve(undefined);
            }
          });
          if (active?.execId === execId) {
            active.pendingStdinResolve = undefined;
          }
          store.clearPendingInput(execId);
          if (value !== undefined) {
            kernel.sendStdinReply(value);
          }
          // undefined means interrupted — skip reply, continue consuming outputs
          continue;
        }
        const stored = store.appendOutput(execId, output);
        if (active?.execId === execId && active.attachedSocket && !active.attachedSocket.destroyed) {
          active.attachedSocket.write(encode({ type: 'output', output: stored }));
        }
      }
      store.complete(execId);
      const active = execState.activeExecution;
      if (active?.execId === execId && active.attachedSocket && !active.attachedSocket.destroyed) {
        active.attachedSocket.write(encode({ type: 'exec_done' }));
      }
    } catch (err) {
      let message = err instanceof Error ? err.message : String(err);
      if (!kernel.isConnected && !kernel.isReconnecting) {
        message += '. Run `colab runtime restart` to restore exec (resets kernel state; shell and port-forward unaffected).';
      }
      store.fail(execId, message);
      const active = execState.activeExecution;
      if (active?.execId === execId && active.attachedSocket && !active.attachedSocket.destroyed) {
        active.attachedSocket.write(encode({ type: 'exec_error', message }));
      }
    } finally {
      if (execState.activeExecution?.execId === execId) {
        execState.activeExecution = undefined;
      }
    }
  }

  // Backstop sweeper (B7): debounced 90s after the last shell close. If a
  // killPtyRelay ever fails permanently (exec channel congestion, network
  // disturbance at teardown time), this catches the leftover relay
  // processes + /tmp files. Never touches relays of running/reconnecting
  // shells (they're passed in as the live set).
  let orphanSweepTimer: NodeJS.Timeout | undefined;
  const scheduleOrphanSweep = () => {
    if (orphanSweepTimer) clearTimeout(orphanSweepTimer);
    orphanSweepTimer = setTimeout(() => {
      orphanSweepTimer = undefined;
      if (shuttingDown) return;
      const live = [...shellState.shells.values()]
        .filter((s) => s.status !== 'closed')
        .map((s) => s.shellId);
      kernelReady
        .then(() => sweepOrphanRelays(kernel, live))
        .catch(() => {});
    }, 90_000);
  };

  // Start Unix socket server early so CLI detects daemon quickly
  socketServer = net.createServer((socket) =>
    handleClient(
      socket,
      kernel,
      kernelReady,
      execState,
      store,
      runExecution,
      shellState,
      forwardState,
      colabClient,
      server.endpoint,
      scheduleOrphanSweep,
    ),
  );

  // Atomically claim the socket. If a peer daemon won the race, exit cleanly
  // (without touching its files). Only after the listener is bound do we
  // publish the PID file — that way `isDaemonRunning` never observes a PID
  // belonging to a process that has not yet (or never will) bind the socket.
  await claimSocket(socketServer, SOCKET_PATH);
  fs.chmodSync(SOCKET_PATH, 0o600);
  fs.writeFileSync(PID_FILE, String(process.pid));
  console.log('Daemon ready on', SOCKET_PATH);

  process.on('SIGTERM', () => shutdown());
  process.on('SIGINT', () => shutdown());
  keepAlive.start();
  refresher.start();

  // The daemon stays alive as long as the socket server runs. Kernel failure
  // is handled per-exec via `await kernelReady` in runExecution; shell and
  // port-forward sessions are independent of kernel state.
}

function handleClient(
  socket: net.Socket,
  kernel: KernelConnection,
  kernelReady: Promise<void>,
  execState: {
    activeExecution?: ActiveExecution;
  },
  store: ExecutionStore,
  runExecution: (execId: number, code: string) => Promise<void>,
  shellState: {
    shells: Map<number, ActiveShell>;
    nextShellId: number;
  },
  forwardState: {
    sessions: Map<number, ForwardSession>;
    nextId: number;
  },
  colabClient: ColabClient,
  endpoint: string,
  scheduleOrphanSweep: () => void,
) {
  const send = (msg: ServerMessage) => {
    if (!socket.destroyed) socket.write(encode(msg));
  };

  send({ type: 'ready' });

  // Mark a live shell as closed: notify its attached driver, drop the
  // socket reference, then retain the entry briefly so a closing-curve
  // poll (`shell list`, snapshot attach) still sees the final state
  // before the relay + entry are evicted. No-op if already closed.
  // Push a shell_status transition to the attached driver (if any) so live
  // attach sessions can surface "reconnecting…" / "reconnected" immediately
  // instead of guessing from output silence. Close transitions already send
  // shell_closed via markShellClosed.
  const pushShellStatus = (shell: ActiveShell) => {
    if (shell.attachedSocket && !shell.attachedSocket.destroyed) {
      shell.attachedSocket.write(
        encode({
          type: 'shell_status',
          shellId: shell.shellId,
          status: shell.status,
          ...(shell.lastEvent ? { reason: shell.lastEvent } : {}),
        }),
      );
    }
  };

  const markShellClosed = (shellId: number, reason: string) => {
    const shell = shellState.shells.get(shellId);
    if (!shell || shell.status === 'closed') return;
    shell.status = 'closed';
    shell.lastEvent = reason;
    console.log(`Shell ${shellId} closed: ${reason}`);
    if (shell.attachedSocket && !shell.attachedSocket.destroyed) {
      shell.attachedSocket.write(encode({ type: 'shell_closed', shellId, reason }));
    }
    shell.attachedSocket = undefined;
    setTimeout(() => {
      const s = shellState.shells.get(shellId);
      if (!s) return;
      closePtyRelay(s.relay, kernel).catch(() => {});
      shellState.shells.delete(shellId);
    }, SHELL_CLOSED_RETENTION_MS);
    scheduleOrphanSweep();
  };

  const rl = readline.createInterface({ input: socket });
  rl.on('error', () => {});
  rl.on('line', async (line) => {
    let msg: ClientMessage;
    try {
      msg = JSON.parse(line);
    } catch {
      return;
    }

    switch (msg.type) {
      case 'exec': {
        if (execState.activeExecution) {
          const busyId = execState.activeExecution.execId;
          // Phase 1.13 FIX6: include the daemon's endpoint so the user knows
          // which runtime the busy execId lives on (exec IDs are per-daemon,
          // not globally unique). Without this, a `colab exec list` with no
          // `--endpoint` may resolveTarget to a *different* latest runtime and
          // return "No executions." — the user is left stranded. Adding the
          // endpoint here surfaces the canonical hint and removes the ambiguity
          // observed in the T8 stale-state test session report.
          send({
            type: 'exec_error',
            message:
              `Daemon on runtime ${endpoint} is already executing code for another session (exec ${busyId}). ` +
              `Inspect it with \`colab exec list -e ${endpoint}\`; ` +
              `interrupt it with \`colab exec send ${busyId} --interrupt -e ${endpoint}\`; ` +
              `or wait for it to finish. ` +
              `(Exec IDs are per-runtime; if you just spawned a new background exec but see this error ` +
              `without --endpoint, your CLI process likely resolvedTarget to a different daemon.)`,
          });
          return;
        }

        const execId = store.create(msg.code, msg.outputDir);

        if (msg.background) {
          // Background mode: return exec ID immediately, run without awaiting
          execState.activeExecution = {
            execId,
            pendingAuthRequests: new Map(),
          };
          send({ type: 'exec_started', execId });
          // Fire-and-forget — execution continues after client disconnects
          runExecution(execId, msg.code).catch((err) => {
            console.error('Background execution error:', err);
          });
        } else {
          // Foreground mode: attach this socket and await completion
          execState.activeExecution = {
            execId,
            attachedSocket: socket,
            pendingAuthRequests: new Map(),
          };
          await runExecution(execId, msg.code);
        }
        break;
      }

      case 'exec_attach': {
        const exec = store.get(msg.execId);
        if (!exec) {
          send({ type: 'exec_error', message: `Execution ${msg.execId} not found` });
          return;
        }

        if (msg.noWait) {
          // Snapshot mode: send batch of outputs and return
          const outputs = store.getOutputs(msg.execId, msg.tail);
          send({
            type: 'exec_attach_batch',
            execId: msg.execId,
            outputs,
            status: exec.status,
            ...(exec.pendingInput ? { pendingInput: exec.pendingInput } : {}),
            ...(exec.pendingAuth ? { pendingAuth: exec.pendingAuth } : {}),
          });
        } else {
          // Streaming mode: replay buffered outputs then attach for live
          for (const output of exec.outputs) {
            send({ type: 'output', output });
          }

          if (exec.status === 'done') {
            send({ type: 'exec_done' });
            return;
          }
          if (exec.status === 'error') {
            send({ type: 'exec_error', message: exec.errorMessage ?? 'Unknown error' });
            return;
          }

          // Still running — attach this socket for live output
          const active = execState.activeExecution;
          if (active && active.execId === msg.execId) {
            active.attachedSocket = socket;
            // If there's a pending stdin request, forward it immediately
            if (exec.pendingInput) {
              send({
                type: 'input_request',
                prompt: exec.pendingInput.prompt,
                password: exec.pendingInput.password,
              });
            }
            // Surface the current auth URL on streaming attach so users don't
            // have to switch to --no-wait just to retrieve it.
            if (exec.pendingAuth?.authUrl) {
              send({
                type: 'output',
                output: {
                  type: 'stream',
                  name: 'stderr',
                  text:
                    '[waiting for authorization — visit the URL below; execution will resume automatically after authorization completes]\n' +
                    `[auth url: ${exec.pendingAuth.authUrl}]\n`,
                },
              });
            }
          }
        }
        break;
      }

      case 'exec_list': {
        send({ type: 'exec_list_result', executions: store.list() });
        break;
      }

      case 'exec_send': {
        const active = execState.activeExecution;
        if (!active || active.execId !== msg.execId) {
          // Phase 1.13 FIX6: exec IDs are per-daemon. If the CLI resolved to
          // this daemon but the execId isn't currently running here, recommend
          // `exec list -e <endpoint>` to enumerate what is here and to confirm
          // the user isn't accidentally targeting the wrong runtime.
          send({
            type: 'exec_error',
            message:
              `Execution ${msg.execId} is not currently running on runtime ${endpoint}. ` +
              `Exec IDs are per-runtime — ` +
              `run \`colab exec list -e ${endpoint}\` to see what's running here, ` +
              `or specify the correct \`--endpoint\` if you meant a different runtime.`,
          });
          return;
        }
        if (msg.interrupt) {
          try {
            await kernel.interrupt();
          } catch (err) {
            send({
              type: 'exec_error',
              message: `Interrupt failed: ${err instanceof Error ? err.message : String(err)}`,
            });
            return;
          }
          if (active.pendingStdinResolve) {
            active.pendingStdinResolve(undefined);
            active.pendingStdinResolve = undefined;
          }
          send({ type: 'exec_send_ack', execId: msg.execId });
        } else if (msg.stdin !== undefined) {
          if (!active.pendingStdinResolve) {
            send({
              type: 'exec_error',
              message: `Execution ${msg.execId} is not waiting for input`,
            });
            return;
          }
          active.pendingStdinResolve(msg.stdin);
          active.pendingStdinResolve = undefined;
          send({ type: 'exec_send_ack', execId: msg.execId });
        }
        break;
      }

      case 'exec_clear': {
        const count = store.clear(msg.execId);
        send({ type: 'exec_clear_result', count });
        break;
      }

      case 'auth_response': {
        const active = execState.activeExecution;
        const resolve = active?.pendingAuthRequests.get(msg.requestId);
        if (!resolve) return;
        active!.pendingAuthRequests.delete(msg.requestId);
        resolve(msg.error);
        break;
      }
      case 'stdin_reply': {
        const active = execState.activeExecution;
        if (active?.pendingStdinResolve) {
          active.pendingStdinResolve(msg.value);
          active.pendingStdinResolve = undefined;
        }
        break;
      }
      case 'interrupt':
        kernel.interrupt().catch((err) => {
          console.error('Interrupt failed:', err instanceof Error ? err.message : err);
        });
        if (execState.activeExecution?.pendingStdinResolve) {
          execState.activeExecution.pendingStdinResolve(undefined);
          execState.activeExecution.pendingStdinResolve = undefined;
        }
        if (execState.activeExecution?.pendingAuthInterruptResolve) {
          execState.activeExecution.pendingAuthInterruptResolve();
          execState.activeExecution.pendingAuthInterruptResolve = undefined;
        }
        break;
      case 'restart':
        try {
          await kernel.restartKernel();
          send({ type: 'restarted' });
        } catch (err) {
          send({
            type: 'restart_error',
            message: err instanceof Error ? err.message : String(err),
          });
        }
        break;
      case 'ping':
        send({ type: 'pong' });
        break;

      case 'shutdown':
        socket.end();
        process.kill(process.pid, 'SIGTERM');
        break;

      // ── Shell session handlers ──

      case 'shell_open': {
        if (shellState.shells.size >= MAX_CONCURRENT_SHELLS) {
          send({ type: 'shell_error', message: `Maximum concurrent shell sessions (${MAX_CONCURRENT_SHELLS}) reached` });
          return;
        }
        const cols = msg.cols || DEFAULT_SHELL_COLS;
        const rows = msg.rows || DEFAULT_SHELL_ROWS;
        const shellId = msg.shellId ?? shellState.nextShellId++;
        if (shellState.shells.has(shellId)) {
          send({ type: 'shell_error', message: `Shell ID ${shellId} already in use` });
          return;
        }
        if (shellId >= shellState.nextShellId) {
          shellState.nextShellId = shellId + 1;
        }

        // Per-shell relay: spawn /tmp/pty_relay_<id>.py on the VM (one PTY,
        // one bash, one websockets server on port SHELL_RELAY_PORT_BASE +
        // shellId*2), port-forward to it, open a daemon-side WS client. The
        // relay's WS close = bash exit = shell closed; no traps, no markers,
        // no shared tmux client, no InputMutex — every shell is fully
        // independent (SSH-style 1:1 lifecycle).
        let relay: PtyRelay;
        try {
          relay = await deployPtyRelay({
            shellId,
            cols,
            rows,
            kernel,
            kernelReady,
            colabClient,
            endpoint,
            handlers: {
              onOutput: (text) => {
                const s = shellState.shells.get(shellId);
                if (!s || s.status === 'closed') return;
                if (s.attachedSocket && !s.attachedSocket.destroyed) {
                  s.attachedSocket.write(encode({ type: 'shell_output', shellId, data: text }));
                }
              },
              onClose: (code, reason) => {
                // Transport-level close (proxy hiccup, forward socket
                // destroyed, heartbeat timeout terminate). The relay process
                // on the VM is almost certainly still alive with bash +
                // user jobs intact — enter `reconnecting` and retry the same
                // local forward for up to the policy window instead of
                // killing the shell. Only if the window expires (relay truly
                // gone: bash exited → relay shutdown, runtime reclaimed,
                // etc.) do we mark the shell closed. (B1)
                const s = shellState.shells.get(shellId);
                if (!s || s.status === 'closed') return;
                s.status = 'reconnecting';
                s.lastEvent = `transport lost (code ${code}${reason ? `: ${reason}` : ''}); reconnecting`;
                console.log(`Shell ${shellId} transport lost (code=${code} reason=${JSON.stringify(reason)}); reconnecting`);
                pushShellStatus(s);
                void reconnectPtyRelay(s.relay).then((ok) => {
                  const cur = shellState.shells.get(shellId);
                  if (!cur || cur.status === 'closed' || cur.relay !== s.relay) return;
                  if (ok) {
                    cur.status = 'running';
                    cur.lastEvent = 'reconnected';
                    console.log(`Shell ${shellId} reconnected`);
                    pushShellStatus(cur);
                  } else {
                    markShellClosed(shellId, `relay unreachable after reconnect window (last transport code ${code})`);
                  }
                });
              },
              onReconnectFailed: (attempt, err, nextDelayMs) => {
                console.log(
                  `Shell ${shellId} reconnect attempt ${attempt} failed: ${err.message}` +
                    (nextDelayMs !== undefined ? ` (retry in ${nextDelayMs}ms)` : ''),
                );
              },
              onError: (err) => {
                console.error(`Shell ${shellId} relay WS error:`, err.message);
              },
            },
          });
        } catch (err) {
          send({
            type: 'shell_error',
            message: `Failed to open shell: ${err instanceof Error ? err.message : String(err)}`,
          });
          return;
        }

        const shell: ActiveShell = {
          shellId,
          relay,
          startedAt: new Date(),
          status: 'running',
          cols,
          rows,
        };
        shellState.shells.set(shellId, shell);
        console.log(`Shell ${shellId} opened (relay port ${relay.port})`);
        send({ type: 'shell_opened', shellId });
        break;
      }

      case 'shell_input': {
        const shell = shellState.shells.get(msg.shellId);
        if (!shell || shell.status === 'closed') {
          send({ type: 'shell_error', message: `Shell ${msg.shellId} not found or closed` });
          return;
        }
        relaySendStdin(shell.relay, msg.data);
        break;
      }

      case 'shell_resize': {
        const shell = shellState.shells.get(msg.shellId);
        if (!shell || shell.status === 'closed') return;
        relaySendResize(shell.relay, msg.cols, msg.rows);
        shell.cols = msg.cols;
        shell.rows = msg.rows;
        break;
      }

      case 'shell_detach': {
        const shell = shellState.shells.get(msg.shellId);
        if (shell && shell.attachedSocket === socket) {
          shell.attachedSocket = undefined;
        }
        break;
      }

      case 'shell_attach': {
        const shell = shellState.shells.get(msg.shellId);
        if (!shell) {
          send({ type: 'shell_error', message: `Shell ${msg.shellId} not found` });
          return;
        }

        if (msg.noWait) {
          // Snapshot: render the buffered output through the virtual screen
          // so cursor-positioning redraws (btop TUI, rich progress panels,
          // tmux status redraws) collapse to the final visible text. `tail`
          // counts lines. Snapshots never touch the WS — multiple agents
          // may poll concurrently with the live driver and with each other.
          const buffered = shell.relay.buffer.getSnapshot(shell.cols, shell.rows, msg.tail);
          send({ type: 'shell_attach_batch', shellId: msg.shellId, buffered, status: shell.status });
        } else {
          // Streaming: SSH-style "last attach wins". The deposed socket (if
          // any) is told it's been detached; the new socket becomes the
          // live forwarder for `shell_output` byte-for-byte. There is NO
          // input mutex here: any other agent may still call `shell_input`
          // or `shell_send` and have its bytes interleave with this driver's
          // keystrokes by design — the worker-of-record (attached socket)
          // is just the one seeing the rendered output live.
          if (shell.status === 'closed') {
            send({ type: 'shell_closed', shellId: msg.shellId, reason: 'session ended before attach' });
            return;
          }
          if (shell.attachedSocket && shell.attachedSocket !== socket && !shell.attachedSocket.destroyed) {
            shell.attachedSocket.write(encode({ type: 'shell_closed', shellId: msg.shellId, reason: 'detached by another client' }));
          }
          shell.attachedSocket = socket;

          const buffered = shell.relay.buffer.getContents();
          send({ type: 'shell_attached', shellId: msg.shellId, buffered });

          if (msg.cols && msg.rows) {
            // status is known non-closed here (closed attaches return above);
            // a resize during `reconnecting` just updates the remembered
            // size and is re-sent to the relay on reconnect.
            relaySendResize(shell.relay, msg.cols, msg.rows);
            shell.cols = msg.cols;
            shell.rows = msg.rows;
          }
        }
        break;
      }

      case 'shell_list': {
        const shells = Array.from(shellState.shells.values()).map((s) => ({
          shellId: s.shellId,
          status: s.status,
          startedAt: s.startedAt.toISOString(),
          attached: s.attachedSocket !== undefined && !s.attachedSocket.destroyed,
          ...(s.lastEvent ? { lastEvent: s.lastEvent } : {}),
        }));
        send({ type: 'shell_list_result', shells });
        break;
      }

      case 'shell_send': {
        const shell = shellState.shells.get(msg.shellId);
        if (!shell || shell.status === 'closed') {
          send({ type: 'shell_error', message: `Shell ${msg.shellId} not found or closed` });
          return;
        }
        // One-shot programmatic send. Reuses the daemon's already-open WS
        // client to the relay (no new connection, no race with a concurrent
        // driver — the relay's PTY-level stdin bytes are atomic and the
        // kernel sees exactly what we write in order).
        relaySendStdin(shell.relay, msg.data);
        send({ type: 'shell_send_ack', shellId: msg.shellId });
        break;
      }

      case 'shell_close': {
        // Explicit close, unlike EOF-into-PTY: tears down the VM-side relay
        // process tree (pkill + process-group SIGKILL of bash/children) even
        // when a foreground process still holds the PTY. This is the
        // documented way to kill e.g. a background training shell. (B5)
        const shell = shellState.shells.get(msg.shellId);
        if (!shell || shell.status === 'closed') {
          send({ type: 'shell_error', message: `Shell ${msg.shellId} not found or closed` });
          return;
        }
        markShellClosed(msg.shellId, 'closed by user');
        // Immediate teardown — don't wait for the retention-window sweeper.
        closePtyRelay(shell.relay, kernel).catch((err) => {
          console.error(`Shell ${msg.shellId} teardown after close failed:`, err instanceof Error ? err.message : err);
        });
        send({ type: 'shell_closed', shellId: msg.shellId, reason: 'closed by user' });
        break;
      }

      // ── Port-forward handlers ──

      case 'port_forward_create': {
        if (forwardState.sessions.size >= MAX_CONCURRENT_PORT_FORWARDS) {
          send({
            type: 'port_forward_error',
            message: `Maximum concurrent port forwards (${MAX_CONCURRENT_PORT_FORWARDS}) reached`,
          });
          return;
        }
        const id = forwardState.nextId++;
        try {
          const tlsCreds = msg.tls ? await getTlsCredentials() : undefined;
          const session = await ForwardSession.open(
            id,
            msg.localHost,
            msg.localPort,
            msg.remotePort,
            colabClient,
            endpoint,
            tlsCreds,
          );
          forwardState.sessions.set(id, session);
          send({
            type: 'port_forward_created',
            id,
            localHost: session.localHost,
            localPort: session.localPort,
            remotePort: session.remotePort,
            proxyUrl: session.proxyUrl,
            tls: session.tls,
          });
          console.log(
            `Port forward ${id}: ${session.localHost}:${session.localPort} → remote ${session.remotePort}`,
          );
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err);
          send({ type: 'port_forward_error', message });
        }
        break;
      }

      case 'port_forward_list': {
        const sessions = Array.from(forwardState.sessions.values()).map((s) => ({
          id: s.id,
          localHost: s.localHost,
          localPort: s.localPort,
          remotePort: s.remotePort,
          startedAt: s.startedAt.toISOString(),
          proxyUrl: s.proxyUrl,
          tls: s.tls,
        }));
        send({ type: 'port_forward_list_result', sessions });
        break;
      }

      case 'port_forward_close': {
        const targets: ForwardSession[] = [];
        if (msg.all) {
          targets.push(...forwardState.sessions.values());
        } else if (msg.id !== undefined) {
          const session = forwardState.sessions.get(msg.id);
          if (!session) {
            send({ type: 'port_forward_error', message: `Port forward ${msg.id} not found` });
            return;
          }
          targets.push(session);
        } else {
          send({ type: 'port_forward_error', message: 'Must specify id or all' });
          return;
        }
        const ids: number[] = [];
        for (const session of targets) {
          try {
            await session.close();
          } catch (err) {
            console.error(`Port forward ${session.id} close error:`, err);
          }
          forwardState.sessions.delete(session.id);
          ids.push(session.id);
        }
        send({ type: 'port_forward_closed', ids });
        break;
      }
    }
  });

  socket.on('error', () => {});
  socket.on('close', () => {
    const active = execState.activeExecution;
    if (active && active.attachedSocket === socket) {
      // Detach socket but do NOT stop execution
      for (const resolve of active.pendingAuthRequests.values()) {
        resolve('CLI session closed before authorization completed');
      }
      active.pendingAuthRequests.clear();
      if (active.pendingStdinResolve) {
        active.pendingStdinResolve(undefined);
        active.pendingStdinResolve = undefined;
      }
      // Detach the socket but keep the execution running in the daemon.
      active.attachedSocket = undefined;
    }
    // Detach this socket from any shell sessions (shells keep running).
    // In the new relay-based architecture each shell owns a long-lived WS
    // client to its dedicated `pty_relay.py`; a CLI-side socket closing
    // only stops the live `shell_output` forwarding — the VM-side bash and
    // its PTY keep running until the user explicitly sends `exit` or the
    // shell is closed elsewhere.
    for (const shell of shellState.shells.values()) {
      if (shell.attachedSocket === socket) {
        shell.attachedSocket = undefined;
      }
    }
    rl.close();
  });
}

main().catch((err) => {
  console.error('Daemon failed:', err);
  cleanupFiles();
  process.exit(1);
});
