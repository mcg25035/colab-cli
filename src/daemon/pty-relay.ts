import WebSocket from 'ws';
import type { KernelConnection, KernelOutput } from '../jupyter/kernel-connection.js';
import { ForwardSession } from '../port-forward/session.js';
import type { ColabClient } from '../colab/client.js';
import { TerminalBuffer } from '../terminal/terminal-buffer.js';
import { log } from '../logging/index.js';

/**
 * Per-shell relay lifecycle on the VM. One `pty_relay.py` process = one PTY
 * + one bash + one websockets server; bash exit tears the whole thing down
 * (SSH-style 1:1 model). Each shell gets its own VM-side port (aliased to
 * the shellId, so no cross-shell contention) and its own port-forward
 * tunneled through Colab's edge proxy.
 */
export const SHELL_RELAY_PORT_BASE = 19000;
const SHELL_RELAY_BOOTSTRAP_TIMEOUT_MS = 30_000;
const SHELL_RELAY_BOOTSTRAP_POLL_INTERVAL_MS = 300;
const SHELL_RELAY_WS_OPEN_TIMEOUT_MS = 15_000;

/**
 * Daemon-side heartbeat to the relay. `ws` (unlike Python websockets) does
 * NOT enable client-side ping by default — an idle WS over Colab's proxy
 * can masquerade as alive forever even when the relay process died (TCP
 * RST not propagated back through the edge proxy). We ping every 30s; if
 * two consecutive pongs go missing we tear the connection down, which now
 * triggers the reconnect loop rather than instantly killing the shell.
 */
const SHELL_RELAY_HEARTBEAT_PING_INTERVAL_MS = 30_000;
const SHELL_RELAY_HEARTBEAT_PONG_TIMEOUT_MS = 60_000;

/**
 * Reconnect policy for the daemon → relay WebSocket. On any transport-level
 * close (proxy hiccup, edge restart, port-forward socket destroyed) we retry
 * with exponential backoff inside a ~2 minute window before declaring the
 * shell dead. The relay keeps buffering PTY output into its backlog while
 * no client is connected, so a successful reconnect resyncs the stream
 * transparently — the terminal keeps streaming (little-or-no data loss).
 */
export interface RelayReconnectPolicy {
  /** Per-attempt delays (ms) between failed attempts and the next try. */
  delaysMs: number[];
  /** Total wall-clock budget (ms) from first close to giving up. */
  budgetMs: number;
  /** WS open timeout per attempt. */
  openTimeoutMs: number;
}

export const DEFAULT_RECONNECT_POLICY: RelayReconnectPolicy = {
  delaysMs: [500, 1_000, 2_000, 4_000, 8_000, 15_000, 15_000, 15_000, 15_000, 15_000],
  budgetMs: 120_000,
  openTimeoutMs: SHELL_RELAY_WS_OPEN_TIMEOUT_MS,
};

/** Max bytes of stdin buffered while the relay WS is down (256 KB). */
const MAX_PENDING_INPUT_BYTES = 256 * 1024;

/** Max attempts for kernel-exec based VM cleanup (killPtyRelay). */
const KILL_RELAY_ATTEMPTS = 4;
const KILL_RELAY_BACKOFF_MS = [0, 5_000, 15_000, 30_000];

/**
 * Per-kernel FIFO for maintenance execs (kill/sweep). The kernel exec
 * channel is a single Jupyter WebSocket — a burst of concurrent
 * `kernel.execute` calls (e.g. 30 `shell close` in 3s) congests it: calls
 * error out with "Connection was temporarily lost" and, worse, were
 * silently swallowed (B7: 0/150 kills landed in the 3h-soak teardown).
 * Serializing maintenance execs through this promise chain keeps bursts
 * lossless; they just take a little longer. Interactive exec
 * (runExecution) is already single-flight upstream and stays off this
 * queue.
 */
const maintenanceQueues = new WeakMap<KernelConnection, Promise<unknown>>();

function enqueueKernelMaintenance<T>(
  kernel: KernelConnection,
  fn: () => Promise<T>,
): Promise<T> {
  const prev = maintenanceQueues.get(kernel) ?? Promise.resolve();
  const next = prev.catch(() => {}).then(fn);
  maintenanceQueues.set(kernel, next);
  return next;
}

/** Run a maintenance snippet and return its full textual output. */
async function runKernelMaintenance(kernel: KernelConnection, code: string): Promise<string> {
  return enqueueKernelMaintenance(kernel, async () => {
    let out = '';
    const outputs = await kernel.execute(code);
    for await (const output of outputs) {
      out += kernelOutputToText(output);
      if (output.type === 'error') {
        throw new Error(`${output.ename}: ${output.evalue}`);
      }
    }
    return out;
  });
}

/** ttyd protocol command bytes (binary frames, first byte is the cmd). */
const TTYD_CMD_STDIN = 0x30;   // '0'
const TTYD_CMD_RESIZE = 0x31;  // '1'

/**
 * The Python relay script (ttyd byte protocol) embedded as source — gets
 * base64-decoded on the VM at deploy time so we avoid any shell-quoting
 * issues. argv: <port> [<cols> <rows>] [<executable> <args>...]; we pass
 * the caller's cols/rows up front so the PTY winsize matches before bash
 * ever renders a prompt.
 */
const PTY_RELAY_PY = `"""PTY relay (ttyd protocol): raw PTY <-> WebSocket frames.

Usage: python3 pty_relay.py <port> [<cols> <rows>] [<executable> <args>...]

Wire protocol (every frame is binary; first byte is the command byte):

  client -> server:
    '0' + pty input bytes       -> write stdin to PTY master
    '1' + JSON {columns,rows}   -> TIOCSWINSZ + SIGWINCH
  server -> client:
    '0' + raw pty output bytes  -> forward to xterm stdout
    '1' + utf8 title string     -> (ignored by CLI; browser only)
    '2' + JSON client options   -> (ignored by CLI; browser only)

One shell = one PTY + one bash + one websockets server. bash exit ->
master_fd EOF -> relay shuts down -> WS clients dropped immediately so the
caller observes close without keepalive trickery.

Backlog / reconnect model: whenever NO ws client is connected, the pump
queues PTY output into 'backlog' (capped at BACKLOG_MAX bytes, oldest
dropped). EVERY new client connection — including the very first, and any
daemon reconnect after a transport failure — atomically snapshots and
flushes the backlog under backlog_lock before joining the live set. That
makes reconnects lossless for the gap up to the cap, and the bash prompt at
startup race-free without a separate first_client_pending flag.

Race-safety: backlog_lock serializes the pump's queue-vs-broadcast decision
AND the handler's snapshot-clear-addws-sendSnapshot. Holding the lock
across "await ws.send(snapshot)" is intentional — it blocks the pump for
the single-frame send (~us), but guarantees the pump never observes the
half-state mid-handler (backlog cleared but client not yet in 'clients').
No deadlock risk: pump and handler both acquire the same lock, never
nested, never re-entrant.
"""
import asyncio, os, pty, subprocess, sys, struct, fcntl, termios, signal, json
import websockets

PORT = int(sys.argv[1])
COLS = int(sys.argv[2]) if len(sys.argv) > 2 else 80
ROWS = int(sys.argv[3]) if len(sys.argv) > 3 else 24
argv = sys.argv[4:] or ['bash']
clients: set = set()

# Bytes the pump read from master_fd while no client was connected —
# pre-first-client startup output AND output produced while a previous
# client was disconnected (daemon reconnect window). Capped so a shell
# detached for hours cannot exhaust VM RAM; oldest bytes are dropped.
BACKLOG_MAX = 1 << 20  # 1 MiB
backlog: bytearray = bytearray()
backlog_lock = asyncio.Lock()

master_fd, slave_fd = pty.openpty()
fcntl.ioctl(slave_fd, termios.TIOCSWINSZ, struct.pack("HHHH", ROWS, COLS, 0, 0))
fcntl.ioctl(master_fd, termios.TIOCSWINSZ, struct.pack("HHHH", ROWS, COLS, 0, 0))


def _acquire_controlling_tty():
    """Run in child via preexec_fn before exec'ing bash:
    (1) become a new session leader so tcsetpgrp has a target;
    (2) acquire slave_fd as the controlling tty.
    """
    os.setsid()
    fcntl.ioctl(slave_fd, termios.TIOCSCTTY, 0)


child = subprocess.Popen(argv, stdin=slave_fd, stdout=slave_fd, stderr=slave_fd, close_fds=False, preexec_fn=_acquire_controlling_tty)
os.close(slave_fd)
print(f"RELAY-UP child={child.pid} port={PORT} size={ROWS}x{COLS}", flush=True)

async def handler(ws):
    # Atomically (under backlog_lock): snapshot + clear the backlog, add self
    # to clients, and flush the snapshot. Runs on EVERY connection, so a
    # daemon reconnect after a transport blip replays everything the PTY
    # emitted while no client was attached (up to BACKLOG_MAX).
    async with backlog_lock:
        snapshot = bytes(backlog)
        backlog.clear()
        clients.add(ws)
        if snapshot:
            try:
                await ws.send(snapshot)
            except Exception:
                pass
    print(f"WS-CLIENT-CONNECT clients={len(clients)} replay={len(snapshot)}", flush=True)
    try:
        async for msg in ws:
            data = msg.encode('utf-8', 'surrogateescape') if isinstance(msg, str) else msg
            if len(data) == 0:
                continue
            cmd = data[0]; payload = data[1:]
            if cmd == 0x30:  # stdin
                os.write(master_fd, payload)
            elif cmd == 0x31:  # resize
                try:
                    spec = json.loads(payload.decode('utf-8'))
                    cols = int(spec.get('columns', 80))
                    rows = int(spec.get('rows', 24))
                    winsize = struct.pack("HHHH", rows, cols, 0, 0)
                    fcntl.ioctl(master_fd, termios.TIOCSWINSZ, winsize)
                    try: os.kill(child.pid, signal.SIGWINCH)
                    except Exception: pass
                    print(f"RESIZE cols={cols} rows={rows}", flush=True)
                except (ValueError, KeyError, json.JSONDecodeError) as e:
                    print(f"resize-bad {e}: {payload[:60]!r}", flush=True)
            else:
                print(f"unknown cmd={chr(cmd)!r} len={len(payload)}", flush=True)
    except Exception as e:
        print(f"ws-err {type(e).__name__} {str(e)[:60]}", flush=True)
    finally:
        clients.discard(ws)
        # Log close code/reason so abnormal terminations (proxy-forced 1006,
        # relay-initiated 1001/1000, policy 1008) are attributable from the
        # per-shell VM log instead of requiring guesswork.
        code = getattr(ws, 'close_code', None)
        reason = getattr(ws, 'close_reason', None)
        print(f"WS-CLIENT-CLOSE code={code} reason={str(reason)[:60]} clients={len(clients)}", flush=True)


async def pump_pty_to_clients():
    loop = asyncio.get_running_loop()
    while True:
        try:
            data = await loop.run_in_executor(None, os.read, master_fd, 4096)
        except OSError:
            break
        if not data:
            break
        framed = b'0' + data
        # Atomically decide (under backlog_lock): no live client -> queue to
        # backlog for the next connection; otherwise snapshot the live set
        # and broadcast outside the lock.
        async with backlog_lock:
            if not clients:
                backlog.extend(framed)
                if len(backlog) > BACKLOG_MAX:
                    del backlog[: len(backlog) - BACKLOG_MAX]
                continue
            live_clients = list(clients)
        # Send OUTSIDE the lock — slow clients shouldn't block the next
        # master_fd read or other handlers' lock acquisitions.
        dead = []
        for c in live_clients:
            try: await c.send(framed)
            except Exception: dead.append(c)
        for c in dead:
            clients.discard(c)
            try: c.close()
            except Exception: pass
    print("PTY-EOF", flush=True)


async def wait_child():
    loop = asyncio.get_running_loop()
    rc = await loop.run_in_executor(None, child.wait)
    print(f"CHILD-EXIT rc={rc}", flush=True)


async def main():
    pump_task = asyncio.create_task(pump_pty_to_clients())
    wait_task = asyncio.create_task(wait_child())
    async with websockets.serve(handler, '0.0.0.0', PORT):
        print(f"WS-UP :{PORT}", flush=True)
        await asyncio.wait({pump_task, wait_task}, return_when=asyncio.FIRST_COMPLETED)
        print("SHUTDOWN", flush=True)
    try:
        if child.poll() is None:
            try: child.kill()
            except Exception: pass
    except Exception: pass
    os.close(master_fd)


asyncio.run(main())
`;

export interface PtyRelay {
  /** Shell this relay belongs to (set by caller). */
  shellId: number;
  /** VM-side port the relay listens on (= SHELL_RELAY_PORT_BASE + shellId*2). */
  port: number;
  /** Local port-forward tunneling to the relay port through Colab's edge proxy. */
  forward: ForwardSession;
  /** Daemon-side WS client talking to the relay (current connection; swapped on reconnect). */
  ws: WebSocket;
  /** xterm-headless buffer used to render snapshot queries (persists across reconnects). */
  buffer: TerminalBuffer;
  /** Last advertised winsize; re-sent to the relay after every reconnect. */
  cols: number;
  rows: number;
  /** Intentional teardown in progress — suppresses onClose/reconnect paths. */
  closing: boolean;
  /** Handlers wired to every (re)connection. */
  handlers: PtyRelayHandlers;
  /** Framed stdin bytes queued while the WS was down; flushed on reconnect. */
  pendingInput: Buffer[];
  pendingInputBytes: number;
  /** In-flight reconnect loop, so overlapping close events don't double-drive. */
  reconnectPromise?: Promise<boolean>;
  /** Re-open the port-forward (e.g. after the local forward server died). */
  reopenForward: () => Promise<void>;
}

export interface PtyRelayHandlers {
  /** Fired for every stdout chunk ('0' frame) coming back from the PTY. */
  onOutput?: (text: string) => void;
  /**
   * Fired when the relay WS closes (code/reason logged daemon-side before
   * this). The caller decides whether to start a reconnect loop
   * (reconnectPtyRelay) or mark the shell closed.
   */
  onClose?: (code: number, reason: string) => void;
  /** Fired on relay WS error (typically a fatal sub-protocol failure). */
  onError?: (err: Error) => void;
  /** Fired when a reconnect attempt fails; attempt is 1-based. */
  onReconnectFailed?: (attempt: number, err: Error, nextDelayMs: number | undefined) => void;
  /** Fired when a reconnect succeeds (after backlog replay frames start arriving). */
  onReconnected?: () => void;
}

export interface DeployPtyRelayOpts {
  shellId: number;
  cols: number;
  rows: number;
  kernel: KernelConnection;
  kernelReady: Promise<void>;
  colabClient: ColabClient;
  endpoint: string;
  localHost?: string;
  handlers: PtyRelayHandlers;
}

/**
 * Open + wire one daemon-side WS client connection to the relay through the
 * relay's current port-forward: listeners are attached BEFORE awaiting
 * 'open' (no first-message race), heartbeat is armed after open, the last
 * known winsize is re-sent, and any stdin queued during the outage is
 * flushed. Throws if the connection cannot be established.
 */
export async function connectRelay(relay: PtyRelay, openTimeoutMs = SHELL_RELAY_WS_OPEN_TIMEOUT_MS): Promise<void> {
  const wsUrl = `ws://${relay.forward.localHost}:${relay.forward.localPort}/`;
  const ws = new WebSocket(wsUrl);

  let heartbeatTimer: ReturnType<typeof setInterval> | undefined;
  let pongTimer: ReturnType<typeof setTimeout> | undefined;
  let missedPongs = 0;
  const stopHeartbeat = () => {
    if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = undefined; }
    if (pongTimer) { clearTimeout(pongTimer); pongTimer = undefined; }
  };
  const startHeartbeat = () => {
    heartbeatTimer = setInterval(() => {
      if (ws.readyState !== WebSocket.OPEN) return;
      try { ws.ping(); } catch { /* socket tearing down */ }
      if (pongTimer) clearTimeout(pongTimer); // avoid stacking timers
      pongTimer = setTimeout(() => {
        missedPongs++;
        if (missedPongs >= 2) {
          log.warn(`pty_relay ${relay.shellId}: heartbeat timeout (2 missed pongs); dropping connection (reconnect will follow)`);
          try { ws.terminate(); } catch { /* best-effort */ }
        }
      }, SHELL_RELAY_HEARTBEAT_PONG_TIMEOUT_MS);
    }, SHELL_RELAY_HEARTBEAT_PING_INTERVAL_MS);
  };

  const handleMessage = (data: WebSocket.RawData) => {
    const buf: Buffer = Array.isArray(data)
      ? Buffer.concat(data as Buffer[])
      : data instanceof ArrayBuffer
        ? Buffer.from(data)
        : (data as Buffer);
    if (buf.length === 0) return;
    const cmd = buf[0];
    const payload = buf.subarray(1);
    if (cmd === TTYD_CMD_STDIN /* '0' = stdout from relay's POV */) {
      const text = payload.toString('utf8');
      relay.buffer.append(text);
      relay.handlers.onOutput?.(text);
    }
    // '1' (title) and '2' (client options) are browser-only — ignored.
  };
  const handleClose = (code: number, reasonBuf: Buffer) => {
    stopHeartbeat();
    const reason = reasonBuf?.toString('utf8') ?? '';
    // B4: always log close code + reason — abnormal deaths must be
    // attributable from the daemon log (proxy-forced 1006 vs relay
    // shutdown 1001 vs heartbeat terminate).
    log.debug(`pty_relay ${relay.shellId}: WS closed code=${code} reason=${JSON.stringify(reason)}`);
    if (relay.closing) return;
    relay.handlers.onClose?.(code, reason);
  };
  const handleError = (err: Error) => {
    log.warn(`pty_relay ${relay.shellId}: WS error:`, err.message);
    relay.handlers.onError?.(err);
  };

  ws.on('message', handleMessage);
  ws.on('close', handleClose);
  ws.on('error', handleError);
  ws.on('pong', () => {
    missedPongs = 0;
    if (pongTimer) { clearTimeout(pongTimer); pongTimer = undefined; }
  });

  relay.ws = ws;

  try {
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        cleanup();
        reject(new Error(`WS open to ${wsUrl} timed out after ${openTimeoutMs}ms`));
      }, openTimeoutMs);
      const onOpen = () => { cleanup(); resolve(); };
      const onError = (err: Error) => {
        cleanup();
        reject(new Error(`WS open to ${wsUrl} failed: ${err.message}`));
      };
      const cleanup = () => {
        clearTimeout(timer);
        ws.off('open', onOpen);
        ws.off('error', onError);
      };
      ws.once('open', onOpen);
      ws.once('error', onError);
    });
  } catch (err) {
    stopHeartbeat();
    try { ws.close(); } catch { /* already closing */ }
    throw err;
  }

  // Re-advertise the last known winsize so the PTY matches what clients
  // last rendered (also arms the relay's SIGWINCH path).
  relaySendResizeRaw(ws, relay.cols, relay.rows);

  // Flush stdin queued while the connection was down, in order.
  if (relay.pendingInput.length > 0) {
    const queued = relay.pendingInput;
    relay.pendingInput = [];
    relay.pendingInputBytes = 0;
    for (const framed of queued) {
      try { ws.send(framed); } catch { break; }
    }
  }

  startHeartbeat();
}

/**
 * Reconnect the daemon → relay WS after a transport-level close, retrying
 * with exponential backoff inside the policy window. Never redeploys or
 * restarts the VM-side relay — user state (bash, running jobs) survives.
 * The relay buffers PTY output into its backlog while no client is
 * connected, so a successful reconnect replays the gap transparently.
 *
 * Returns true on success (new connection live), false if the policy window
 * expired (caller should then mark the shell closed and clean up).
 */
export function reconnectPtyRelay(
  relay: PtyRelay,
  policy: RelayReconnectPolicy = DEFAULT_RECONNECT_POLICY,
): Promise<boolean> {
  // Collapse overlapping close events (error+close pairs, heartbeat
  // terminate + close) into a single reconnect loop.
  if (relay.reconnectPromise) return relay.reconnectPromise;

  relay.reconnectPromise = (async () => {
    const deadline = Date.now() + policy.budgetMs;
    let attempt = 0;
    try {
      while (!relay.closing && Date.now() < deadline) {
        attempt += 1;
        try {
          await connectRelay(relay, policy.openTimeoutMs);
          relay.handlers.onReconnected?.();
          return true;
        } catch (err) {
          const e = err instanceof Error ? err : new Error(String(err));
          // Local forward server gone (ECONNREFUSED) — reopen the
          // port-forward before the next attempt.
          if (/ECONNREFUSED/.test(e.message)) {
            try { await relay.reopenForward(); } catch { /* try again next attempt */ }
          }
          const nextDelay = policy.delaysMs[Math.min(attempt - 1, policy.delaysMs.length - 1)];
          relay.handlers.onReconnectFailed?.(attempt, e, nextDelay);
          if (relay.closing) return false;
          await new Promise((r) => setTimeout(r, nextDelay ?? 0));
        }
      }
      return false;
    } finally {
      relay.reconnectPromise = undefined;
    }
  })();

  return relay.reconnectPromise;
}

/**
 * Bootstrap a per-shell PTY relay on the VM and connect a daemon-side WS
 * client to it through a freshly-opened port-forward.
 *
 * Steps:
 *   1. allocate VM port (shellId-aliased)
 *   2. write the embedded Python script to /tmp/pty_relay_<id>.py on the VM
 *   3. nohup-launch it (killing any stale copy first), polling the log
 *      until 'WS-UP' appears (or timeout)
 *   4. open a Colab port-forward: 127.0.0.1:<os-port> -> remote <$port>
 *   5. open the daemon-side WS client (connectRelay)
 *
 * On any failure the partial resources (port-forward, relay script) are
 * torn down so a naive `shell_open` retry doesn't accumulate garbage.
 */
export async function deployPtyRelay(opts: DeployPtyRelayOpts): Promise<PtyRelay> {
  const shellId = opts.shellId;
  const cols = Math.max(1, opts.cols);
  const rows = Math.max(1, opts.rows);
  const port = SHELL_RELAY_PORT_BASE + shellId * 2;
  const scriptPath = `/tmp/pty_relay_${shellId}.py`;
  const logPath = `/tmp/pty_relay_${shellId}.log`;
  const localHost = opts.localHost ?? '127.0.0.1';

  // 1. write the relay script + launch on the VM
  const pyB64 = Buffer.from(PTY_RELAY_PY, 'utf8').toString('base64');
  const launchCode = [
    `import base64, os, subprocess, time`,
    `open(${JSON.stringify(scriptPath)}, 'w').write(base64.b64decode(${JSON.stringify(pyB64)}).decode())`,
    `subprocess.run(['pkill','-f', ${JSON.stringify(scriptPath)}], timeout=3)`,
    `time.sleep(0.3)`,
    `subprocess.Popen(['bash','-c','nohup python3 ' + ${JSON.stringify(scriptPath + ' ' + String(port) + ' ' + String(cols) + ' ' + String(rows))} + ' > ' + ${JSON.stringify(logPath)} + ' 2>&1 &'], start_new_session=True)`,
    `deadline = time.time() + ${SHELL_RELAY_BOOTSTRAP_TIMEOUT_MS / 1000}`,
    `while time.time() < deadline:`,
    `    time.sleep(${SHELL_RELAY_BOOTSTRAP_POLL_INTERVAL_MS / 1000})`,
    `    try:`,
    `        log = open(${JSON.stringify(logPath)}).read()`,
    `    except FileNotFoundError:`,
    `        continue`,
    `    if 'WS-UP' in log:`,
    `        break`,
    `print(open(${JSON.stringify(logPath)}).read() if os.path.exists(${JSON.stringify(logPath)}) else 'no log')`,
  ].join('\n');

  await opts.kernelReady;
  if (!opts.kernel.isConnected) {
    throw new Error(
      opts.kernel.isReconnecting
        ? 'Kernel WebSocket is reconnecting, retry in a few seconds.'
        : 'Kernel WebSocket disconnected; run `colab runtime restart` to restore exec.',
    );
  }

  let launchLog = '';
  let execError: Error | undefined;
  try {
    const outputs = await opts.kernel.execute(launchCode);
    for await (const output of outputs) {
      launchLog += kernelOutputToText(output);
      if (output.type === 'error') {
        execError = new Error(`${output.ename}: ${output.evalue}`);
      }
    }
  } catch (err) {
    throw new Error(`pty_relay bootstrap exec failed: ${err instanceof Error ? err.message : String(err)}`);
  }
  if (execError) {
    throw new Error(`pty_relay bootstrap exec returned error: ${execError.message}\n${launchLog}`);
  }
  if (!launchLog.includes('WS-UP')) {
    throw new Error(`pty_relay did not report WS-UP within ${SHELL_RELAY_BOOTSTRAP_TIMEOUT_MS}ms. Relay log:\n${launchLog.slice(-1000)}`);
  }

  // 2. open port-forward: local OS-assigned port -> remote $port
  const openForward = () =>
    ForwardSession.open(0, localHost, 0, port, opts.colabClient, opts.endpoint, undefined);

  let forward: ForwardSession;
  try {
    forward = await openForward();
  } catch (err) {
    await killPtyRelay(opts.kernel, shellId).catch(() => {});
    throw new Error(`failed to open port-forward to relay port ${port}: ${err instanceof Error ? err.message : String(err)}`);
  }

  const relay: PtyRelay = {
    shellId,
    port,
    forward,
    ws: undefined as unknown as WebSocket, // set by connectRelay below
    buffer: new TerminalBuffer(undefined, cols, rows),
    cols,
    rows,
    closing: false,
    handlers: opts.handlers,
    pendingInput: [],
    pendingInputBytes: 0,
    reopenForward: async () => {
      const fresh = await openForward();
      const old = relay.forward;
      relay.forward = fresh;
      try { await old.close(); } catch { /* best-effort */ }
    },
  };

  // 3. open the daemon-side WS client. On failure, tear everything down so
  //    a retry starts from a clean slate.
  try {
    relay.closing = true; // suppress onClose/reconnect during teardown-on-failure
    await connectRelay(relay);
    relay.closing = false;
  } catch (err) {
    try { relay.ws?.close(); } catch { /* already closing */ }
    await forward.close().catch(() => {});
    await killPtyRelay(opts.kernel, shellId).catch(() => {});
    relay.buffer.dispose();
    throw err;
  }

  return relay;
}

/**
 * Send stdin bytes to the bash running under this relay (ttyd '0' frame).
 * If the WS is down (reconnect in progress) the bytes are queued (bounded
 * by MAX_PENDING_INPUT_BYTES) and flushed on reconnect, in order.
 * Returns 'sent' | 'queued' | 'dropped' (queue full — bytes were lost).
 */
export function relaySendStdin(relay: PtyRelay, data: string): 'sent' | 'queued' | 'dropped' {
  const src = Buffer.from(data, 'utf8');
  const framed = Buffer.allocUnsafe(1 + src.length);
  framed[0] = TTYD_CMD_STDIN;
  src.copy(framed, 1);

  if (relay.ws && relay.ws.readyState === WebSocket.OPEN) {
    relay.ws.send(framed);
    return 'sent';
  }
  if (relay.closing) return 'dropped';
  if (relay.pendingInputBytes + framed.length > MAX_PENDING_INPUT_BYTES) {
    log.warn(`pty_relay ${relay.shellId}: pending input buffer full (${relay.pendingInputBytes}B); dropping ${framed.length}B of stdin`);
    return 'dropped';
  }
  relay.pendingInput.push(framed);
  relay.pendingInputBytes += framed.length;
  return 'queued';
}

/**
 * Send a winsize update to the relay (ttyd '1' frame). Remembers the size
 * (re-sent on every reconnect) and updates the buffer's render dimensions
 * so subsequent snapshot queries are accurate.
 */
export function relaySendResize(relay: PtyRelay, cols: number, rows: number): void {
  relay.cols = cols;
  relay.rows = rows;
  relay.buffer.resize(cols, rows);
  if (relay.ws && relay.ws.readyState === WebSocket.OPEN) {
    relaySendResizeRaw(relay.ws, cols, rows);
  }
}

/**
 * Kill the VM-side relay process tree for this shellId and remove its
 * script + log files. Serialized through the per-kernel maintenance queue
 * (bursts can't congest the single exec channel), confirmed by requiring
 * the snippet's final `v-clean relay=gone` status line, retried with
 * backoff, and every attempt logged with its outcome. Returns true when
 * the kill was positively confirmed.
 *
 * Process-tree kill: bash was spawned with setsid (pgid == its pid), but
 * interactive bash puts each foreground job in its own process group —
 * killing only the relay or only bash's pgrp orphans jobs like
 * `sleep 600`. We read the child pid from the relay log and SIGKILL every
 * process sharing bash's session id.
 */
export async function killPtyRelay(kernel: KernelConnection, shellId: number): Promise<boolean> {
  const scriptPath = `/tmp/pty_relay_${shellId}.py`;
  const logPath = `/tmp/pty_relay_${shellId}.log`;
  const killCode = [
    `import subprocess, os, re, signal`,
    `session_killed = 0`,
    `try:`,
    `    log = open(${JSON.stringify(logPath)}).read()`,
    `    m = re.search(r'RELAY-UP child=(\\d+)', log)`,
    `    if m:`,
    `        pid = int(m.group(1))`,
    `        # kill the whole session: interactive bash puts each foreground job in`,
    `        # its own process group, so killpg(bash) alone orphans jobs like sleep`,
    `        for p in os.listdir('/proc'):`,
    `            if not p.isdigit(): continue`,
    `            try:`,
    `                st = open('/proc/'+p+'/stat').read()`,
    `                if int(st[st.rfind(')')+2:].split()[3]) == pid:  # session id`,
    `                    try: os.kill(int(p), signal.SIGKILL); session_killed += 1`,
    `                    except (ProcessLookupError, PermissionError): pass`,
    `            except Exception: pass`,
    `        try: os.killpg(pid, signal.SIGKILL)  # pgid == bash pid (setsid)`,
    `        except (ProcessLookupError, PermissionError): pass`,
    `        try: os.kill(pid, signal.SIGKILL)`,
    `        except (ProcessLookupError, PermissionError): pass`,
    `except Exception as e:`,
    `    print('session-scan-error', e)`,
    `subprocess.run(['pkill', '-f', ${JSON.stringify(scriptPath)}], timeout=3)`,
    `# confirm: is the relay process actually gone?`,
    `chk = subprocess.run(['pgrep', '-f', ${JSON.stringify(scriptPath)}], capture_output=True, text=True)`,
    `alive = [l for l in chk.stdout.splitlines() if l.strip() and 'pgrep' not in l]`,
    `files_removed = 0`,
    `for p in [${JSON.stringify(scriptPath)}, ${JSON.stringify(logPath)}]:`,
    `    try: os.remove(p); files_removed += 1`,
    `    except Exception: pass`,
    `print(f'v-clean relay={"alive" if alive else "gone"} session_killed={session_killed} files_removed={files_removed}')`,
  ].join('\n');

  for (let attempt = 0; attempt < KILL_RELAY_ATTEMPTS; attempt++) {
    const backoff = KILL_RELAY_BACKOFF_MS[Math.min(attempt, KILL_RELAY_BACKOFF_MS.length - 1)];
    if (backoff > 0) await new Promise((r) => setTimeout(r, backoff));
    try {
      const out = await runKernelMaintenance(kernel, killCode);
      if (out.includes('v-clean') && out.includes('relay=gone')) {
        const summary = out.match(/v-clean[^\n]*/)?.[0] ?? 'v-clean';
        log.info(`killPtyRelay ${shellId}: ok (attempt ${attempt + 1}/${KILL_RELAY_ATTEMPTS}) ${summary}`);
        return true;
      }
      log.warn(
        `killPtyRelay ${shellId}: attempt ${attempt + 1}/${KILL_RELAY_ATTEMPTS} exec ran but relay not confirmed gone. Output tail: ${out.slice(-300)}`,
      );
    } catch (err) {
      log.warn(
        `killPtyRelay ${shellId}: exec failed (attempt ${attempt + 1}/${KILL_RELAY_ATTEMPTS}):`,
        err instanceof Error ? err.message : String(err),
      );
    }
  }
  log.warn(`killPtyRelay ${shellId}: all ${KILL_RELAY_ATTEMPTS} attempts failed; relay/files may linger (backstop sweeper will retry)`);
  return false;
}

/**
 * Orphan sweep: kill every `pty_relay` process on the VM whose shellId is
 * NOT in `liveShellIds` (orphans from a crashed daemon or a killPtyRelay
 * that exhausted its retries), and remove their /tmp files. Relays on the
 * live list are never touched. Serialized through the same maintenance
 * queue so it can't congest the exec channel either.
 */
export async function sweepOrphanRelays(
  kernel: KernelConnection,
  liveShellIds: number[] = [],
): Promise<void> {
  const sweepCode = [
    `import subprocess, glob, os, re`,
    `live = {${liveShellIds.join(',')}}`,
    `killed = 0; removed = 0`,
    `for path in glob.glob('/tmp/pty_relay_*.py'):`,
    `    m = re.match(r'/tmp/pty_relay_(\\d+)\\.py$', path)`,
    `    if not m: continue`,
    `    sid = int(m.group(1))`,
    `    if sid in live: continue`,
    `    subprocess.run(['pkill', '-f', path], timeout=3)`,
    `    killed += 1`,
    `    for p in (path, path[:-3] + '.log'):`,
    `        try: os.remove(p); removed += 1`,
    `        except Exception: pass`,
    `print(f'sweep killed={killed} removed={removed} live={${liveShellIds.length}}')`,
  ].join('\n');
  try {
    const out = await runKernelMaintenance(kernel, sweepCode);
    log.info(`sweepOrphanRelays: ${out.trim().split('\n').pop() ?? 'done'}`);
  } catch (err) {
    log.warn('sweepOrphanRelays: exec failed:', err instanceof Error ? err.message : String(err));
  }
}

/**
 * Tear everything down for a relay that's no longer needed: suppress the
 * reconnect path, close the WS, close the port-forward, dispose the buffer,
 * and kill the VM-side process tree. Errors are logged, never swallowed —
 * a failed VM-side kill is visible in the daemon log and left to the
 * backstop sweeper.
 */
export async function closePtyRelay(relay: PtyRelay, kernel: KernelConnection): Promise<void> {
  relay.closing = true;
  relay.pendingInput = [];
  relay.pendingInputBytes = 0;
  try { relay.ws.close(); } catch { /* ignore */ }
  try { await relay.forward.close(); } catch (err) {
    log.warn(`closePtyRelay ${relay.shellId}: forward.close failed:`, err instanceof Error ? err.message : String(err));
  }
  try { relay.buffer.dispose(); } catch { /* ignore */ }
  try {
    const ok = await killPtyRelay(kernel, relay.shellId);
    if (!ok) {
      log.warn(`closePtyRelay ${relay.shellId}: VM-side kill not confirmed; backstop sweeper will retry`);
    }
  } catch (err) {
    log.warn(`closePtyRelay ${relay.shellId}: killPtyRelay threw:`, err instanceof Error ? err.message : String(err));
  }
}

// ── internal helpers ──

function relaySendResizeRaw(ws: WebSocket, cols: number, rows: number): void {
  const payload = JSON.stringify({ columns: cols, rows: rows });
  const payloadBytes = Buffer.from(payload, 'utf8');
  const framed = Buffer.allocUnsafe(1 + payloadBytes.length);
  framed[0] = TTYD_CMD_RESIZE;
  payloadBytes.copy(framed, 1);
  ws.send(framed);
}

function kernelOutputToText(output: KernelOutput): string {
  if (output.type === 'stream') return output.text;
  if (output.type === 'error') return `${output.ename}: ${output.evalue}\n${output.traceback.join('\n')}`;
  if (output.type === 'execute_result') return output.data['text/plain'] ?? '';
  if (output.type === 'display_data') return output.data['text/plain'] ?? '';
  if (output.type === 'status') return `[${output.executionState}]\n`;
  return '';
}



