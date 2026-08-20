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
 * a pong doesn't arrive within 60s we mark the shell closed. Real network
 * jitter only delays things by multiples of the ping interval — a dropped
 * pong means the next ping resets the timeout; only three consecutive
 * pings with zero pongs triggers teardown. Threshold was chosen so a busy
 * relay going through Colab's wrapping proxy tolerates a 60s opaque
 * stall without being falsely diagnosed dead (the longest healthy-colab
 * stall observed in practice is ~40s during runtime reassignment).
 */
const SHELL_RELAY_HEARTBEAT_PING_INTERVAL_MS = 30_000;
const SHELL_RELAY_HEARTBEAT_PONG_TIMEOUT_MS = 60_000;

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

Bootstrap race fix: bash is spawned at startup and emits its first prompt
into master_fd before websockets.serve even listens (let alone before any
WS client connects). The pump reads those bytes but has no client to
forward to, so without a backlog buffer they would be silently dropped and
a fresh "attach --no-wait" would see an empty screen. We queue early PTY
bytes into backlog while clients is empty; the first client to connect
flushes the backlog atomically before joining the live set.

Race-safety: backlog_lock serializes the pump's queue-vs-broadcast decision
AND the handler's snapshot-clear-addws-sendSnapshot. Holding the lock
across "await ws.send(snapshot)" is intentional — it blocks the pump for
the single-frame send (~us), but guarantees pump never observes the
half-state "first_client_pending already cleared but clients still empty"
mid-handler-flight (which would otherwise drop bytes into a backlog that
no one ever flushes). No deadlock risk: pump and handler both acquire the
same lock, never nested, never re-entrant.
"""
import asyncio, os, pty, subprocess, sys, struct, fcntl, termios, signal, json
import websockets

PORT = int(sys.argv[1])
COLS = int(sys.argv[2]) if len(sys.argv) > 2 else 80
ROWS = int(sys.argv[3]) if len(sys.argv) > 3 else 24
argv = sys.argv[4:] or ['bash']
clients: set = set()

# Bytes the pump read from master_fd before any WS client connected — most
# importantly, the bash startup prompt. The first client flushes these;
# later clients see an empty backlog. See module-level comment above for
# the race-safety argument.
backlog: bytearray = bytearray()
backlog_lock = asyncio.Lock()
first_client_pending = True

master_fd, slave_fd = pty.openpty()
fcntl.ioctl(slave_fd, termios.TIOCSWINSZ, struct.pack("HHHH", ROWS, COLS, 0, 0))
fcntl.ioctl(master_fd, termios.TIOCSWINSZ, struct.pack("HHHH", ROWS, COLS, 0, 0))


def _acquire_controlling_tty():
    """Run in child via preexec_fn before exec'ing bash:
    (1) become a new session leader so tcsetpgrp has a target;
    (2) acquire slave_fd as the controlling tty.

    Without this, bash prints two stderr warnings on every boot
    ("bash: cannot set terminal process group ...: Inappropriate
    ioctl for device" and "bash: no job control in this shell")
    because pty.openpty() opens both ends without O_NOCTTY cleared,
    so the child never auto-acquires controlling tty, and bash's
    tcsetpgrp fails. This is cosmetic harm (job control disabled)
    but the noise pollutes every "attach --no-wait" snapshot.
    """
    os.setsid()
    fcntl.ioctl(slave_fd, termios.TIOCSCTTY, 0)


child = subprocess.Popen(argv, stdin=slave_fd, stdout=slave_fd, stderr=slave_fd, close_fds=False, preexec_fn=_acquire_controlling_tty)
os.close(slave_fd)
print(f"RELAY-UP child={child.pid} port={PORT} size={ROWS}x{COLS}", flush=True)


async def handler(ws):
    global first_client_pending
    # Atomically (under backlog_lock): snapshot backlog (only if we're the
    # first client), clear it, add self to clients, AND send the snapshot.
    # Pump acquires the same lock before its queue-vs-broadcast decision,
    # so it never observes the half-state where first_client_pending is
    # False but clients is still empty (which would lead to dropped bytes
    # in backlog that no later client will flush).
    async with backlog_lock:
        if first_client_pending:
            first_client_pending = False
            snapshot = bytes(backlog)
            backlog.clear()
        else:
            snapshot = b''
        clients.add(ws)
        if snapshot:
            try:
                await ws.send(snapshot)
            except Exception:
                # Best-effort flush — if send fails the live pump will pick
                # up subsequent bytes anyway, and a redraw-triggering input
                # from the caller recovers the (small, early) lost bytes.
                pass
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
        print("ws-err", type(e).__name__, str(e)[:60], flush=True)
    finally:
        clients.discard(ws)


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
        # Atomically decide (under backlog_lock): still in pre-first-client
        # phase (queue to backlog), or in live-broadcast phase (clients
        # non-empty AND first_client_pending already cleared). Taking the
        # lock before checking state prevents observing a momentary
        # "clients empty + first_client_pending False" race that would
        # otherwise orphan backlog bytes.
        async with backlog_lock:
            if first_client_pending or not clients:
                backlog.extend(framed)
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
  /** Daemon-side WS client talking to the relay (already connected when returned). */
  ws: WebSocket;
  /** xterm-headless buffer used to render snapshot queries. */
  buffer: TerminalBuffer;
}

export interface PtyRelayHandlers {
  /** Fired for every stdout chunk ('0' frame) coming back from the PTY. */
  onOutput?: (text: string) => void;
  /** Fired when the relay WS closes (bash exited, port-forward dropped, etc.). */
  onClose?: () => void;
  /** Fired on relay WS error (typically a fatal sub-protocol failure). */
  onError?: (err: Error) => void;
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
 * Bootstrap a per-shell PTY relay on the VM and connect a daemon-side WS
 * client to it through a freshly-opened port-forward.
 *
 * Steps:
 *   1. allocate VM port (shellId-aliased)
 *   2. write the embedded Python script to /tmp/pty_relay_<id>.py on the VM
 *   3. nohup-launch it (killing any stale copy first), polling the log
 *      until 'WS-UP' appears (or timeout)
 *   4. open a Colab port-forward: 127.0.0.1:<os-port> -> remote <$port>
 *   5. open the daemon-side WS client to ws://127.0.0.1:<forward.localPort>/
 *      and wire its 'message'/'close'/'error' handlers up front to avoid
 *      the "first message arrives before listener is attached" race
 *   6. send the initial resize (caller's cols/rows)
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
  let forward: ForwardSession;
  try {
    forward = await ForwardSession.open(
      /* id (unused; not registered in forwardState) */ 0,
      localHost,
      /* localPort: 0 = OS-assigned, avoids collisions across shells */ 0,
      port,
      opts.colabClient,
      opts.endpoint,
      undefined,
    );
  } catch (err) {
    await killPtyRelay(opts.kernel, shellId).catch(() => {});
    throw new Error(`failed to open port-forward to relay port ${port}: ${err instanceof Error ? err.message : String(err)}`);
  }

  // 3. open the daemon-side WS client. Listeners must be wired BEFORE we
  // await 'open' so an early bash prompt (relay pumps output to clients
  // the instant their WS connects) doesn't arrive before 'message' is set.
  const buffer = new TerminalBuffer(undefined, cols, rows);
  const wsUrl = `ws://${forward.localHost}:${forward.localPort}/`;
  const ws = new WebSocket(wsUrl);
  let closed = false;

  const handleClose = () => {
    if (closed) return;
    closed = true;
    log.debug(`pty_relay ${shellId}: WS closed`);
    opts.handlers.onClose?.();
  };
  const handleError = (err: Error) => {
    if (closed) return;
    log.warn(`pty_relay ${shellId}: WS error:`, err.message);
    opts.handlers.onError?.(err);
  };
  const handleMessage = (data: WebSocket.RawData, isBinary: boolean) => {
    // Normalize the RawData union (`Buffer | ArrayBuffer | Buffer[]`) to a
    // single Buffer. The relay always sends binary frames, so in practice
    // we'll see `Buffer` (the default binaryType='nodebuffer' delivers both
    // binary AND text frames as Buffer), but we cover the other arms
    // defensively so the type narrowing is exhaustive. `isBinary` is unused
    // — we treat text frames identically since ws already decoded them to
    // UTF-8 bytes in the Buffer.
    void isBinary;
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
      buffer.append(text);
      opts.handlers.onOutput?.(text);
    }
    // '1' (title) and '2' (client options) are browser-only — ignored.
  };

  ws.on('message', handleMessage);
  ws.on('close', handleClose);
  ws.on('error', handleError);

  // Heartbeat: periodic ping + hard pong timeout. `ws` does NOT auto-ping
  // on the client side; without this an idle relay that died on the VM
  // (process killed, runtime recycled, etc.) would never trigger our
  // `close` handler if Colab's edge proxy doesn't forward the TCP RST back
  // to us — which has been observed. Python `websockets` lib auto-pongs
  // incoming pings (RFC 6455 server-side obligation), so a live relay will
  // always respond. Two consecutive missed pongs → teardown.
  let heartbeatTimer: ReturnType<typeof setTimeout> | undefined;
  let pongTimer: ReturnType<typeof setTimeout> | undefined;
  let missedPongs = 0;
  const startHeartbeat = () => {
    heartbeatTimer = setInterval(() => {
      if (ws.readyState !== WebSocket.OPEN) return;
      // Set pong timeout first; clear it when a pong arrives. On the next
      // ping cycle, if no pong has reset `missedPongs`, we increment it.
      pongTimer = setTimeout(() => {
        missedPongs++;
        if (missedPongs >= 2) {
          log.warn(`pty_relay ${shellId}: heartbeat timeout (2 missed pongs); forcing close`);
          // `ws.terminate()` aborts the underlying socket and emits
          // 'close' → `handleClose` runs `onClose` → markShellClosed.
          try { (ws as unknown as { terminate: () => void }).terminate(); }
          catch { /* best-effort */ }
        }
      }, SHELL_RELAY_HEARTBEAT_PONG_TIMEOUT_MS);
    }, SHELL_RELAY_HEARTBEAT_PING_INTERVAL_MS);
  };
  ws.on('pong', () => {
    missedPongs = 0;
    if (pongTimer) { clearTimeout(pongTimer); pongTimer = undefined; }
  });
  const stopHeartbeat = () => {
    if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = undefined; }
    if (pongTimer) { clearTimeout(pongTimer); pongTimer = undefined; }
  };
  ws.on('close', () => stopHeartbeat());
  ws.on('error', () => stopHeartbeat());

  // 4. wait for WS open with timeout. On timeout we tear down forward +
  //    kill the VM relay so a retry starts from a clean slate.
  try {
    await new Promise<void>((resolve, reject) => {
      const onOpen = () => {
        cleanup();
        resolve();
      };
      const onError = (err: Error) => {
        cleanup();
        reject(new Error(`WS open to ${wsUrl} failed: ${err.message}`));
      };
      const timer = setTimeout(() => {
        cleanup();
        reject(new Error(`WS open to ${wsUrl} timed out after ${SHELL_RELAY_WS_OPEN_TIMEOUT_MS}ms`));
      }, SHELL_RELAY_WS_OPEN_TIMEOUT_MS);
      const cleanup = () => {
        clearTimeout(timer);
        ws.off('open', onOpen);
        ws.off('error', onError);
      };
      ws.once('open', onOpen);
      ws.once('error', onError);
    });
  } catch (err) {
    try { ws.close(); } catch { /* already closing */ }
    await forward.close().catch(() => {});
    await killPtyRelay(opts.kernel, shellId).catch(() => {});
    buffer.dispose();
    throw err;
  }

  // 5. send initial resize so the relay's PTY winsize mirrors the caller's
  //    desired cols/rows (the script already set this at spawn time, but
  //    explicit resize also arms the SIGWINCH code path clients depend on).
  relaySendResizeRaw(ws, cols, rows);

  // 6. arm the heartbeat now that the WS is confirmed open. A ping emitted
  //    immediately after open mortgages against the relay dying between
  //    opening and the next shell_input; this is the phantom-entry edge case
  //    documented in the r4 test report.
  startHeartbeat();

  return { shellId, port, forward, ws, buffer };
}

/**
 * Send stdin bytes to the bash running under this relay (ttyd '0' frame).
 * No-op if the WS isn't open — the close handler on the relay has already
 * (or will shortly) mark the shell as closed for the caller.
 */
export function relaySendStdin(relay: PtyRelay, data: string): void {
  if (relay.ws.readyState !== WebSocket.OPEN) return;
  relaySendStdinRaw(relay.ws, data);
}

/**
 * Send a winsize update to the relay (ttyd '1' frame). Also updates the
 * buffer's render dimensions so subsequent snapshot queries are accurate.
 */
export function relaySendResize(relay: PtyRelay, cols: number, rows: number): void {
  relay.buffer.resize(cols, rows);
  if (relay.ws.readyState !== WebSocket.OPEN) return;
  relaySendResizeRaw(relay.ws, cols, rows);
}

/**
 * Kill the VM-side relay process for this shellId and remove its script +
 * log files. Safe to call multiple times (subsequent calls are no-ops once
 * the process is gone). Used both on explicit shell teardown and as the
 * fallback sweeper when the WS closes unexpectedly.
 */
export async function killPtyRelay(kernel: KernelConnection, shellId: number): Promise<void> {
  const scriptPath = `/tmp/pty_relay_${shellId}.py`;
  const logPath = `/tmp/pty_relay_${shellId}.log`;
  const killCode = [
    `import subprocess, os`,
    `subprocess.run(['pkill', '-f', ${JSON.stringify(scriptPath)}], timeout=3)`,
    `for p in [${JSON.stringify(scriptPath)}, ${JSON.stringify(logPath)}]:`,
    `    try: os.remove(p)`,
    `    except Exception: pass`,
    `print('v-clean')`,
  ].join('\n');
  try {
    const outputs = await kernel.execute(killCode);
    for await (const _ of outputs) { /* drain */ }
  } catch (err) {
    log.warn(`killPtyRelay ${shellId}: kernel exec failed:`, err instanceof Error ? err.message : String(err));
  }
}

/**
 * Tear everything down for a relay that's no longer needed: close the WS,
 * close the port-forward, dispose the buffer, and sweep the VM-side
 * process. All steps are best-effort — no error here should mask the
 * original cause that triggered the teardown.
 */
export async function closePtyRelay(relay: PtyRelay, kernel: KernelConnection): Promise<void> {
  try { relay.ws.close(); } catch { /* ignore */ }
  try { await relay.forward.close(); } catch (err) {
    log.debug(`closePtyRelay ${relay.shellId}: forward.close failed:`, err instanceof Error ? err.message : String(err));
  }
  try { relay.buffer.dispose(); } catch { /* ignore */ }
  await killPtyRelay(kernel, relay.shellId).catch(() => {});
}

// ── internal helpers ──

function relaySendStdinRaw(ws: WebSocket, data: string): void {
  const src = Buffer.from(data, 'utf8');
  const framed = Buffer.allocUnsafe(1 + src.length);
  framed[0] = TTYD_CMD_STDIN;
  src.copy(framed, 1);
  ws.send(framed);
}

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
