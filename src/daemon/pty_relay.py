"""PTY relay (ttyd protocol): raw PTY <-> WebSocket frames.

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

