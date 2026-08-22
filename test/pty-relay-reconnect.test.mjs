/**
 * Local unit tests for the daemon<->relay reconnect logic (B1/B3 regression).
 * No Colab runtime needed: a fake relay WS server mimics pty_relay.py's
 * backlog semantics (buffer PTY output while no client, flush on connect).
 *
 * Run: npm run build && node --test test/pty-relay-reconnect.test.mjs
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { WebSocketServer } from 'ws';
import {
  connectRelay,
  reconnectPtyRelay,
  relaySendStdin,
  relaySendResize,
} from '../dist/daemon/pty-relay.js';
import { TerminalBuffer } from '../dist/terminal/terminal-buffer.js';

/**
 * Fake relay server emulating pty_relay.py: '0' frames are stdout,
 * backlog accumulates while no client and is flushed to each new client.
 * `emit(text)` = the PTY produced output.
 */
function startFakeRelay(port) {
  const wss = new WebSocketServer({ port });
  const backlog = [];
  let client = null;
  const received = []; // framed stdin bytes received from daemon
  wss.on('connection', (ws) => {
    client = ws;
    if (backlog.length) {
      for (const f of backlog) ws.send(f);
      backlog.length = 0;
    }
    ws.on('message', (data) => received.push(Buffer.from(data)));
  });
  return {
    wss,
    received,
    emit(text) {
      const framed = Buffer.concat([Buffer.from([0x30]), Buffer.from(text, 'utf8')]);
      if (client) client.send(framed);
      else backlog.push(framed);
    },
    /** Hard-kill the current client connection (simulate proxy RST). */
    killClient() {
      if (client) { client.terminate(); client = null; }
    },
    close() {
      return new Promise((r) => { wss.close(() => r()); });
    },
  };
}

let portCounter = 25301;

function makeRelay(port, handlers = {}) {
  return {
    shellId: 999,
    port,
    forward: { localHost: '127.0.0.1', localPort: port, close: async () => {} },
    ws: undefined,
    buffer: new TerminalBuffer(undefined, 80, 24),
    cols: 80,
    rows: 24,
    closing: false,
    handlers,
    pendingInput: [],
    pendingInputBytes: 0,
    reconnectPromise: undefined,
    reopenForward: async () => {},
  };
}

test('initial connect + output flows through buffer and onOutput', async () => {
  const port = portCounter++;
  const relay = startFakeRelay(port);
  const outputs = [];
  const r = makeRelay(port, { onOutput: (t) => outputs.push(t) });
  try {
    relay.emit('BOOT');
    await connectRelay(r, 2000);
    assert.deepEqual(outputs, ['BOOT']); // backlog replayed on connect
    relay.emit('LIVE');
    await new Promise((r2) => setTimeout(r2, 50));
    assert.deepEqual(outputs, ['BOOT', 'LIVE']);
    assert.match(r.buffer.getContents(), /BOOT[\s\S]*LIVE/);
  } finally {
    r.closing = true;
    try { r.ws?.close(); } catch {}
    await relay.close();
  }
});

test('reconnect after transport close replays gap output and flushes queued stdin', async () => {
  const port = portCounter++;
  const relay = startFakeRelay(port);
  const outputs = [];
  const events = [];
  const r = makeRelay(port, {
    onOutput: (t) => outputs.push(t),
    onReconnected: () => events.push('reconnected'),
    onReconnectFailed: (a, e) => events.push(`fail${a}:${e.message.slice(0, 20)}`),
  });
  try {
    await connectRelay(r, 2000);
    relay.emit('A');

    // queue stdin while connected, then kill transport
    assert.equal(relaySendStdin(r, 'echo ok\n'), 'sent');

    await new Promise((r2) => setTimeout(r2, 50));
    relay.killClient(); // simulate proxy-forced close; daemon detects via 'close'
    await new Promise((r2) => setTimeout(r2, 100));

    // during outage: relay buffers output; daemon queues input
    relay.emit('GAP_OUTPUT');
    assert.equal(relaySendStdin(r, 'echo queued\n'), 'queued');

    // run the reconnect loop with a short policy
    const ok = await reconnectPtyRelay(r, { delaysMs: [50, 50], budgetMs: 2000, openTimeoutMs: 1000 });
    assert.equal(ok, true);
    assert.deepEqual(events.filter((e) => e === 'reconnected'), ['reconnected']);

    await new Promise((r2) => setTimeout(r2, 100));
    // backlog replay: gap output delivered after reconnect
    assert.ok(outputs.join('').includes('GAP_OUTPUT'), `outputs: ${JSON.stringify(outputs)}`);
    // queued stdin flushed in order (frame '0'+payload, after resize frame '1')
    const stdinFrames = relay.received.filter((f) => f[0] === 0x30).map((f) => f.subarray(1).toString('utf8'));
    assert.deepEqual(stdinFrames, ['echo ok\n', 'echo queued\n']);

    // resize during outage is applied on the relay object and re-sent
    relaySendResize(r, 120, 40);
    assert.equal(r.cols, 120);
    assert.equal(r.rows, 40);
  } finally {
    r.closing = true;
    try { r.ws?.close(); } catch {}
    await relay.close();
  }
});

test('reconnect gives up after budget when relay is truly gone', async () => {
  const port = portCounter++; // nothing listening on this port
  const fails = [];
  const r = makeRelay(port, { onReconnectFailed: (a) => fails.push(a) });
  const ok = await reconnectPtyRelay(r, { delaysMs: [30, 30, 30], budgetMs: 300, openTimeoutMs: 200 });
  assert.equal(ok, false);
  assert.ok(fails.length >= 2, `expected multiple attempts, got ${fails.length}`);
});

test('reconnect is single-flight and aborted by closing flag', async () => {
  const port = portCounter++;
  const r = makeRelay(port, {});
  const p1 = reconnectPtyRelay(r, { delaysMs: [50, 50, 50, 50], budgetMs: 5000, openTimeoutMs: 200 });
  const p2 = reconnectPtyRelay(r, { delaysMs: [50], budgetMs: 5000, openTimeoutMs: 200 });
  assert.equal(p1, p2, 'overlapping reconnect calls share one loop');
  r.closing = true; // intentional teardown mid-loop
  const ok = await p1;
  assert.equal(ok, false);
});

test('pending input drops (not queue) past the cap rather than growing unboundedly', async () => {
  const port = portCounter++;
  const r = makeRelay(port, {});
  // no ws at all: every send queues
  const chunk = 'x'.repeat(64 * 1024);
  // Each frame is chunk+1 command byte; 256 KiB cap fits 3 frames, the 4th
  // spills over the cap and must be dropped rather than queued.
  assert.equal(relaySendStdin(r, chunk), 'queued');
  assert.equal(relaySendStdin(r, chunk), 'queued');
  assert.equal(relaySendStdin(r, chunk), 'queued');
  assert.equal(relaySendStdin(r, chunk), 'dropped');
});
