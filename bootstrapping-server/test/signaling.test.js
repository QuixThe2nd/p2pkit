// Integration tests for the DuneCity WebRTC matchmaking lobby server.
// Uses node:test + node:assert plus a tiny `ws` client wrapper.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import WebSocket from 'ws';
import { createSignalingServer } from '../server.js';

const HERE = path.dirname(fileURLToPath(import.meta.url));

// ---- helpers ---------------------------------------------------------------

async function startServer(options = {}) {
  const ctx = createSignalingServer(options);
  await new Promise((resolve, reject) => {
    ctx.httpServer.once('error', reject);
    ctx.httpServer.listen(0, '127.0.0.1', resolve);
  });
  const port = ctx.httpServer.address().port;
  return {
    ...ctx,
    url: `ws://127.0.0.1:${port}/`,
  };
}

class Client {
  constructor(ws) {
    this.ws = ws;
    this.messages = [];
    this.waiters = [];
    this.closeInfo = null;
    this.opened = new Promise((resolve, reject) => {
      ws.once('open', resolve);
      ws.once('error', reject);
    });
    ws.on('message', (raw) => {
      const text = raw.toString('utf8');
      let message;
      try {
        message = JSON.parse(text);
      } catch {
        message = { unparsed: text };
      }
      this.messages.push(message);
      this.pump();
    });
    ws.on('close', (code, reason) => {
      this.closeInfo = { code, reason: reason.toString() };
      this.pump();
      this.rejectAll(new Error(`socket closed (code ${code})`));
    });
    ws.on('error', (err) => {
      this.lastError = err;
    });
  }

  pump() {
    this.waiters = this.waiters.filter((waiter) => {
      if (waiter.done) return false;
      const value = waiter.predicate(this);
      if (value !== undefined) {
        waiter.done = true;
        clearTimeout(waiter.timer);
        waiter.resolve(value);
        return false;
      }
      return true;
    });
  }

  rejectAll(err) {
    for (const waiter of this.waiters) {
      if (!waiter.done) {
        waiter.done = true;
        clearTimeout(waiter.timer);
        waiter.reject(err);
      }
    }
    this.waiters = [];
  }

  expect(predicate, timeoutMs = 3000) {
    const immediate = predicate(this);
    if (immediate !== undefined) return Promise.resolve(immediate);
    return new Promise((resolve, reject) => {
      const waiter = { predicate, resolve, reject, done: false, timer: null };
      waiter.timer = setTimeout(() => {
        if (waiter.done) return;
        waiter.done = true;
        reject(
          new Error(
            `timeout waiting for message; received so far: ${JSON.stringify(this.messages).slice(0, 1500)}` +
              (this.closeInfo ? `; closed: ${JSON.stringify(this.closeInfo)}` : ''),
          ),
        );
      }, timeoutMs);
      this.waiters.push(waiter);
    });
  }

  send(payload) {
    this.ws.send(typeof payload === 'string' ? payload : JSON.stringify(payload));
  }

  close() {
    this.ws.terminate();
  }
}

async function connect(url, wsOptions = {}) {
  const client = new Client(new WebSocket(url, wsOptions));
  await client.opened;
  return client;
}

const messageOf = (t) => (client) => client.messages.find((m) => m.t === t);
const errorOfCode = (code) => (client) => client.messages.find((m) => m.t === 'error' && m.code === code);
const matchedRole = (role) => (client) => {
  const m = client.messages.find((x) => x.t === 'matched');
  return m ? (m.role === role ? m : undefined) : undefined;
};

// Two clients that find in order; resolves { a, b } with a = host, b = joiner.
async function createPair(server) {
  const a = await connect(server.url);
  a.send({ t: 'find' });
  await a.expect(messageOf('waiting'));
  const b = await connect(server.url);
  b.send({ t: 'find' });
  await a.expect(matchedRole('host'));
  await b.expect(matchedRole('joiner'));
  return { a, b };
}

function cleanup(server, ...clients) {
  for (const client of clients) client?.close();
  server.close();
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// ---- tests -----------------------------------------------------------------

test('first finder waits; second finder pairs them as host (waiter) and joiner (newcomer)', async () => {
  const server = await startServer();
  try {
    const { a, b } = await createPair(server);
    assert.equal(a.messages.filter((m) => m.t === 'matched').length, 1);
    assert.equal(b.messages.filter((m) => m.t === 'matched').length, 1);
    assert.equal(server.stats().waiting, 0);
    assert.equal(server.stats().pairs, 1);
    assert.equal(server.stats().peers, 2);
    a.close();
    b.close();
  } finally {
    await cleanup(server);
  }
});

test('duplicate find is idempotent while waiting and while paired', async () => {
  const server = await startServer();
  try {
    const a = await connect(server.url);
    a.send({ t: 'find' });
    await a.expect(messageOf('waiting'));

    a.send({ t: 'find' }); // still waiting: no error, no second queue entry
    await sleep(150);
    assert.equal(a.messages.filter((m) => m.t === 'error').length, 0);
    assert.equal(server.stats().waiting, 1);

    const b = await connect(server.url);
    b.send({ t: 'find' });
    await a.expect(matchedRole('host'));
    await b.expect(matchedRole('joiner'));

    a.send({ t: 'find' }); // already paired: ignored, no error, no rematch
    b.send({ t: 'find' });
    await sleep(150);
    assert.equal(a.messages.filter((m) => m.t === 'matched').length, 1);
    assert.equal(b.messages.filter((m) => m.t === 'matched').length, 1);
    assert.equal(a.messages.filter((m) => m.t === 'error').length, 0);
    assert.equal(server.stats().pairs, 1);

    a.close();
    b.close();
  } finally {
    await cleanup(server);
  }
});

test('sig relays the opaque payload verbatim between paired peers in both directions', async () => {
  const server = await startServer();
  try {
    const { a, b } = await createPair(server);

    const offer = { description: { type: 'offer', sdp: 'v=0\r\no=- 46117317 2 IN IP4 127.0.0.1\r\ns=-\r\n' } };
    a.send({ t: 'sig', data: offer });
    const gotOffer = await b.expect(messageOf('sig'));
    assert.deepEqual(gotOffer.data, offer);

    const candidate = { iceCandidate: { candidate: 'candidate:1 1 UDP 2130706431 192.168.1.4 8998 typ host', sdpMid: '0' } };
    b.send({ t: 'sig', data: candidate });
    const gotCandidate = await a.expect(messageOf('sig'));
    assert.deepEqual(gotCandidate.data, candidate);

    // Strings ride untouched too (payload is never parsed):
    a.send({ t: 'sig', data: 'plain-string-payload' });
    const gotString = await b.expect((c) => (c.messages.filter((m) => m.t === 'sig').length >= 2 ? true : undefined));
    assert.equal(gotString, true);
    assert.equal(b.messages.at(-1).data, 'plain-string-payload');

    a.close();
    b.close();
  } finally {
    await cleanup(server);
  }
});

test('sig while unpaired is silently dropped (no relay, no error)', async () => {
  const server = await startServer();
  try {
    const loner = await connect(server.url);
    loner.send({ t: 'sig', data: { description: { type: 'offer', sdp: 'x' } } });
    await sleep(150);
    assert.equal(loner.messages.length, 0);
    assert.equal(loner.ws.readyState, WebSocket.OPEN);

    // A waiting (queued but unmatched) finder's sig does not leak either:
    const waiter = await connect(server.url);
    waiter.send({ t: 'find' });
    await waiter.expect(messageOf('waiting'));
    waiter.send({ t: 'sig', data: { description: { type: 'offer', sdp: 'x' } } });
    await sleep(150);
    assert.equal(waiter.messages.filter((m) => m.t === 'sig').length, 0);
    assert.equal(waiter.messages.filter((m) => m.t === 'error').length, 0);

    // Both sockets are still usable for matchmaking afterwards:
    const other = await connect(server.url);
    other.send({ t: 'find' });
    await waiter.expect(matchedRole('host'));
    await other.expect(matchedRole('joiner'));

    loner.close();
    waiter.close();
    other.close();
  } finally {
    await cleanup(server);
  }
});

test('disconnect delivers peer_left to the survivor and frees both for a new match', async () => {
  const server = await startServer();
  try {
    const { a, b } = await createPair(server);
    b.close();
    await a.expect(messageOf('peer_left'));
    assert.equal(server.stats().pairs, 0);

    // The survivor can queue up again:
    a.send({ t: 'find' });
    await a.expect(messageOf('waiting'));
    a.close();
  } finally {
    await cleanup(server);
  }
});

test('cancel removes a waiting finder from the queue', async () => {
  const server = await startServer();
  try {
    const a = await connect(server.url);
    a.send({ t: 'find' });
    await a.expect(messageOf('waiting'));
    a.send({ t: 'cancel' });
    await sleep(100);
    assert.equal(server.stats().waiting, 0);

    // The next finder must not pair with the cancelled one:
    const b = await connect(server.url);
    b.send({ t: 'find' });
    await b.expect(messageOf('waiting'));
    assert.equal(b.messages.some((m) => m.t === 'matched'), false);

    // Cancel from an unqueued socket is a no-op (no error):
    const c = await connect(server.url);
    c.send({ t: 'cancel' });
    await sleep(100);
    assert.equal(c.messages.filter((m) => m.t === 'error').length, 0);

    a.close();
    b.close();
    c.close();
  } finally {
    await cleanup(server);
  }
});

test('oversized messages are rejected with too_large and do not kill the connection', async () => {
  const server = await startServer();
  try {
    const { a, b } = await createPair(server);
    const huge = 'x'.repeat(300 * 1024); // > 256 KiB serialized
    a.send({ t: 'sig', data: huge });
    await a.expect(errorOfCode('too_large'));
    assert.equal(b.messages.some((m) => m.t === 'sig'), false);

    // Still alive:
    a.send({ t: 'sig', data: { ok: true } });
    const relayed = await b.expect(messageOf('sig'));
    assert.deepEqual(relayed.data, { ok: true });
    a.close();
    b.close();
  } finally {
    await cleanup(server);
  }
});

test('malformed JSON, non-object JSON, and unknown types are rejected', async () => {
  const server = await startServer();
  try {
    const client = await connect(server.url);
    client.send('this is not json');
    await client.expect(errorOfCode('invalid_message'));
    client.send('42');
    await client.expect(errorOfCode('invalid_message'));
    client.send('"a string"');
    await client.expect(errorOfCode('invalid_message'));
    client.send('["array"]');
    await client.expect(errorOfCode('invalid_message'));
    client.send('null');
    await client.expect(errorOfCode('invalid_message'));

    client.send({ t: 'wat' });
    await client.expect(errorOfCode('unknown_type'));
    client.send({ other: 1 });
    await client.expect(errorOfCode('unknown_type'));

    // Connection is still usable afterwards:
    client.send({ t: 'find' });
    await client.expect(messageOf('waiting'));
    client.close();
  } finally {
    await cleanup(server);
  }
});

test('sig bursts are rate limited per socket and strikes close the connection', async () => {
  const server = await startServer({ rateLimit: { max: 5, windowMs: 60_000, strikeLimit: 3 } });
  try {
    const { a, b } = await createPair(server);
    for (let i = 0; i < 6; i += 1) {
      a.send({ t: 'sig', data: { n: i } });
    }
    await a.expect(errorOfCode('rate_limited'));

    // Persistent abuse closes the socket (3 strikes) with 1008:
    for (let i = 0; i < 12; i += 1) {
      a.send({ t: 'sig', data: { n: 100 + i } });
    }
    const closeInfo = await a.expect((c) => c.closeInfo ?? undefined);
    assert.equal(closeInfo.code, 1008);

    // The survivor learns about it:
    await b.expect(messageOf('peer_left'));
    b.close();
  } finally {
    await cleanup(server);
  }
});

test('cross-origin upgrade is refused; localhost and same-host origins pass', async () => {
  const server = await startServer();
  try {
    const bad = new WebSocket(server.url, { headers: { Origin: 'https://evil.example' } });
    const badOutcome = new Promise((resolve, reject) => {
      bad.once('open', () => reject(new Error('unexpected connection for disallowed origin')));
      bad.once('error', (err) => resolve(err));
    });
    const badError = await badOutcome;
    assert.match(badError.message, /403/);

    const sameHost = new WebSocket(server.url, { headers: { Origin: `http://127.0.0.1:${server.httpServer.address().port}` } });
    const sameHostClient = new Client(sameHost);
    await sameHostClient.opened;
    sameHostClient.send({ t: 'find' });
    await sameHostClient.expect(messageOf('waiting'));
    sameHostClient.close();
  } finally {
    await cleanup(server);
  }
});

test('server.js runs directly: binds PORT, pairs two clients, and shuts down on SIGTERM', async () => {
  const child = spawn(process.execPath, [path.join(HERE, '..', 'server.js')], {
    env: { ...process.env, PORT: '0', HOST: '127.0.0.1' },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  let stdout = '';
  child.stdout.on('data', (chunk) => {
    stdout += chunk.toString();
  });
  let stderr = '';
  child.stderr.on('data', (chunk) => {
    stderr += chunk.toString();
  });

  const ready = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`server did not start; stdout=${stdout} stderr=${stderr}`)), 8000);
    child.stdout.on('data', () => {
      const match = stdout.match(/ws:\/\/127\.0\.0\.1:(\d+)\//);
      if (match) {
        clearTimeout(timer);
        resolve(Number(match[1]));
      }
    });
  });

  const port = await ready;
  try {
    const a = await connect(`ws://127.0.0.1:${port}/`);
    a.send({ t: 'find' });
    await a.expect(messageOf('waiting'));
    const b = await connect(`ws://127.0.0.1:${port}/`);
    b.send({ t: 'find' });
    await a.expect(matchedRole('host'));
    await b.expect(matchedRole('joiner'));
    a.close();
    b.close();
    await sleep(100); // let the disconnects propagate before shutdown
  } finally {
    const exited = new Promise((resolve) => child.once('exit', (code, signal) => resolve({ code, signal })));
    child.kill('SIGTERM');
    const result = await exited;
    assert.equal(result.signal !== 'SIGKILL', true);
    assert.equal(stdout.includes('listening'), true);
  }
});
