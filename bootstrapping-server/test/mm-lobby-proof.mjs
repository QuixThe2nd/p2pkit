#!/usr/bin/env node
/*
 * End-to-end proof of the DuneCity matchmaking lobby protocol.
 *
 * Boots the real server in-process on port 8799, drives it with two bare
 * `ws` clients (no game glue, no mocks), and walks the whole wire contract:
 *
 *   find -> waiting -> find -> matched(host)/matched(joiner)
 *   sig  -> verbatim relay in both directions between the paired peers
 *   close -> peer_left delivered to the survivor
 *
 * Prints PASS and exits 0 when every step holds; any deviation exits 1.
 */
import { createSignalingServer } from '../dist/server.js';
import WebSocket from 'ws';

const PORT = 8799;
const URL = `ws://127.0.0.1:${PORT}/`;

const fail = (message) => {
  console.error(`FAIL: ${message}`);
  process.exitCode = 1;
  throw new Error(message);
};

const assert = (condition, message) => {
  if (!condition) fail(message);
};

// Minimal collecting client: waits for one message matching a predicate.
class BareClient {
  constructor(name) {
    this.name = name;
    this.messages = [];
    this.closed = false;
    this.ws = new WebSocket(URL);
    this.opened = new Promise((resolve, reject) => {
      this.ws.once('open', resolve);
      this.ws.once('error', reject);
    });
    this.ws.on('message', (raw) => {
      this.messages.push(JSON.parse(raw.toString('utf8')));
    });
    this.ws.on('close', () => {
      this.closed = true;
    });
  }

  send(payload) {
    this.ws.send(JSON.stringify(payload));
  }

  async expect(predicate, label, timeoutMs = 5000) {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const found = this.messages.find(predicate);
      if (found !== undefined) return found;
      if (Date.now() > deadline) fail(`${this.name}: timed out waiting for ${label}; got ${JSON.stringify(this.messages)}`);
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  }

  close() {
    this.ws.terminate();
  }
}

async function main() {
  const server = createSignalingServer({ host: '127.0.0.1', port: PORT });
  await new Promise((resolve, reject) => {
    server.httpServer.once('error', reject);
    server.httpServer.listen(PORT, '127.0.0.1', resolve);
  });

  let a = null;
  let b = null;
  try {
    // 1. First finder queues and is told it is waiting.
    a = new BareClient('A');
    await a.opened;
    a.send({ t: 'find' });
    await a.expect((m) => m.t === 'waiting', 'waiting');

    // 2. Second finder pairs with the waiter: FIFO, server-assigned roles.
    b = new BareClient('B');
    await b.opened;
    b.send({ t: 'find' });
    const aMatched = await a.expect((m) => m.t === 'matched', 'matched');
    const bMatched = await b.expect((m) => m.t === 'matched', 'matched');
    assert(aMatched.role === 'host', `A (waiter) must be host, got ${JSON.stringify(aMatched)}`);
    assert(bMatched.role === 'joiner', `B (newcomer) must be joiner, got ${JSON.stringify(bMatched)}`);
    assert(server.stats().pairs === 1 && server.stats().waiting === 0, 'exactly one pair, queue empty');

    // 3. sig relays the opaque payload verbatim, host -> joiner.
    const offer = { description: { type: 'offer', sdp: 'v=0\r\no=- proof 1 IN IP4 127.0.0.1\r\ns=-\r\n' } };
    a.send({ t: 'sig', data: offer });
    const bGot = await b.expect((m) => m.t === 'sig', 'sig relay host->joiner');
    assert(JSON.stringify(bGot.data) === JSON.stringify(offer), 'offer payload relayed verbatim');

    // 4. sig relays verbatim, joiner -> host.
    const candidate = { iceCandidate: { candidate: 'candidate:1 1 UDP 2130706431 192.168.1.4 8998 typ host', sdpMid: '0' } };
    b.send({ t: 'sig', data: candidate });
    const aGot = await a.expect((m) => m.t === 'sig', 'sig relay joiner->host');
    assert(JSON.stringify(aGot.data) === JSON.stringify(candidate), 'candidate payload relayed verbatim');

    // 5. Socket close delivers peer_left to the survivor.
    b.close();
    await a.expect((m) => m.t === 'peer_left', 'peer_left');
    assert(server.stats().pairs === 0, 'pair torn down');

    console.log('PASS');
  } finally {
    a?.close();
    b?.close();
    await server.close();
  }
}

main().catch((err) => {
  console.error(err.message);
  process.exit(process.exitCode ?? 1);
});
