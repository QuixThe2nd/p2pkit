# P2PKit Bootstrapping Server

A minimal Node.js WebSocket service that pairs game peers into WebRTC
peer-to-peer sessions. The first client that looks for a match waits; the next
one is paired with it (FIFO). The server assigns the roles — the waiting client
becomes the **host** (offerer), the newcomer the **joiner** (answerer) — and
then relays SDP/ICE between exactly the two paired peers, and nothing else.

It deliberately does **not** provide:

- persistence of any kind (the queue and pairs live in memory only)
- accounts, authentication, or authorization of peers
- rooms, room codes, or any HTTP API
- game data relay (once WebRTC connects, traffic is peer-to-peer)
- TURN/STUN services (configure those separately, e.g. coturn)

## Running

The server is written in TypeScript (`src/`); `npm run build` compiles it to
`dist/server.js` (esbuild bundle) plus `dist/*.d.ts` declarations. The lobby
wire types live in the p2pkit client library (`../src/signalling/lobby.ts`,
exported as `p2pkit/signalling`) and are synced in at build time, so the wire
format has a single definition shared by server and browser clients.

```bash
npm install        # once; installs the runtime dependency `ws` + toolchain
npm run build      # compiles TypeScript to dist/ (required before start/test)
npm start          # runs `node dist/server.js`
```

Defaults: `ws://127.0.0.1:8788/` (WebSocket endpoint, path is not restricted).

### Environment variables

| Variable                       | Default     | Meaning                                             |
| ------------------------------ | ----------- | --------------------------------------------------- |
| `PORT`                         | `8788`      | TCP port (`0` picks an ephemeral port)               |
| `HOST`                         | `127.0.0.1` | Bind address (use `0.0.0.0` to expose externally)    |

### Using as a module

```js
import { createSignalingServer } from './dist/server.js'; // after npm run build

const ctx = createSignalingServer({
  // rateLimit: { max: 120, windowMs: 5000, strikeLimit: 3 },
  // maxQueue: 200, maxMessageBytes: 256 * 1024, pingIntervalMs: 30_000,
});
ctx.httpServer.listen(8788, '127.0.0.1');
// ctx: { httpServer, wss, queue, states, options, stats(), close() }
```

`stats()` returns `{ waiting, pairs, peers }` (waiting = clients currently
queued, pairs = matched pairs, peers = connected sockets). `close()` terminates
all client sockets, clears timers, and closes the HTTP server; it returns a
Promise that resolves when shutdown completes.

## Wire protocol

JSON **text frames** over a single WebSocket endpoint (default path `/`).
Every message is a small object discriminated by `"t"`. There is no protocol
version field and no peer identity: pairing is anonymous and the server relays
only between the two sockets it paired itself.

### Client -> server

| Message                        | Effect                                                                                                                          |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------- |
| `{"t":"find"}`                 | Enter the lobby. If someone is already waiting, the two of you are paired immediately (see `matched` below); otherwise you are queued and receive `{"t":"waiting"}`. Duplicate `find` (while waiting or paired) is ignored. |
| `{"t":"cancel"}`               | Leave the waiting queue (no effect once paired; a pairing is only left by closing the socket).                                    |
| `{"t":"sig","data":<opaque>}`  | Relay `data` verbatim to your paired peer. Dropped silently while unpaired. `data` is never parsed or inspected.                  |

### Server -> client

| Message                                  | When                                                        |
| ---------------------------------------- | ----------------------------------------------------------- |
| `{"t":"waiting"}`                        | You are queued; the next finder pairs with you              |
| `{"t":"matched","role":"host"\|"joiner"}` | You were paired. The waiter is `host` (creates the WebRTC offer), the newcomer is `joiner` (answers). Both messages are sent in the same tick. |
| `{"t":"sig","data":<opaque>}`            | Your paired peer sent you a `sig`; `data` is their payload unchanged |
| `{"t":"peer_left"}`                      | Your paired peer's socket closed; the pairing is dissolved   |
| `{"t":"error","code":"..."}`             | Any invalid input or policy violation                        |

`data` is opaque JSON (SDP descriptions, ICE candidate objects, strings, ...)
and is relayed unchanged. Maximum serialized message size is **256 KiB**.

Error codes:

| Code               | Meaning                                                                     |
| ------------------ | ---------------------------------------------------------------------------- |
| `invalid_message`  | Not UTF-8 JSON, or not a JSON object                                         |
| `unknown_type`     | `t` missing or not one of `find` / `cancel` / `sig`                          |
| `too_large`        | Message exceeds 256 KiB serialized                                           |
| `rate_limited`     | Per-socket `sig` rate limit exceeded (see below)                             |
| `lobby_full`       | The waiting queue is at `maxQueue` (defensive; the queue holds one waiter in practice) |

## Limits

| Limit        | Default       | Behavior                                                                                                        |
| ------------ | ------------- | ---------------------------------------------------------------------------------------------------------------- |
| Message size | 256 KiB       | Larger messages get `too_large`; a hard transport cap of 2 MiB (`ws` `maxPayload`) drops absurd frames            |
| Rate limit   | 120 `sig` / 5 s per socket (fixed window) | The first over-limit `sig` gets `rate_limited`. If the socket keeps exceeding the limit (3 strikes within the window), it is closed with close code `1008`. Counters and strikes reset when a new window starts. |
| Queue        | 200 waiters   | A finder beyond `maxQueue` gets `lobby_full`                                                                   |
| Keepalive    | 30 s ping     | Sockets that miss a pong interval are terminated                                                               |

All limits are configurable via `createSignalingServer` options
(`maxMessageBytes`, `rateLimit`, `maxQueue`, `pingIntervalMs`) — which is also
how the test suite exercises them quickly.

## Security notes

- **Origin policy**: WebSocket upgrades are accepted from non-browser clients
  (no `Origin` header), same-host pages, and `localhost`/`127.0.0.1`/`[::1]`;
  anything else is rejected with `403`. For production behind a proxy, put TLS
  (WSS) termination in front and keep the lobby private to the game's origin.
- There is **no authentication**: anyone connecting can occupy the next match
  slot. Signaling carries no secrets (SDP/ICE only), but do not expose the
  lobby to the open internet without an origin/proxy policy.
- No message contents are logged; the only log line is the startup banner.
- Rate limiting and size caps exist to bound abuse, not to replace a real
  firewall or DDoS protection.
- WebRTC media/data itself is peer-to-peer and never touches this server.

## Tests

```bash
npm run build                     # tests run against the built dist/server.js
npm test                          # node --test test/*.test.js
node test/mm-lobby-proof.mjs      # protocol proof on port 8799; prints PASS
npm run typecheck                 # tsc --noEmit over src/
```
