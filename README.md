# P2PKit

Define your application protocol and call typed methods on peers. P2PKit provides a TypeScript mesh API with WebRTC connections, runtime-validated RPC, gossip discovery, and optional identity verification, signing, and encryption. Transport interfaces support custom links.

> **Status: experimental implementation.** The core API and tests exist; public APIs and the wire protocol may change before a stable release.

| Status | Capability |
|---|---|
| Implemented | WebRTC peer connections and transport injection; typed RPC with runtime validation; identity handshake; optional connection encryption covering RPC and mesh traffic |
| Implemented | TTL-limited broadcast flooding, origin signatures, origin/relay metadata, topic-scoped local delivery, sequence replay windows, gossip peer exchange |
| Experimental | Standalone HTTP, µTP, and DHT transports; DHT address discovery; runtime/backend adapters and NAT helpers |
| Planned | Automatic endpoint exchange, transport negotiation/fallback in `P2PKit`, subscriber-directed network routing, topic schemas |

Tests cover memory topologies and local transport integration. They do not establish reliability across arbitrary public networks. Wire version **2** replaces the old message-only encryption format with a complete encrypted frame envelope; upgrade all communicating peers together.

## Install

```sh
npm install p2pkit
```

## Module map

| Import path | Exports |
|---|---|
| `p2pkit` | `P2PKit`, `Peer` |
| `p2pkit/rpc` | `defineAPI`, `defineProtocol`, `RPCError` |
| `p2pkit/discovery` | `Discovery`, `GossipDiscovery`, `DHTDiscovery` |
| `p2pkit/auth` | `Signer`, `ECDSASigner`, `NoopSigner`, `KeyManager` |
| `p2pkit/transports` | `Transport`, `RTCTransport`, `UTPTransport`, `HTTPTransport`, `DHTTransport` |
| `p2pkit/signalling` | `SignallingChannel`, `WebSocketSignalling` |
| `p2pkit/nat` | `mapPort` |
| `p2pkit/backends` | `getRTC` |
| `p2pkit/framing` | `Chunker` |
| `p2pkit/utils` | `extractIP`, `DEFAULT_ICE_SERVERS`, `promiseWithTimeout`, `ErrorTimeout` |

Everything is exposed through the `P2PKit` class; the sub-modules are there when you want a piece on its own.

---

## 1. Quick start

`P2PKit` connects you to peers over WebRTC and hands you each one as it joins. Supply `createTransport` to use an already configured custom transport.

```ts
import { P2PKit } from "p2pkit"
import { WebSocketSignalling } from "p2pkit/signalling"

const kit = new P2PKit<{ type: "chat"; body: string }>({
  self: "0xabc...", // this peer's id
  signalling: new WebSocketSignalling("wss://rooms.example/room-1")
})

kit.on("peer", peer => {
  peer.on("connect", () => {
    console.log(`Connected to ${kit.peers.size} peers`)
    peer.send({ type: "chat", body: "hi" })
  })
  peer.on("message", msg => console.log(peer.remote, msg))
  peer.on("disconnect", () => {
    console.warn(`Disconnected from ${peer.transportName} peer`)
    console.log(`Connected to ${kit.peers.size} peers`)
  })
})

// ...or handle every peer's messages in one place
kit.on("message", (msg, peer, metadata) => {
  console.log(metadata.origin, msg.body, 'via', metadata.via)
  console.log('Origin verified:', metadata.originVerified, 'latency:', peer.latency)
})

await kit.start() // join the mesh; peers start connecting

kit.broadcast({ type: "chat", body: "hey all" }) // send to entire mesh network
```

`kit.broadcast()` floods a message across the mesh - each peer relays it onward, so it reaches peers you aren't directly connected to. This matters when full connectivity is impossible: large networks, or two peers both behind firewalls that can each reach a shared relay but not each other. To stop a flood from storming, every message carries a TTL and id, and peers drop anything past the hop limit or already seen within a dedup window; tune both with `broadcast: { ttl, dedupWindow }`. Cap direct connections with `maxPeers`; relaying reaches other nodes only while a connected path exists. The cap does not guarantee mesh connectivity.

---

## 2. Request/response (`p2pkit/rpc`)

Declare an **API schema** once, in a module both peers import. From it you get a typed client, a typed router, and runtime validation at every boundary - the two ends can't drift.
```ts
// api.ts
import { defineAPI } from "p2pkit/rpc"
import { z } from "zod"

export const api = defineAPI({
  getBlock: {
    request:  z.object({ height: z.number().int().nonnegative() }),
    response: z.object({ hash: z.string(), txs: z.array(z.string()) })
  },
  putBlock: {
    request:  z.object({ hash: z.string(), txs: z.array(z.string()) }),
    response: z.object({ accepted: z.boolean() })
  }
})
```

### 2.1 Serve

Use `api.router` to define handlers for each method.

```ts
import { api } from "./api"

const router = api.router({
  getBlock: async ({ height }) => {
    const block = await chain.at(height)
    return { hash: block.hash, txs: block.txs }
  },
  putBlock: async ({ hash, txs }, ctx) => {
    return { accepted: await chain.accept(hash, txs, ctx.from) } // ctx.from is verified when a real signer is configured
  }
})

const kit = new P2PKit({ signalling, signer, api, router })
```

Serve a subset with `api.router.partial({...})`; unimplemented methods answer callers with a `no_handler` error.

### 2.2 Call

To call a peer's method, use `peer.client(api)`.

```ts
import { RPCError } from "p2pkit/rpc"

const client = peer.client(api)

const block = await client.getBlock({ height: 42 })
if (block instanceof RPCError) return console.error(block.code, block.method)
```

RPC calls resolve to `RPCError("disconnected")` when the connection closes, `RPCError("send_failed")` if sending fails, or `RPCError("timeout")` if no response arrives within the configured timeout. Calls waiting for authentication also time out and are not sent later.

### 2.3 One-way messages

For fire-and-forget messages (no reply), type them with `defineProtocol`. Each key becomes a message `type`, giving you the same `{ type, ... }` shape as the quick start:

```ts
import { defineProtocol } from "p2pkit/rpc"

const protocol = defineProtocol({
  chat: z.object({ body: z.string() }),
  typing: z.object({ active: z.boolean() })
})

const kit = new P2PKit({ self, signalling, protocol }) // inbound kit messages are runtime-validated
```

Send with `peer.send` or `kit.broadcast` and receive through `on("message")`:

```ts
peer.send({ type: "chat", body: "hi" })
kit.broadcast({ type: "typing", active: true })

kit.on("message", (msg, peer) => {
  console.log(peer.remote, msg) // peer.remote identifies the immediate delivering peer
})
```

---

## 3. Pub/sub (`p2pkit` topics)

A **topic** scopes local application delivery to subscribers. Publishes currently flood all connected links, including through non-subscribers, subject to TTL and deduplication. Subscription gossip tracks known members; it does not yet select forwarding paths. Publishers do not receive their own publishes as local echoes.

```ts
const room = kit.topic<{ type: "chat"; body: string }>("chat/general")

room.on("message", (msg, from) => console.log(from, msg.body))
room.publish({ type: "chat", body: "hi room" })

room.peers   // subscribers we know about
room.leave() // unsubscribe
```

Topic generics provide TypeScript types only. Topic schema validation is planned; validate application payloads in your listener when needed.

### Replay protection

Each publish carries a positive integer sequence and nonce. Receivers remember accepted sequence numbers within a 1024-sequence window for each topic/sender and reject duplicates and stale sequences for the lifetime of that node. Replay state is in memory, so it resets when the node is recreated; this is not a durable freshness guarantee. Unsigned origins can be spoofed.

Use `signed: true` with a verifying signer to sign outgoing publishes and **require verified signatures on incoming publishes** before delivery or relay:

```ts
const room = kit.topic("prices", { signed: true })
room.on("message", (msg, from, metadata) => {
  console.log(from, metadata.via, metadata.originVerified, msg)
})
```

Frame validation and signature checks run before replay state changes. A topic's signing policy is fixed when created; requesting the same topic with conflicting explicit options throws. Leave it first to recreate it with a different policy.

---

## 4. Discovery (`p2pkit/discovery`)

By default peers find each other through signalling rooms. To grow or heal the mesh without central servers, add a `Discovery` channel.


**DHT address discovery** (Node, experimental): announces a shared infohash and finds network addresses. Install `bittorrent-dht` and provide `onPeer` to bridge addresses into your own dialing/identity flow; it does not automatically create `Peer` connections.

**Gossip peer-exchange**: once connected to one peer, peers trade their known-peer lists, so a single bootstrap connection fans out to the whole mesh.

```ts
import { GossipDiscovery, DHTDiscovery } from "p2pkit/discovery"

const dhtDiscovery = new DHTDiscovery({
  bootstrapHash: "ffffffff11615786c201f5330b7561f8d8b09479", // any shared 40-hex id
  port: 20000,
  onPeer: address => console.log("Discovered endpoint to dial:", address)
})

const kit = new P2PKit({ self, signalling, discovery: [dhtDiscovery, new GossipDiscovery()] })
```

The DHT transport (§6) and DHT discovery run on **one shared DHT node** - set `bootstrapHash`/`port` in a single place and the other reuses it; you don't configure (or match) them twice.

---

## 5. Identity & signing (`p2pkit/auth`)

Set a verifying `signer` and peers verify each other's identity before dispatching RPC, messages, mesh frames, or application events. Only handshake frames are processed until authentication completes. `peer.on("connect")` fires after the handshake; `peer.authenticated` reports whether a non-noop signer verified the remote. Direct messages then ride that authenticated connection; they aren't signed individually. The bundled `ECDSASigner` uses secp256k1 (Ethereum-style addresses).
```ts
import { KeyManager, ECDSASigner } from "p2pkit/auth"

const keys = new KeyManager("node-1") // loads or creates a persisted keypair
const kit = new P2PKit<Msg>({
  signalling,
  signer: new ECDSASigner(keys) // identity; self is derived from the signer
})
```

`KeyManager` persists the keypair as an fs file on Node/Bun/Deno or via IndexedDB in the browser. With a signer set, `self` is derived from it and can be omitted - pass `self` explicitly only when you have no signer.

Without a signer (or with `NoopSigner`), the handshake accepts claimed identities without cryptographic proof. Do not use those identities for authorization. `NoopSigner` cannot be used for signed topics or `signedBroadcasts`.

### Signed broadcasts

Broadcasts are relayed through other peers, so a receiver can't rely on the connection to know who really sent one. Instead, have the origin sign each broadcast and every hop verify it:

```ts
const kit = new P2PKit<Msg>({ signalling, signer, signedBroadcasts: true })
```

Signed envelopes carry a timestamp and nonce. Verifying nodes reject invalid signatures, timestamps more than 60 seconds from their clock, and remembered nonces. Nodes without a signer may relay signed traffic but report its origin as unverified. Replay/dedup state is in memory.

The third argument to `kit.on("message", (msg, peer, metadata) => ...)` separates `metadata.origin` (the original sender), `metadata.via` (the immediate relay), and `metadata.originVerified`. The existing `peer` argument always represents the immediate connection. In A → B → C, C sees origin A and via B; only a verified origin signature makes `originVerified` true for a broadcast. Direct messages use the handshake verification result. An unsigned broadcast remains unverified even when its relay authenticated.

### Encryption

Set `encrypted: true` on both ends (requires `ECDSASigner`) to encrypt every post-handshake frame with XChaCha20-Poly1305, including direct messages, complete RPC requests/responses and errors, broadcasts, topic traffic, gossip, and latency probes. Receivers require the authenticated encrypted envelope and drop plaintext, malformed, or tampered application frames. Queued sends use the same encryption path once the handshake completes.

The connection key is derived by ECDH from identity keypairs. Hello/ack identity handshakes remain visible. Encryption protects each direct connection independently of the underlying transport. Mesh relays decrypt and re-encrypt forwarded traffic, so they can read broadcast/topic contents; use application-level group encryption if relays must not see them. This identity-key scheme does not provide forward secrecy or a separate replay counter for direct frames.

---

## 6. Transports (`p2pkit/transports`)

`Peer` and `P2PKit` use WebRTC by default. Other transport classes are standalone building blocks. `chooseTransport` can select a shared capability, but endpoint exchange, dialing and fallback are not wired into the high-level API; there is no `transports` option.

| Transport | Runtime | |
|---|---|---|
| `RTCTransport` | universal | WebRTC data channel (default) |
| `HTTPTransport` | needs 1 Node participant | request/response over HTTP(S) |
| `UTPTransport` | Node | µTP (UDP), NAT-friendly |
| `DHTTransport` | Node | RPC over the BitTorrent DHT (from `bonana`) |

```ts
const kit = new P2PKit<Msg>({
  self,
  signalling,
  createTransport: ({ remote }) => configuredTransports.get(remote)
  // Return a Transport<Frame>, or undefined to fall back to WebRTC.
})
```

For a single connection, pass `transport` to `new Peer({ remote, self, transport })`. Configure remote endpoints yourself. µTP/DHT require their optional Node networking dependencies.

---

# Advanced

Extension points and internals. Most apps never touch these.

---

## 7. Manual connections

`P2PKit` builds `Peer`s for you from discovery. To open a single connection directly - no mesh, no discovery - construct one:

```ts
import { Peer } from "p2pkit"
import { WebSocketSignalling } from "p2pkit/signalling"

const peer = new Peer<Msg>({
  self: "0xabc...",
  remote: "0xdef...",
  signalling: new WebSocketSignalling("wss://rooms.example/room-1")
})
```

## 8. Custom implementations

Every default is swappable, implement the interface and pass it in.

**`SignallingChannel`**: route the WebRTC handshake over your own server, a pub/sub topic, or manual copy-paste instead of `WebSocketSignalling`:

```ts
type SignallingMessage =
  | { announce: true; from: PeerId }
  | { description: RTCSessionDescription; from: PeerId; to: PeerId } // offer & answer
  | { iceCandidate: RTCIceCandidate; from: PeerId; to: PeerId }

interface SignallingChannel {
  send(message: SignallingMessage): void
  onMessage(handler: (message: SignallingMessage) => void): void
  readonly ready: Promise<void>
}
```

**`Transport`**: add a link type beyond the built-ins:

```ts
type TransportEvents<Msg> = {
  connect: () => void
  message: (msg: Msg) => void
  disconnect: () => void
  error: (err: Error) => void
}

interface Transport<Msg> {
  readonly remote: PeerId
  readonly bufferedAmount: number    // bytes queued but not yet flushed
  send(message: Msg): Promise<void>  // resolves once flushed - await it, or watch bufferedAmount, to throttle slow peers
  on<E extends keyof TransportEvents<Msg>>(event: E, handler: TransportEvents<Msg>[E]): void
  disconnect(): void
}
```

**`Signer`**: bring your own signing scheme in place of `ECDSASigner`:

```ts
interface Signer {
  readonly id: PeerId
  sign(payload: string): Promise<string>
  verify(signature: string, payload: string, from: PeerId): Promise<boolean>
}
```

## 9. NAT traversal (`p2pkit/nat`)

**WebRTC** does its own traversal via ICE, but STUN alone (all that `DEFAULT_ICE_SERVERS` provides) only punches through cone NATs. Symmetric NAT and locked-down corporate networks *will* fail to connect without a **TURN relay** - and TURN is bring-your-own: pass your relay servers as `iceServers`. Omitting TURN is the single most common reason real WebRTC deployments are flaky, so budget for it.

**Port-listening transports** (µTP, HTTP, DHT) instead need an open port. `mapPort` requests a UPnP / NAT-PMP mapping so peers behind a router are reachable without manual forwarding (Node-only):

```ts
import { mapPort } from "p2pkit/nat"

const mapping = await mapPort({ port: 20000, protocol: "udp", ttl: 3600 })
await mapping.close() // release on shutdown
```

## 10. Backends (`p2pkit/backends`)

P2PKit uses the runtime's `RTCPeerConnection` (browser native, Node `@roamhq/wrtc`, Deno `werift`). Only pass `backend` to override detection.

```ts
import * as werift from "werift" // use "npm:werift" in Deno
const kit = new P2PKit<Msg>({ self, signalling, backend: werift })
```

## 11. Message framing (`p2pkit/framing`)

Data channels cap message size (~256 KB). `Chunker` splits and reassembles large payloads; applied automatically, exposed for direct use.

```ts
import { Chunker } from "p2pkit/framing"

const chunker = new Chunker({ maxPacketSize: 8 * 1024 })
for (const packet of chunker.split(id, largeString)) channel.send(packet)
const full = chunker.ingest(JSON.parse(incoming)) // string once complete, else undefined
```

## 12. Utilities (`p2pkit/utils`)

```ts
import { extractIP, DEFAULT_ICE_SERVERS, promiseWithTimeout } from "p2pkit/utils"

extractIP(sdp: string): string | undefined // first non-0.0.0.0 c=IN IP4/IP6 line
DEFAULT_ICE_SERVERS: RTCIceServer[] // Google + public STUN fallbacks
promiseWithTimeout<T>(p: Promise<T>, ms: number): Promise<T | ErrorTimeout>
```

## 13. Bootstrapping server

`bootstrapping-server/` contains a minimal find/pair/relay WebSocket server for two-peer games: clients send `{"t":"find"}`, the server FIFO-pairs them (first waiter = host), then relays opaque `{"t":"sig"}` payloads verbatim between the pair until disconnect (`peer_left`). It is written in TypeScript and shares its lobby wire types with the client library (`src/signalling/lobby.ts`, exported as `p2pkit/signalling`). Build it with `npm install && npm run build` in `bootstrapping-server/`, then run `node bootstrapping-server/dist/server.js`. See its README.

## 14. Emscripten SDK (optional, C++)

`emscripten/` contains an optional C++/Emscripten SDK for browser games: packet
headers (`include/p2pkit-wasm/`), a `WebRtcTransport` C event-pump wrapper, and
a game-generic browser bridge (`js/p2pkit_webrtc_glue.cjs`) that adapts the
core `RTCTransport` (raw binary mode) to the lobby matchmaking dialect. The
core (`src/`, `dist/`) stays MIT; everything under `emscripten/` is GPL-2.0
(see `emscripten/README.md` and `emscripten/LICENSE`). Because the published
package ships both source sets, its `license` metadata reads
`SEE LICENSE IN README.md`: each directory's files stay under their own
license and per-file notices — nothing is relicensed. It is packaged via the
`p2pkit/emscripten/*` export paths.

---
