# P2PKit

A transport-agnostic P2P mesh library. Connect peers over WebRTC, µTP, HTTP, or the DHT; gossip discovery, typed request/response, and message signing built in. Universal (browser + Node + Bun + Deno).

> **Status: design draft.** Nothing is built yet. This document specifies the intended public API. Signatures may change before `0.1.0`.

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

`P2PKit` connects you to peers and hands you each one as it joins, negotiating the best transport automatically.

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
    console.warn(`Disconnected from ${peer.transport} peer`)
    console.log(`Connected to ${kit.peers.size} peers`)
  })
})

// ...or handle every peer's messages in one place
kit.on("message", (msg, peer) => {
  console.log('Latency:', peer.latency)
  if (msg === 'I hate you!') peer.disconnect()
})

await kit.start() // join the mesh; peers start connecting

kit.broadcast({ type: "chat", body: "hey all" }) // send to entire mesh network
```

`kit.broadcast()` floods a message across the mesh - each peer relays it onward, so it reaches peers you aren't directly connected to. This matters when full connectivity is impossible: large networks, or two peers both behind firewalls that can each reach a shared relay but not each other. To stop a flood from storming, every message carries a TTL and id, and peers drop anything past the hop limit or already seen within a dedup window; tune both with `broadcast: { ttl, dedupWindow }`. Cap direct connections with `maxPeers` - relaying keeps a capped node fully reachable.

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
    return { accepted: await chain.accept(hash, txs, ctx.from) } // ctx.from is the verified sender
  }
})

const kit = new P2PKit({ self, signalling, api, router })
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

### 2.3 One-way messages

For fire-and-forget messages (no reply), type them with `defineProtocol`. Each key becomes a message `type`, giving you the same `{ type, ... }` shape as the quick start:

```ts
import { defineProtocol } from "p2pkit/rpc"

const protocol = defineProtocol({
  chat: z.object({ body: z.string() }),
  typing: z.object({ active: z.boolean() })
})

const kit = new P2PKit({ self, signalling, protocol }) // message type inferred from protocol
```

Send with `peer.send` or `kit.broadcast` and receive through `on("message")`:

```ts
peer.send({ type: "chat", body: "hi" })
kit.broadcast({ type: "typing", active: true })

kit.on("message", (msg, peer) => {
  if (msg.type === "chat") console.log(peer.remote, msg.body)
})
```

---

## 3. Pub/sub (`p2pkit` topics)

Broadcast hits the whole mesh; a **topic** scopes it to the peers who've subscribed - a room. Join a topic, publish to it, and only its subscribers receive. Subscriptions are gossiped, so a publish is relayed only toward peers that want the topic (not flooded everywhere) and still reaches subscribers you aren't directly connected to.

```ts
const room = kit.topic<{ type: "chat"; body: string }>("chat/general")

room.on("message", (msg, from) => console.log(from, msg.body))
room.publish({ type: "chat", body: "hi room" })

room.peers   // subscribers we know about
room.leave() // unsubscribe
```

Topics take a `defineProtocol` schema too - pass it instead of a generic and the message type is inferred and validated, exactly like §2.3.

### Replay protection

Replay protection is on by default. Every published message carries a per-sender sequence number and nonce; subscribers track the highest sequence seen per sender within a sliding window and drop anything stale or already seen - so a relay, or a malicious peer, can't re-inject an old message into the topic. Add `signed: true` (requires a `signer`) to also verify the publisher's identity, pinning both *who* sent each message and that it's *fresh*:

```ts
const room = kit.topic("prices", { signed: true })
```

---

## 4. Discovery (`p2pkit/discovery`)

By default peers find each other through signalling rooms. To grow or heal the mesh without central servers, add a `Discovery` channel.


**DHT bootstrap** (Node): serverless discovery via the BitTorrent DHT: every node announces a shared infohash and finds anyone else announcing it. Use it to bootstrap, then let gossip take over.

**Gossip peer-exchange**: once connected to one peer, peers trade their known-peer lists, so a single bootstrap connection fans out to the whole mesh.

```ts
import { GossipDiscovery, DHTDiscovery } from "p2pkit/discovery"

const dhtDiscovery = new DHTDiscovery({
  bootstrapHash: "ffffffff11615786c201f5330b7561f8d8b09479", // any shared 40-hex id
  port: 20000
})

const kit = new P2PKit({ self, signalling, discovery: [dhtDiscovery, new GossipDiscovery()] })
```

The DHT transport (§6) and DHT discovery run on **one shared DHT node** - set `bootstrapHash`/`port` in a single place and the other reuses it; you don't configure (or match) them twice.

---

## 5. Identity & signing (`p2pkit/auth`)

Set a `signer` and peers verify each other's identity on connection. Each side proves it owns its address before any data flows, so `peer.on("connect")` only fires once `peer.remote` is a proven address. Direct messages then ride that authenticated connection; they aren't signed individually. The bundled `ECDSASigner` uses secp256k1 (Ethereum-style addresses).
```ts
import { KeyManager, ECDSASigner } from "p2pkit/auth"

const keys = new KeyManager("node-1") // loads or creates a persisted keypair
const kit = new P2PKit<Msg>({
  signalling,
  signer: new ECDSASigner(keys) // identity; self is derived from the signer
})
```

`KeyManager` persists the keypair as an fs file on Node/Bun/Deno or via IndexedDB in the browser. With a signer set, `self` is derived from it and can be omitted - pass `self` explicitly only when you have no signer.

### Signed broadcasts

Broadcasts are relayed through other peers, so a receiver can't rely on the connection to know who really sent one. Instead, have the origin sign each broadcast and every hop verify it:

```ts
const kit = new P2PKit<Msg>({ signalling, signer, signedBroadcasts: true })
```

Signing defeats spoofing but not replay, so the signed envelope also carries a timestamp and nonce; receivers reject stale or already-seen broadcasts, so a relay can't re-inject an old one. (Worth committing to now - the envelope shape is hard to change later.)

### Encryption

Set `encrypted: true` (requires a `signer`) to end-to-end encrypt message payloads. Peers derive a shared key from their identity keypairs (ECDH), so no relay - and no transport - ever sees plaintext, regardless of whether the underlying link (WebRTC's DTLS, plain HTTP) encrypts its own hop. Broadcasts fan out to the whole mesh, so they stay readable unless you encrypt to a shared group key yourself.

---

## 6. Transports (`p2pkit/transports`)

`P2PKit` negotiates the best transport with each peer automatically - you never pick per peer. By default all transports are enabled, you can optionally define a list of allowed transports manually.

| Transport | Runtime | |
|---|---|---|
| `RTCTransport` | universal | WebRTC data channel (default) |
| `HTTPTransport` | needs 1 Node participant | request/response over HTTP(S) |
| `UTPTransport` | Node | µTP (UDP), NAT-friendly |
| `DHTTransport` | Node | RPC over the BitTorrent DHT (from `bonana`) |

```ts
const kit = new P2PKit<Msg>({
  self,
  signalling,                             // offers WebRTC
  transports: {                           // offer more, all optional
    rtc: true,
    dht: { bootstrapHash: "ffffffff…" }, // one DHT node, shared with DHT discovery (§4)
    utp: true,
    http: true
  }
})
```

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
import { werift } from "npm:werift"
const kit = new P2PKit<Msg>({ self, signalling, transports: { rtc: werift } })
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

---
