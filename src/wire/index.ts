import type { PeerId } from "../utils/types.js"
import type { SignallingMessage } from "../signalling/types.js"

/**
 * On-the-wire frame version. Every frame carries `v`; a receiver rejects frames
 * whose version it does not understand. Bump this when the envelope changes in a
 * backward-incompatible way (see README — the envelope is "hard to change
 * later", so it is versioned rather than guessed at).
 */
export const WIRE_VERSION = 2 as const

/** Handshake: announce identity + capabilities, carry a nonce to be signed. */
export interface HelloFrame {
  v: typeof WIRE_VERSION
  k: "hello"
  from: PeerId
  /** Transport/feature capabilities this side offers (e.g. `["rtc","http"]`). */
  caps: string[]
  /** Random nonce the peer must sign to prove ownership of `from`. */
  nonce: string
  /** Signature over the peer's own nonce, present when a signer is configured. */
  sig?: string
}

/** Handshake reply: proves identity by signing the counterpart's `hello` nonce. */
export interface AckFrame {
  v: typeof WIRE_VERSION
  k: "ack"
  from: PeerId
  /** The nonce received in the counterpart's `hello`. */
  nonce: string
  /** Signature over that nonce, present when a signer is configured. */
  sig?: string
}

/** A direct, connection-scoped application message. */
export interface MsgFrame {
  v: typeof WIRE_VERSION
  k: "msg"
  body: unknown
}

/** RPC request. */
export interface ReqFrame {
  v: typeof WIRE_VERSION
  k: "req"
  id: string
  method: string
  body: unknown
}

/** RPC response (success or error). */
export interface ResFrame {
  v: typeof WIRE_VERSION
  k: "res"
  id: string
  ok: boolean
  body?: unknown
  err?: { code: string; method: string; message?: string }
}

/** Mesh-wide flood message with hop limit + dedup id. */
export interface BcastFrame {
  v: typeof WIRE_VERSION
  k: "bcast"
  id: string
  ttl: number
  from: PeerId
  body: unknown
  /** Freshness timestamp, present on signed broadcasts. */
  ts?: number
  /** Anti-replay nonce, present on signed broadcasts. */
  nonce?: string
  /** Origin signature over the envelope, present on signed broadcasts. */
  sig?: string
}

/** Topic subscribe / unsubscribe (gossiped to advertise subscriber membership). */
export interface SubFrame {
  v: typeof WIRE_VERSION
  k: "sub" | "unsub"
  topic: string
  /** The subscribing peer (preserved as the frame is gossiped onward). */
  from: PeerId
  /** Gossip dedup id. */
  id: string
}

/** Topic publish, scoped to subscribers, with per-sender replay protection. */
export interface PubFrame {
  v: typeof WIRE_VERSION
  k: "pub"
  topic: string
  from: PeerId
  /** Per-sender monotonically increasing sequence number (also the relay dedup key). */
  seq: number
  /** Per-message nonce within the replay window. */
  nonce: string
  /** Hop limit for flooding. */
  ttl: number
  body: unknown
  /** Origin signature, present on `{ signed: true }` topics. */
  sig?: string
}

/** Peer-exchange: share known-peer ids so one bootstrap connection fans out. */
export interface GossipFrame {
  v: typeof WIRE_VERSION
  k: "gossip"
  peers: PeerId[]
}

/** One fragment of a chunked frame (see {@link ../framing}). */
export interface ChunkFrame {
  v: typeof WIRE_VERSION
  k: "chunk"
  id: string
  i: number
  n: number
  part: string
}

/**
 * Peer-brokered signalling (README §4): one {@link SignallingMessage} carried
 * over an existing link because the lobby is unreachable. The carried `signal`
 * is the lobby's envelope, unchanged — the broker is a postman, so SDP and
 * candidates stay direct-only. `ttl` counts remaining *forward* hops: senders
 * set 1, a forwarder delivers or passes it on at 0, and nothing is ever flooded
 * to more than one peer. `id` dedups a signal that arrives by two routes.
 */
export interface SigRelayFrame {
  v: typeof WIRE_VERSION
  k: "sig-relay"
  /** Relay dedup id. */
  id: string
  /** Remaining forward hops. */
  ttl: number
  /** The peer the carried signal came from (not the forwarding peer). */
  from: PeerId
  /** The peer the carried signal is ultimately for. */
  to: PeerId
  /** The signalling envelope, byte-identical to what the lobby would carry. */
  signal: SignallingMessage
}

/** Liveness / latency probe. */
export interface PingFrame {
  v: typeof WIRE_VERSION
  k: "ping"
  id: string
}

/** Liveness / latency reply. */
export interface PongFrame {
  v: typeof WIRE_VERSION
  k: "pong"
  id: string
}

/** Authenticated encryption of one complete post-handshake frame. */
export interface SealedFrame {
  v: typeof WIRE_VERSION
  k: "sealed"
  body: string
}

/** The full set of frames P2PKit exchanges. */
export type Frame =
  | SealedFrame
  | HelloFrame
  | AckFrame
  | MsgFrame
  | ReqFrame
  | ResFrame
  | BcastFrame
  | SubFrame
  | PubFrame
  | GossipFrame
  | SigRelayFrame
  | ChunkFrame
  | PingFrame
  | PongFrame

/** All frame kinds, for validation. */
export type FrameKind = Frame["k"]

/** Thrown when a frame cannot be decoded or fails version/shape validation. */
export class WireError extends Error {
  constructor(message: string) {
    super(message)
    this.name = "WireError"
  }
}

const KINDS: ReadonlySet<string> = new Set<FrameKind>([
  "sealed",
  "hello",
  "ack",
  "msg",
  "req",
  "res",
  "bcast",
  "sub",
  "unsub",
  "pub",
  "gossip",
  "sig-relay",
  "chunk",
  "ping",
  "pong",
])

/**
 * Encodes/decodes {@link Frame}s. JSON on the wire for 0.1.0 — readable and
 * debuggable; a binary codec can slot in behind this interface later without
 * touching callers.
 */
export const FrameCodec = {
  encode(frame: Frame): string {
    return JSON.stringify(frame)
  },

  decode(raw: string): Frame {
    let obj: unknown
    try {
      obj = JSON.parse(raw)
    } catch {
      throw new WireError("frame is not valid JSON")
    }
    if (typeof obj !== "object" || obj === null) throw new WireError("frame is not an object")
    const rec = obj as Record<string, unknown>
    if (rec["v"] !== WIRE_VERSION)
      throw new WireError(`unsupported wire version: ${String(rec["v"])}`)
    if (typeof rec["k"] !== "string" || !KINDS.has(rec["k"])) {
      throw new WireError(`unknown frame kind: ${String(rec["k"])}`)
    }
    validateFrame(obj)
    return obj as Frame
  },
}

/** Validate even frames from injected transports, which may bypass the codec. */
export function validateFrame(value: unknown): asserts value is Frame {
  if (!value || typeof value !== "object") throw new WireError("frame is not an object")
  const f = value as Record<string, unknown>
  if (f.v !== WIRE_VERSION) throw new WireError("unsupported wire version")
  const str = (key: string) => typeof f[key] === "string" && (f[key] as string).length > 0
  const int = (key: string, min = 0) => Number.isSafeInteger(f[key]) && (f[key] as number) >= min
  const sig = f.sig === undefined || typeof f.sig === "string"
  let valid = false
  switch (f.k) {
    case "hello":
      valid =
        str("from") &&
        str("nonce") &&
        Array.isArray(f.caps) &&
        f.caps.every(x => typeof x === "string") &&
        sig
      break
    case "ack":
      valid = str("from") && str("nonce") && sig
      break
    case "sealed":
      valid = str("body")
      break
    case "msg":
      valid = f.enc === undefined
      break
    case "req":
      valid = str("id") && str("method")
      break
    case "res": {
      const err = f.err as Record<string, unknown> | undefined
      valid =
        str("id") &&
        typeof f.ok === "boolean" &&
        (f.ok ||
          (!!err &&
            typeof err.code === "string" &&
            typeof err.method === "string" &&
            (err.message === undefined || typeof err.message === "string")))
      break
    }
    case "bcast":
      valid =
        str("id") &&
        str("from") &&
        int("ttl", 1) &&
        sig &&
        (f.ts === undefined || int("ts")) &&
        (f.nonce === undefined || str("nonce"))
      break
    case "pub":
      valid = str("topic") && str("from") && str("nonce") && int("seq", 1) && int("ttl", 1) && sig
      break
    case "sub":
    case "unsub":
      valid = str("topic") && str("from") && str("id")
      break
    case "gossip":
      valid = Array.isArray(f.peers) && f.peers.every(x => typeof x === "string" && x.length > 0)
      break
    case "chunk":
      valid =
        str("id") &&
        int("i") &&
        int("n", 1) &&
        (f.i as number) < (f.n as number) &&
        typeof f.part === "string"
      break
    case "sig-relay": {
      // The carried envelope is only shape-checked here; the receiving
      // RTCTransport re-checks `from`/`to` against its own session.
      const signal = f.signal as Record<string, unknown> | undefined
      const carried =
        !!signal &&
        typeof signal === "object" &&
        typeof signal["from"] === "string" &&
        (signal["announce"] === true ||
          (typeof signal["description"] === "object" && signal["description"] !== null) ||
          (typeof signal["iceCandidate"] === "object" && signal["iceCandidate"] !== null))
      valid = str("id") && str("from") && str("to") && int("ttl") && carried
      break
    }
    case "ping":
    case "pong":
      valid = str("id")
      break
  }
  if (!valid) throw new WireError("invalid frame shape")
}
