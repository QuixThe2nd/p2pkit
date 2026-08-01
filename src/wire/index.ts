import type { PeerId } from "../utils/types.js"

/**
 * On-the-wire frame version. Every frame carries `v`; a receiver rejects frames
 * whose version it does not understand. Bump this when the envelope changes in a
 * backward-incompatible way (see README — the envelope is "hard to change
 * later", so it is versioned rather than guessed at).
 */
export const WIRE_VERSION = 1 as const

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
  /** Set when `body` is an encrypted (base64) AEAD payload rather than plaintext. */
  enc?: boolean
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

/** Topic subscribe / unsubscribe (gossiped so publishes route only toward subscribers). */
export interface SubFrame {
  v: typeof WIRE_VERSION
  k: "sub" | "unsub"
  topic: string
}

/** Topic publish, scoped to subscribers, with per-sender replay protection. */
export interface PubFrame {
  v: typeof WIRE_VERSION
  k: "pub"
  topic: string
  from: PeerId
  /** Per-sender monotonically increasing sequence number. */
  seq: number
  /** Per-message nonce within the replay window. */
  nonce: string
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

/** The full set of frames P2PKit exchanges. */
export type Frame =
  | HelloFrame
  | AckFrame
  | MsgFrame
  | ReqFrame
  | ResFrame
  | BcastFrame
  | SubFrame
  | PubFrame
  | GossipFrame
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
    if (rec["v"] !== WIRE_VERSION) throw new WireError(`unsupported wire version: ${String(rec["v"])}`)
    if (typeof rec["k"] !== "string" || !KINDS.has(rec["k"])) {
      throw new WireError(`unknown frame kind: ${String(rec["k"])}`)
    }
    return obj as Frame
  },
}
