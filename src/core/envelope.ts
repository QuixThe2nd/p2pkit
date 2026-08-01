import type { PeerId } from "../utils/types.js"

/**
 * Deterministic string signed over a broadcast so every hop can verify the
 * origin and freshness. Excludes `ttl` (it changes per hop). The verifier
 * re-serializes the received fields identically because `body` is parsed from
 * the same JSON the origin produced, preserving key order.
 */
export function broadcastSignPayload(f: {
  from: PeerId
  id: string
  ts: number
  nonce: string
  body: unknown
}): string {
  return JSON.stringify(["bcast", f.from, f.id, f.ts, f.nonce, f.body])
}

/** Deterministic string signed over a topic publish on `{ signed: true }` topics. */
export function pubSignPayload(f: {
  topic: string
  from: PeerId
  seq: number
  nonce: string
  body: unknown
}): string {
  return JSON.stringify(["pub", f.topic, f.from, f.seq, f.nonce, f.body])
}

/**
 * Time-bounded set of seen ids, for broadcast/publish dedup and nonce replay
 * protection. Entries expire after `windowMs`.
 */
export class SeenCache {
  private readonly entries = new Map<string, number>()
  constructor(private readonly windowMs: number) {}

  /** Record `id`; returns `true` if it was already present (a duplicate). */
  seen(id: string): boolean {
    this.prune()
    if (this.entries.has(id)) return true
    this.entries.set(id, Date.now() + this.windowMs)
    return false
  }

  has(id: string): boolean {
    const expiry = this.entries.get(id)
    if (expiry === undefined) return false
    if (expiry < Date.now()) {
      this.entries.delete(id)
      return false
    }
    return true
  }

  private prune(): void {
    const now = Date.now()
    for (const [id, expiry] of this.entries) if (expiry < now) this.entries.delete(id)
  }
}
