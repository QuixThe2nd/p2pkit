/**
 * A single fragment of a chunked payload. Emitted (JSON-encoded) by
 * {@link Chunker.split} and consumed by {@link Chunker.ingest}.
 */
export interface ChunkPacket {
  /** Group id shared by all fragments of one payload. */
  id: string
  /** Fragment index, `0 … n-1`. */
  i: number
  /** Total fragment count. */
  n: number
  /** This fragment's slice of the original string. */
  part: string
}

export interface ChunkerOptions {
  /**
   * Maximum characters per fragment. Data channels cap message size (~256 KB);
   * the default leaves headroom for JSON/UTF-8 overhead. A single character can
   * encode to up to 4 UTF-8 bytes, so keep this well under the byte cap.
   */
  maxPacketSize?: number
  /**
   * Opt-in hardened reassembly (used by direct-mode {@link RTCTransport} in
   * `src/transports/rtc.ts`, which passes payload strings it received from an
   * untrusted peer). Off by default, preserving the lax contract above. When
   * set, both directions enforce {@link CHUNK_LIMITS}: `split` rejects invalid
   * group ids, oversized messages and fragment counts, and `ingest` validates
   * every fragment shape, rejects conflicting duplicates and group-size
   * changes, bounds concurrently-pending groups/bytes, and expires partial
   * groups past `CHUNK_LIMITS.lifetimeMs` (see {@link checkDeadline}) — a
   * malformed or hostile stream can never allocate unbounded memory and always
   * throws instead of returning a garbled payload.
   */
  hardened?: boolean
}

const DEFAULT_MAX_PACKET_SIZE = 16_000

/**
 * Resource bounds enforced when a {@link Chunker} is constructed with
 * `hardened: true` (opt-in; direct-mode {@link RTCTransport} uses these).
 */
export const CHUNK_LIMITS = Object.freeze({
  /** Maximum characters of payload per fragment (also the default `maxPacketSize`). */
  packetChars: 16_000,
  /** Maximum UTF-8 bytes of one whole message, counting characters and bytes. */
  messageBytes: 1_048_576,
  /** Maximum fragments (`n`) of one message. */
  fragments: 128,
  /** Maximum concurrently-incomplete messages kept in the inbox. */
  pendingGroups: 16,
  /** Maximum total UTF-8 bytes held across all incomplete messages. */
  pendingBytes: 4_194_304,
  /** A partially-received message older than this fails (see {@link Chunker.checkDeadline}). */
  lifetimeMs: 15_000,
})

function validId(id: unknown): id is string {
  return typeof id === "string" && /^[A-Za-z0-9_-]{1,64}$/.test(id)
}

const encoder = new TextEncoder()

/**
 * Splits large strings into fragments that fit a transport's per-message cap and
 * reassembles them on the far side. Applied automatically by transports; exposed
 * for direct use (README §11).
 */
export class Chunker {
  readonly maxPacketSize: number
  private readonly hardened: boolean
  private readonly inbox = new Map<string, { parts: (string | undefined)[]; received: number; bytes: number; created: number }>()
  private bytes = 0

  constructor(options: ChunkerOptions = {}) {
    this.hardened = options.hardened === true
    this.maxPacketSize = options.maxPacketSize ?? DEFAULT_MAX_PACKET_SIZE
    if (
      this.hardened &&
      (!Number.isSafeInteger(this.maxPacketSize) ||
        this.maxPacketSize < 1 ||
        this.maxPacketSize > CHUNK_LIMITS.packetChars)
    ) {
      throw new Error("Invalid chunk size")
    }
  }

  /**
   * Split `data` into JSON-encoded {@link ChunkPacket}s under `id`. Always yields
   * at least one packet (even for the empty string). Send each yielded string as
   * one transport message.
   */
  *split(id: string, data: string): Generator<string> {
    if (this.hardened) {
      if (
        !validId(id) ||
        typeof data !== "string" ||
        data.length > CHUNK_LIMITS.messageBytes ||
        encoder.encode(data).length > CHUNK_LIMITS.messageBytes
      ) {
        throw new Error("Message too large")
      }
      const total = Math.max(1, Math.ceil(data.length / this.maxPacketSize))
      if (total > CHUNK_LIMITS.fragments) throw new Error("Too many fragments")
      yield* this.splitUnchecked(id, data, total)
      return
    }
    yield* this.splitUnchecked(id, data, data.length === 0 ? 1 : Math.ceil(data.length / this.maxPacketSize))
  }

  private *splitUnchecked(id: string, data: string, n: number): Generator<string> {
    const size = this.maxPacketSize
    for (let i = 0; i < n; i++) {
      const packet: ChunkPacket = { id, i, n, part: data.slice(i * size, (i + 1) * size) }
      yield JSON.stringify(packet)
    }
  }

  /**
   * Fail if any partially-received message has been incomplete for longer than
   * `CHUNK_LIMITS.lifetimeMs`. Called by hardened `ingest` on every fragment and
   * by direct-mode transports on a timer, so a peer that goes silent mid-message
   * cannot pin memory forever. No-op when nothing is pending.
   */
  checkDeadline(now: number = performance.now()): void {
    for (const entry of this.inbox.values()) {
      if (now - entry.created >= CHUNK_LIMITS.lifetimeMs) throw new Error("Incomplete message expired")
    }
  }

  /**
   * Feed one received {@link ChunkPacket}. Returns the fully reassembled string
   * once the final missing fragment arrives, otherwise `undefined`. Duplicate
   * fragments are ignored; fragments may arrive in any order.
   *
   * Hardened mode additionally validates the fragment's shape (throwing on
   * malformed, conflicting or budget-exceeding input — never returning a
   * corrupted payload) and enforces {@link CHUNK_LIMITS}.
   */
  ingest(packet: ChunkPacket): string | undefined {
    if (this.hardened) return this.ingestHardened(packet)
    const { id, i, n, part } = packet
    if (n <= 1) return part

    let entry = this.inbox.get(id)
    if (!entry) {
      entry = { parts: new Array<string | undefined>(n).fill(undefined), received: 0, bytes: 0, created: 0 }
      this.inbox.set(id, entry)
    }
    if (i < 0 || i >= n) return undefined
    if (entry.parts[i] === undefined) {
      entry.parts[i] = part
      entry.received++
    }
    if (entry.received === n) {
      this.inbox.delete(id)
      return entry.parts.join("")
    }
    return undefined
  }

  private ingestHardened(packet: ChunkPacket): string | undefined {
    this.checkDeadline()
    if (!packet || typeof packet !== "object" || Array.isArray(packet)) throw new Error("Invalid fragment")
    const { id, i, n, part } = packet
    if (
      !validId(id) ||
      !Number.isSafeInteger(n) ||
      n < 1 ||
      n > CHUNK_LIMITS.fragments ||
      !Number.isSafeInteger(i) ||
      i < 0 ||
      i >= n ||
      typeof part !== "string" ||
      part.length > this.maxPacketSize
    ) {
      throw new Error("Invalid fragment")
    }
    let entry = this.inbox.get(id)
    if (entry && entry.parts.length !== n) throw new Error("Fragment count changed")
    const bytes = encoder.encode(part).length
    if (n === 1) return part
    if (!entry) {
      if (this.inbox.size >= CHUNK_LIMITS.pendingGroups) throw new Error("Too many incomplete messages")
      entry = { parts: new Array<string | undefined>(n).fill(undefined), received: 0, bytes: 0, created: performance.now() }
      this.inbox.set(id, entry)
    }
    const previous = entry.parts[i]
    if (previous !== undefined) {
      if (previous !== part) throw new Error("Conflicting fragment")
      return undefined
    }
    if (bytes > CHUNK_LIMITS.messageBytes - entry.bytes || bytes > CHUNK_LIMITS.pendingBytes - this.bytes) {
      throw new Error("Reassembly budget exceeded")
    }
    entry.parts[i] = part
    entry.received++
    entry.bytes += bytes
    this.bytes += bytes
    if (entry.received !== n) return undefined
    this.inbox.delete(id)
    this.bytes -= entry.bytes
    return entry.parts.join("")
  }

  /** Drop any partially-received payloads (e.g. on disconnect). */
  reset(): void {
    this.inbox.clear()
    this.bytes = 0
  }
}
