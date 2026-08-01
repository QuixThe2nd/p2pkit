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
}

const DEFAULT_MAX_PACKET_SIZE = 16_000

/**
 * Splits large strings into fragments that fit a transport's per-message cap and
 * reassembles them on the far side. Applied automatically by transports; exposed
 * for direct use (README §11).
 */
export class Chunker {
  readonly maxPacketSize: number
  private readonly inbox = new Map<string, { parts: (string | undefined)[]; received: number }>()

  constructor(options: ChunkerOptions = {}) {
    this.maxPacketSize = options.maxPacketSize ?? DEFAULT_MAX_PACKET_SIZE
  }

  /**
   * Split `data` into JSON-encoded {@link ChunkPacket}s under `id`. Always yields
   * at least one packet (even for the empty string). Send each yielded string as
   * one transport message.
   */
  *split(id: string, data: string): Generator<string> {
    const size = this.maxPacketSize
    const n = data.length === 0 ? 1 : Math.ceil(data.length / size)
    for (let i = 0; i < n; i++) {
      const packet: ChunkPacket = { id, i, n, part: data.slice(i * size, (i + 1) * size) }
      yield JSON.stringify(packet)
    }
  }

  /**
   * Feed one received {@link ChunkPacket}. Returns the fully reassembled string
   * once the final missing fragment arrives, otherwise `undefined`. Duplicate
   * fragments are ignored; fragments may arrive in any order.
   */
  ingest(packet: ChunkPacket): string | undefined {
    const { id, i, n, part } = packet
    if (n <= 1) return part

    let entry = this.inbox.get(id)
    if (!entry) {
      entry = { parts: new Array<string | undefined>(n).fill(undefined), received: 0 }
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

  /** Drop any partially-received payloads (e.g. on disconnect). */
  reset(): void {
    this.inbox.clear()
  }
}
