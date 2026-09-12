import type { PeerId } from "../utils/types.js"

/**
 * Configuration for the shared BitTorrent-DHT node (README §4, §6). The DHT
 * transport and DHT discovery run on **one** node — set `bootstrapHash`/`port`
 * in a single place and the other reuses it.
 */
export interface DHTNodeConfig {
  /** A shared 40-hex infohash every participant announces and looks up. */
  bootstrapHash: string
  /** UDP port the DHT node binds. Default 20000. */
  port?: number
}

/** A discovered peer on the DHT. */
export interface DHTPeer {
  host: string
  port: number
  /** The announcing node's id, when the DHT reports it. */
  nodeId?: PeerId
}

/** The slice of `bittorrent-dht` this module drives. */
interface RawDHT {
  listen(port: number, cb?: () => void): void
  announce(infoHash: string, port: number, cb?: (err: Error | null) => void): void
  lookup(infoHash: string, cb?: (err: Error | null) => void): void
  on(
    event: "peer",
    handler: (peer: { host: string; port: number }, infoHash: Buffer, from: unknown) => void,
  ): void
  on(event: "ready" | "error", handler: (arg?: unknown) => void): void
  destroy(cb?: () => void): void
  nodeId: Buffer
}

interface DHTModule {
  new (opts?: { bootstrap?: string[] }): RawDHT
}

const DEFAULT_PORT = 20000

/** A running DHT node, announcing a shared infohash and surfacing found peers. */
export class DHTNode {
  private constructor(
    private readonly raw: RawDHT,
    private readonly config: Required<DHTNodeConfig>,
  ) {}

  /** This node's DHT id as hex. */
  get nodeId(): PeerId {
    return this.raw.nodeId.toString("hex")
  }

  /** The infohash this node announces. */
  get infoHash(): string {
    return this.config.bootstrapHash
  }

  /** Announce our presence and start looking for others under the shared infohash. */
  announce(): void {
    this.raw.announce(this.config.bootstrapHash, this.config.port)
    this.raw.lookup(this.config.bootstrapHash)
  }

  /** Subscribe to peers found under the infohash. */
  onPeer(handler: (peer: DHTPeer) => void): void {
    this.raw.on("peer", (peer, infoHash) => {
      if (infoHash.toString("hex") === this.config.bootstrapHash)
        handler({ host: peer.host, port: peer.port })
    })
  }

  /** Shut the node down. */
  destroy(): Promise<void> {
    return new Promise(resolve => {
      if (shared === this) shared = undefined
      this.raw.destroy(() => resolve())
    })
  }

  // ---- shared singleton -------------------------------------------------

  /**
   * Get the process-wide DHT node, creating it on first use (README §4/§6 — one
   * shared node). The first caller to supply `bootstrapHash` fixes the config;
   * later callers may pass nothing and reuse it.
   */
  static async shared(config?: Partial<DHTNodeConfig>): Promise<DHTNode> {
    if (shared) return shared
    if (!config?.bootstrapHash) {
      throw new Error(
        "The shared DHT node needs a `bootstrapHash` on first use (set it on the DHT transport or discovery).",
      )
    }
    const DHT = await loadDHT()
    const raw = new DHT()
    const resolved: Required<DHTNodeConfig> = {
      bootstrapHash: config.bootstrapHash,
      port: config.port ?? DEFAULT_PORT,
    }
    await new Promise<void>(resolve => raw.listen(resolved.port, resolve))
    shared = new DHTNode(raw, resolved)
    return shared
  }
}

let shared: DHTNode | undefined

// Widened to `string` so TS won't resolve the optional module at build time.
const BITTORRENT_DHT: string = "bittorrent-dht"

async function loadDHT(): Promise<DHTModule> {
  try {
    const mod = (await import(/* @vite-ignore */ BITTORRENT_DHT)) as Record<string, unknown>
    const candidate = (mod["default"] ?? mod) as DHTModule
    if (typeof candidate === "function") return candidate
  } catch {
    /* not installed */
  }
  throw new Error("DHT features require the optional `bittorrent-dht` dependency (Node-only).")
}
