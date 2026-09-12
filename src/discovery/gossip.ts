import type { PeerId } from "../utils/types.js"
import type { Discovery, DiscoveryHost } from "./types.js"

export interface GossipDiscoveryOptions {
  /** Cap on how many peer ids to share in one exchange. Default 32. */
  maxShare?: number
}

const DEFAULT_MAX_SHARE = 32

/**
 * Peer-exchange discovery (README §4). Once connected to one peer, peers trade
 * their known-peer lists, so a single bootstrap connection fans out toward the
 * rest of the mesh: on each new connection we send our peer list, and on each
 * inbound list we connect to any id we don't already know.
 *
 * Fan-out is bounded by transport reachability — an id only becomes a connection
 * if the host can actually reach it (a shared signalling room, or a by-id
 * transport such as the DHT). Gossip supplies the *who*; the transport the *how*.
 */
export class GossipDiscovery implements Discovery {
  private host?: DiscoveryHost
  private readonly offs: Array<() => void> = []
  private readonly maxShare: number

  constructor(opts: GossipDiscoveryOptions = {}) {
    this.maxShare = opts.maxShare ?? DEFAULT_MAX_SHARE
  }

  start(host: DiscoveryHost): void {
    this.host = host
    this.offs.push(host.onPeerConnected(peer => this.exchange(peer)))
    this.offs.push(host.onGossip((from, peers) => this.ingest(from, peers)))
  }

  stop(): void {
    for (const off of this.offs) off()
    this.offs.length = 0
    this.host = undefined
  }

  /** A peer just connected: tell it about everyone else we know. */
  private exchange(peer: PeerId): void {
    const host = this.host
    if (!host) return
    const share = host
      .peerIds()
      .filter(p => p !== peer)
      .slice(0, this.maxShare)
    if (share.length > 0) host.sendGossip(peer, share)
  }

  /** A peer shared its list: connect to anyone new. */
  private ingest(from: PeerId, peers: PeerId[]): void {
    const host = this.host
    if (!host) return
    const known = new Set<PeerId>([host.self, ...host.peerIds()])
    for (const p of peers) {
      if (!known.has(p)) {
        known.add(p) // dedup within this batch
        host.connect(p)
      }
    }
  }
}
