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
 * The trade runs both ways: a link that just came up is announced to the peers
 * we were already linked to as well. Without that, a peer joining through a
 * single link stays invisible to the rest of the mesh — its id never reaches
 * anyone but the peer it seeded from, and nobody offers to it (the smaller id
 * offers, so a peer that has never heard of us certainly doesn't).
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

  /** A peer just connected: trade peer lists, and introduce it to the rest. */
  private exchange(peer: PeerId): void {
    const host = this.host
    if (!host) return
    const share = host
      .peerIds()
      .filter(p => p !== peer)
      .slice(0, this.maxShare)
    // One id per existing link, so the newcomer is dialable from everywhere and
    // not just from the peer it first reached. Inbound lists dedup, and each
    // link announces a given peer at most once, so this terminates.
    for (const other of host.peerIds()) {
      if (other !== peer && other !== host.self) host.sendGossip(other, [peer])
    }
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
