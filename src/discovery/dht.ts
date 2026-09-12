import type { Discovery, DiscoveryHost } from "./types.js"
import type { DHTPeer } from "./dht-node.js"
import { DHTNode, type DHTNodeConfig } from "./dht-node.js"

export interface DHTDiscoveryOptions extends DHTNodeConfig {
  /**
   * Called with each peer address found on the DHT. The BitTorrent DHT yields
   * network addresses, not mesh {@link ../utils/types.PeerId}s, so bridging a
   * found address into the mesh needs an address-dialing transport (µTP/DHT) —
   * wire it here. Once one connection is up, {@link ./gossip.GossipDiscovery}
   * takes over (README §4).
   */
  onPeer?: (peer: DHTPeer) => void
}

/**
 * Serverless bootstrap over the BitTorrent DHT (README §4, Node-only). Every
 * node announces a shared infohash and finds anyone else announcing it; use it
 * to make the first connection, then let gossip fan out. Requires the optional
 * `bittorrent-dht` dependency and a live DHT network.
 *
 * Shares one DHT node with {@link ../transports/dht.DHTTransport} — set
 * `bootstrapHash`/`port` in a single place (see {@link DHTNode.shared}).
 */
export class DHTDiscovery implements Discovery {
  private node?: DHTNode
  private stopped = false

  constructor(private readonly options: DHTDiscoveryOptions) {}

  async start(_host: DiscoveryHost): Promise<void> {
    this.node = await DHTNode.shared(this.options)
    if (this.stopped) return
    this.node.onPeer(peer => {
      if (!this.stopped) this.options.onPeer?.(peer)
    })
    this.node.announce()
  }

  stop(): void {
    this.stopped = true
    // The node is shared; leave teardown to whoever owns it (or process exit).
    this.node = undefined
  }
}
