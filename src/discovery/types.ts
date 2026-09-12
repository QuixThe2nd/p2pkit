import type { PeerId } from "../utils/types.js"

/**
 * The slice of `P2PKit` a {@link Discovery} drives. `P2PKit` implements this and
 * passes itself to {@link Discovery.start}. A discovery channel uses it to see
 * who we're connected to, to ask the mesh to connect to a newly-found id, and —
 * for peer-exchange — to send and receive gossip over the existing links.
 */
export interface DiscoveryHost {
  /** This node's id. */
  readonly self: PeerId
  /** Ids of the currently-connected peers. */
  peerIds(): PeerId[]
  /** Ask the host to open a connection to a discovered peer id (no-op if already known). */
  connect(peer: PeerId): void
  /** Send a list of known-peer ids to one connected peer (peer-exchange). */
  sendGossip(to: PeerId, peers: PeerId[]): void
  /** Subscribe to a peer connecting. Returns an unsubscribe function. */
  onPeerConnected(handler: (peer: PeerId) => void): () => void
  /** Subscribe to inbound gossip. Returns an unsubscribe function. */
  onGossip(handler: (from: PeerId, peers: PeerId[]) => void): () => void
}

/**
 * A way to grow or heal the mesh beyond the signalling room (README §4).
 * `P2PKit` starts each configured `Discovery` with a {@link DiscoveryHost`}` and
 * stops it on shutdown.
 */
export interface Discovery {
  /** Begin discovering, using `host` to reach the mesh. */
  start(host: DiscoveryHost): void | Promise<void>
  /** Stop and release any resources (timers, sockets, DHT nodes). */
  stop(): void
}
