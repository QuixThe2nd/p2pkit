// p2pkit/discovery — grow and heal the mesh without central servers (README §4).
export type { Discovery, DiscoveryHost } from "./types.js"
export { GossipDiscovery, type GossipDiscoveryOptions } from "./gossip.js"
// Node-only, serverless bootstrap over the BitTorrent DHT; shares one DHT node
// with `DHTTransport` (README §4, §6). Its `bittorrent-dht` dep is optional.
export { DHTDiscovery, type DHTDiscoveryOptions } from "./dht.js"
export { DHTNode, type DHTNodeConfig, type DHTPeer } from "./dht-node.js"
