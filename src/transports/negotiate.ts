import type { PeerId } from "../utils/types.js"

/** Built-in transport names, best-first by default preference. */
export type TransportName = "rtc" | "utp" | "http" | "dht"

/**
 * Default preference order (README §6 — WebRTC is the universal default). RTC
 * works browser-to-browser and self-traverses NAT via ICE; µTP and HTTP are
 * Node-reachable fallbacks; DHT is the last resort.
 */
export const DEFAULT_TRANSPORT_ORDER: readonly TransportName[] = ["rtc", "utp", "http", "dht"]

/**
 * Choose the transport two peers should use: the most-preferred capability both
 * advertise. This helper is not automatically called by Peer/P2PKit.
 * Returns `undefined` when there is no overlap.
 *
 * This is the *selection* half of negotiation. Actually dialing a non-RTC
 * transport also needs the peer's endpoint (an HTTP url, µTP port, or DHT id);
 * RTC alone self-negotiates over signalling. Exchanging those endpoints in the
 * `hello` handshake is the remaining wiring before non-RTC transports connect
 * automatically — until then, supply them via `createTransport`/`Peer({ transport })`.
 */
export function chooseTransport(
  localCaps: readonly string[],
  remoteCaps: readonly string[],
  order: readonly TransportName[] = DEFAULT_TRANSPORT_ORDER,
): TransportName | undefined {
  const remote = new Set(remoteCaps)
  const local = new Set(localCaps)
  for (const name of order) {
    if (local.has(name) && remote.has(name)) return name
  }
  return undefined
}

/** The capabilities to advertise given which transports are enabled. */
export function capsFor(enabled: Partial<Record<TransportName, unknown>>): TransportName[] {
  return DEFAULT_TRANSPORT_ORDER.filter(name => Boolean(enabled[name]))
}

/** Deterministic initiator for a link: the lexicographically-smaller id offers (avoids glare). */
export function isInitiator(self: PeerId, remote: PeerId): boolean {
  return self < remote
}
