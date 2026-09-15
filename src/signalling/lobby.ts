/**
 * Wire protocol of the bootstrapping-server matchmaking lobby (see the repo's
 * `bootstrapping-server/` directory): a global FIFO that pairs two waiting
 * peers, assigns host/joiner roles, and then relays opaque signalling payloads
 * between the pair until one side disconnects. Both the server and browser
 * clients import these types, so the wire format has a single definition.
 */

/** Side of a matched pair, assigned by the server: the waiting peer hosts, the newcomer joins. */
export type LobbyRole = "host" | "joiner"

/** Client → server frames, discriminated on `t`. */
export type LobbyClientMessage =
  | { t: "find" } // enter the lobby; pairs with the current waiter or queues
  | { t: "cancel" } // leave the waiting queue (a pairing is only left by closing the socket)
  | { t: "sig"; data: unknown } // opaque payload, relayed verbatim to the paired peer

/** Server → client frames, discriminated on `t`. */
export type LobbyServerMessage =
  | { t: "waiting" } // queued; no partner yet
  | { t: "matched"; role: LobbyRole } // paired; the waiter is "host" (offers), the newcomer "joiner" (answers)
  | { t: "sig"; data: unknown } // the paired peer's `sig` payload, unchanged
  | { t: "peer_left" } // the paired peer disconnected; the pairing dissolved
  | { t: "error"; code: string } // invalid input or policy violation (too_large, invalid_message, unknown_type, rate_limited, lobby_full)
