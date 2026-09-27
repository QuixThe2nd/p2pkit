import type { PeerId } from "../utils/types.js"

/** Events every {@link Transport} emits (README §8). */
export type TransportEvents<Msg> = {
  connect: () => void
  message: (msg: Msg) => void
  disconnect: () => void
  error: (err: Error) => void
}

/**
 * What a transport can say about the **local** end of its link: the scheme
 * actually carrying frames from this side, and this side's role in that
 * transport's own handshake. Everything here is read off the local socket or
 * the local server — never off anything the remote advertised, and never off a
 * guess. A door behind a TLS-terminating proxy truthfully reports `ws` while
 * the browser that dialled it reports `wss`; both are right, because each is
 * describing its own end.
 */
export interface TransportInfo {
  /** The local link transport, e.g. `"ws"`, `"wss"`, `"rtc"`, or a custom name. */
  readonly scheme: string
  /**
   * This side's role in the transport's own handshake, when the transport has
   * one. WebSocket links are `client` on the side that dialled and `server` on
   * the side that accepted; roles are about the handshake, never the peer.
   */
  readonly role?: "client" | "server"
}

/**
 * A single point-to-point link to one remote peer. Add a link type beyond the
 * built-ins by implementing this interface (README §8). `P2PKit` drives
 * transports generically over the wire {@link ../wire.Frame} type.
 */
export interface Transport<Msg> {
  /** The peer on the other end. */
  readonly remote: PeerId
  /** Bytes queued but not yet flushed to the network. */
  readonly bufferedAmount: number
  /** Send a message; resolves once flushed (await it, or watch `bufferedAmount`). */
  send(message: Msg): Promise<void>
  on<E extends keyof TransportEvents<Msg>>(event: E, handler: TransportEvents<Msg>[E]): void
  /** Close the link. */
  disconnect(): void
  /**
   * Local link diagnostics — the scheme carrying this link from here, and this
   * side's role in it. Optional, so a custom transport that cannot say
   * anything truthful about its own carriage simply omits it and a caller
   * reports "unknown" rather than guess.
   */
  readonly info?: TransportInfo
}
