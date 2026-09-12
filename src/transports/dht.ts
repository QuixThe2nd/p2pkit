import type { Transport, TransportEvents } from "./types.js"
import type { PeerId } from "../utils/types.js"
import { Emitter } from "../utils/emitter.js"

/**
 * A bidirectional RPC channel to one peer over the DHT. `bonana` provides this;
 * it is isolated behind {@link DHTTransportOptions.openSocket} so the transport
 * logic stays testable and the exact `bonana` binding is a single adapter point.
 */
export interface DHTRPCSocket {
  /** Send one already-serialised frame. */
  send(data: string): void | Promise<void>
  on(event: "message", handler: (data: string) => void): void
  on(event: "connect", handler: () => void): void
  on(event: "close", handler: () => void): void
  on(event: "error", handler: (err: Error) => void): void
  close(): void
}

export interface DHTTransportOptions {
  self: PeerId
  remote: PeerId
  /** Shared with DHT discovery (README §4/§6). */
  bootstrapHash?: string
  port?: number
  /**
   * Open a `bonana` RPC socket to `remote`. Defaults to the built-in loader
   * (requires the optional `bonana` dependency); inject your own for a custom
   * binding or for testing.
   */
  openSocket?: (
    remote: PeerId,
    config: { bootstrapHash?: string; port?: number },
  ) => Promise<DHTRPCSocket>
}

/**
 * RPC-over-DHT transport (README §6, Node-only). Routes {@link ../wire.Frame}s to
 * a peer by id over the BitTorrent DHT via `bonana`, sharing one DHT node with
 * {@link ../discovery/dht.DHTDiscovery}. Requires the optional `bonana`
 * dependency and a live DHT network.
 */
export class DHTTransport<T = unknown> implements Transport<T> {
  readonly remote: PeerId
  readonly name = "dht"
  bufferedAmount = 0

  private readonly emitter = new Emitter<TransportEvents<T>>()
  private socket?: DHTRPCSocket
  private open = false
  private closed = false

  constructor(options: DHTTransportOptions) {
    this.remote = options.remote
    const open = options.openSocket ?? defaultOpenSocket
    void open(options.remote, { bootstrapHash: options.bootstrapHash, port: options.port })
      .then(socket => this.attach(socket))
      .catch(err => this.emitter.emit("error", err instanceof Error ? err : new Error(String(err))))
  }

  on<E extends keyof TransportEvents<T>>(event: E, handler: TransportEvents<T>[E]): void {
    this.emitter.on(event, handler)
  }

  async send(message: T): Promise<void> {
    if (this.closed || !this.socket) return
    await this.socket.send(JSON.stringify(message))
  }

  disconnect(): void {
    if (this.closed) return
    this.closed = true
    this.socket?.close()
    if (this.open) {
      this.open = false
      this.emitter.emit("disconnect")
    }
  }

  private attach(socket: DHTRPCSocket): void {
    if (this.closed) {
      socket.close()
      return
    }
    this.socket = socket
    socket.on("message", data => {
      try {
        this.emitter.emit("message", JSON.parse(data) as T)
      } catch (err) {
        this.emitter.emit("error", err instanceof Error ? err : new Error(String(err)))
      }
    })
    socket.on("connect", () => this.markOpen())
    socket.on("close", () => this.disconnect())
    socket.on("error", err => this.emitter.emit("error", err))
    // bonana sockets returned already-connected won't emit `connect`; open on next tick.
    queueMicrotask(() => this.markOpen())
  }

  private markOpen(): void {
    if (this.open || this.closed) return
    this.open = true
    this.emitter.emit("connect")
  }
}

async function defaultOpenSocket(): Promise<DHTRPCSocket> {
  throw new Error(
    "DHTTransport requires the optional `bonana` dependency (Node-only). Install it, or pass `openSocket` to bind your own DHT-RPC implementation.",
  )
}
