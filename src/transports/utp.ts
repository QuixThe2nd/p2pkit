import type { Duplex } from "node:stream"
import type { Transport, TransportEvents } from "./types.js"
import type { PeerId } from "../utils/types.js"
import { Emitter } from "../utils/emitter.js"

export interface UTPTransportOptions {
  self: PeerId
  remote: PeerId
  /**
   * Dial the remote over µTP (uses the optional `utp-native` dependency). Omit
   * when injecting an already-connected `socket`.
   */
  connect?: { port: number; host?: string }
  /**
   * An already-open duplex stream to wrap instead of dialing — a `utp-native`
   * server connection, or any Node duplex (advanced/testing). Set together with
   * `connected: true` when the stream is already established.
   */
  socket?: Duplex
  /** Whether an injected `socket` is already connected (skip waiting for `connect`). */
  connected?: boolean
}

/** Shape of the `utp-native` module surface this transport uses. */
interface UTPModule {
  connect(port: number, host?: string): Duplex
}

// Widened to `string` so TS won't resolve the optional native module at build time.
const UTP_NATIVE: string = "utp-native"

async function loadUTP(): Promise<UTPModule | undefined> {
  try {
    const mod = (await import(/* @vite-ignore */ UTP_NATIVE)) as Record<string, unknown>
    const candidate = (mod["default"] ?? mod) as Partial<UTPModule>
    if (typeof candidate.connect === "function") return candidate as UTPModule
  } catch {
    /* not installed */
  }
  return undefined
}

/**
 * µTP (UDP) transport (README §6, Node-only). µTP is congestion-controlled and
 * NAT-friendly, a good fit for peers behind home routers. The wire is a byte
 * stream, so frames are newline-delimited JSON (a {@link ../wire.Frame} never
 * contains a raw newline once `JSON.stringify`d).
 *
 * Dialing requires the optional `utp-native` dependency; the listening side is
 * built by a µTP server and injected as `socket`.
 */
export class UTPTransport<T = unknown> implements Transport<T> {
  readonly remote: PeerId
  readonly name = "utp"

  private readonly self: PeerId
  private readonly emitter = new Emitter<TransportEvents<T>>()
  private socket?: Duplex
  private inbound = ""
  private open = false
  private closed = false

  constructor(options: UTPTransportOptions) {
    this.self = options.self
    this.remote = options.remote

    if (options.socket) {
      this.attach(options.socket, options.connected ?? true)
    } else if (options.connect) {
      void this.dial(options.connect)
    } else {
      throw new Error("UTPTransport requires either `socket` or `connect`")
    }
  }

  /** Bytes buffered in the underlying stream but not yet flushed to the socket. */
  get bufferedAmount(): number {
    return (this.socket as { writableLength?: number } | undefined)?.writableLength ?? 0
  }

  on<E extends keyof TransportEvents<T>>(event: E, handler: TransportEvents<T>[E]): void {
    this.emitter.on(event, handler)
  }

  send(message: T): Promise<void> {
    if (this.closed || !this.socket) return Promise.resolve()
    return new Promise<void>((resolve, reject) => {
      this.socket!.write(JSON.stringify(message) + "\n", err => (err ? reject(err) : resolve()))
    })
  }

  disconnect(): void {
    if (this.closed) return
    this.closed = true
    this.socket?.destroy()
    if (this.open) {
      this.open = false
      this.emitter.emit("disconnect")
    }
  }

  private async dial(target: { port: number; host?: string }): Promise<void> {
    const utp = await loadUTP()
    if (!utp) {
      this.emitter.emit(
        "error",
        new Error(
          "UTPTransport dialing requires the optional `utp-native` dependency (Node-only).",
        ),
      )
      return
    }
    this.attach(utp.connect(target.port, target.host), false)
  }

  private attach(socket: Duplex, alreadyConnected: boolean): void {
    this.socket = socket
    socket.on("data", (chunk: Buffer | string) => this.onData(chunk))
    socket.on("error", err =>
      this.emitter.emit("error", err instanceof Error ? err : new Error(String(err))),
    )
    socket.on("close", () => this.disconnect())
    if (alreadyConnected) queueMicrotask(() => this.markOpen())
    else socket.on("connect", () => this.markOpen())
  }

  private markOpen(): void {
    if (this.open || this.closed) return
    this.open = true
    this.emitter.emit("connect")
  }

  private onData(chunk: Buffer | string): void {
    this.inbound += typeof chunk === "string" ? chunk : chunk.toString("utf8")
    let newline: number
    while ((newline = this.inbound.indexOf("\n")) !== -1) {
      const line = this.inbound.slice(0, newline)
      this.inbound = this.inbound.slice(newline + 1)
      if (line.length === 0) continue
      try {
        this.emitter.emit("message", JSON.parse(line) as T)
      } catch (err) {
        this.emitter.emit("error", err instanceof Error ? err : new Error(String(err)))
      }
    }
  }
}
