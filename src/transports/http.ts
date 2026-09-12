import { createServer, request, type Server, type IncomingMessage } from "node:http"
import type { Transport, TransportEvents } from "./types.js"
import type { PeerId } from "../utils/types.js"
import { Emitter } from "../utils/emitter.js"

export interface HTTPTransportOptions {
  self: PeerId
  remote: PeerId
  /**
   * Dial the remote's endpoint (client role), e.g. `"http://host:9000"`. The
   * client POSTs its outbound frames and polls for inbound ones on the response.
   */
  url?: string
  /** Listen for the remote to dial us (server role). */
  listen?: { port: number; host?: string }
  /** Client poll interval in ms for inbound frames when idle. Default 1000. */
  pollInterval?: number
}

interface Envelope {
  from: PeerId
  frames: unknown[]
}

const DEFAULT_POLL_INTERVAL = 1000

/**
 * HTTP(S) request/response transport (README §6). One participant must be a Node
 * peer with a reachable port (`listen`); the other dials it (`url`). HTTP is
 * one-directional per exchange, so bidirectionality is emulated: the dialer
 * POSTs its outbound frames and the listener returns any queued frames for that
 * peer on the response body, with the dialer polling while idle.
 */
export class HTTPTransport<T = unknown> implements Transport<T> {
  readonly remote: PeerId
  readonly name = "http"
  bufferedAmount = 0

  private readonly self: PeerId
  private readonly emitter = new Emitter<TransportEvents<T>>()
  private readonly outbound: unknown[] = [] // queued for the remote (server role)
  private readonly pollInterval: number
  private readonly url?: string
  private server?: Server
  private pollTimer?: ReturnType<typeof setInterval>
  private connected = false
  private closed = false

  /** Server role: resolves with the bound port once listening (useful with `port: 0`). */
  readonly listening?: Promise<number>
  private resolveListening?: (port: number) => void

  constructor(options: HTTPTransportOptions) {
    this.self = options.self
    this.remote = options.remote
    this.url = options.url
    this.pollInterval = options.pollInterval ?? DEFAULT_POLL_INTERVAL

    if (options.listen) {
      this.listening = new Promise<number>(resolve => (this.resolveListening = resolve))
      this.startServer(options.listen)
    } else if (options.url) {
      this.startClient()
    } else {
      throw new Error("HTTPTransport requires either `listen` (server) or `url` (client)")
    }
  }

  on<E extends keyof TransportEvents<T>>(event: E, handler: TransportEvents<T>[E]): void {
    this.emitter.on(event, handler)
  }

  async send(message: T): Promise<void> {
    if (this.closed) return
    if (this.url) {
      await this.post([message]) // client: deliver immediately on a request
    } else {
      this.outbound.push(message) // server: hand off on the next poll/POST
    }
  }

  disconnect(): void {
    if (this.closed) return
    this.closed = true
    if (this.pollTimer) clearInterval(this.pollTimer)
    this.server?.close()
    if (this.connected) this.emitter.emit("disconnect")
  }

  // ---- server role ------------------------------------------------------

  private startServer(listen: { port: number; host?: string }): void {
    this.server = createServer((req, res) => {
      if (req.method !== "POST") {
        res.statusCode = 405
        res.end()
        return
      }
      void readBody(req)
        .then(body => {
          const envelope = JSON.parse(body || '{"from":"","frames":[]}') as Envelope
          this.markConnected()
          for (const frame of envelope.frames) this.deliver(frame)
          const drained = this.outbound.splice(0)
          res.setHeader("content-type", "application/json")
          res.end(JSON.stringify({ from: this.self, frames: drained } satisfies Envelope))
        })
        .catch(err => {
          res.statusCode = 400
          res.end()
          this.emitter.emit("error", err instanceof Error ? err : new Error(String(err)))
        })
    })
    this.server.on("error", err => this.emitter.emit("error", err))
    this.server.on("listening", () => {
      const addr = this.server?.address()
      if (addr && typeof addr === "object") this.resolveListening?.(addr.port)
    })
    this.server.listen(listen.port, listen.host)
  }

  // ---- client role ------------------------------------------------------

  private startClient(): void {
    // Poll for inbound frames while otherwise idle.
    this.pollTimer = setInterval(() => void this.post([]).catch(() => {}), this.pollInterval)
    if (typeof this.pollTimer === "object" && "unref" in this.pollTimer) {
      ;(this.pollTimer as { unref: () => void }).unref()
    }
    // Kick an initial poll so `connect` fires promptly.
    void this.post([]).catch(err =>
      this.emitter.emit("error", err instanceof Error ? err : new Error(String(err))),
    )
  }

  private post(frames: unknown[]): Promise<void> {
    const url = this.url!
    const payload = JSON.stringify({ from: this.self, frames } satisfies Envelope)
    return new Promise<void>((resolve, reject) => {
      const req = request(
        url,
        { method: "POST", headers: { "content-type": "application/json" } },
        res => {
          void readBody(res)
            .then(body => {
              this.markConnected()
              if (body) {
                const envelope = JSON.parse(body) as Envelope
                for (const frame of envelope.frames) this.deliver(frame)
              }
              resolve()
            })
            .catch(reject)
        },
      )
      req.on("error", reject)
      req.end(payload)
    })
  }

  // ---- shared -----------------------------------------------------------

  private markConnected(): void {
    if (this.connected || this.closed) return
    this.connected = true
    this.emitter.emit("connect")
  }

  private deliver(frame: unknown): void {
    if (!this.closed) this.emitter.emit("message", frame as T)
  }
}

function readBody(stream: IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    let data = ""
    stream.setEncoding("utf8")
    stream.on("data", chunk => (data += chunk))
    stream.on("end", () => resolve(data))
    stream.on("error", reject)
  })
}
