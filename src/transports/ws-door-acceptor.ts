import type { PeerId } from "../utils/types.js"
import type { Transport } from "./types.js"
import type { Frame } from "../wire/index.js"
import type { DoorSocket, WSTransportOptions } from "./ws-door.js"
import { WSTransport } from "./ws-door.js"

/**
 * The slice of `P2PKit` a {@link DoorAcceptor} drives. `P2PKit` satisfies it out
 * of the box — a door is not a separate kind of node, just a peer that listens.
 */
export interface DoorHost {
  /** This node's id, offered as the door's identity in the welcome exchange. */
  readonly selfId: PeerId
  /** Adopt an open link as a direct peer connection. */
  acceptLink(transport: Transport<Frame>): void
}

/** The `ws` server constructor, injectable so tests need no network. */
export interface DoorServerLike {
  on(event: "connection", handler: (socket: DoorSocket, request: unknown) => void): void
  close(cb?: () => void): void
}

/** The slice of `node:http`/`node:https` server the acceptor owns. */
export interface DoorHttpServerLike {
  listen(port: number, hostname?: string, cb?: () => void): void
  close(cb?: () => void): void
  address(): unknown
  once(event: "listening" | "error", cb: (arg?: Error) => void): void
}

export interface DoorAcceptorOptions {
  /** The kit whose mesh every accepted link joins. */
  host: DoorHost
  /** Port to bind. Default `0` — an ephemeral port. */
  port?: number
  /** Interface to bind. Default all. */
  hostname?: string
  /** Reject handshakes whose URL path is not this one. Default: accept any. */
  path?: string
  /** Serve `wss://` rather than `ws://` (material for `node:https`). */
  tls?: { cert: string | Uint8Array; key: string | Uint8Array }
  /** Capabilities advertised in the welcome exchange. Default `["ws"]`. */
  caps?: string[]
  /** Keepalive probe interval in ms; `0` disables. Default 15000. */
  keepAliveMs?: number
  /** Silence window after which the link is treated as dead. Default 10000. */
  keepAliveTimeoutMs?: number
  /** Override the WebSocket server constructor (defaults to the `ws` package). */
  WebSocketServer?: new (options: unknown) => DoorServerLike
}

/**
 * The door: a WebSocket endpoint whose every connection becomes a peer link
 * into one kit. That is the whole server-side feature — there is no relay behind
 * it, no room bookkeeping, and nothing to forward. A browser that knows nothing
 * but this URL attaches to the mesh as surely as if it had dialled a friend,
 * because the thing it connected to *is* a friend.
 *
 * One acceptor is one mesh. Run several, each with its own `path` or port, for
 * several meshes.
 */
export class DoorAcceptor {
  private readonly options: DoorAcceptorOptions
  private server?: DoorServerLike
  private httpServer?: DoorHttpServerLike
  private listening = false
  private closed = false
  private bound?: { port: number; hostname?: string }
  private readonly links = new Set<WSTransport>()

  constructor(options: DoorAcceptorOptions) {
    this.options = options
  }

  /** `ws://host:port` — or `wss://…` — once listening. */
  get url(): string {
    if (!this.bound) throw new Error("door is not listening")
    const scheme = this.options.tls ? "wss" : "ws"
    const host = this.bound.hostname ?? "127.0.0.1"
    return `${scheme}://${host}:${this.bound.port}`
  }

  /** The bound port, once listening. */
  get port(): number {
    if (!this.bound) throw new Error("door is not listening")
    return this.bound.port
  }

  /** How many links the door is holding open right now. */
  get linkCount(): number {
    return this.links.size
  }

  /** Bind and start accepting. */
  async listen(): Promise<void> {
    if (this.listening) return
    if (this.closed) throw new Error("door is closed")

    const WSS =
      this.options.WebSocketServer ??
      ((await import(/* @vite-ignore */ "ws")).WebSocketServer as unknown as new (
        options: unknown,
      ) => DoorServerLike)
    if (this.closed) return

    // The acceptor owns its HTTP server either way, so one code path serves
    // ws:// and wss:// and the bound port is never guessed at.
    const httpServer = this.options.tls
      ? await this.httpsServer(this.options.tls)
      : await this.plainServer()
    const server = new WSS({ server: httpServer, path: this.options.path })

    await new Promise<void>((resolve, reject) => {
      httpServer.once("listening", () => resolve())
      httpServer.once("error", err => reject(err instanceof Error ? err : new Error(String(err))))
      httpServer.listen(this.options.port ?? 0, this.options.hostname)
    })

    this.httpServer = httpServer
    this.server = server
    this.bound = {
      port: readPort(httpServer.address()),
      hostname: this.options.hostname,
    }
    server.on("connection", (socket, request) => this.accept(socket, request))
    this.listening = true
  }

  /** Stop listening and drop every link. */
  async close(): Promise<void> {
    if (this.closed) return
    this.closed = true
    this.listening = false
    for (const link of [...this.links]) link.disconnect()
    this.links.clear()

    const server = this.server
    this.server = undefined
    const httpServer = this.httpServer
    this.httpServer = undefined
    this.bound = undefined

    await new Promise<void>(resolve => {
      let pending = 0
      const done = () => {
        pending -= 1
        if (pending <= 0) resolve()
      }
      if (server) {
        pending += 1
        server.close(done)
      }
      if (httpServer) {
        pending += 1
        httpServer.close(done)
      }
      if (pending === 0) resolve()
    })
  }

  // ---- one connection, one peer link -----------------------------------

  private accept(socket: DoorSocket, request: unknown): void {
    const drop = () => {
      try {
        socket.close()
      } catch {
        /* already gone */
      }
    }
    if (this.closed) return drop()
    if (this.options.path !== undefined && !urlPathOf(request).startsWith(this.options.path)) {
      return drop()
    }

    let selfId: PeerId
    try {
      selfId = this.options.host.selfId
    } catch {
      return drop() // the kit has not started, so there is no identity to offer
    }

    const options: WSTransportOptions = {
      socket,
      self: selfId,
      caps: this.options.caps,
      keepAliveMs: this.options.keepAliveMs,
      keepAliveTimeoutMs: this.options.keepAliveTimeoutMs,
    }
    const link = new WSTransport(options)
    this.links.add(link)
    const forget = () => this.links.delete(link)
    link.on("disconnect", forget)
    link.on("error", forget)

    void link.identified
      .then(() => {
        if (this.closed || !link.connected) return
        this.options.host.acceptLink(link)
      })
      .catch(() => {
        /* a link that never identified itself is already closed */
      })
  }

  private async plainServer(): Promise<DoorHttpServerLike> {
    const http = await import(/* @vite-ignore */ "node:http")
    return http.createServer() as unknown as DoorHttpServerLike
  }

  private async httpsServer(tls: { cert: string | Uint8Array; key: string | Uint8Array }) {
    const https = await import(/* @vite-ignore */ "node:https")
    // `Uint8Array` is the universal spelling; `node:https` wants `Buffer`.
    const material = { cert: tls.cert as Buffer, key: tls.key as Buffer }
    return https.createServer(material) as unknown as DoorHttpServerLike
  }
}

/** The URL path of a handshake request, for `path`-scoped doors. */
function urlPathOf(request: unknown): string {
  const url = (request as { url?: string } | undefined)?.url
  if (typeof url !== "string") return "/"
  try {
    return new URL(url, "ws://door.invalid").pathname
  } catch {
    return "/"
  }
}

function readPort(address: unknown): number {
  return address && typeof address === "object" && "port" in address
    ? (address as { port: number }).port
    : 0
}
