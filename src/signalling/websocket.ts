import type { SignallingChannel, SignallingMessage } from "./types.js"

/** The subset of the WebSocket API we rely on (native + the `ws` package both satisfy it). */
interface WebSocketLike {
  send(data: string): void
  close(): void
  readyState: number
  onopen: ((ev: unknown) => void) | null
  onmessage: ((ev: { data: unknown }) => void) | null
  onclose: ((ev: unknown) => void) | null
  onerror: ((ev: unknown) => void) | null
}

type WebSocketCtor = new (url: string) => WebSocketLike

export interface WebSocketReconnectOptions {
  /** First retry delay in ms; doubles each attempt. Default 500. */
  minBackoffMs?: number
  /** Cap on the retry delay in ms. Default 15000. */
  maxBackoffMs?: number
  /**
   * Largest retained outbound queue while the socket is down; once full, the
   * oldest message is dropped for each new one. Default 256.
   */
  maxQueue?: number
}

export interface WebSocketSignallingOptions {
  /** Override the WebSocket constructor (defaults to native, then the `ws` package). */
  WebSocket?: WebSocketCtor
  /**
   * Called once the socket has opened and then dropped (or errored). Lets a
   * caller switch to another route — peer-brokered signalling, say — rather than
   * keep writing into a dead socket. With `reconnect` on, also called when a
   * socket dies before ever opening: a room never reached is as much a loss as
   * one that dropped.
   */
  onDown?: () => void
  /**
   * Called on every socket open, the first and every reconnect. Consumers use
   * it to rejoin the room; the channel itself only flushes its outbox.
   */
  onUp?: () => void
  /**
   * Reconnect after the socket drops, with capped exponential backoff. Default
   * `false`: one socket, and a drop after open only reports through `onDown`.
   * At most one retry is ever pending; `close()` cancels it, and events from a
   * replaced socket are ignored.
   */
  reconnect?: WebSocketReconnectOptions | false
}

const OPEN = 1

const DEFAULT_MIN_BACKOFF_MS = 500
const DEFAULT_MAX_BACKOFF_MS = 15_000
const DEFAULT_MAX_QUEUE = 256

/**
 * {@link SignallingChannel} over a WebSocket relay server. The server's only job
 * is to forward {@link SignallingMessage}s to the other peers in a room (one
 * room per URL). Uses the native `WebSocket` where available (browser, Node 21+,
 * Bun, Deno) and falls back to the `ws` package on older Node.
 *
 * With `reconnect` enabled the channel survives the room going away: a drop is
 * reported through `onDown`, one bounded-backoff retry is scheduled at a time,
 * and every (re)open is reported through `onUp` so the consumer can rejoin.
 * `ready` still resolves on the first open only — a channel that never reaches
 * the room stays pending, which the broker settles via its own down signal.
 */
export class WebSocketSignalling implements SignallingChannel {
  readonly ready: Promise<void>
  private ws?: WebSocketLike
  private readonly handlers = new Set<(message: SignallingMessage) => void>()
  private readonly outbox: string[] = []
  private readonly onDown?: () => void
  private readonly onUp?: () => void
  private readonly reconnect: WebSocketReconnectOptions | false
  private readonly url: string
  private readonly options: WebSocketSignallingOptions
  private opened = false
  private closed = false
  private attempt = 0
  private retryTimer?: ReturnType<typeof setTimeout>
  private readonly resolveReady: () => void
  private readonly rejectReady: (err: Error) => void

  constructor(url: string, options: WebSocketSignallingOptions = {}) {
    this.url = url
    this.options = options
    this.onDown = options.onDown
    this.onUp = options.onUp
    this.reconnect = options.reconnect ?? false
    let resolve!: () => void
    let reject!: (err: Error) => void
    this.ready = new Promise<void>((res, rej) => {
      resolve = res
      reject = rej
    })
    // A lobby that never answers leaves `ready` pending on purpose; keep the
    // rejection handled so a one-shot failure never surfaces unhandled.
    this.ready.catch(() => {})
    this.resolveReady = resolve
    this.rejectReady = reject
    void this.connect()
  }

  private async connect(): Promise<void> {
    if (this.closed) return
    const WS: WebSocketCtor =
      this.options.WebSocket ??
      (globalThis as { WebSocket?: WebSocketCtor }).WebSocket ??
      ((await import(/* @vite-ignore */ "ws")).default as unknown as WebSocketCtor)

    const ws = new WS(this.url)
    this.ws = ws
    // Events from a socket a retry replaced (or a close() retired) must not
    // drive state: each attempt gets its own latch, and stale sockets fail this.
    let down = false
    const goDown = (err?: unknown) => {
      if (down || this.closed || this.ws !== ws) return
      down = true
      if (this.opened || this.reconnect) {
        this.onDown?.()
        this.scheduleRetry()
        return
      }
      this.rejectReady(err instanceof Error ? err : new Error("signalling socket error"))
    }

    ws.onmessage = ({ data }) => {
      if (this.closed || this.ws !== ws) return
      const raw = typeof data === "string" ? data : String(data)
      let message: SignallingMessage
      try {
        message = JSON.parse(raw) as SignallingMessage
      } catch {
        return
      }
      for (const handler of [...this.handlers]) handler(message)
    }

    ws.onopen = () => {
      if (down || this.closed || this.ws !== ws) return
      this.attempt = 0
      const firstOpen = !this.opened
      this.opened = true
      for (const raw of this.outbox.splice(0)) {
        try {
          ws.send(raw)
        } catch {
          /* dropped on reopen race */
        }
      }
      if (firstOpen) this.resolveReady()
      this.onUp?.()
    }
    ws.onerror = err => goDown(err)
    ws.onclose = () => goDown()
  }

  private scheduleRetry(): void {
    if (this.closed || !this.reconnect || this.retryTimer !== undefined) return
    const min = this.reconnect.minBackoffMs ?? DEFAULT_MIN_BACKOFF_MS
    const max = this.reconnect.maxBackoffMs ?? DEFAULT_MAX_BACKOFF_MS
    const delay = Math.min(max, min * 2 ** Math.min(this.attempt, 6))
    this.attempt += 1
    this.retryTimer = setTimeout(() => {
      this.retryTimer = undefined
      void this.connect().catch(() => this.scheduleRetry())
    }, delay)
    if (typeof this.retryTimer === "object" && "unref" in this.retryTimer) {
      ;(this.retryTimer as { unref: () => void }).unref()
    }
  }

  send(message: SignallingMessage): void {
    const raw = JSON.stringify(message)
    if (this.ws && this.ws.readyState === OPEN) {
      this.ws.send(raw)
      return
    }
    // Bounded while offline: once full, the oldest queued message makes room.
    const max = this.reconnect ? (this.reconnect.maxQueue ?? DEFAULT_MAX_QUEUE) : Infinity
    if (this.outbox.length >= max) this.outbox.shift()
    this.outbox.push(raw)
  }

  onMessage(handler: (message: SignallingMessage) => void): void {
    this.handlers.add(handler)
  }

  /** Whether the room is reachable through a live socket right now. */
  isOpen(): boolean {
    return this.ws?.readyState === OPEN
  }

  /** Close the underlying socket and cancel any pending retry. */
  close(): void {
    this.closed = true
    if (this.retryTimer !== undefined) {
      clearTimeout(this.retryTimer)
      this.retryTimer = undefined
    }
    this.outbox.length = 0
    this.ws?.close()
  }
}
