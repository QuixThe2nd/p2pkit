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

export interface WebSocketSignallingOptions {
  /** Override the WebSocket constructor (defaults to native, then the `ws` package). */
  WebSocket?: WebSocketCtor
}

const OPEN = 1

/**
 * {@link SignallingChannel} over a WebSocket relay server. The server's only job
 * is to forward {@link SignallingMessage}s to the other peers in a room (one
 * room per URL). Uses the native `WebSocket` where available (browser, Node 21+,
 * Bun, Deno) and falls back to the `ws` package on older Node.
 */
export class WebSocketSignalling implements SignallingChannel {
  readonly ready: Promise<void>
  private ws?: WebSocketLike
  private readonly handlers = new Set<(message: SignallingMessage) => void>()
  private readonly outbox: string[] = []

  constructor(url: string, options: WebSocketSignallingOptions = {}) {
    this.ready = this.connect(url, options)
  }

  private async connect(url: string, options: WebSocketSignallingOptions): Promise<void> {
    const WS: WebSocketCtor =
      options.WebSocket ??
      (globalThis as { WebSocket?: WebSocketCtor }).WebSocket ??
      ((await import(/* @vite-ignore */ "ws")).default as unknown as WebSocketCtor)

    const ws = new WS(url)
    this.ws = ws

    ws.onmessage = ({ data }) => {
      const raw = typeof data === "string" ? data : String(data)
      let message: SignallingMessage
      try {
        message = JSON.parse(raw) as SignallingMessage
      } catch {
        return
      }
      for (const handler of [...this.handlers]) handler(message)
    }

    await new Promise<void>((resolve, reject) => {
      ws.onopen = () => {
        for (const raw of this.outbox.splice(0)) ws.send(raw)
        resolve()
      }
      ws.onerror = err => reject(err instanceof Error ? err : new Error("signalling socket error"))
    })
  }

  send(message: SignallingMessage): void {
    const raw = JSON.stringify(message)
    if (this.ws && this.ws.readyState === OPEN) this.ws.send(raw)
    else this.outbox.push(raw)
  }

  onMessage(handler: (message: SignallingMessage) => void): void {
    this.handlers.add(handler)
  }

  /** Close the underlying socket. */
  close(): void {
    this.ws?.close()
  }
}
