import { WebSocketServer, type WebSocket } from "ws"
import type { AddressInfo } from "node:net"

export interface MockSignallingServer {
  /** `ws://127.0.0.1:<port>` — one room; pass to `WebSocketSignalling`. */
  url: string
  close(): Promise<void>
}

/**
 * A minimal in-process signalling relay for tests: forwards every message from
 * one client to all other connected clients (one room per server). Mirrors what
 * a real `WebSocketSignalling` server must do — relay, never inspect.
 */
export async function createMockSignallingServer(): Promise<MockSignallingServer> {
  const wss = new WebSocketServer({ host: "127.0.0.1", port: 0 })
  const clients = new Set<WebSocket>()

  wss.on("connection", socket => {
    clients.add(socket)
    socket.on("message", data => {
      const raw = typeof data === "string" ? data : data.toString()
      for (const peer of clients) {
        if (peer !== socket && peer.readyState === peer.OPEN) peer.send(raw)
      }
    })
    socket.on("close", () => clients.delete(socket))
  })

  await new Promise<void>(resolve => wss.on("listening", () => resolve()))
  const { port } = wss.address() as AddressInfo

  return {
    url: `ws://127.0.0.1:${port}`,
    close: () =>
      new Promise<void>(resolve => {
        for (const c of clients) c.terminate()
        wss.close(() => resolve())
      }),
  }
}
