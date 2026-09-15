// Type surface of the bootstrapping server. The wire-protocol types are the
// single source of truth shared with the p2pkit client library
// (src/signalling/lobby.ts, importable as `p2pkit/signalling`). The build
// (build.mjs) copies that file in as src/lobby.ts — the sibling referenced
// below — before compiling, so the emitted dist/*.d.ts never reference files
// outside this package.

export type * from './lobby.js';

import type { Server as HttpServer } from 'node:http';
import type { WebSocket, WebSocketServer } from 'ws';

/** Per-socket `sig` throughput policy (fixed window + strikes). */
export interface RateLimitOptions {
  /** Allowed `sig` frames per window before `rate_limited` errors. */
  max: number;
  /** Window length in milliseconds. */
  windowMs: number;
  /** Strikes (over-limit windows) before the socket is closed with 1008. */
  strikeLimit: number;
}

/** Options accepted by {@link createSignalingServer}; all optional. */
export interface SignalingServerOptions {
  host?: string;
  port?: number;
  /** Cap on one incoming text frame; larger frames get `too_large`. */
  maxMessageBytes?: number;
  /** Transport cap (`ws` maxPayload); leaves room for the `too_large` error. */
  wsMaxPayloadBytes?: number;
  rateLimit?: Partial<RateLimitOptions>;
  /** Waiting finders beyond this get `lobby_full`. */
  maxQueue?: number;
  /** WS protocol keepalive interval. */
  pingIntervalMs?: number;
}

/** Options after merging user input onto {@link DEFAULTS}: everything resolved. */
export interface SignalingServerSettings {
  host: string;
  port: number;
  maxMessageBytes: number;
  wsMaxPayloadBytes: number;
  rateLimit: RateLimitOptions;
  maxQueue: number;
  pingIntervalMs: number;
}

/** Mutable rate-limit counters, one per socket. */
export interface RateState {
  count: number;
  windowStart: number;
  strikes: number;
}

/** Per-connection state; `partner` links the two sides of a matched pair. */
export interface SocketState {
  ws: WebSocket;
  partner: SocketState | null;
  queued: boolean;
  rate: RateState;
  alive: boolean;
}

/** Snapshot returned by {@link SignalingContext.stats}. */
export interface SignalingStats {
  /** Clients currently queued. */
  waiting: number;
  /** Matched pairs. */
  pairs: number;
  /** Connected sockets. */
  peers: number;
}

/** What {@link createSignalingServer} returns. */
export interface SignalingContext {
  httpServer: HttpServer;
  wss: WebSocketServer;
  queue: SocketState[];
  states: Set<SocketState>;
  options: SignalingServerSettings;
  stats(): SignalingStats;
  /** Terminates clients, clears timers, closes the HTTP server. Resolves when done. */
  close(): Promise<void>;
}
