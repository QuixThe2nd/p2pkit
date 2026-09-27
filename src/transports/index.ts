export type { Transport, TransportEvents } from "./types.js"
export {
  RTCTransport,
  RTCTransportConnectTimeoutError,
  RTCTransportBackpressureDropError,
  directIceServers,
  validateDirectCandidate,
  validateDirectDescription,
  type RTCTransportOptions,
  type RTCTransportEvents,
  type RTCChannelSpec,
} from "./rtc.js"
export {
  RTCDataChannelSendQueue,
  RTC_SEND_QUEUE_FLUSH_THRESHOLD,
  type RTCDataChannelLike,
  type RTCDataChannelSendQueueOptions,
} from "./rtc-send-queue.js"
export { HTTPTransport, type HTTPTransportOptions } from "./http.js"
// A WebSocket link: the client half is universal, the acceptor is Node-only and
// imports `ws` + `node:http` lazily, so both are safe to import anywhere.
export {
  WSTransport,
  DOOR_MIN_BACKOFF_MS,
  DOOR_MAX_BACKOFF_MS,
  type WSTransportOptions,
  type DoorOptions,
  type DoorSocket,
  type PongRegistrar,
} from "./ws-door.js"
export {
  DoorAcceptor,
  type DoorAcceptorOptions,
  type DoorHost,
  type DoorServerLike,
  type DoorHttpServerLike,
} from "./ws-door-acceptor.js"
// Node-only transports; their network deps (utp-native, bonana) are optional and
// loaded lazily, so importing these is safe even when the deps aren't installed.
export { UTPTransport, type UTPTransportOptions } from "./utp.js"
export { DHTTransport, type DHTTransportOptions, type DHTRPCSocket } from "./dht.js"
export {
  chooseTransport,
  capsFor,
  isInitiator,
  DEFAULT_TRANSPORT_ORDER,
  type TransportName,
} from "./negotiate.js"
