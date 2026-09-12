// p2pkit/rpc — typed request/response and one-way messages (README §2).
export { defineAPI, type API, type RouterFactory } from "./api.js"
export { createRouter, type Router, type DispatchResult } from "./router.js"
export { createClient, type Client, type RPCRequester, type ClientOptions } from "./client.js"
export {
  defineProtocol,
  type Protocol,
  type AnyProtocol,
  type ProtocolSchema,
  type ProtocolMessage,
} from "./protocol.js"
export { RPCError, type RPCErrorInfo } from "./error.js"
export type {
  Schema,
  Infer,
  MethodSchema,
  APISchema,
  RPCContext,
  Handler,
  Handlers,
} from "./schema.js"
