import type { PeerId } from "../utils/types.js"
import type { API } from "./api.js"
import type { APISchema, Infer } from "./schema.js"
import type { DispatchResult } from "./router.js"
import { RPCError } from "./error.js"

/**
 * A typed client for an API (README §2.2). Each method takes the request type
 * and resolves to the response type **or** an {@link RPCError} — errors are
 * returned, not thrown, so a single call site handles both.
 */
export type Client<S extends APISchema> = {
  [K in keyof S]: (request: Infer<S[K]["request"]>) => Promise<Infer<S[K]["response"]> | RPCError>
}

/**
 * What {@link createClient} needs from a peer: its id, and a way to send a
 * request and await the correlated response. `Peer` implements this — the
 * request/response correlation and timeout live there, next to the transport.
 */
export interface RPCRequester {
  readonly remote: PeerId
  requestRPC(method: string, body: unknown, timeoutMs: number): Promise<DispatchResult | RPCError>
}

export interface ClientOptions {
  /** Per-call timeout in ms before the call resolves to an `RPCError("timeout")`. Default 30000. */
  timeout?: number
}

const DEFAULT_TIMEOUT = 30_000

/** Build a {@link Client} that issues calls through `requester`. */
export function createClient<S extends APISchema>(
  requester: RPCRequester,
  api: API<S>,
  opts: ClientOptions = {},
): Client<S> {
  const timeout = opts.timeout ?? DEFAULT_TIMEOUT
  const client: Record<string, (request: unknown) => Promise<unknown>> = {}

  for (const method of Object.keys(api.schema)) {
    const methodSchema = api.schema[method]!
    client[method] = async (request: unknown): Promise<unknown> => {
      // Validate the request locally so a bad call fails fast, before the round-trip.
      let body: unknown
      try {
        body = methodSchema.request.parse(request)
      } catch (err) {
        return new RPCError(
          "invalid_request",
          method,
          err instanceof Error ? err.message : String(err),
        )
      }

      const result = await requester.requestRPC(method, body, timeout)
      if (result instanceof RPCError) return result
      if (!result.ok) return RPCError.from(result.err)

      // Validate the response too — the caller's static type is only sound if the
      // bytes on the wire actually match it.
      try {
        return methodSchema.response.parse(result.body)
      } catch (err) {
        return new RPCError(
          "invalid_response",
          method,
          err instanceof Error ? err.message : String(err),
        )
      }
    }
  }

  return client as Client<S>
}
