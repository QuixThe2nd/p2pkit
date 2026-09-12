import type { APISchema, Handlers, RPCContext } from "./schema.js"
import type { RPCErrorInfo } from "./error.js"
import { RPCError } from "./error.js"

/** The outcome of dispatching one request — success carries `body`, failure `err`. */
export type DispatchResult = { ok: true; body: unknown } | { ok: false; err: RPCErrorInfo }

/**
 * Server side of an API (README §2.1). Holds the schema and the handlers, and
 * validates the request going in and the response coming out — the two runtime
 * boundaries that stop the ends from drifting. `P2PKit`/`Peer` call
 * {@link Router.dispatch} for every incoming `req` frame.
 */
export interface Router<S extends APISchema = APISchema> {
  readonly schema: S
  /** Whether a handler is registered for `method`. */
  has(method: string): boolean
  /** Validate, run the handler, validate the response. Never throws. */
  dispatch(method: string, request: unknown, ctx: RPCContext): Promise<DispatchResult>
}

const detail = (err: unknown): string => (err instanceof Error ? err.message : String(err))

/** Build a {@link Router} from a schema and a (possibly partial) set of handlers. */
export function createRouter<S extends APISchema>(
  schema: S,
  handlers: Partial<Handlers<S>>,
): Router<S> {
  return {
    schema,
    has: method => method in schema && handlers[method as keyof S] !== undefined,

    async dispatch(method, request, ctx): Promise<DispatchResult> {
      const methodSchema = schema[method as keyof S]
      const handler = handlers[method as keyof S]
      if (!methodSchema || !handler) {
        return { ok: false, err: { code: "no_handler", method } }
      }

      let parsed: unknown
      try {
        parsed = methodSchema.request.parse(request)
      } catch (err) {
        return { ok: false, err: { code: "invalid_request", method, message: detail(err) } }
      }

      let output: unknown
      try {
        output = await handler(parsed as never, ctx)
      } catch (err) {
        // A handler may throw RPCError to answer with a coded failure; anything
        // else is an unexpected fault surfaced as `handler_error`.
        if (err instanceof RPCError) {
          return { ok: false, err: { code: err.code, method, message: err.message || undefined } }
        }
        return { ok: false, err: { code: "handler_error", method, message: detail(err) } }
      }

      try {
        return { ok: true, body: methodSchema.response.parse(output) }
      } catch (err) {
        return { ok: false, err: { code: "invalid_response", method, message: detail(err) } }
      }
    },
  }
}
