import type { APISchema, Handlers } from "./schema.js"
import type { Router } from "./router.js"
import { createRouter } from "./router.js"

/**
 * `api.router` (README §2.1): call it with a full set of handlers, or
 * `api.router.partial({...})` with a subset — methods left out answer callers
 * with a `no_handler` error.
 */
export interface RouterFactory<S extends APISchema> {
  (handlers: Handlers<S>): Router<S>
  partial(handlers: Partial<Handlers<S>>): Router<S>
}

/** A defined API: the schema, a typed {@link RouterFactory}, ready to serve or call. */
export interface API<S extends APISchema> {
  readonly schema: S
  readonly router: RouterFactory<S>
}

/**
 * Declare an API schema once, in a module both peers import (README §2). From
 * it you get a typed client (`peer.client(api)`), a typed router
 * (`api.router({...})`), and runtime validation at every boundary.
 */
export function defineAPI<S extends APISchema>(schema: S): API<S> {
  const factory = ((handlers: Handlers<S>) => createRouter(schema, handlers)) as RouterFactory<S>
  factory.partial = handlers => createRouter(schema, handlers)
  return { schema, router: factory }
}
