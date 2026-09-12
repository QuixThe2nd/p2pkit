import type { PeerId } from "../utils/types.js"

/**
 * The one thing p2pkit needs from a schema: a `parse` that returns the validated
 * value or throws. Zod schemas satisfy this structurally, so p2pkit validates
 * with zod **without importing it** — zod is an optional peer dependency, and any
 * parser with the same shape (a hand-written guard, valibot, …) works just as
 * well. This is the boundary that keeps the two ends of an API from drifting.
 */
export interface Schema<T = unknown> {
  parse(input: unknown): T
}

/** Infer a schema's output type — the p2pkit equivalent of `z.infer`. */
export type Infer<S> = S extends Schema<infer T> ? T : unknown

/** One method's request/response contract. */
export interface MethodSchema {
  request: Schema
  response: Schema
}

/** A full API contract: a named set of {@link MethodSchema}s. */
export type APISchema = Record<string, MethodSchema>

/** Context passed to every handler. */
export interface RPCContext {
  /** The sender's id — a *verified* address when a `signer` is configured (README §2.1). */
  readonly from: PeerId
}

/** A handler for one method: takes the parsed request (+ context), returns the response. */
export type Handler<M extends MethodSchema> = (
  request: Infer<M["request"]>,
  ctx: RPCContext,
) => Infer<M["response"]> | Promise<Infer<M["response"]>>

/** The full set of handlers a router must implement. */
export type Handlers<S extends APISchema> = { [K in keyof S]: Handler<S[K]> }
