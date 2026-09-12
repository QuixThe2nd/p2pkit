import type { Infer, Schema } from "./schema.js"

/** A set of one-way message types, each key a message `type` (README §2.3). */
export type ProtocolSchema = Record<string, Schema>

/**
 * The discriminated union of messages a protocol describes. Each key `K` becomes
 * `{ type: K } & <its fields>`, giving the same `{ type, ... }` shape as the
 * quick start — so `msg.type === "chat"` narrows `msg` to the chat fields.
 */
export type ProtocolMessage<S extends ProtocolSchema> = {
  [K in keyof S]: { type: K } & Infer<S[K]>
}[keyof S]

/**
 * The protocol surface consumers (`P2PKit`, `Topic`) depend on, erased of its
 * schema type. A concrete `Protocol<S>` is invariant in `S` — its `parse`/
 * `validate` mention `S` in their result — so an option typed `Protocol<S>`
 * couldn't accept an arbitrary user protocol. This structural shape can.
 */
export interface AnyProtocol {
  parse(message: unknown): unknown
  validate(message: unknown): unknown
}

/**
 * A defined one-way message protocol (README §2.3, §3). Validates the fields of
 * each `{ type, ... }` message against the schema registered for its `type`.
 */
export interface Protocol<S extends ProtocolSchema> extends AnyProtocol {
  readonly schema: S
  /** Validate a message, returning it typed — or throwing if it doesn't match. */
  parse(message: unknown): ProtocolMessage<S>
  /** Non-throwing variant used on the receive path: returns `undefined` on mismatch. */
  validate(message: unknown): ProtocolMessage<S> | undefined
}

/**
 * Type one-way messages (README §2.3). Each key becomes a message `type`; the
 * schema validates that type's fields. Pass the result as `protocol` to
 * `P2PKit`/`kit.topic` and the message type is inferred and validated.
 */
export function defineProtocol<S extends ProtocolSchema>(schema: S): Protocol<S> {
  const parse = (message: unknown): ProtocolMessage<S> => {
    if (typeof message !== "object" || message === null || !("type" in message)) {
      throw new Error("protocol message must be an object with a `type`")
    }
    const { type, ...fields } = message as { type: unknown } & Record<string, unknown>
    const s = typeof type === "string" ? schema[type] : undefined
    if (!s) throw new Error(`unknown message type: ${String(type)}`)
    // Validate the fields (excluding `type`), then re-attach `type`.
    return { type, ...(s.parse(fields) as object) } as ProtocolMessage<S>
  }

  return {
    schema,
    parse,
    validate: message => {
      try {
        return parse(message)
      } catch {
        return undefined
      }
    },
  }
}
