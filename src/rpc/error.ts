/** Serialisable error carried on a {@link ../wire.ResFrame}. */
export interface RPCErrorInfo {
  /** Machine-readable failure code, e.g. `no_handler`, `invalid_request`, `timeout`. */
  code: string
  /** The method the call targeted. */
  method: string
  /** Optional human-readable detail. */
  message?: string
}

/**
 * The value a typed client call resolves to on failure (README §2.2). Returned,
 * not thrown — callers test with `result instanceof RPCError`, mirroring
 * {@link ../utils.ErrorTimeout}. Handlers may also `throw new RPCError(code)` to
 * answer a caller with a coded error; the router fills in `method`.
 */
export class RPCError extends Error {
  readonly code: string
  readonly method: string

  constructor(code: string, method = "", message?: string) {
    super(message ?? (method ? `${code} (${method})` : code))
    this.name = "RPCError"
    this.code = code
    this.method = method
  }

  /** Rebuild an {@link RPCError} from the wire representation. */
  static from(info: RPCErrorInfo): RPCError {
    return new RPCError(info.code, info.method, info.message)
  }

  /** The wire representation. */
  toInfo(): RPCErrorInfo {
    return { code: this.code, method: this.method, message: this.message || undefined }
  }
}
