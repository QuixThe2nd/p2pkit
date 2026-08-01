/**
 * Returned (not thrown) by {@link promiseWithTimeout} when the deadline elapses.
 * Callers test with `result instanceof ErrorTimeout` rather than catching.
 */
export class ErrorTimeout extends Error {
  readonly ms: number
  constructor(ms: number) {
    super(`Operation timed out after ${ms}ms`)
    this.name = "ErrorTimeout"
    this.ms = ms
  }
}

/**
 * Race `p` against a `ms` deadline. Resolves with `p`'s value if it settles
 * first, or with an {@link ErrorTimeout} if the deadline wins. A genuine
 * rejection from `p` is propagated (rejects), so it is never confused with a
 * timeout.
 */
export function promiseWithTimeout<T>(p: Promise<T>, ms: number): Promise<T | ErrorTimeout> {
  return new Promise<T | ErrorTimeout>((resolve, reject) => {
    const timer = setTimeout(() => resolve(new ErrorTimeout(ms)), ms)
    p.then(
      value => {
        clearTimeout(timer)
        resolve(value)
      },
      error => {
        clearTimeout(timer)
        reject(error)
      },
    )
  })
}
