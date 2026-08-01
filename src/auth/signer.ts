import type { PeerId } from "../utils/types.js"

/**
 * Proves ownership of a {@link PeerId}. With a signer set, peers verify each
 * other's identity on connection (README §5). Bring your own scheme by
 * implementing this interface (README §8).
 */
export interface Signer {
  /** This signer's peer id (address). */
  readonly id: PeerId
  /** Produce a signature over `payload`. */
  sign(payload: string): Promise<string>
  /** Verify `signature` was produced by `from` over `payload`. */
  verify(signature: string, payload: string, from: PeerId): Promise<boolean>
}

/**
 * A signer that signs nothing and trusts everyone. Used when no `signer` is
 * configured: the handshake still runs but identities are unverified and
 * `id` is whatever `self` the caller supplied.
 */
export class NoopSigner implements Signer {
  readonly id: PeerId
  constructor(id: PeerId) {
    this.id = id
  }
  sign(_payload: string): Promise<string> {
    return Promise.resolve("")
  }
  verify(_signature: string, _payload: string, _from: PeerId): Promise<boolean> {
    return Promise.resolve(true)
  }
}
