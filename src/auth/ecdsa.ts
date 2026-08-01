import type { Signer } from "./signer.js"
import type { PeerId } from "../utils/types.js"
import { KeyManager } from "./keymanager.js"
import {
  addressOf,
  publicKeyOf,
  signPayload,
  recoverAddress,
  recoverPublicKey,
  sharedKey,
  generatePrivateKey,
} from "./crypto.js"

export interface ECDSASignerOptions {
  /** Use a raw private key instead of a {@link KeyManager} (mainly for tests). */
  privateKey?: Uint8Array
}

/**
 * secp256k1 signer with Ethereum-style addresses (README §5). `id` is derived
 * from the keypair. Because key material may be loaded asynchronously
 * (`KeyManager`), await {@link ECDSASigner.ready} before reading `id` — `P2PKit`
 * does this during `start()`.
 */
export class ECDSASigner implements Signer {
  /** Resolves once the keypair is loaded and `id`/`publicKey` are available. */
  readonly ready: Promise<void>

  private priv?: Uint8Array
  private pub?: Uint8Array
  private address?: PeerId

  constructor(source: KeyManager | ECDSASignerOptions = {}) {
    this.ready = this.init(source)
  }

  private async init(source: KeyManager | ECDSASignerOptions): Promise<void> {
    if (source instanceof KeyManager) {
      this.priv = await source.getPrivateKey()
    } else {
      this.priv = source.privateKey ?? generatePrivateKey()
    }
    this.pub = publicKeyOf(this.priv)
    this.address = addressOf(this.pub)
  }

  /** The signer's address. Throws if read before {@link ready} resolves. */
  get id(): PeerId {
    if (!this.address) throw new Error("ECDSASigner not ready — await signer.ready first")
    return this.address
  }

  /** The signer's uncompressed public key. Throws before {@link ready}. */
  get publicKey(): Uint8Array {
    if (!this.pub) throw new Error("ECDSASigner not ready — await signer.ready first")
    return this.pub
  }

  sign(payload: string): Promise<string> {
    if (!this.priv) throw new Error("ECDSASigner not ready — await signer.ready first")
    return signPayload(this.priv, payload)
  }

  async verify(signature: string, payload: string, from: PeerId): Promise<boolean> {
    try {
      return recoverAddress(signature, payload).toLowerCase() === from.toLowerCase()
    } catch {
      return false
    }
  }

  /**
   * Derive the ECDH shared key with a remote peer, using the remote's public
   * key recovered from a handshake signature. Used for end-to-end encryption
   * (README §5).
   */
  sharedKeyWith(remotePublicKey: Uint8Array): Uint8Array {
    if (!this.priv) throw new Error("ECDSASigner not ready — await signer.ready first")
    return sharedKey(this.priv, remotePublicKey)
  }

  /** Recover a peer's public key from a signature they produced. */
  static recoverPublicKey(signature: string, payload: string): Uint8Array {
    return recoverPublicKey(signature, payload)
  }
}
