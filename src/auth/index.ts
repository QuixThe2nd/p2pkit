export type { Signer } from "./signer.js"
export { NoopSigner } from "./signer.js"
export { ECDSASigner, type ECDSASignerOptions } from "./ecdsa.js"
export { KeyManager, type KeyManagerOptions } from "./keymanager.js"

// Low-level primitives, exposed for advanced use (encryption, custom signers).
export {
  keccakUtf8,
  addressOf,
  publicKeyOf,
  generatePrivateKey,
  signPayload,
  recoverAddress,
  recoverPublicKey,
  sharedKey,
  encrypt,
  decrypt,
} from "./crypto.js"
