import * as secp from "@noble/secp256k1"
import { hmac } from "@noble/hashes/hmac"
import { sha256 } from "@noble/hashes/sha256"
import { keccak_256 } from "@noble/hashes/sha3"
import { xchacha20poly1305 } from "@noble/ciphers/chacha"

// Wire @noble/hashes into @noble/secp256k1 so signing works in every runtime
// (the library ships without a bundled HMAC to stay dependency-light).
secp.etc.hmacSha256Async = async (key, ...msgs) => hmac(sha256, key, secp.etc.concatBytes(...msgs))
secp.etc.hmacSha256Sync = (key, ...msgs) => hmac(sha256, key, secp.etc.concatBytes(...msgs))

const { bytesToHex, hexToBytes, concatBytes } = secp.etc
const utf8 = new TextEncoder()

export { bytesToHex, hexToBytes }

/** keccak-256 of a UTF-8 string, matching Ethereum's hashing. */
export function keccakUtf8(payload: string): Uint8Array {
  return keccak_256(utf8.encode(payload))
}

/** Generate a fresh 32-byte secp256k1 private key. */
export function generatePrivateKey(): Uint8Array {
  return secp.utils.randomPrivateKey()
}

/** Uncompressed (65-byte) public key for a private key. */
export function publicKeyOf(priv: Uint8Array): Uint8Array {
  return secp.getPublicKey(priv, false)
}

/**
 * Ethereum-style address (EIP-55 checksummed `0x…`) for an uncompressed public
 * key: last 20 bytes of `keccak256(pubkey[1:])`.
 */
export function addressOf(pubUncompressed: Uint8Array): string {
  const hashed = keccak_256(pubUncompressed.subarray(1))
  return toChecksumAddress(bytesToHex(hashed.subarray(-20)))
}

function toChecksumAddress(hexNoPrefix: string): string {
  const lower = hexNoPrefix.toLowerCase()
  const hash = bytesToHex(keccak_256(utf8.encode(lower)))
  let out = "0x"
  for (let i = 0; i < lower.length; i++) {
    out += parseInt(hash[i]!, 16) >= 8 ? lower[i]!.toUpperCase() : lower[i]!
  }
  return out
}

/**
 * Sign `payload` with `priv`. Returns `0x` + 64-byte compact signature +
 * 1-byte recovery id (132 hex chars), so the signer's public key (and thus
 * address) can be recovered from the signature alone.
 */
export async function signPayload(priv: Uint8Array, payload: string): Promise<string> {
  const sig = await secp.signAsync(keccakUtf8(payload), priv)
  return "0x" + sig.toCompactHex() + sig.recovery.toString(16).padStart(2, "0")
}

function parseSignature(signature: string): { sig: secp.Signature; hashOf: (p: string) => Uint8Array } {
  const hex = signature.startsWith("0x") ? signature.slice(2) : signature
  if (hex.length !== 130) throw new Error("malformed signature")
  const recovery = parseInt(hex.slice(128), 16)
  const sig = secp.Signature.fromCompact(hex.slice(0, 128)).addRecoveryBit(recovery)
  return { sig, hashOf: keccakUtf8 }
}

/** Recover the uncompressed public key that produced `signature` over `payload`. */
export function recoverPublicKey(signature: string, payload: string): Uint8Array {
  const { sig, hashOf } = parseSignature(signature)
  return sig.recoverPublicKey(hashOf(payload)).toRawBytes(false)
}

/** Recover the signer's address from a signature over `payload`. */
export function recoverAddress(signature: string, payload: string): string {
  return addressOf(recoverPublicKey(signature, payload))
}

/**
 * ECDH shared symmetric key between our private key and a remote uncompressed
 * public key, hashed to 32 bytes. Both peers derive the same key from their
 * identity keypairs, so no relay ever sees it.
 */
export function sharedKey(priv: Uint8Array, remotePub: Uint8Array): Uint8Array {
  const secret = secp.getSharedSecret(priv, remotePub) // 33-byte compressed point
  return sha256(secret)
}

const base64 = {
  encode(bytes: Uint8Array): string {
    let bin = ""
    for (const b of bytes) bin += String.fromCharCode(b)
    return btoa(bin)
  },
  decode(str: string): Uint8Array {
    const bin = atob(str)
    const out = new Uint8Array(bin.length)
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i)
    return out
  },
}

/** AEAD-encrypt `plaintext` under `key`; output is base64 of `nonce(24) || ciphertext`. */
export function encrypt(key: Uint8Array, plaintext: string): string {
  const nonce = crypto.getRandomValues(new Uint8Array(24))
  const ct = xchacha20poly1305(key, nonce).encrypt(utf8.encode(plaintext))
  return base64.encode(concatBytes(nonce, ct))
}

/** Reverse of {@link encrypt}. Throws if authentication fails. */
export function decrypt(key: Uint8Array, payload: string): string {
  const raw = base64.decode(payload)
  const nonce = raw.subarray(0, 24)
  const ct = raw.subarray(24)
  return new TextDecoder().decode(xchacha20poly1305(key, nonce).decrypt(ct))
}
