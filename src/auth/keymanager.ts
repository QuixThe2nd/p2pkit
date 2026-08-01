import { generatePrivateKey, publicKeyOf, addressOf, bytesToHex, hexToBytes } from "./crypto.js"
import type { PeerId } from "../utils/types.js"

export interface KeyManagerOptions {
  /** Override the directory for the on-disk keyfile (Node/Bun/Deno). */
  dir?: string
}

interface KeyStore {
  read(name: string): Promise<string | undefined>
  write(name: string, hex: string): Promise<void>
  remove(name: string): Promise<void>
}

/** Persist the keypair via IndexedDB in the browser, or an fs keyfile elsewhere. */
function pickStore(opts: KeyManagerOptions): KeyStore {
  if (typeof globalThis.indexedDB !== "undefined") return indexedDbStore()
  return fsStore(opts.dir)
}

/**
 * Loads or creates a persisted secp256k1 keypair, identified by `name`. The
 * private key never leaves the process; only the public key / address are
 * exposed. Persistence is fs on Node/Bun/Deno and IndexedDB in the browser
 * (README §5).
 */
export class KeyManager {
  readonly name: string
  private readonly store: KeyStore
  private priv?: Uint8Array
  private loading?: Promise<Uint8Array>

  constructor(name: string, options: KeyManagerOptions = {}) {
    this.name = name
    this.store = pickStore(options)
  }

  /** Load the persisted private key, creating and persisting one on first use. */
  getPrivateKey(): Promise<Uint8Array> {
    if (this.priv) return Promise.resolve(this.priv)
    return (this.loading ??= this.loadOrCreate())
  }

  private async loadOrCreate(): Promise<Uint8Array> {
    const existing = await this.store.read(this.name)
    if (existing) {
      this.priv = hexToBytes(existing)
    } else {
      this.priv = generatePrivateKey()
      await this.store.write(this.name, bytesToHex(this.priv))
    }
    return this.priv
  }

  /** Uncompressed public key. */
  async getPublicKey(): Promise<Uint8Array> {
    return publicKeyOf(await this.getPrivateKey())
  }

  /** The address (peer id) derived from the keypair. */
  async getAddress(): Promise<PeerId> {
    return addressOf(await this.getPublicKey())
  }

  /** Delete the persisted keypair. */
  async clear(): Promise<void> {
    this.priv = undefined
    this.loading = undefined
    await this.store.remove(this.name)
  }
}

function fsStore(dir?: string): KeyStore {
  const resolveDir = async () => {
    const os = await import("node:os")
    const path = await import("node:path")
    return { path, dir: dir ?? path.join(os.homedir(), ".p2pkit") }
  }
  const safe = (name: string) => name.replace(/[^a-zA-Z0-9._-]/g, "_")
  return {
    async read(name) {
      const fs = await import("node:fs/promises")
      const { path, dir } = await resolveDir()
      try {
        return (await fs.readFile(path.join(dir, `${safe(name)}.key`), "utf8")).trim()
      } catch {
        return undefined
      }
    },
    async write(name, hex) {
      const fs = await import("node:fs/promises")
      const { path, dir } = await resolveDir()
      await fs.mkdir(dir, { recursive: true })
      await fs.writeFile(path.join(dir, `${safe(name)}.key`), hex, { mode: 0o600 })
    },
    async remove(name) {
      const fs = await import("node:fs/promises")
      const { path, dir } = await resolveDir()
      await fs.rm(path.join(dir, `${safe(name)}.key`), { force: true })
    },
  }
}

function indexedDbStore(): KeyStore {
  const DB = "p2pkit"
  const STORE = "keys"
  const open = () =>
    new Promise<IDBDatabase>((resolve, reject) => {
      const req = indexedDB.open(DB, 1)
      req.onupgradeneeded = () => req.result.createObjectStore(STORE)
      req.onsuccess = () => resolve(req.result)
      req.onerror = () => reject(req.error)
    })
  const tx = async <T>(mode: IDBTransactionMode, fn: (s: IDBObjectStore) => IDBRequest<T>): Promise<T> => {
    const db = await open()
    return new Promise<T>((resolve, reject) => {
      const req = fn(db.transaction(STORE, mode).objectStore(STORE))
      req.onsuccess = () => resolve(req.result)
      req.onerror = () => reject(req.error)
    })
  }
  return {
    async read(name) {
      return (await tx<string | undefined>("readonly", s => s.get(name))) ?? undefined
    },
    async write(name, hex) {
      await tx("readwrite", s => s.put(hex, name))
    },
    async remove(name) {
      await tx("readwrite", s => s.delete(name))
    },
  }
}
