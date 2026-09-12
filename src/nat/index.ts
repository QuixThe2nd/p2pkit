// p2pkit/nat — request a router port mapping so a listening transport (µTP, HTTP,
// DHT) is reachable from outside without manual forwarding (README §9, Node-only).

export interface MapPortOptions {
  /** The local port to expose. */
  port: number
  /** Transport protocol. Default `"udp"` (µTP/DHT); use `"tcp"` for HTTP. */
  protocol?: "udp" | "tcp"
  /** Requested lease time in seconds. Default 3600. */
  ttl?: number
  /** External port to request; defaults to `port`. */
  externalPort?: number
  /** Human-readable description registered on the router. */
  description?: string
}

/** A live port mapping; call {@link PortMapping.close} to release it on shutdown. */
export interface PortMapping {
  /** The external port the router assigned (usually equal to the requested port). */
  readonly external: number
  /** The protocol that was mapped. */
  readonly protocol: "udp" | "tcp"
  /** Release the mapping. */
  close(): Promise<void>
}

/** Minimal shape of the `nat-upnp` client this module drives. */
interface UPnPClient {
  portMapping(
    opts: {
      public: number
      private: number
      protocol?: string
      ttl?: number
      description?: string
    },
    cb: (err: Error | null) => void,
  ): void
  portUnmapping(opts: { public: number; protocol?: string }, cb: (err: Error | null) => void): void
  close?(): void
}

interface UPnPModule {
  createClient(): UPnPClient
}

// Specifier widened to `string` so TypeScript does not try to resolve the
// optional (often-uninstalled) module at build time — mirrors `backends`.
const NAT_UPNP: string = "nat-upnp"

async function loadUPnP(): Promise<UPnPModule | undefined> {
  try {
    const mod = (await import(/* @vite-ignore */ NAT_UPNP)) as Record<string, unknown>
    const candidate = (mod["default"] ?? mod) as Partial<UPnPModule>
    if (typeof candidate.createClient === "function") return candidate as UPnPModule
  } catch {
    /* not installed */
  }
  return undefined
}

/**
 * Request a UPnP / NAT-PMP port mapping (README §9). Node-only, and requires the
 * optional `nat-upnp` dependency plus a router that speaks UPnP/NAT-PMP — WebRTC
 * does its own NAT traversal and never needs this; the port-listening transports
 * (µTP, HTTP, DHT) do.
 *
 * ```ts
 * const mapping = await mapPort({ port: 20000, protocol: "udp", ttl: 3600 })
 * await mapping.close() // release on shutdown
 * ```
 */
export async function mapPort(options: MapPortOptions): Promise<PortMapping> {
  const upnp = await loadUPnP()
  if (!upnp) {
    throw new Error(
      "mapPort requires the optional `nat-upnp` dependency (Node-only). Install it, or forward the port manually.",
    )
  }

  const protocol = options.protocol ?? "udp"
  const external = options.externalPort ?? options.port
  const client = upnp.createClient()

  await new Promise<void>((resolve, reject) => {
    client.portMapping(
      {
        public: external,
        private: options.port,
        protocol,
        ttl: options.ttl ?? 3600,
        description: options.description ?? "p2pkit",
      },
      err => (err ? reject(err) : resolve()),
    )
  })

  return {
    external,
    protocol,
    close: () =>
      new Promise<void>((resolve, reject) => {
        client.portUnmapping({ public: external, protocol }, err => {
          client.close?.()
          if (err) reject(err)
          else resolve()
        })
      }),
  }
}
