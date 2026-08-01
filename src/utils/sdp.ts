/**
 * Extract the first meaningful connection address from an SDP blob: the first
 * `c=IN IP4 …` / `c=IN IP6 …` line whose address is not the `0.0.0.0`
 * placeholder ICE uses before candidates are gathered.
 */
export function extractIP(sdp: string): string | undefined {
  for (const raw of sdp.split(/\r?\n/)) {
    const match = /^c=IN IP[46] (\S+)/.exec(raw.trim())
    const ip = match?.[1]
    if (ip && ip !== "0.0.0.0" && ip !== "::") return ip
  }
  return undefined
}
