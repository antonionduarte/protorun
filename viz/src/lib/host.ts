import type { Host } from "./trace";

/**
 * Shorten a host for a node badge. "10.0.0.11:6010" -> "11:6010" (last octet +
 * port) which stays unique within a localhost/10.x cluster while fitting a
 * small label. Falls back to the raw string if it doesn't look like IP:port.
 */
export function shortHost(host: Host): string {
  const colon = host.lastIndexOf(":");
  if (colon < 0) return host;
  const ip = host.slice(0, colon);
  const port = host.slice(colon + 1);
  const octets = ip.split(".");
  if (octets.length === 4) return `${octets[3]}:${port}`;
  return host;
}

/** Even shorter: just the last IP octet, for dense diagrams. */
export function tinyHost(host: Host): string {
  const colon = host.lastIndexOf(":");
  const ip = colon < 0 ? host : host.slice(0, colon);
  const octets = ip.split(".");
  if (octets.length === 4) return octets[3];
  return colon < 0 ? host : host.slice(colon + 1);
}

/** Strip a leading pointer/star and package path from a %T protocol string. */
export function shortProtocol(t: string): string {
  let s = t.replace(/^\*+/, "");
  // Drop generic type params entirely for readability.
  const lt = s.indexOf("[");
  if (lt >= 0) s = s.slice(0, lt);
  const dot = s.lastIndexOf(".");
  return dot >= 0 ? s.slice(dot + 1) : s;
}
