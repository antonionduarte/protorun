// trace.ts — a tolerant parser for the protoviz/1 JSONL trace format.
//
// The authoritative schema is pkg/prototest/trace.go (traceEvent). Every line
// is one JSON object with a "kind" discriminator and kind-specific fields
// (omitempty on the Go side, so absent fields decode to their zero value).
//
// The parser NEVER throws on a bad line: malformed JSON or an unknown kind is
// collected as a warning and skipped, so a truncated or partially-corrupt
// trace still opens.

export const TRACE_FORMAT = "protoviz/1";

/** A logical host, always rendered "IP:port" in the trace. */
export type Host = string;

/** Unordered pair key for a link between two hosts (sorted, "a|b"). */
export type PairKey = string;

export function pairKey(a: Host, b: Host): PairKey {
  return a < b ? `${a}|${b}` : `${b}|${a}`;
}

export function pairHosts(key: PairKey): [Host, Host] {
  const i = key.indexOf("|");
  return [key.slice(0, i), key.slice(i + 1)];
}

/** Reconstruct a host string from a {Port, IP} shape found in state snapshots. */
export function hostFromAddr(addr: unknown): Host | null {
  if (!addr || typeof addr !== "object") return null;
  const a = addr as { IP?: unknown; Port?: unknown };
  if (typeof a.IP !== "string" || a.IP === "") return null;
  const port = typeof a.Port === "number" ? a.Port : 0;
  return `${a.IP}:${port}`;
}

export type EventKind =
  | "meta"
  | "node"
  | "session"
  | "deliver"
  | "drop"
  | "clock"
  | "fault"
  | "state"
  // Live-mode kinds (Stage 3). "send" is the sender-side counterpart of
  // "deliver", forwarded by the live server but not folded into topology
  // (delivers are receiver-authoritative). The lifecycle kinds are runtime
  // supervision/loss events, carried for future lenses; the fold ignores
  // them today. Session lifecycle arrives folded as "session" (the server
  // and the live-line decoder both map "session-connected" etc. onto it).
  | "send"
  | "restart"
  | "stop"
  | "escalate"
  | "dead-letter";

export type SessionEventName =
  | "connected"
  | "disconnected"
  | "failed"
  | "unknown";

export type FaultMutation =
  | "cut"
  | "heal"
  | "isolate"
  | "loss"
  | "delay"
  | string;

/** One decoded trace event, discriminated by `kind`. */
export interface TraceEvent {
  /** Index in the file (0-based). Stable across the run; the fold cursor. */
  idx: number;
  /** Simulator step counter — a total order over progress units. */
  step: number;
  /** Elapsed virtual time as a display string (e.g. "12.450s"). */
  t: string;
  kind: EventKind;

  // meta
  format?: string;
  seed?: number;
  start?: string;

  // node / meta roster / fault hosts
  host?: Host;
  protocols?: string[];
  nodes?: Host[];

  // session
  event?: SessionEventName;
  node?: Host; // observing node, or the subject of a state snapshot
  peer?: Host;

  // deliver / drop
  from?: Host;
  to?: Host;
  wire?: string;
  bytes?: number;
  reason?: string;

  // fault
  mutation?: FaultMutation;
  params?: Record<string, unknown>;

  // state
  protocol?: string;
  state?: unknown;

  // live lifecycle (restart / stop / escalate / dead-letter): a free-text
  // label, e.g. the protocol type or "protocol/kind" for a dead letter.
  detail?: string;
}

export interface TraceMeta {
  format: string;
  seed: number;
  start: string;
  nodes: Host[];
}

export interface ParsedTrace {
  meta: TraceMeta;
  events: TraceEvent[];
  /** Max step reached (the scrubber's upper bound). */
  maxStep: number;
  /** All distinct wire-type names seen, sorted. */
  wireTypes: string[];
  /** All distinct protocol labels seen in state snapshots, sorted. */
  protocols: string[];
  /** Union of every node's protocol stack (%T strings) from node events. */
  nodeProtocols: string[];
  /** Non-fatal problems collected while parsing. */
  warnings: string[];
}

const KNOWN_KINDS = new Set<string>([
  "meta",
  "node",
  "session",
  "deliver",
  "drop",
  "clock",
  "fault",
  "state",
  "send",
  "restart",
  "stop",
  "escalate",
  "dead-letter",
]);

/**
 * Normalize a raw kind that may be one of the runtime's live session kinds
 * ("session-connected" / "-disconnected" / "-failed" / "-givenup") onto the
 * folded {kind:"session", event} shape the engine understands. Given-up is a
 * terminal disconnect, so it maps to "failed" (edge removal). Any other kind
 * is returned unchanged with no session event.
 */
function normalizeKind(kind: string): {
  kind: string;
  event?: SessionEventName;
} {
  if (!kind.startsWith("session-")) return { kind };
  const suffix = kind.slice("session-".length);
  const event: SessionEventName =
    suffix === "connected"
      ? "connected"
      : suffix === "disconnected"
        ? "disconnected"
        : suffix === "failed" || suffix === "givenup"
          ? "failed"
          : "unknown";
  return { kind: "session", event };
}

/**
 * Decode one already-parsed JSON object into a TraceEvent, or null if its
 * kind is unknown. Shared by the batch file parser and the live line
 * decoder so both tolerate the same shapes. `meta` is not a fold event and
 * returns null here (callers handle the roster separately).
 */
function decodeEvent(
  obj: Record<string, unknown>,
  idx: number
): TraceEvent | null {
  const rawKind = typeof obj.kind === "string" ? obj.kind : "";
  const { kind, event: liveSession } = normalizeKind(rawKind);
  if (kind === "meta" || !KNOWN_KINDS.has(kind)) return null;

  return {
    idx,
    step: num(obj.step) ?? 0,
    t: str(obj.t) ?? "",
    kind: kind as EventKind,
    host: str(obj.host),
    protocols: has(obj, "protocols") ? strArr(obj.protocols) : undefined,
    nodes: has(obj, "nodes") ? strArr(obj.nodes) : undefined,
    event: liveSession ?? (str(obj.event) as SessionEventName | undefined),
    node: str(obj.node),
    peer: str(obj.peer),
    from: str(obj.from),
    to: str(obj.to),
    wire: str(obj.wire),
    bytes: num(obj.bytes),
    reason: str(obj.reason),
    mutation: str(obj.mutation),
    params: isObj(obj.params)
      ? (obj.params as Record<string, unknown>)
      : undefined,
    protocol: str(obj.protocol),
    state: has(obj, "state") ? obj.state : undefined,
    detail: str(obj.detail),
  };
}

/**
 * Decode one live JSONL line (from the /events SSE stream). Returns a meta
 * roster, a fold event, or nothing (blank / unparseable / unknown line).
 * `idx` is the next event index the caller will assign. Never throws.
 */
export function parseLiveLine(
  raw: string,
  idx: number
): { meta?: TraceMeta; event?: TraceEvent } {
  const line = raw.trim();
  if (line === "") return {};
  let obj: Record<string, unknown>;
  try {
    obj = JSON.parse(line) as Record<string, unknown>;
  } catch {
    return {};
  }
  if (obj.kind === "meta") {
    return {
      meta: {
        format: str(obj.format) ?? "",
        seed: num(obj.seed) ?? 0,
        start: str(obj.start) ?? "",
        nodes: strArr(obj.nodes),
      },
    };
  }
  const event = decodeEvent(obj, idx);
  return event ? { event } : {};
}

/**
 * Parse a protoviz/1 JSONL string into a ParsedTrace. Tolerant: bad lines are
 * skipped with a warning and never abort the parse.
 */
export function parseTrace(text: string): ParsedTrace {
  const warnings: string[] = [];
  const events: TraceEvent[] = [];
  let meta: TraceMeta | null = null;

  const wireTypes = new Set<string>();
  const protocols = new Set<string>();
  const nodeProtocols = new Set<string>();
  let maxStep = 0;

  const lines = text.split("\n");
  for (let ln = 0; ln < lines.length; ln++) {
    const raw = lines[ln].trim();
    if (raw === "") continue;

    let obj: Record<string, unknown>;
    try {
      obj = JSON.parse(raw) as Record<string, unknown>;
    } catch {
      warnings.push(`line ${ln + 1}: invalid JSON, skipped`);
      continue;
    }

    const rawKind = typeof obj.kind === "string" ? obj.kind : "";

    if (rawKind === "meta") {
      meta = {
        format: str(obj.format) ?? "",
        seed: num(obj.seed) ?? 0,
        start: str(obj.start) ?? "",
        nodes: strArr(obj.nodes),
      };
      if (meta.format !== TRACE_FORMAT) {
        warnings.push(
          `meta format "${meta.format}" != expected "${TRACE_FORMAT}"; parsing best-effort`
        );
      }
      // meta itself is not a fold event; skip pushing.
      continue;
    }

    const ev = decodeEvent(obj, events.length);
    if (!ev) {
      warnings.push(`line ${ln + 1}: unknown kind "${rawKind}", skipped`);
      continue;
    }

    if (ev.step > maxStep) maxStep = ev.step;
    if (ev.wire) wireTypes.add(ev.wire);
    if (ev.kind === "state" && ev.protocol) protocols.add(ev.protocol);
    if (ev.kind === "node" && ev.protocols) {
      for (const p of ev.protocols) nodeProtocols.add(p);
    }
    events.push(ev);
  }

  if (!meta) {
    // A trace with no meta line is unusual but not fatal; synthesize one from
    // whatever node roster we can see, and warn.
    warnings.push("no meta line found; synthesizing from node events");
    const roster = new Set<Host>();
    for (const ev of events) {
      if (ev.kind === "node" && ev.host) roster.add(ev.host);
    }
    meta = {
      format: "",
      seed: 0,
      start: "",
      nodes: [...roster],
    };
  }

  return {
    meta,
    events,
    maxStep,
    wireTypes: [...wireTypes].sort(),
    protocols: [...protocols].sort(),
    nodeProtocols: [...nodeProtocols].sort(),
    warnings,
  };
}

/**
 * An empty trace, the starting point for a live connection: the fold engine
 * grows it in place as events stream in. It carries no meta (no seed, no
 * roster) — live runs have no reproduce seed and no seed banner.
 */
export function emptyTrace(): ParsedTrace {
  return {
    meta: { format: "", seed: 0, start: "", nodes: [] },
    events: [],
    maxStep: 0,
    wireTypes: [],
    protocols: [],
    nodeProtocols: [],
    warnings: [],
  };
}

function has(o: Record<string, unknown>, k: string): boolean {
  return Object.prototype.hasOwnProperty.call(o, k);
}
function str(v: unknown): string | undefined {
  return typeof v === "string" ? v : undefined;
}
function num(v: unknown): number | undefined {
  return typeof v === "number" ? v : undefined;
}
function isObj(v: unknown): boolean {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}
function strArr(v: unknown): Host[] {
  if (!Array.isArray(v)) return [];
  return v.filter((x): x is string => typeof x === "string");
}
