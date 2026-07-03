// fold.ts — the world-state engine.
//
// A view at step S is a FOLD over all events with step <= S. The engine folds
// over the flat event array (indexed by `idx`), because several events can
// share a step number (a `drop` or `fault` or `state` is stamped with the step
// currently being processed; only deliver/session/clock advance the counter).
// The UI scrubs by STEP; `indexForStep` maps a step to the last event index at
// that step, and the fold runs up to (and including) that index.
//
// Performance: full keyframe snapshots are captured every KEYFRAME_EVERY
// events. Scrubbing = clone the nearest keyframe <= target and forward-fold the
// remaining events. This keeps scrubbing the 6.4k-line hyparview trace instant.

import {
  hostFromAddr,
  pairHosts,
  pairKey,
  type Host,
  type PairKey,
  type ParsedTrace,
  type TraceEvent,
} from "./trace";

export const KEYFRAME_EVERY = 500;

/** Latest state snapshot for a (node, protocol), plus its immediate prior. */
export interface StateSlot {
  /** The decoded state object at or before the current index. */
  state: unknown;
  /** The step at which `state` was captured. */
  step: number;
  /** The previous distinct snapshot for this (node, protocol), for diffing. */
  prev: unknown;
  prevStep: number;
}

export interface LossyLink {
  key: PairKey;
  p: number;
}

/** The folded world at a given event index. */
export interface WorldState {
  /** Event index this world reflects (inclusive). -1 = empty (before start). */
  idx: number;
  /** Display step for this world. */
  step: number;
  /** Alive nodes, insertion-ordered (roster order, then late joins). */
  nodes: Host[];
  /** Live sessions as unordered pair keys. */
  sessions: Set<PairKey>;
  /** state[node][protocol] = latest snapshot slot. */
  state: Map<Host, Map<string, StateSlot>>;
  /** Cut pairs (hard partition). */
  cutPairs: Set<PairKey>;
  /** Fully isolated nodes. */
  isolated: Set<Host>;
  /** Lossy links keyed by pair -> drop probability. */
  lossy: Map<PairKey, number>;
  /** Delayed links keyed by pair -> params. */
  delayed: Map<PairKey, Record<string, unknown>>;
  /**
   * The events stamped at exactly `step` (what "just happened") — used to
   * animate the current delivery/drop and to highlight faults. Recomputed per
   * fold target, not carried in keyframes.
   */
  current: TraceEvent[];
}

function emptyWorld(): WorldState {
  return {
    idx: -1,
    step: 0,
    nodes: [],
    sessions: new Set(),
    state: new Map(),
    cutPairs: new Set(),
    isolated: new Set(),
    lossy: new Map(),
    delayed: new Map(),
    current: [],
  };
}

function cloneWorld(w: WorldState): WorldState {
  const state = new Map<Host, Map<string, StateSlot>>();
  for (const [node, byProto] of w.state) {
    const inner = new Map<string, StateSlot>();
    for (const [proto, slot] of byProto) inner.set(proto, { ...slot });
    state.set(node, inner);
  }
  return {
    idx: w.idx,
    step: w.step,
    nodes: [...w.nodes],
    sessions: new Set(w.sessions),
    state,
    cutPairs: new Set(w.cutPairs),
    isolated: new Set(w.isolated),
    lossy: new Map(w.lossy),
    delayed: new Map(w.delayed),
    current: [],
  };
}

/** Apply a single event to a world in place (does not touch `current`). */
function apply(w: WorldState, ev: TraceEvent): void {
  switch (ev.kind) {
    case "node": {
      if (ev.host && !w.nodes.includes(ev.host)) w.nodes.push(ev.host);
      break;
    }
    case "session": {
      if (!ev.node || !ev.peer) break;
      const key = pairKey(ev.node, ev.peer);
      if (ev.event === "connected") w.sessions.add(key);
      else if (ev.event === "disconnected" || ev.event === "failed")
        w.sessions.delete(key);
      break;
    }
    case "fault": {
      applyFault(w, ev);
      break;
    }
    case "state": {
      if (!ev.node || !ev.protocol) break;
      let byProto = w.state.get(ev.node);
      if (!byProto) {
        byProto = new Map();
        w.state.set(ev.node, byProto);
      }
      const existing = byProto.get(ev.protocol);
      // Only rotate prev when the snapshot actually differs, so the inspector
      // diff compares against the last *changed* state, not a duplicate.
      if (existing && !deepEqual(existing.state, ev.state)) {
        byProto.set(ev.protocol, {
          state: ev.state,
          step: ev.step,
          prev: existing.state,
          prevStep: existing.step,
        });
      } else if (!existing) {
        byProto.set(ev.protocol, {
          state: ev.state,
          step: ev.step,
          prev: undefined,
          prevStep: -1,
        });
      } else {
        // unchanged: keep prev, refresh step to latest sighting
        existing.step = ev.step;
      }
      break;
    }
    // deliver / drop / clock have no persistent world effect (they animate via
    // `current`); membership and faults carry the durable state.
    default:
      break;
  }
}

function applyFault(w: WorldState, ev: TraceEvent): void {
  const nodes = ev.nodes ?? [];
  switch (ev.mutation) {
    case "cut": {
      if (nodes.length >= 2) w.cutPairs.add(pairKey(nodes[0], nodes[1]));
      break;
    }
    case "heal": {
      if (nodes.length >= 2) {
        w.cutPairs.delete(pairKey(nodes[0], nodes[1]));
        w.lossy.delete(pairKey(nodes[0], nodes[1]));
        w.delayed.delete(pairKey(nodes[0], nodes[1]));
      } else if (nodes.length === 1) {
        w.isolated.delete(nodes[0]);
      }
      break;
    }
    case "isolate": {
      if (nodes.length >= 1) w.isolated.add(nodes[0]);
      break;
    }
    case "loss": {
      if (nodes.length >= 2) {
        const p =
          typeof ev.params?.p === "number" ? (ev.params.p as number) : 1;
        w.lossy.set(pairKey(nodes[0], nodes[1]), p);
      }
      break;
    }
    case "delay": {
      if (nodes.length >= 2)
        w.delayed.set(pairKey(nodes[0], nodes[1]), ev.params ?? {});
      break;
    }
    default:
      break;
  }
}

/**
 * The fold engine over a parsed trace: precomputes keyframes and answers
 * world-state queries by step (or index) in ~O(KEYFRAME_EVERY).
 *
 * Live-capable: `append` extends the trace in place and folds only the new
 * events, so a live stream grows the same engine incrementally rather than
 * rebuilding from scratch. Keyframes are captured at every global index that
 * is a multiple of KEYFRAME_EVERY, an invariant that holds under incremental
 * growth because events are appended contiguously. `indexForStep` binary-
 * searches the events (whose `step` is non-decreasing in arrival order), so
 * no fixed-size step→index table is needed.
 */
export class FoldEngine {
  readonly trace: ParsedTrace;
  private keyframes: WorldState[] = [];
  /** Running fold cursor and count of events already folded into keyframes. */
  private cursor: WorldState = emptyWorld();
  private built = 0;

  constructor(trace: ParsedTrace) {
    this.trace = trace;
    this.ingest();
  }

  /** Fold every not-yet-folded event, capturing keyframes on the way. */
  private ingest(): void {
    const events = this.trace.events;
    for (let i = this.built; i < events.length; i++) {
      const ev = events[i];
      apply(this.cursor, ev);
      this.cursor.idx = i;
      this.cursor.step = ev.step;
      if (i % KEYFRAME_EVERY === 0) {
        this.keyframes.push(cloneWorld(this.cursor));
      }
    }
    this.built = events.length;
  }

  /**
   * Append live events: push them onto the trace, refresh the trace's derived
   * aggregates (maxStep, wire/protocol rosters), and fold only the new tail.
   */
  append(newEvents: TraceEvent[]): void {
    if (newEvents.length === 0) return;
    const t = this.trace;
    const wireTypes = new Set(t.wireTypes);
    const protocols = new Set(t.protocols);
    const nodeProtocols = new Set(t.nodeProtocols);
    const roster = new Set(t.meta.nodes);
    for (const ev of newEvents) {
      ev.idx = t.events.length;
      t.events.push(ev);
      if (ev.step > t.maxStep) t.maxStep = ev.step;
      if (ev.wire) wireTypes.add(ev.wire);
      if (ev.kind === "state" && ev.protocol) protocols.add(ev.protocol);
      if (ev.kind === "node") {
        if (ev.host) roster.add(ev.host);
        if (ev.protocols) for (const p of ev.protocols) nodeProtocols.add(p);
      }
    }
    t.wireTypes = [...wireTypes].sort();
    t.protocols = [...protocols].sort();
    t.nodeProtocols = [...nodeProtocols].sort();
    t.meta.nodes = [...roster];
    this.ingest();
  }

  /** Map a display step to the inclusive event index the fold should reach. */
  indexForStep(step: number): number {
    const events = this.trace.events;
    if (step < 0 || events.length === 0) return -1;
    // Largest index whose step <= the target (events are step-sorted).
    let lo = 0;
    let hi = events.length - 1;
    let ans = -1;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      if (events[mid].step <= step) {
        ans = mid;
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    return ans;
  }

  /** Fold to an inclusive event index. */
  worldAtIndex(targetIdx: number): WorldState {
    const events = this.trace.events;
    if (targetIdx < 0 || events.length === 0) {
      const w = emptyWorld();
      w.current = [];
      return w;
    }
    const clamped = Math.min(targetIdx, events.length - 1);

    // Nearest keyframe at or before the target.
    const kfIdx = Math.floor(clamped / KEYFRAME_EVERY);
    let kf: WorldState | undefined = this.keyframes[kfIdx];
    if (!kf || kf.idx > clamped) {
      // Fall back to a linear scan (defensive; should not happen).
      kf = undefined;
      for (const k of this.keyframes) if (k.idx <= clamped) kf = k;
    }

    const w = kf ? cloneWorld(kf) : emptyWorld();
    for (let i = (kf ? kf.idx : -1) + 1; i <= clamped; i++) apply(w, events[i]);
    w.idx = clamped;
    w.step = events[clamped].step;

    // Populate `current`: all events stamped at this exact step.
    const step = w.step;
    const cur: TraceEvent[] = [];
    // Walk backward from clamped while step matches, then forward for any tail.
    let lo = clamped;
    while (lo > 0 && events[lo - 1].step === step) lo--;
    let hi = clamped;
    while (hi + 1 < events.length && events[hi + 1].step === step) hi++;
    for (let i = lo; i <= hi; i++) cur.push(events[i]);
    w.current = cur;

    return w;
  }

  /** Fold to a display step. */
  worldAtStep(step: number): WorldState {
    return this.worldAtIndex(this.indexForStep(step));
  }
}

/** Reconstruct a Host from a state-snapshot {IP,Port} address, if present. */
export function addrToHost(addr: unknown): Host | null {
  return hostFromAddr(addr);
}

export { pairHosts, pairKey };

// Small structural equality for state snapshots (JSON-shaped values only).
function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (typeof a !== typeof b) return false;
  if (a === null || b === null) return a === b;
  if (typeof a !== "object") return false;
  const aa = a as Record<string, unknown>;
  const bb = b as Record<string, unknown>;
  if (Array.isArray(a) !== Array.isArray(b)) return false;
  const ak = Object.keys(aa);
  const bk = Object.keys(bb);
  if (ak.length !== bk.length) return false;
  for (const k of ak) if (!deepEqual(aa[k], bb[k])) return false;
  return true;
}
