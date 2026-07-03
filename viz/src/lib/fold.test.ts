import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { parseTrace, pairKey, type Host } from "./trace";
import { FoldEngine } from "./fold";

const dir = path.dirname(fileURLToPath(import.meta.url));
const samplesDir = path.resolve(dir, "../../sample-traces");
function sample(name: string): string {
  return readFileSync(path.join(samplesDir, name), "utf8");
}

/** A deliberately naive, from-scratch reference fold — no keyframes — used as
 * the oracle the keyframe engine must agree with. */
function referenceFold(events: ReturnType<typeof parseTrace>["events"], idx: number) {
  const nodes: Host[] = [];
  const sessions = new Set<string>();
  const cut = new Set<string>();
  const isolated = new Set<string>();
  for (let i = 0; i <= idx && i < events.length; i++) {
    const ev = events[i];
    if (ev.kind === "node" && ev.host && !nodes.includes(ev.host))
      nodes.push(ev.host);
    else if (ev.kind === "session" && ev.node && ev.peer) {
      const k = pairKey(ev.node, ev.peer);
      if (ev.event === "connected") sessions.add(k);
      else sessions.delete(k);
    } else if (ev.kind === "fault") {
      const n = ev.nodes ?? [];
      if (ev.mutation === "cut" && n.length >= 2) cut.add(pairKey(n[0], n[1]));
      else if (ev.mutation === "heal" && n.length >= 2)
        cut.delete(pairKey(n[0], n[1]));
      else if (ev.mutation === "heal" && n.length === 1) isolated.delete(n[0]);
      else if (ev.mutation === "isolate" && n.length >= 1) isolated.add(n[0]);
    }
  }
  return { nodes, sessions, cut, isolated };
}

function setEq(a: Set<string>, b: Set<string>): boolean {
  if (a.size !== b.size) return false;
  for (const x of a) if (!b.has(x)) return false;
  return true;
}

// A tiny deterministic PRNG so "random steps × 3 seeds" is reproducible.
function mulberry32(seed: number) {
  return () => {
    seed |= 0;
    seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

describe("FoldEngine keyframe correctness", () => {
  it("session set matches a hand-computed prefix on raft-partition (first 120 events)", () => {
    const trace = parseTrace(sample("raft-partition.jsonl"));
    const engine = new FoldEngine(trace);
    for (let idx = 0; idx < 120; idx++) {
      const ref = referenceFold(trace.events, idx);
      const w = engine.worldAtIndex(idx);
      expect(setEq(w.sessions, ref.sessions)).toBe(true);
      expect(w.nodes.slice().sort()).toEqual(ref.nodes.slice().sort());
    }
  });

  const files = [
    "raft-partition.jsonl",
    "hyparview-churn.jsonl",
    "broadcast.jsonl",
    "paxos-duel.jsonl",
  ];

  for (const file of files) {
    it(`keyframe+refold equals full fold at random indices on ${file} (×3 seeds)`, () => {
      const trace = parseTrace(sample(file));
      const engine = new FoldEngine(trace);
      const n = trace.events.length;
      for (const seed of [1, 42, 1337]) {
        const rand = mulberry32(seed);
        for (let k = 0; k < 40; k++) {
          const idx = Math.floor(rand() * n);
          const ref = referenceFold(trace.events, idx);
          const w = engine.worldAtIndex(idx);
          expect(setEq(w.sessions, ref.sessions)).toBe(true);
          expect(setEq(w.cutPairs, ref.cut)).toBe(true);
          expect(setEq(w.isolated, ref.isolated)).toBe(true);
          expect(w.nodes.slice().sort()).toEqual(ref.nodes.slice().sort());
        }
      }
    });
  }

  it("worldAtStep resolves to the last event index at that step", () => {
    const trace = parseTrace(sample("raft-partition.jsonl"));
    const engine = new FoldEngine(trace);
    const w = engine.worldAtStep(trace.maxStep);
    expect(w.idx).toBe(trace.events.length - 1);
    // current events all share the final step.
    for (const ev of w.current) expect(ev.step).toBe(w.step);
  });

  it("latest state snapshot per (node,protocol) is exposed with a prior for diffing", () => {
    const trace = parseTrace(sample("raft-partition.jsonl"));
    const engine = new FoldEngine(trace);
    const w = engine.worldAtStep(trace.maxStep);
    let sawState = false;
    for (const [, byProto] of w.state) {
      const slot = byProto.get("raft");
      if (slot) {
        sawState = true;
        expect(slot.step).toBeGreaterThan(0);
      }
    }
    expect(sawState).toBe(true);
  });
});
