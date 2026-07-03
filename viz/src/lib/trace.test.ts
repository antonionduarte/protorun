import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { parseTrace, TRACE_FORMAT, pairKey } from "./trace";

const dir = path.dirname(fileURLToPath(import.meta.url));
const samplesDir = path.resolve(dir, "../../sample-traces");
function sample(name: string): string {
  return readFileSync(path.join(samplesDir, name), "utf8");
}

const SAMPLES = [
  { file: "raft-partition.jsonl", nodes: 5, wire: "raft.AppendEntries" },
  { file: "hyparview-churn.jsonl", nodes: 12, wire: "hyparview.Shuffle" },
  { file: "broadcast.jsonl", nodes: 8, wire: "plumtree.Gossip" },
  { file: "paxos-duel.jsonl", nodes: 5, wire: "paxos.Prepare" },
];

describe("parseTrace on real sample traces", () => {
  for (const s of SAMPLES) {
    it(`parses ${s.file}`, () => {
      const parsed = parseTrace(sample(s.file));
      expect(parsed.meta.format).toBe(TRACE_FORMAT);
      expect(parsed.meta.seed).toBe(42);
      expect(parsed.meta.nodes.length).toBe(s.nodes);
      expect(parsed.events.length).toBeGreaterThan(0);
      expect(parsed.maxStep).toBeGreaterThan(0);
      expect(parsed.wireTypes).toContain(s.wire);
      // A clean recorded trace should have no parse warnings.
      expect(parsed.warnings).toEqual([]);
    });
  }

  it("collects node protocol stacks", () => {
    const parsed = parseTrace(sample("raft-partition.jsonl"));
    expect(parsed.nodeProtocols.some((p) => p.includes("raft"))).toBe(true);
    expect(parsed.protocols).toContain("raft");
  });

  it("is tolerant of malformed and unknown lines", () => {
    const good = `{"kind":"meta","format":"protoviz/1","seed":7,"nodes":["a:1"]}`;
    const text = [
      good,
      "not json at all",
      `{"kind":"mystery","step":2}`,
      `{"kind":"deliver","step":3,"from":"a:1","to":"b:2","wire":"X"}`,
    ].join("\n");
    const parsed = parseTrace(text);
    expect(parsed.meta.seed).toBe(7);
    // Only the deliver survives as a fold event.
    expect(parsed.events.length).toBe(1);
    expect(parsed.warnings.length).toBe(2);
  });

  it("keys unordered pairs stably", () => {
    expect(pairKey("a:1", "b:2")).toBe(pairKey("b:2", "a:1"));
  });
});
