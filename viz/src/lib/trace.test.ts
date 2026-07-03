import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { parseLiveLine, parseTrace, TRACE_FORMAT, pairKey } from "./trace";

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

  it("folds runtime live session kinds onto the session shape", () => {
    // The live server emits "session", but the parser must also tolerate the
    // runtime's own "session-connected" style kinds directly.
    const text = [
      `{"kind":"meta","format":"protoviz/1","seed":1,"nodes":["a:1","b:2"]}`,
      `{"kind":"session-connected","step":1,"node":"a:1","peer":"b:2"}`,
      `{"kind":"session-givenup","step":2,"node":"a:1","peer":"b:2"}`,
    ].join("\n");
    const parsed = parseTrace(text);
    expect(parsed.warnings).toEqual([]);
    expect(parsed.events.length).toBe(2);
    expect(parsed.events[0].kind).toBe("session");
    expect(parsed.events[0].event).toBe("connected");
    // given-up is a terminal disconnect -> mapped to "failed" (edge removal).
    expect(parsed.events[1].kind).toBe("session");
    expect(parsed.events[1].event).toBe("failed");
  });

  it("tolerates live send and lifecycle kinds without warnings", () => {
    const text = [
      `{"kind":"meta","format":"protoviz/1","seed":1,"nodes":["a:1","b:2"]}`,
      `{"kind":"send","step":1,"from":"a:1","to":"b:2","wire":"X"}`,
      `{"kind":"restart","step":2,"detail":"raft.Raft"}`,
      `{"kind":"dead-letter","step":3,"peer":"b:2","detail":"raft.Raft/message"}`,
    ].join("\n");
    const parsed = parseTrace(text);
    expect(parsed.warnings).toEqual([]);
    expect(parsed.events.map((e) => e.kind)).toEqual([
      "send",
      "restart",
      "dead-letter",
    ]);
    expect(parsed.events[1].detail).toBe("raft.Raft");
  });
});

describe("parseLiveLine (SSE line decoder)", () => {
  it("decodes a meta line into a roster", () => {
    const { meta, event } = parseLiveLine(
      `{"kind":"meta","format":"protoviz/1","seed":9,"nodes":["a:1"]}`,
      0
    );
    expect(event).toBeUndefined();
    expect(meta?.nodes).toEqual(["a:1"]);
  });

  it("decodes a deliver line, assigning the given idx", () => {
    const { event } = parseLiveLine(
      `{"kind":"deliver","step":5,"from":"a:1","to":"b:2","wire":"X"}`,
      42
    );
    expect(event?.idx).toBe(42);
    expect(event?.kind).toBe("deliver");
    expect(event?.from).toBe("a:1");
  });

  it("maps a live session-connected line", () => {
    const { event } = parseLiveLine(
      `{"kind":"session-connected","step":1,"node":"a:1","peer":"b:2"}`,
      0
    );
    expect(event?.kind).toBe("session");
    expect(event?.event).toBe("connected");
  });

  it("returns nothing for blank, unparseable, or unknown lines", () => {
    expect(parseLiveLine("", 0)).toEqual({});
    expect(parseLiveLine("not json", 0)).toEqual({});
    expect(parseLiveLine(`{"kind":"mystery"}`, 0)).toEqual({});
  });
});
