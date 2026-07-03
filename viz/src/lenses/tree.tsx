// tree.tsx (plumtree) — the broadcast tree. eager links solid, lazy dashed;
// deliver animations (as in topology) make a broadcast visibly flood the tree.
//
// DEVIATION FROM DESIGN: the sample trace's plumtree state
// (plumtree.DebugStatsReply) carries only COUNTERS — Delivered, Duplicates,
// Eager (a count), Lazy (a count) — not per-peer eager/lazy lists. So the tree
// edges cannot come from state. They are instead DERIVED from the message
// stream up to the current step, which is exactly what those links mean:
//   plumtree.Gossip delivery / Graft -> an eager (tree) link
//   plumtree.Prune                   -> removes that eager link
//   plumtree.IHave                   -> a lazy link
// A node that has delivered the broadcast (Delivered > 0) gets a primary ring,
// so the flood frontier is visible as you scrub.

import { useMemo } from "react";
import type { LensProps } from "@/lib/lens-types";
import { pairKey } from "@/lib/fold";
import { decodePlumtree } from "@/lib/protocols";
import type { GraphEdge } from "./graph-common";
import { GraphView } from "./graph-common";

export function TreeLens({
  trace,
  world,
  filters,
  selectedNode,
  onSelectNode,
}: LensProps) {
  const nodes = useMemo(
    () => world.nodes.filter((h) => !filters.hiddenNodes.has(h)),
    [world.nodes, filters.hiddenNodes]
  );

  const edges = useMemo<GraphEdge[]>(() => {
    const eager = new Set<string>();
    const lazy = new Set<string>();
    const upto = world.idx;
    for (let i = 0; i <= upto && i < trace.events.length; i++) {
      const e = trace.events[i];
      if (!e.from || !e.to) continue;
      const key = pairKey(e.from, e.to);
      switch (e.wire) {
        case "plumtree.Gossip":
          if (e.kind === "deliver") {
            eager.add(key);
            lazy.delete(key);
          }
          break;
        case "plumtree.Graft":
          if (e.kind === "deliver") {
            eager.add(key);
            lazy.delete(key);
          }
          break;
        case "plumtree.Prune":
          if (e.kind === "deliver") eager.delete(key);
          break;
        case "plumtree.IHave":
          if (e.kind === "deliver" && !eager.has(key)) lazy.add(key);
          break;
        default:
          break;
      }
    }
    const out: GraphEdge[] = [];
    for (const key of eager) {
      const [a, b] = key.split("|");
      if (filters.hiddenNodes.has(a) || filters.hiddenNodes.has(b)) continue;
      out.push({ a, b, style: "solid", tone: "primary" });
    }
    for (const key of lazy) {
      const [a, b] = key.split("|");
      if (filters.hiddenNodes.has(a) || filters.hiddenNodes.has(b)) continue;
      out.push({ a, b, style: "dashed", tone: "muted" });
    }
    return out;
  }, [trace.events, world.idx, filters.hiddenNodes]);

  const delivered = useMemo(() => {
    const s = new Set<string>();
    for (const h of nodes) {
      const st = decodePlumtree(world.state.get(h)?.get("plumtree")?.state);
      if (st && st.delivered > 0) s.add(h);
    }
    return s;
  }, [nodes, world.state]);

  const delivery = useMemo(() => {
    const ev = world.current.find(
      (e) =>
        e.kind === "deliver" &&
        e.from &&
        e.to &&
        !filters.hiddenWires.has(e.wire ?? "")
    );
    return ev ? { from: ev.from!, to: ev.to!, wire: ev.wire ?? "" } : null;
  }, [world.current, filters.hiddenWires]);

  const drop = useMemo(() => {
    const ev = world.current.find((e) => e.kind === "drop" && e.from && e.to);
    return ev ? { from: ev.from!, to: ev.to!, wire: ev.wire ?? "" } : null;
  }, [world.current]);

  return (
    <GraphView
      nodes={nodes}
      edges={edges}
      isolated={world.isolated}
      delivery={delivery}
      drop={drop}
      selectedNode={selectedNode}
      onSelectNode={onSelectNode}
      nodeAccent={(h) => (delivered.has(h) ? "hsl(var(--primary))" : null)}
      animKey={world.step}
    />
  );
}
