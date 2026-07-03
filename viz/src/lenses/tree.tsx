// tree.tsx (plumtree) — the broadcast tree. eager links solid, lazy dashed;
// deliver animations (as in topology) make a broadcast visibly flood the tree.
//
// Edge source, in preference order:
//  1. STATE — plumtree.DebugStatsReply.EagerPeers/LazyPeers, the node's real
//     directional link sets sampled into the trace. An edge renders eager if
//     EITHER endpoint lists the other as eager (display is undirected).
//  2. FALLBACK — traces recorded before the per-peer lists existed carry only
//     counters, so edges are reconstructed from the message stream
//     (Gossip/Graft add eager, Prune removes, IHave marks lazy). This
//     reconstruction is approximate: it uses undirected pair keys while real
//     Plumtree links are directional, so prefer regenerating the trace.
// A node that has delivered the broadcast (Delivered > 0) gets a primary ring,
// so the flood frontier is visible as you scrub.

import { useMemo } from "react";
import type { LensProps } from "@/lib/lens-types";
import { pairKey } from "@/lib/fold";
import { decodePlumtree } from "@/lib/protocols";
import type { GraphEdge } from "./graph-common";
import { GraphView } from "./graph-common";

function buildEdges(
  eager: Set<string>,
  lazy: Set<string>,
  hidden: Set<string>
): GraphEdge[] {
  const out: GraphEdge[] = [];
  for (const key of eager) {
    const [a, b] = key.split("|");
    if (hidden.has(a) || hidden.has(b)) continue;
    out.push({ a, b, style: "solid", tone: "primary" });
  }
  for (const key of lazy) {
    const [a, b] = key.split("|");
    if (hidden.has(a) || hidden.has(b)) continue;
    out.push({ a, b, style: "dashed", tone: "muted" });
  }
  return out;
}

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

    // Preferred path: real per-peer link sets from sampled state. A
    // snapshot "has lists" only in traces recorded after
    // DebugStatsReply gained EagerPeers/LazyPeers; the moment any
    // snapshot at or before this step carries them, state is
    // authoritative. Until then (old traces, or the pre-overlay start
    // of a new one) the message-stream fallback below applies.
    let haveState = false;
    for (const h of world.nodes) {
      const st = decodePlumtree(world.state.get(h)?.get("plumtree")?.state);
      if (!st) continue;
      if (st.eagerPeers.length === 0 && st.lazyPeers.length === 0) continue;
      haveState = true;
      for (const p of st.eagerPeers) eager.add(pairKey(h, p));
      for (const p of st.lazyPeers) lazy.add(pairKey(h, p));
    }
    if (haveState) {
      // Eager wins over lazy when the two directions disagree.
      for (const key of eager) lazy.delete(key);
      return buildEdges(eager, lazy, filters.hiddenNodes);
    }

    // Fallback: reconstruct from the message stream (older traces).
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
    return buildEdges(eager, lazy, filters.hiddenNodes);
  }, [trace.events, world.idx, world.nodes, world.state, filters.hiddenNodes]);

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
