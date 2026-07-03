// membership.tsx (hyparview) — a topology variant whose edges are driven by
// hyparview DebugState (Active/Passive views) rather than raw sessions:
//   active view      -> solid edge
//   passive view     -> dashed ghost edge (muted)
//   asymmetric active (A lists B in Active, B does NOT list A) -> half edge
//                       drawn from A's side, highlighted amber (a symmetric-
//                       view violation, which HyParView tries to avoid).
// Uses the real hyparview.DebugStateReply shape: {Active:[{IP,Port}], Passive}.

import { useMemo } from "react";
import type { LensProps } from "@/lib/lens-types";
import { pairKey } from "@/lib/fold";
import { decodeHyparview } from "@/lib/protocols";
import type { Host } from "@/lib/trace";
import type { GraphEdge } from "./graph-common";
import { GraphView } from "./graph-common";

export function MembershipLens({
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
    // Directed active adjacency + undirected passive set.
    const active = new Map<Host, Set<Host>>();
    const passivePairs = new Set<string>();
    for (const h of nodes) {
      const byProto = world.state.get(h);
      const slot = byProto?.get("hyparview");
      const st = slot ? decodeHyparview(slot.state) : null;
      if (!st) continue;
      active.set(h, new Set(st.active.filter((p) => nodes.includes(p))));
      for (const p of st.passive) {
        if (nodes.includes(p)) passivePairs.add(pairKey(h, p));
      }
    }

    const out: GraphEdge[] = [];
    const drawnActive = new Set<string>();
    for (const [a, peers] of active) {
      for (const b of peers) {
        const key = pairKey(a, b);
        const symmetric = active.get(b)?.has(a) ?? false;
        if (symmetric) {
          if (drawnActive.has(key)) continue;
          drawnActive.add(key);
          out.push({ a, b, style: "solid", tone: "default" });
        } else {
          // Asymmetric: A lists B but not vice versa -> amber half edge from A.
          out.push({ a, b, style: "solid", tone: "amber", half: true });
        }
      }
    }
    // Passive ghosts only where there's no active edge already.
    for (const key of passivePairs) {
      if (drawnActive.has(key)) continue;
      const [a, b] = key.split("|");
      out.push({ a, b, style: "dashed", tone: "muted" });
    }
    return out;
  }, [nodes, world.state]);

  const delivery = useMemo(() => {
    const ev = world.current.find(
      (e) =>
        e.kind === "deliver" &&
        e.from &&
        e.to &&
        !filters.hiddenNodes.has(e.from) &&
        !filters.hiddenNodes.has(e.to) &&
        !filters.hiddenWires.has(e.wire ?? "")
    );
    return ev ? { from: ev.from!, to: ev.to!, wire: ev.wire ?? "" } : null;
  }, [world.current, filters]);

  const drop = useMemo(() => {
    const ev = world.current.find(
      (e) => e.kind === "drop" && e.from && e.to
    );
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
      animKey={world.step}
    />
  );
}
