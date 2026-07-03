// topology.tsx (universal) — the overlay graph, reborn. Nodes are alive nodes;
// edges are live sessions. The delivery at the current step animates as a dot
// travelling its edge, drops flash a red X mid-edge. Cut pairs render as no
// edge; isolated nodes get a muted dashed ring. Node positions stay stable
// across scrubbing (persistent simulation in useGraphLayout).

import { useMemo } from "react";
import type { LensProps } from "@/lib/lens-types";
import { pairHosts } from "@/lib/fold";
import type { GraphEdge } from "./graph-common";
import { GraphView } from "./graph-common";

export function TopologyLens({
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
    const out: GraphEdge[] = [];
    for (const key of world.sessions) {
      if (world.cutPairs.has(key)) continue; // cut => no edge
      const [a, b] = pairHosts(key);
      if (filters.hiddenNodes.has(a) || filters.hiddenNodes.has(b)) continue;
      const lossy = world.lossy.has(key);
      out.push({ a, b, style: lossy ? "dashed" : "solid", tone: "default" });
    }
    return out;
  }, [world.sessions, world.cutPairs, world.lossy, filters.hiddenNodes]);

  // Current-step delivery / drop for animation.
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
      (e) =>
        e.kind === "drop" &&
        e.from &&
        e.to &&
        !filters.hiddenNodes.has(e.from) &&
        !filters.hiddenNodes.has(e.to) &&
        !filters.hiddenWires.has(e.wire ?? "")
    );
    return ev ? { from: ev.from!, to: ev.to!, wire: ev.wire ?? "" } : null;
  }, [world.current, filters]);

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
