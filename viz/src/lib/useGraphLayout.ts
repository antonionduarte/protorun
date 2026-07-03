// useGraphLayout — a persistent d3-force simulation whose node positions stay
// STABLE across scrubbing. The old overlay-graph-visualizer got this right by
// recycling existing node objects on every membership recompute ("oldNodes"
// trick) so unchanged nodes keep their coordinates and only genuinely new
// members are seeded; the simulation is only re-heated (alpha nudged) when the
// membership set actually changes, never on a mere edge/scrub update.

import { useEffect, useMemo, useRef, useState } from "react";
import {
  forceCenter,
  forceLink,
  forceManyBody,
  forceSimulation,
  forceCollide,
  type Simulation,
  type SimulationNodeDatum,
  type SimulationLinkDatum,
} from "d3-force";
import type { Host } from "./trace";

interface GNode extends SimulationNodeDatum {
  id: Host;
}
interface GLink extends SimulationLinkDatum<GNode> {
  source: string | GNode;
  target: string | GNode;
}

export interface Positioned {
  id: Host;
  x: number;
  y: number;
}

export interface GraphEdgeInput {
  a: Host;
  b: Host;
}

/**
 * Returns a Map<Host, {x,y}> of current node positions plus a version counter
 * that bumps every animation frame so consumers re-render. Positions persist
 * across calls; only a change in the node membership re-heats the layout.
 */
export function useGraphLayout(
  nodes: Host[],
  edges: GraphEdgeInput[],
  width: number,
  height: number
): { positions: Map<Host, Positioned>; tick: number } {
  const simRef = useRef<Simulation<GNode, GLink> | null>(null);
  const nodeMapRef = useRef<Map<Host, GNode>>(new Map());
  const [tick, setTick] = useState(0);
  const rafRef = useRef<number>(0);

  // A stable membership signature so we only reheat when the SET changes.
  const membership = useMemo(() => [...nodes].sort().join(","), [nodes]);

  // Initialize the simulation once.
  useEffect(() => {
    const sim = forceSimulation<GNode, GLink>([])
      .force("charge", forceManyBody<GNode>().strength(-240))
      .force("collide", forceCollide<GNode>(26))
      .force("link", forceLink<GNode, GLink>([]).id((d) => d.id).distance(90))
      .force("center", forceCenter(width / 2, height / 2))
      .alpha(0.9)
      .alphaDecay(0.05)
      .on("tick", () => {
        rafRef.current = requestAnimationFrame(() => setTick((t) => t + 1));
      });
    simRef.current = sim;
    return () => {
      sim.stop();
      cancelAnimationFrame(rafRef.current);
    };
    // width/height captured at init; a resize just recentres below.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Recompute node set when membership changes — recycle existing coordinates.
  useEffect(() => {
    const sim = simRef.current;
    if (!sim) return;
    const prev = nodeMapRef.current;
    const next = new Map<Host, GNode>();
    let changed = prev.size !== nodes.length;
    for (const id of nodes) {
      const existing = prev.get(id);
      if (existing) {
        next.set(id, existing); // keep x/y/vx/vy — stable position
      } else {
        // Seed a new node near the centre with a small jitter.
        next.set(id, {
          id,
          x: width / 2 + (Math.random() - 0.5) * 60,
          y: height / 2 + (Math.random() - 0.5) * 60,
        });
        changed = true;
      }
    }
    nodeMapRef.current = next;
    sim.nodes([...next.values()]);
    if (changed) sim.alpha(0.6).restart(); // gentle nudge only on membership change
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [membership]);

  // Update links every render (cheap; does not reheat).
  useEffect(() => {
    const sim = simRef.current;
    if (!sim) return;
    const nodeMap = nodeMapRef.current;
    const glinks: GLink[] = [];
    for (const e of edges) {
      if (nodeMap.has(e.a) && nodeMap.has(e.b)) {
        glinks.push({ source: e.a, target: e.b });
      }
    }
    const linkForce = sim.force("link") as ReturnType<
      typeof forceLink<GNode, GLink>
    >;
    linkForce.links(glinks);
    // Keep alpha where it is; a tiny top-up keeps a static graph from freezing
    // mid-settle without visibly jolting positions.
    if (sim.alpha() < 0.02) sim.alpha(0.03).restart();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [edges]);

  // Keep the centre force in sync with the container size.
  useEffect(() => {
    const sim = simRef.current;
    if (!sim) return;
    sim.force("center", forceCenter(width / 2, height / 2));
  }, [width, height]);

  const positions = useMemo(() => {
    const m = new Map<Host, Positioned>();
    for (const [id, n] of nodeMapRef.current) {
      m.set(id, { id, x: n.x ?? width / 2, y: n.y ?? height / 2 });
    }
    return m;
    // Recompute on every tick.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tick, membership]);

  return { positions, tick };
}
