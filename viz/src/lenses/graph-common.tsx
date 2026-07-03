// graph-common.tsx — the shared force-directed graph canvas used by the
// topology, membership, and broadcast-tree lenses. It owns the persistent
// simulation (via useGraphLayout, positions stable across scrubbing), node
// rendering, selection, and the moving-dot delivery / red-X drop animation.

import { useEffect, useRef, useState } from "react";
import type { Host } from "@/lib/trace";
import { shortHost } from "@/lib/host";
import { useGraphLayout, type GraphEdgeInput } from "@/lib/useGraphLayout";
import { useSize } from "@/lib/useSize";
import { cn } from "@/lib/utils";

export type EdgeStyle = "solid" | "dashed";
export type EdgeTone = "default" | "muted" | "amber" | "primary";

export interface GraphEdge extends GraphEdgeInput {
  style?: EdgeStyle;
  tone?: EdgeTone;
  /** Draw only the a->b half (for asymmetric active-view edges). */
  half?: boolean;
}

export interface DeliveryAnim {
  from: Host;
  to: Host;
  wire: string;
}
export interface DropAnim {
  from: Host;
  to: Host;
  wire: string;
}

export interface GraphViewProps {
  nodes: Host[];
  edges: GraphEdge[];
  isolated?: Set<Host>;
  delivery?: DeliveryAnim | null;
  drop?: DropAnim | null;
  selectedNode: Host | null;
  onSelectNode: (h: Host | null) => void;
  /** Optional per-node accent ring color (CSS color), e.g. role coloring. */
  nodeAccent?: (h: Host) => string | null;
  /** Animation key — bump to restart the delivery dot (usually the step). */
  animKey: number;
}

/** progress 0->1 that restarts whenever `key` changes (~700ms ease). */
function useStepProgress(key: number): number {
  const [p, setP] = useState(1);
  const raf = useRef(0);
  useEffect(() => {
    const start = performance.now();
    const dur = 700;
    setP(0);
    const step = (now: number) => {
      const t = Math.min(1, (now - start) / dur);
      // easeOutCubic
      setP(1 - Math.pow(1 - t, 3));
      if (t < 1) raf.current = requestAnimationFrame(step);
    };
    raf.current = requestAnimationFrame(step);
    return () => cancelAnimationFrame(raf.current);
  }, [key]);
  return p;
}

const toneStroke: Record<EdgeTone, string> = {
  default: "hsl(var(--border))",
  muted: "hsl(var(--muted-foreground) / 0.25)",
  amber: "#d97706",
  primary: "hsl(var(--primary))",
};

export function GraphView(props: GraphViewProps) {
  const {
    nodes,
    edges,
    isolated,
    delivery,
    drop,
    selectedNode,
    onSelectNode,
    nodeAccent,
    animKey,
  } = props;
  const [ref, size] = useSize<HTMLDivElement>();
  const { positions } = useGraphLayout(nodes, edges, size.width, size.height);
  const progress = useStepProgress(animKey);

  const pos = (h: Host) => positions.get(h);

  return (
    <div ref={ref} className="h-full w-full overflow-hidden">
      <svg
        width={size.width}
        height={size.height}
        className="block"
        onClick={() => onSelectNode(null)}
      >
        {/* edges */}
        <g>
          {edges.map((e, i) => {
            const a = pos(e.a);
            const b = pos(e.b);
            if (!a || !b) return null;
            const stroke = toneStroke[e.tone ?? "default"];
            let x2 = b.x;
            let y2 = b.y;
            if (e.half) {
              x2 = a.x + (b.x - a.x) * 0.5;
              y2 = a.y + (b.y - a.y) * 0.5;
            }
            return (
              <line
                key={`e${i}`}
                x1={a.x}
                y1={a.y}
                x2={x2}
                y2={y2}
                stroke={stroke}
                strokeWidth={e.tone === "primary" ? 2 : 1.5}
                strokeDasharray={e.style === "dashed" ? "4 4" : undefined}
                strokeLinecap="round"
              />
            );
          })}
        </g>

        {/* delivery dot */}
        {delivery &&
          (() => {
            const a = pos(delivery.from);
            const b = pos(delivery.to);
            if (!a || !b) return null;
            const x = a.x + (b.x - a.x) * progress;
            const y = a.y + (b.y - a.y) * progress;
            return (
              <g key="delivery" pointerEvents="none">
                <line
                  x1={a.x}
                  y1={a.y}
                  x2={b.x}
                  y2={b.y}
                  stroke="hsl(var(--primary))"
                  strokeWidth={2}
                  strokeOpacity={0.5}
                />
                <circle cx={x} cy={y} r={5} fill="hsl(var(--primary))" />
                <text
                  x={x + 8}
                  y={y - 8}
                  fontSize={10}
                  fill="hsl(var(--foreground))"
                  className="font-mono"
                >
                  {delivery.wire}
                </text>
              </g>
            );
          })()}

        {/* drop: red X mid-edge */}
        {drop &&
          (() => {
            const a = pos(drop.from);
            const b = pos(drop.to);
            if (!a || !b) return null;
            const mx = (a.x + b.x) / 2;
            const my = (a.y + b.y) / 2;
            const r = 6;
            return (
              <g key="drop" pointerEvents="none">
                <line
                  x1={mx - r}
                  y1={my - r}
                  x2={mx + r}
                  y2={my + r}
                  stroke="hsl(var(--destructive))"
                  strokeWidth={2.5}
                />
                <line
                  x1={mx - r}
                  y1={my + r}
                  x2={mx + r}
                  y2={my - r}
                  stroke="hsl(var(--destructive))"
                  strokeWidth={2.5}
                />
                <text
                  x={mx + 10}
                  y={my - 4}
                  fontSize={10}
                  fill="hsl(var(--destructive))"
                  className="font-mono"
                >
                  {drop.wire}
                </text>
              </g>
            );
          })()}

        {/* nodes */}
        <g>
          {nodes.map((h) => {
            const p = pos(h);
            if (!p) return null;
            const selected = h === selectedNode;
            const iso = isolated?.has(h);
            const accent = nodeAccent?.(h) ?? null;
            return (
              <g
                key={h}
                transform={`translate(${p.x},${p.y})`}
                onClick={(ev) => {
                  ev.stopPropagation();
                  onSelectNode(h);
                }}
                className="cursor-pointer"
              >
                {iso && (
                  <circle
                    r={20}
                    fill="none"
                    stroke="hsl(var(--muted-foreground))"
                    strokeOpacity={0.4}
                    strokeDasharray="3 3"
                  />
                )}
                <circle
                  r={14}
                  fill={
                    selected ? "hsl(var(--primary))" : "hsl(var(--secondary))"
                  }
                  stroke={
                    accent ??
                    (selected
                      ? "hsl(var(--primary))"
                      : "hsl(var(--border))")
                  }
                  strokeWidth={accent ? 3 : 1.5}
                />
                <text
                  textAnchor="middle"
                  dy={4}
                  fontSize={9}
                  className={cn(
                    "font-mono select-none",
                    selected
                      ? "fill-[hsl(var(--primary-foreground))]"
                      : "fill-[hsl(var(--secondary-foreground))]"
                  )}
                >
                  {shortHost(h)}
                </text>
              </g>
            );
          })}
        </g>
      </svg>
    </div>
  );
}
