// sequence.tsx (universal) — a Lamport diagram. Vertical lanes per node
// (sorted), time flowing DOWN by event order; deliver events are lane->lane
// arrows labeled with wire type, drops are truncated arrows ending in a red X,
// session events are lane tick marks. Rendering is WINDOWED: only events within
// ±WINDOW of the current step are drawn, so the 6.4k-line hyparview trace stays
// smooth. Wire-type filtering (from ⌘K or the legend) hides arrow classes.
//
// The diagram is factored into <SequenceDiagram/> so the consensus lens can
// layer per-lane header badges and leader-change markers on top of it.

import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import type { LensProps } from "@/lib/lens-types";
import type { ParsedTrace, Host, TraceEvent } from "@/lib/trace";
import type { WorldState } from "@/lib/fold";
import type { ViewFilters } from "@/lib/lens-types";
import { shortHost } from "@/lib/host";
import { cn } from "@/lib/utils";

const WINDOW = 200;
const ROW_H = 22;
const HEADER_H = 44;
const LEFT_PAD = 8;

/** Natural host sort: by IP octets then port. */
export function sortHosts(hosts: Host[]): Host[] {
  return [...hosts].sort((a, b) => {
    const pa = parseHost(a);
    const pb = parseHost(b);
    for (let i = 0; i < 4; i++) {
      if (pa.octets[i] !== pb.octets[i]) return pa.octets[i] - pb.octets[i];
    }
    return pa.port - pb.port;
  });
}
function parseHost(h: Host): { octets: number[]; port: number } {
  const colon = h.lastIndexOf(":");
  const ip = colon < 0 ? h : h.slice(0, colon);
  const port = colon < 0 ? 0 : Number(h.slice(colon + 1)) || 0;
  const octets = ip.split(".").map((n) => Number(n) || 0);
  while (octets.length < 4) octets.push(0);
  return { octets, port };
}

export interface SequenceDiagramProps {
  trace: ParsedTrace;
  world: WorldState;
  filters: ViewFilters;
  selectedNode: Host | null;
  onSelectNode: (h: Host | null) => void;
  /** Optional extra content per lane header (consensus badges). */
  laneHeader?: (host: Host) => ReactNode;
  /** Extra header height when laneHeader is supplied. */
  headerExtra?: number;
  /** Horizontal markers across the diagram at given steps (leader changes). */
  markers?: { step: number; label: string; host?: Host }[];
}

export function SequenceDiagram({
  trace,
  world,
  filters,
  selectedNode,
  onSelectNode,
  laneHeader,
  headerExtra = 0,
  markers = [],
}: SequenceDiagramProps) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const [hovered, setHovered] = useState<number | null>(null);
  const [width, setWidth] = useState(800);
  const wrapRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = wrapRef.current;
    if (!el) return;
    const ro = new ResizeObserver((es) => {
      for (const e of es) if (e.contentRect.width > 0) setWidth(e.contentRect.width);
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const lanes = useMemo(
    () =>
      sortHosts(
        (world.nodes.length ? world.nodes : trace.meta.nodes).filter(
          (h) => !filters.hiddenNodes.has(h)
        )
      ),
    [world.nodes, trace.meta.nodes, filters.hiddenNodes]
  );

  const laneX = useMemo(() => {
    const m = new Map<Host, number>();
    const gap =
      lanes.length > 1
        ? Math.max(70, (width - 2 * LEFT_PAD - 40) / (lanes.length - 1))
        : 0;
    lanes.forEach((h, i) => m.set(h, LEFT_PAD + 20 + i * gap));
    return m;
  }, [lanes, width]);

  const headH = HEADER_H + headerExtra;

  // The window of drawable events around the current index.
  const start = Math.max(0, world.idx - WINDOW);
  const end = Math.min(trace.events.length, world.idx + WINDOW + 1);
  const windowEvents = useMemo(() => {
    const out: { ev: TraceEvent; local: number }[] = [];
    for (let i = start; i < end; i++) {
      const ev = trace.events[i];
      if (ev.kind !== "deliver" && ev.kind !== "drop" && ev.kind !== "session")
        continue;
      if (ev.wire && filters.hiddenWires.has(ev.wire)) continue;
      out.push({ ev, local: i - start });
    }
    return out;
  }, [trace.events, start, end, filters.hiddenWires]);

  const svgH = (end - start) * ROW_H + 16;

  // Auto-scroll so the current step sits near the vertical centre.
  useEffect(() => {
    const el = scrollRef.current;
    if (!el) return;
    const y = (world.idx - start) * ROW_H;
    el.scrollTo({ top: Math.max(0, y - el.clientHeight / 2), behavior: "auto" });
  }, [world.idx, start]);

  return (
    <div ref={wrapRef} className="flex h-full flex-col">
      {/* sticky lane headers */}
      <div
        className="relative shrink-0 border-b bg-background"
        style={{ height: headH }}
      >
        <svg width={width} height={headH}>
          {lanes.map((h) => {
            const x = laneX.get(h)!;
            const sel = h === selectedNode;
            return (
              <g
                key={h}
                transform={`translate(${x},0)`}
                className="cursor-pointer"
                onClick={() => onSelectNode(sel ? null : h)}
              >
                <text
                  textAnchor="middle"
                  y={14}
                  fontSize={10}
                  className={cn(
                    "font-mono",
                    sel ? "fill-[hsl(var(--primary))]" : "fill-foreground"
                  )}
                >
                  {shortHost(h)}
                </text>
              </g>
            );
          })}
        </svg>
        {laneHeader && (
          <div className="pointer-events-none absolute inset-0" style={{ top: 18 }}>
            {lanes.map((h) => {
              const x = laneX.get(h)!;
              return (
                <div
                  key={h}
                  className="pointer-events-auto absolute -translate-x-1/2"
                  style={{ left: x, top: 0 }}
                >
                  {laneHeader(h)}
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* scrollable body */}
      <div ref={scrollRef} className="relative flex-1 overflow-y-auto">
        <svg width={width} height={svgH} className="block">
          <defs>
            <marker
              id="seq-arrow"
              viewBox="0 0 10 10"
              refX="9"
              refY="5"
              markerWidth="6"
              markerHeight="6"
              orient="auto-start-reverse"
            >
              <path d="M 0 0 L 10 5 L 0 10 z" fill="hsl(var(--foreground))" />
            </marker>
          </defs>

          {/* lane lines */}
          {lanes.map((h) => {
            const x = laneX.get(h)!;
            return (
              <line
                key={h}
                x1={x}
                y1={0}
                x2={x}
                y2={svgH}
                stroke="hsl(var(--border))"
                strokeWidth={1}
              />
            );
          })}

          {/* horizontal markers (leader changes) */}
          {markers.map((m, i) => {
            const idx = trace.events.findIndex((e) => e.step === m.step);
            if (idx < start || idx >= end) return null;
            const y = (idx - start) * ROW_H + ROW_H / 2;
            return (
              <g key={`m${i}`}>
                <line
                  x1={0}
                  y1={y}
                  x2={width}
                  y2={y}
                  stroke="hsl(var(--primary))"
                  strokeOpacity={0.4}
                  strokeDasharray="6 4"
                />
                <text
                  x={LEFT_PAD}
                  y={y - 3}
                  fontSize={9}
                  className="fill-[hsl(var(--primary))] font-mono"
                >
                  {m.label}
                </text>
              </g>
            );
          })}

          {/* current-step highlight band */}
          {world.idx >= start && world.idx < end && (
            <rect
              x={0}
              y={(world.idx - start) * ROW_H}
              width={width}
              height={ROW_H}
              fill="hsl(var(--primary))"
              fillOpacity={0.06}
            />
          )}

          {/* events */}
          {windowEvents.map(({ ev, local }) => {
            const y = local * ROW_H + ROW_H / 2;
            const isCurrent = ev.idx === world.idx;
            const showLabel = isCurrent || hovered === ev.idx;

            if (ev.kind === "session") {
              const x = laneX.get(ev.node ?? "");
              if (x === undefined) return null;
              const color =
                ev.event === "connected"
                  ? "hsl(var(--primary))"
                  : ev.event === "failed"
                    ? "hsl(var(--destructive))"
                    : "hsl(var(--muted-foreground))";
              return (
                <g
                  key={ev.idx}
                  onMouseEnter={() => setHovered(ev.idx)}
                  onMouseLeave={() => setHovered(null)}
                >
                  <rect x={x - 5} y={y - 3} width={10} height={6} fill={color} rx={1} />
                  {showLabel && (
                    <text
                      x={x + 8}
                      y={y + 3}
                      fontSize={9}
                      className="fill-muted-foreground font-mono"
                    >
                      {ev.event} {ev.peer ? shortHost(ev.peer) : ""}
                    </text>
                  )}
                </g>
              );
            }

            // deliver / drop
            const x1 = laneX.get(ev.from ?? "");
            const x2 = laneX.get(ev.to ?? "");
            if (x1 === undefined || x2 === undefined) return null;
            if (ev.kind === "drop") {
              const dropx = x1 + (x2 - x1) * 0.55;
              return (
                <g
                  key={ev.idx}
                  onMouseEnter={() => setHovered(ev.idx)}
                  onMouseLeave={() => setHovered(null)}
                >
                  <line
                    x1={x1}
                    y1={y}
                    x2={dropx}
                    y2={y}
                    stroke="hsl(var(--destructive))"
                    strokeWidth={1.5}
                    strokeDasharray="3 2"
                  />
                  <line x1={dropx - 4} y1={y - 4} x2={dropx + 4} y2={y + 4} stroke="hsl(var(--destructive))" strokeWidth={2} />
                  <line x1={dropx - 4} y1={y + 4} x2={dropx + 4} y2={y - 4} stroke="hsl(var(--destructive))" strokeWidth={2} />
                  {showLabel && (
                    <text x={dropx + 8} y={y - 4} fontSize={9} className="fill-[hsl(var(--destructive))] font-mono">
                      {ev.wire} (drop: {ev.reason})
                    </text>
                  )}
                </g>
              );
            }
            // deliver
            return (
              <g
                key={ev.idx}
                onMouseEnter={() => setHovered(ev.idx)}
                onMouseLeave={() => setHovered(null)}
              >
                <line
                  x1={x1}
                  y1={y}
                  x2={x2}
                  y2={y}
                  stroke={isCurrent ? "hsl(var(--primary))" : "hsl(var(--foreground))"}
                  strokeOpacity={isCurrent ? 1 : 0.55}
                  strokeWidth={isCurrent ? 2 : 1}
                  markerEnd="url(#seq-arrow)"
                />
                {showLabel && (
                  <text
                    x={(x1 + x2) / 2}
                    y={y - 4}
                    textAnchor="middle"
                    fontSize={9}
                    className={cn(
                      "font-mono",
                      isCurrent ? "fill-[hsl(var(--primary))]" : "fill-muted-foreground"
                    )}
                  >
                    {ev.wire}
                  </text>
                )}
              </g>
            );
          })}
        </svg>
      </div>
    </div>
  );
}

export function SequenceLens({
  trace,
  world,
  filters,
  selectedNode,
  onSelectNode,
}: LensProps) {
  return (
    <SequenceDiagram
      trace={trace}
      world={world}
      filters={filters}
      selectedNode={selectedNode}
      onSelectNode={onSelectNode}
    />
  );
}
