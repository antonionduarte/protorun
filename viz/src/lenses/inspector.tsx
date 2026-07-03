// inspector.tsx (universal chrome, right panel) — the current step's event
// detail plus the selected node's latest state snapshot per protocol, pretty-
// printed with per-key diff highlighting against that (node, protocol)'s
// PREVIOUS distinct snapshot (added keys green, changed keys amber). Degrades
// gracefully when a trace carries no state events.

import { useMemo } from "react";
import type { Host, ParsedTrace, TraceEvent } from "@/lib/trace";
import type { WorldState } from "@/lib/fold";
import { shortHost, shortProtocol } from "@/lib/host";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { ScrollArea } from "@/components/ui/scroll-area";
import { cn } from "@/lib/utils";

export interface InspectorProps {
  trace: ParsedTrace;
  world: WorldState;
  selectedNode: Host | null;
  onSelectNode: (h: Host | null) => void;
}

export function Inspector({ world, selectedNode }: InspectorProps) {
  const stateByProto = selectedNode ? world.state.get(selectedNode) : undefined;

  return (
    <ScrollArea className="h-full">
      <div className="space-y-4 p-4 text-sm">
        <section>
          <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
            Step {world.step}
          </h3>
          <CurrentEvents events={world.current} />
        </section>

        <Separator />

        <section>
          <div className="mb-2 flex items-center justify-between">
            <h3 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
              Node
            </h3>
            {selectedNode && (
              <Badge variant="secondary" className="font-mono">
                {shortHost(selectedNode)}
              </Badge>
            )}
          </div>
          {!selectedNode && (
            <p className="text-muted-foreground">
              Click a node in the graph or a lane header to inspect its state.
            </p>
          )}
          {selectedNode && !stateByProto && (
            <p className="text-muted-foreground">
              No state snapshots recorded for this node yet.
            </p>
          )}
          {selectedNode &&
            stateByProto &&
            [...stateByProto.entries()].map(([proto, slot]) => (
              <div key={proto} className="mb-3">
                <div className="mb-1 flex items-center gap-2">
                  <span className="font-medium">{shortProtocol(proto)}</span>
                  <span className="text-xs text-muted-foreground">
                    @ step {slot.step}
                  </span>
                </div>
                <StateDiff current={slot.state} prev={slot.prev} />
              </div>
            ))}
        </section>
      </div>
    </ScrollArea>
  );
}

function CurrentEvents({ events }: { events: TraceEvent[] }) {
  if (events.length === 0) {
    return <p className="text-muted-foreground">No events at this step.</p>;
  }
  return (
    <div className="space-y-1 font-mono text-xs">
      {events.map((ev) => (
        <div key={ev.idx} className="rounded border bg-muted/40 px-2 py-1">
          <EventLine ev={ev} />
        </div>
      ))}
    </div>
  );
}

function EventLine({ ev }: { ev: TraceEvent }) {
  switch (ev.kind) {
    case "deliver":
      return (
        <span>
          <Kind tone="deliver">deliver</Kind> {shortHost(ev.from ?? "")} →{" "}
          {shortHost(ev.to ?? "")}{" "}
          <span className="text-primary">{ev.wire}</span>
          {ev.bytes ? (
            <span className="text-muted-foreground"> ({ev.bytes}B)</span>
          ) : null}
        </span>
      );
    case "drop":
      return (
        <span>
          <Kind tone="drop">drop</Kind> {shortHost(ev.from ?? "")} →{" "}
          {shortHost(ev.to ?? "")} <span>{ev.wire}</span>{" "}
          <span className="text-destructive">[{ev.reason}]</span>
        </span>
      );
    case "session":
      return (
        <span>
          <Kind tone="session">{ev.event}</Kind> {shortHost(ev.node ?? "")} —{" "}
          {shortHost(ev.peer ?? "")}
        </span>
      );
    case "fault":
      return (
        <span>
          <Kind tone="fault">{ev.mutation}</Kind>{" "}
          {(ev.nodes ?? []).map(shortHost).join(" — ")}
          {ev.params ? (
            <span className="text-muted-foreground"> {JSON.stringify(ev.params)}</span>
          ) : null}
        </span>
      );
    case "clock":
      return (
        <span>
          <Kind tone="clock">clock</Kind> {ev.t}
        </span>
      );
    case "state":
      return (
        <span>
          <Kind tone="state">state</Kind> {shortHost(ev.node ?? "")} /{" "}
          {ev.protocol}
        </span>
      );
    default:
      return <span>{ev.kind}</span>;
  }
}

function Kind({
  tone,
  children,
}: {
  tone: string;
  children: React.ReactNode;
}) {
  const cls =
    tone === "drop" || tone === "fault"
      ? "text-destructive"
      : tone === "deliver"
        ? "text-primary"
        : "text-muted-foreground";
  return <span className={cn("font-semibold", cls)}>{children}</span>;
}

/** Pretty-print an object with per-top-level-key diff highlighting. */
function StateDiff({ current, prev }: { current: unknown; prev: unknown }) {
  const rows = useMemo(() => diffRows(current, prev), [current, prev]);
  if (rows === null) {
    // Non-object state: just show raw JSON.
    return (
      <pre className="overflow-x-auto rounded bg-muted/40 p-2 font-mono text-xs">
        {safeJson(current)}
      </pre>
    );
  }
  return (
    <div className="overflow-hidden rounded border font-mono text-xs">
      {rows.map((r) => (
        <div
          key={r.key}
          className={cn(
            "flex items-start gap-2 px-2 py-0.5",
            r.status === "changed" && "bg-amber-500/15",
            r.status === "added" && "bg-emerald-500/15"
          )}
        >
          <span className="text-muted-foreground">{r.key}</span>
          <span className="ml-auto whitespace-pre-wrap break-all text-right">
            {r.value}
          </span>
        </div>
      ))}
    </div>
  );
}

interface DiffRow {
  key: string;
  value: string;
  status: "same" | "changed" | "added";
}

function diffRows(current: unknown, prev: unknown): DiffRow[] | null {
  if (!current || typeof current !== "object" || Array.isArray(current)) {
    return null;
  }
  const cur = current as Record<string, unknown>;
  const old = (prev && typeof prev === "object" ? prev : {}) as Record<
    string,
    unknown
  >;
  const hasPrev = prev !== undefined && prev !== null;
  return Object.keys(cur).map((key) => {
    const value = safeJson(cur[key]);
    let status: DiffRow["status"] = "same";
    if (hasPrev) {
      if (!(key in old)) status = "added";
      else if (safeJson(old[key]) !== value) status = "changed";
    }
    return { key, value, status };
  });
}

function safeJson(v: unknown): string {
  try {
    return JSON.stringify(v);
  } catch {
    return String(v);
  }
}
