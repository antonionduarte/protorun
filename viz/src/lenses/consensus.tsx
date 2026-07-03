// consensus.tsx (raft + paxos) — the sequence diagram with per-lane header
// badges driven by state snapshots, plus leader-change markers drawn across the
// diagram.
//   raft:  role-colored badge (follower/candidate/leader) + term + a
//          commit/applied bar (commitIndex vs lastLogIndex).
//   paxos: promised / accepted ballot, and a "decided" chip.
// Shapes come straight from the sample traces' state events
// (raft.DebugStateReply / paxos.DebugStateReply).

import { useMemo } from "react";
import type { LensProps } from "@/lib/lens-types";
import type { Host } from "@/lib/trace";
import { hasProtocol } from "@/lib/lenses";
import { decodePaxos, decodeRaft } from "@/lib/protocols";
import { Badge } from "@/components/ui/badge";
import { SequenceDiagram } from "./sequence";

const ROLE_COLOR: Record<string, string> = {
  leader: "#16a34a",
  candidate: "#d97706",
  follower: "hsl(var(--muted-foreground))",
  unknown: "hsl(var(--muted-foreground))",
};

export function ConsensusLens(props: LensProps) {
  const { trace, world } = props;
  const isRaft = hasProtocol(trace, "raft");

  // Leader-change markers from the raft state stream (whole trace).
  const markers = useMemo(() => {
    if (!isRaft) return [];
    const out: { step: number; label: string }[] = [];
    let last: string | null = null;
    for (const ev of trace.events) {
      if (ev.kind !== "state" || ev.protocol !== "raft") continue;
      const st = decodeRaft(ev.state);
      if (!st || !st.hasLeader || !st.leader) continue;
      const tag = `${st.leader}@T${st.term}`;
      if (tag !== last) {
        last = tag;
        out.push({ step: ev.step, label: `leader ${short(st.leader)} · T${st.term}` });
      }
    }
    return out;
  }, [trace.events, isRaft]);

  const laneHeader = (host: Host) => {
    const byProto = world.state.get(host);
    if (isRaft) {
      const st = decodeRaft(byProto?.get("raft")?.state);
      if (!st) return null;
      const frac =
        st.lastLogIndex > 0 ? st.commitIndex / st.lastLogIndex : 0;
      return (
        <div className="flex w-24 flex-col items-center gap-0.5">
          <span
            className="rounded px-1.5 py-0.5 text-[9px] font-semibold uppercase text-white"
            style={{ backgroundColor: ROLE_COLOR[st.roleName] }}
          >
            {st.roleName} · T{st.term}
          </span>
          <div
            className="h-1.5 w-full overflow-hidden rounded bg-muted"
            title={`commit ${st.commitIndex} / log ${st.lastLogIndex} (applied ${st.lastApplied})`}
          >
            <div
              className="h-full bg-primary"
              style={{ width: `${Math.round(frac * 100)}%` }}
            />
          </div>
          <span className="text-[8px] tabular-nums text-muted-foreground">
            {st.commitIndex}/{st.lastLogIndex}
          </span>
        </div>
      );
    }
    // paxos
    const st = decodePaxos(byProto?.get("paxos")?.state);
    if (!st) return null;
    return (
      <div className="flex w-24 flex-col items-center gap-0.5">
        <Badge
          variant={st.decided ? "default" : "secondary"}
          className="px-1.5 py-0 text-[9px]"
        >
          {st.decided ? `decided b${st.decidedBallot}` : st.proposing ? "proposing" : "idle"}
        </Badge>
        <span className="text-[8px] tabular-nums text-muted-foreground">
          prom {st.promised} · acc {st.hasAccepted ? st.acceptedBallot : "–"}
        </span>
      </div>
    );
  };

  return (
    <SequenceDiagram
      trace={props.trace}
      world={props.world}
      filters={props.filters}
      selectedNode={props.selectedNode}
      onSelectNode={props.onSelectNode}
      laneHeader={laneHeader}
      headerExtra={46}
      markers={markers}
    />
  );
}

function short(host: Host): string {
  const colon = host.lastIndexOf(":");
  const ip = colon < 0 ? host : host.slice(0, colon);
  const octets = ip.split(".");
  return octets.length === 4 ? octets[3] : host;
}
