// lenses.ts — the lens registry. Universal lenses always match; protocol
// lenses register against a predicate over the trace's declared protocol names
// (the %T stacks on `node` events, e.g. "*raft.Protocol", plus the labels on
// `state` events, e.g. "raft"). The floor is universal (Topology + Sequence +
// Inspector); the ceiling is pluggable per protocol.

import type { Lens } from "./lens-types";
import type { ParsedTrace } from "./trace";
import { TopologyLens } from "@/lenses/topology";
import { SequenceLens } from "@/lenses/sequence";
import { MembershipLens } from "@/lenses/membership";
import { TreeLens } from "@/lenses/tree";
import { ConsensusLens } from "@/lenses/consensus";

/** True if any of `needles` appears (case-insensitively) in the trace's
 * protocol identifiers (node %T stacks or state labels). */
export function hasProtocol(trace: ParsedTrace, ...needles: string[]): boolean {
  const hay = [...trace.nodeProtocols, ...trace.protocols].map((s) =>
    s.toLowerCase()
  );
  return needles.some((n) => {
    const needle = n.toLowerCase();
    return hay.some((h) => h.includes(needle));
  });
}

const REGISTRY: Lens[] = [
  {
    id: "topology",
    title: "Topology",
    canRender: () => true,
    Component: TopologyLens,
  },
  {
    id: "sequence",
    title: "Sequence",
    canRender: () => true,
    Component: SequenceLens,
  },
  {
    id: "membership",
    title: "Membership",
    canRender: (t) => hasProtocol(t, "hyparview"),
    Component: MembershipLens,
  },
  {
    id: "tree",
    title: "Broadcast tree",
    canRender: (t) => hasProtocol(t, "plumtree"),
    Component: TreeLens,
  },
  {
    id: "consensus",
    title: "Consensus",
    canRender: (t) => hasProtocol(t, "raft", "paxos"),
    Component: ConsensusLens,
  },
];

/** Lenses that apply to a trace, in registry (stable) order. */
export function lensesFor(trace: ParsedTrace): Lens[] {
  return REGISTRY.filter((l) => l.canRender(trace));
}

export { REGISTRY as allLenses };
