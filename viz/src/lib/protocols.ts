// protocols.ts — typed decoders for the DebugState shapes the sample traces
// actually carry. These match the Go reply structs verbatim (ground truth):
//   raft.DebugStateReply, paxos.DebugStateReply, hyparview.DebugStateReply,
//   plumtree.DebugStatsReply.
// Each decoder is defensive: a shape that doesn't match returns null so a lens
// can fall back to raw JSON.

import { hostFromAddr, type Host } from "./trace";

export interface RaftState {
  role: number; // 0 follower, 1 candidate, 2 leader
  roleName: "follower" | "candidate" | "leader" | "unknown";
  term: number;
  commitIndex: number;
  lastApplied: number;
  lastLogIndex: number;
  lastLogTerm: number;
  leader: Host | null;
  hasLeader: boolean;
}

const RAFT_ROLES = ["follower", "candidate", "leader"] as const;

export function decodeRaft(state: unknown): RaftState | null {
  if (!state || typeof state !== "object") return null;
  const s = state as Record<string, unknown>;
  if (!("Role" in s) || !("Term" in s)) return null;
  const role = numOr(s.Role, 0);
  return {
    role,
    roleName: RAFT_ROLES[role] ?? "unknown",
    term: numOr(s.Term, 0),
    commitIndex: numOr(s.CommitIndex, 0),
    lastApplied: numOr(s.LastApplied, 0),
    lastLogIndex: numOr(s.LastLogIndex, 0),
    lastLogTerm: numOr(s.LastLogTerm, 0),
    leader: s.HasLeader ? hostFromAddr(s.Leader) : null,
    hasLeader: Boolean(s.HasLeader),
  };
}

export interface PaxosState {
  promised: number;
  acceptedBallot: number;
  hasAccepted: boolean;
  decided: boolean;
  decidedBallot: number;
  proposing: boolean;
  myBallot: number;
}

export function decodePaxos(state: unknown): PaxosState | null {
  if (!state || typeof state !== "object") return null;
  const s = state as Record<string, unknown>;
  if (!("Promised" in s) || !("MyBallot" in s)) return null;
  return {
    promised: numOr(s.Promised, 0),
    acceptedBallot: numOr(s.AcceptedBallot, 0),
    hasAccepted: Boolean(s.HasAccepted),
    decided: Boolean(s.Decided),
    decidedBallot: numOr(s.DecidedBallot, 0),
    proposing: Boolean(s.Proposing),
    myBallot: numOr(s.MyBallot, 0),
  };
}

export interface HyparviewState {
  active: Host[];
  passive: Host[];
}

export function decodeHyparview(state: unknown): HyparviewState | null {
  if (!state || typeof state !== "object") return null;
  const s = state as Record<string, unknown>;
  if (!("Active" in s) || !("Passive" in s)) return null;
  return {
    active: hostList(s.Active),
    passive: hostList(s.Passive),
  };
}

export interface PlumtreeStats {
  delivered: number;
  duplicates: number;
  eager: number; // count only — the trace carries no per-peer lists
  lazy: number;
}

export function decodePlumtree(state: unknown): PlumtreeStats | null {
  if (!state || typeof state !== "object") return null;
  const s = state as Record<string, unknown>;
  if (!("Eager" in s) || !("Lazy" in s)) return null;
  return {
    delivered: numOr(s.Delivered, 0),
    duplicates: numOr(s.Duplicates, 0),
    eager: numOr(s.Eager, 0),
    lazy: numOr(s.Lazy, 0),
  };
}

function numOr(v: unknown, d: number): number {
  return typeof v === "number" ? v : d;
}

function hostList(v: unknown): Host[] {
  if (!Array.isArray(v)) return [];
  const out: Host[] = [];
  for (const item of v) {
    const h = hostFromAddr(item);
    if (h) out.push(h);
  }
  return out;
}

/** Role -> a semantic tone used by the consensus lens badges. */
export function raftRoleTone(
  role: number
): "leader" | "candidate" | "follower" {
  if (role === 2) return "leader";
  if (role === 1) return "candidate";
  return "follower";
}
