package raft

import (
	"fmt"

	"github.com/antonionduarte/protorun/pkg/protorun"
	"github.com/antonionduarte/protorun/pkg/transport"
)

// This file is the public IPC surface of the Raft protocol: the requests,
// replies, and notifications other protocols (and tests) use to drive and
// observe it. All of it is local, same-runtime IPC — never on the wire —
// so the types embed the protorun Base* markers and carry no codec.

// Propose asks this node to append Command to the replicated log. It
// succeeds only on the leader: the reply carries the index and term the
// entry was assigned (NOT a commit acknowledgement — the entry is
// committed asynchronously and surfaces later as an Applied
// notification). On a non-leader the request fails with a *NotLeaderError
// naming the leader this node currently believes in, if any, so a client
// can redirect.
type Propose struct {
	protorun.BaseRequest
	Command []byte
}

// ProposeReply is the successful answer to a Propose: the log position the
// command was assigned by the leader.
type ProposeReply struct {
	protorun.BaseReply
	Index uint64
	Term  uint64
}

// NotLeaderError is returned (via Responder.Fail) when a Propose reaches a
// server that is not the leader. Leader/HasLeader name the server this
// node last accepted as leader for its current term, so a client can
// redirect; HasLeader is false when this node knows of no leader (e.g. an
// election is in progress).
type NotLeaderError struct {
	Leader    transport.Host
	HasLeader bool
}

func (e *NotLeaderError) Error() string {
	if e.HasLeader {
		return fmt.Sprintf("raft: not leader, try %s", e.Leader.String())
	}
	return "raft: not leader, no known leader"
}

// Applied is published, in strict log order, each time a committed entry
// is applied to the state machine. Subscribers see every command exactly
// once and in the same order on every node — this is the observable form
// of Raft's State Machine Safety property.
type Applied struct {
	protorun.BaseNotification
	Index   uint64
	Term    uint64
	Command []byte
}

// LeaderChanged is published whenever this node's belief about (leader,
// term) changes: when it becomes leader itself, or when it accepts an
// AppendEntries from a new leader. Collected across nodes, the stream
// witnesses Election Safety — for any given term, every LeaderChanged
// names the same leader.
type LeaderChanged struct {
	protorun.BaseNotification
	Leader transport.Host
	Term   uint64
}

// DebugState requests a snapshot of internal Raft state for tests and
// operational tooling. Reading it through IPC keeps introspection on the
// framework's supported path — a runtime-routed request handled on the
// protocol's own event loop — rather than a data race on protocol fields.
type DebugState struct{ protorun.BaseRequest }

// Role is a server's Raft role.
type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// DebugStateReply is a consistent snapshot of the node's Raft state at the
// instant the request was handled.
type DebugStateReply struct {
	protorun.BaseReply
	Role         Role
	Term         uint64
	CommitIndex  uint64
	LastApplied  uint64
	LastLogIndex uint64
	LastLogTerm  uint64
	Leader       transport.Host
	HasLeader    bool
}
