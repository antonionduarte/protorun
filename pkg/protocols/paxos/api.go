package paxos

import (
	"fmt"

	"github.com/antonionduarte/protorun/pkg/protorun"
)

// This file is the public IPC surface of the Paxos protocol: the requests,
// replies, and notifications other protocols (and tests) use to drive and
// observe it. All of it is local, same-runtime IPC — never on the wire —
// so the types embed the protorun Base* markers and carry no codec.

// Propose asks this node to try to get Value chosen as the single decree.
// It succeeds — with an empty ack — when a proposal is now underway (or
// already running); the reply is NOT a decision. The chosen value surfaces
// later, exactly once, as a Decided notification. If the decree has ALREADY
// been decided, Propose fails with an *AlreadyDecidedError carrying the
// value that was chosen, so the caller learns the outcome immediately
// without proposing.
//
// Because Paxos decides a single value, a Propose whose Value differs from
// an already-underway or already-chosen value does not overwrite it: the
// value-adoption rule may cause a completely different value to be chosen
// than the one proposed. Callers that need their exact value must treat
// Propose as "nominate", not "set".
type Propose struct {
	protorun.BaseRequest
	Value []byte
}

// ProposeReply is the successful answer to a Propose: an acknowledgement
// that a proposal is underway. It intentionally carries no value — the
// decision is delivered asynchronously via the Decided notification.
type ProposeReply struct{ protorun.BaseReply }

// AlreadyDecidedError is returned (via Responder.Fail) when a Propose
// reaches a node that already knows the decree's outcome. Value is the
// chosen value and Ballot the ballot at which it was chosen, so a caller
// can read the result off the error instead of waiting for a Decided it
// may have missed.
type AlreadyDecidedError struct {
	Value  []byte
	Ballot uint64
}

func (e *AlreadyDecidedError) Error() string {
	return fmt.Sprintf("paxos: already decided value %q at ballot %d", e.Value, e.Ballot)
}

// Decided is published exactly once per node, the moment this node learns
// the chosen value (a majority of acceptors accepted it at Ballot).
// Collected across nodes, the stream witnesses the two headline safety
// properties: Agreement (every Decided names the same value) and Integrity
// (each node publishes exactly one Decided, and only for a value that was
// actually proposed).
type Decided struct {
	protorun.BaseNotification
	Value  []byte
	Ballot uint64
}

// DebugState requests a snapshot of internal Paxos state for tests and
// operational tooling. Reading it through IPC keeps introspection on the
// framework's supported path — a runtime-routed request handled on the
// protocol's own event loop — rather than a data race on protocol fields.
type DebugState struct{ protorun.BaseRequest }

// DebugStateReply is a consistent snapshot of the node's Paxos state at the
// instant the request was handled. Promised / AcceptedBallot / AcceptedValue
// are the acceptor's durable state; Decided / DecidedValue / DecidedBallot
// are the learner's; Proposing / MyBallot are the proposer's.
type DebugStateReply struct {
	protorun.BaseReply
	Promised       uint64
	AcceptedBallot uint64
	AcceptedValue  []byte
	HasAccepted    bool

	Decided       bool
	DecidedValue  []byte
	DecidedBallot uint64

	Proposing bool
	MyBallot  uint64
}
