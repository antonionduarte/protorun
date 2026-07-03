package paxos

import (
	"bytes"

	"github.com/antonionduarte/protorun/pkg/protorun"
	"github.com/antonionduarte/protorun/pkg/wire"
)

// The Paxos synod RPCs. Each crosses the network, so each carries a
// WireName (freezes the wire id across a future rename of the Go type) and
// a hand-written SelfMarshaler codec, matching the house style in
// raft/hyparview/plumtree. The proposer/acceptor identity is always the
// message's `from` parameter (supplied out of band by the framework), so
// no message carries a transport.Host.
//
// Message flow (proposer P, acceptors A, learners L — every node is all
// three at once):
//
//	Prepare(n)      P -> A     Phase 1a
//	Promise(...)    A -> P     Phase 1b (OK), or a NACK carrying MaxBallot
//	Accept(n, v)    P -> A     Phase 2a
//	Accepted(n, v)  A -> all L Phase 2b (a learner announcement, broadcast)
//	AcceptNack      A -> P     rejection of a stale Accept (liveness only)

// Prepare is Phase 1a: a proposer solicits promises for ballot Ballot.
type Prepare struct {
	protorun.BaseMessage
	Ballot uint64
}

func (Prepare) WireName() string { return "paxos.Prepare" }

func (m *Prepare) MarshalWire() ([]byte, error) {
	var b bytes.Buffer
	if err := wire.WriteUint64(&b, m.Ballot); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (m *Prepare) UnmarshalWire(data []byte) error {
	v, err := wire.ReadUint64(bytes.NewReader(data))
	if err != nil {
		return err
	}
	m.Ballot = v
	return nil
}

// Promise is Phase 1b: an acceptor's reply to a Prepare. When OK, the
// acceptor promises never to accept a ballot below Ballot and reports the
// highest (ballot, value) it has already accepted (HasAccepted /
// AcceptedBallot / AcceptedValue) so the proposer can apply the adoption
// rule. When OK is false the Prepare lost to a higher promise; MaxBallot
// carries that higher promised ballot so the proposer can jump straight
// past it on its next round.
type Promise struct {
	protorun.BaseMessage
	Ballot         uint64
	OK             bool
	MaxBallot      uint64
	AcceptedBallot uint64
	AcceptedValue  []byte
	HasAccepted    bool
}

func (Promise) WireName() string { return "paxos.Promise" }

func (m *Promise) MarshalWire() ([]byte, error) {
	var b bytes.Buffer
	if err := wire.WriteUint64(&b, m.Ballot); err != nil {
		return nil, err
	}
	b.WriteByte(boolByte(m.OK))
	if err := wire.WriteUint64(&b, m.MaxBallot); err != nil {
		return nil, err
	}
	if err := wire.WriteUint64(&b, m.AcceptedBallot); err != nil {
		return nil, err
	}
	b.WriteByte(boolByte(m.HasAccepted))
	if err := wire.WriteBytes(&b, m.AcceptedValue); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (m *Promise) UnmarshalWire(data []byte) error {
	r := bytes.NewReader(data)
	ballot, err := wire.ReadUint64(r)
	if err != nil {
		return err
	}
	okB, err := r.ReadByte()
	if err != nil {
		return err
	}
	maxBallot, err := wire.ReadUint64(r)
	if err != nil {
		return err
	}
	accBallot, err := wire.ReadUint64(r)
	if err != nil {
		return err
	}
	hasB, err := r.ReadByte()
	if err != nil {
		return err
	}
	accValue, err := wire.ReadBytes(r)
	if err != nil {
		return err
	}
	m.Ballot, m.OK, m.MaxBallot = ballot, okB != 0, maxBallot
	m.AcceptedBallot, m.HasAccepted, m.AcceptedValue = accBallot, hasB != 0, accValue
	return nil
}

// Accept is Phase 2a: a proposer asks the acceptors to accept Value at
// ballot Ballot. Value is the one selected by the adoption rule (an
// already-accepted value if the promise quorum revealed one, otherwise the
// proposer's own).
type Accept struct {
	protorun.BaseMessage
	Ballot uint64
	Value  []byte
}

func (Accept) WireName() string { return "paxos.Accept" }

func (m *Accept) MarshalWire() ([]byte, error) {
	var b bytes.Buffer
	if err := wire.WriteUint64(&b, m.Ballot); err != nil {
		return nil, err
	}
	if err := wire.WriteBytes(&b, m.Value); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (m *Accept) UnmarshalWire(data []byte) error {
	r := bytes.NewReader(data)
	ballot, err := wire.ReadUint64(r)
	if err != nil {
		return err
	}
	value, err := wire.ReadBytes(r)
	if err != nil {
		return err
	}
	m.Ballot, m.Value = ballot, value
	return nil
}

// Accepted is Phase 2b: an acceptor announces to every learner that it has
// accepted Value at ballot Ballot. Learners count distinct acceptors per
// ballot; once a majority is reached the value is chosen. It is broadcast
// (not unicast to the proposer) precisely because every node is a learner,
// and it is re-sent to a peer on reconnect as the partition-heal catch-up
// path.
type Accepted struct {
	protorun.BaseMessage
	Ballot uint64
	Value  []byte
}

func (Accepted) WireName() string { return "paxos.Accepted" }

func (m *Accepted) MarshalWire() ([]byte, error) {
	var b bytes.Buffer
	if err := wire.WriteUint64(&b, m.Ballot); err != nil {
		return nil, err
	}
	if err := wire.WriteBytes(&b, m.Value); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (m *Accepted) UnmarshalWire(data []byte) error {
	r := bytes.NewReader(data)
	ballot, err := wire.ReadUint64(r)
	if err != nil {
		return err
	}
	value, err := wire.ReadBytes(r)
	if err != nil {
		return err
	}
	m.Ballot, m.Value = ballot, value
	return nil
}

// AcceptNack tells a proposer its Accept was rejected because a higher
// ballot has since been promised (MaxBallot). It carries no value: its only
// job is liveness, letting the proposer learn a higher ballot exists sooner
// than its retry timer would. Safety never depends on it — a lost AcceptNack
// only delays a retry.
type AcceptNack struct {
	protorun.BaseMessage
	MaxBallot uint64
}

func (AcceptNack) WireName() string { return "paxos.AcceptNack" }

func (m *AcceptNack) MarshalWire() ([]byte, error) {
	var b bytes.Buffer
	if err := wire.WriteUint64(&b, m.MaxBallot); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (m *AcceptNack) UnmarshalWire(data []byte) error {
	v, err := wire.ReadUint64(bytes.NewReader(data))
	if err != nil {
		return err
	}
	m.MaxBallot = v
	return nil
}

func boolByte(v bool) byte {
	if v {
		return 1
	}
	return 0
}
