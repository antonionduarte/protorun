package raft

import (
	"bytes"

	"github.com/antonionduarte/protorun/pkg/protorun"
	"github.com/antonionduarte/protorun/pkg/wire"
)

// The Raft RPCs. Each crosses the network, so each carries a WireName
// (freezes the wire id across a future rename of the Go type) and a
// hand-written SelfMarshaler codec.
//
// These messages carry only fixed-width integers and byte slices — no
// transport.Host (the sender/candidate/leader identity is always the
// message's `from` parameter, which the framework supplies out of band).
// They could therefore ride the reflective WireCodec. They are hand-
// encoded anyway, for two reasons: (1) LogEntry carries a []byte command
// and lists of them, which the SelfMarshaler path expresses directly and
// cheaply; and (2) it keeps the whole protocol package on one encoding
// path, matching the house style in hyparview/plumtree.

// writeEntries encodes a slice of LogEntry as a uint32 count followed by
// each entry (uint64 term, length-prefixed command bytes).
func writeEntries(b *bytes.Buffer, entries []LogEntry) error {
	if err := wire.WriteUint32(b, uint32(len(entries))); err != nil {
		return err
	}
	for _, e := range entries {
		if err := wire.WriteUint64(b, e.Term); err != nil {
			return err
		}
		if err := wire.WriteBytes(b, e.Command); err != nil {
			return err
		}
	}
	return nil
}

func readEntries(r *bytes.Reader) ([]LogEntry, error) {
	n, err := wire.ReadUint32(r)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	entries := make([]LogEntry, 0, n)
	for range int(n) {
		term, err := wire.ReadUint64(r)
		if err != nil {
			return nil, err
		}
		cmd, err := wire.ReadBytes(r)
		if err != nil {
			return nil, err
		}
		entries = append(entries, LogEntry{Term: term, Command: cmd})
	}
	return entries, nil
}

// RequestVote is a candidate's solicitation of a vote (§5.2, §5.4.1). The
// candidate's identity is the message sender. Term is the candidate's
// term; LastLogIndex/LastLogTerm describe the candidate's log so the
// voter can apply the up-to-date check before granting.
type RequestVote struct {
	protorun.BaseMessage
	Term         uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

func (RequestVote) WireName() string { return "raft.RequestVote" }

func (m *RequestVote) MarshalWire() ([]byte, error) {
	var b bytes.Buffer
	for _, v := range []uint64{m.Term, m.LastLogIndex, m.LastLogTerm} {
		if err := wire.WriteUint64(&b, v); err != nil {
			return nil, err
		}
	}
	return b.Bytes(), nil
}

func (m *RequestVote) UnmarshalWire(data []byte) error {
	r := bytes.NewReader(data)
	for _, p := range []*uint64{&m.Term, &m.LastLogIndex, &m.LastLogTerm} {
		v, err := wire.ReadUint64(r)
		if err != nil {
			return err
		}
		*p = v
	}
	return nil
}

// RequestVoteReply answers a RequestVote. Term lets a stale candidate
// discover a higher term and step down; VoteGranted is the actual vote.
type RequestVoteReply struct {
	protorun.BaseMessage
	Term        uint64
	VoteGranted bool
}

func (RequestVoteReply) WireName() string { return "raft.RequestVoteReply" }

func (m *RequestVoteReply) MarshalWire() ([]byte, error) {
	var b bytes.Buffer
	if err := wire.WriteUint64(&b, m.Term); err != nil {
		return nil, err
	}
	b.WriteByte(boolByte(m.VoteGranted))
	return b.Bytes(), nil
}

func (m *RequestVoteReply) UnmarshalWire(data []byte) error {
	r := bytes.NewReader(data)
	term, err := wire.ReadUint64(r)
	if err != nil {
		return err
	}
	gb, err := r.ReadByte()
	if err != nil {
		return err
	}
	m.Term, m.VoteGranted = term, gb != 0
	return nil
}

// AppendEntries is the leader's replication + heartbeat RPC (§5.3). The
// leader's identity is the sender. PrevLogIndex/PrevLogTerm anchor the
// entries against the follower's log for the consistency check; Entries
// is empty for a pure heartbeat; LeaderCommit advances the follower's
// commit index.
type AppendEntries struct {
	protorun.BaseMessage
	Term         uint64
	PrevLogIndex uint64
	PrevLogTerm  uint64
	LeaderCommit uint64
	Entries      []LogEntry
}

func (AppendEntries) WireName() string { return "raft.AppendEntries" }

func (m *AppendEntries) MarshalWire() ([]byte, error) {
	var b bytes.Buffer
	for _, v := range []uint64{m.Term, m.PrevLogIndex, m.PrevLogTerm, m.LeaderCommit} {
		if err := wire.WriteUint64(&b, v); err != nil {
			return nil, err
		}
	}
	if err := writeEntries(&b, m.Entries); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (m *AppendEntries) UnmarshalWire(data []byte) error {
	r := bytes.NewReader(data)
	for _, p := range []*uint64{&m.Term, &m.PrevLogIndex, &m.PrevLogTerm, &m.LeaderCommit} {
		v, err := wire.ReadUint64(r)
		if err != nil {
			return err
		}
		*p = v
	}
	entries, err := readEntries(r)
	if err != nil {
		return err
	}
	m.Entries = entries
	return nil
}

// AppendEntriesReply answers an AppendEntries. Term drives higher-term
// stepdown; Success reports whether the consistency check passed. On
// success MatchIndex is the highest log index the follower now has that
// matches the leader (PrevLogIndex + len(Entries)), which the leader uses
// to advance nextIndex/matchIndex without guessing.
type AppendEntriesReply struct {
	protorun.BaseMessage
	Term       uint64
	MatchIndex uint64
	Success    bool
}

func (AppendEntriesReply) WireName() string { return "raft.AppendEntriesReply" }

func (m *AppendEntriesReply) MarshalWire() ([]byte, error) {
	var b bytes.Buffer
	if err := wire.WriteUint64(&b, m.Term); err != nil {
		return nil, err
	}
	if err := wire.WriteUint64(&b, m.MatchIndex); err != nil {
		return nil, err
	}
	b.WriteByte(boolByte(m.Success))
	return b.Bytes(), nil
}

func (m *AppendEntriesReply) UnmarshalWire(data []byte) error {
	r := bytes.NewReader(data)
	term, err := wire.ReadUint64(r)
	if err != nil {
		return err
	}
	match, err := wire.ReadUint64(r)
	if err != nil {
		return err
	}
	sb, err := r.ReadByte()
	if err != nil {
		return err
	}
	m.Term, m.MatchIndex, m.Success = term, match, sb != 0
	return nil
}

func boolByte(v bool) byte {
	if v {
		return 1
	}
	return 0
}
