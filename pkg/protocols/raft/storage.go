package raft

import "github.com/antonionduarte/protorun/pkg/transport"

// LogEntry is one replicated command tagged with the leader term under
// which it was created. Term is what Raft's log-matching and commit rules
// reason about (§5.3, §5.4.2); Command is the opaque application payload
// that surfaces in an Applied notification once the entry commits.
type LogEntry struct {
	Term    uint64
	Command []byte
}

// PersistentState is the subset of a Raft server's state that MUST
// survive a crash for the safety proofs to hold (§5, "Persistent state on
// all servers"): the current term, the vote cast in that term, and the
// log. Everything else (commitIndex, lastApplied, role, the leader's
// nextIndex/matchIndex) is volatile and rebuilt after a restart.
type PersistentState struct {
	CurrentTerm uint64
	// VotedFor is the candidate this server voted for in CurrentTerm;
	// valid only when HasVoted is true. Modeled as a value + flag rather
	// than a pointer so the zero PersistentState is unambiguously
	// "term 0, no vote, empty log".
	VotedFor transport.Host
	HasVoted bool
	// Log is the replicated log, index 1..N (Log[0] holds the entry at
	// Raft index 1). Index 0 is the implicit empty entry at term 0.
	Log []LogEntry
}

// Storage is the durable persistence seam. A production deployment MUST
// supply a crash-durable implementation (fsync'd file, WAL, embedded KV):
// Raft's Election Safety and Leader Completeness proofs assume that a
// server which acknowledges a vote or a log append has DURABLY recorded
// it before replying. An implementation that loses state on restart
// reduces Raft to its non-crash guarantees only — see MemoryStorage.
//
// The seam is incremental on purpose: term/vote changes and log appends
// are disjoint, and appends carry only the changed suffix, so the cost
// of persisting a commit is O(entries appended), not O(log). (The
// original whole-state Persist forced every implementation into an
// O(log) copy per commit; the load benchmarks caught it as per-commit
// cost growing linearly with log length.) Both methods are called
// synchronously from the protocol event loop before the corresponding
// reply is sent, so the paper's "persist, then reply" ordering is
// preserved by the caller; an implementation need only be durable by
// the time it returns, and must not retain (alias) the entries slice
// or the command bytes it is handed.
type Storage interface {
	// Load returns the persisted state at startup (the zero
	// PersistentState for a fresh server).
	Load() PersistentState
	// SaveTerm durably records the current term and vote. Called on
	// every term adoption, self-vote, and vote grant.
	SaveTerm(term uint64, votedFor transport.Host, hasVoted bool)
	// AppendEntries durably replaces the log suffix starting at Raft
	// index from (1-based) with entries: everything at from and above
	// is discarded, then entries are appended. from is lastIndex+1 for
	// a pure append (the common case); lower only on follower conflict
	// truncation, in which case entries is non-empty.
	AppendEntries(from uint64, entries []LogEntry)
}

// MemoryStorage is the default Storage: it keeps state in memory only.
//
// IMPORTANT: MemoryStorage is NOT durable. On process restart it returns
// the zero state, which means a restarted server forgets its term, its
// vote, and its entire log. That VIOLATES Raft's crash-recovery model and
// can break safety across restarts (a server could vote twice in one
// term, or a committed entry could be lost if enough servers restart). It
// exists so tests and single-run demos need no disk; it makes Raft's
// guarantees hold only for the lifetime of the process. Do not ship it.
//
// AppendEntries copies only the appended suffix, so persisting a commit
// is O(entries appended); the whole log is copied once, at Load.
type MemoryStorage struct {
	term     uint64
	votedFor transport.Host
	hasVoted bool
	log      []LogEntry
}

// NewMemoryStorage returns an empty in-memory Storage.
func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (m *MemoryStorage) Load() PersistentState {
	return PersistentState{
		CurrentTerm: m.term,
		VotedFor:    m.votedFor,
		HasVoted:    m.hasVoted,
		Log:         m.log,
	}.clone()
}

func (m *MemoryStorage) SaveTerm(term uint64, votedFor transport.Host, hasVoted bool) {
	m.term, m.votedFor, m.hasVoted = term, votedFor, hasVoted
}

func (m *MemoryStorage) AppendEntries(from uint64, entries []LogEntry) {
	if from < 1 {
		from = 1
	}
	if keep := from - 1; keep < uint64(len(m.log)) {
		m.log = m.log[:keep]
	}
	for _, e := range entries {
		kept := LogEntry{Term: e.Term}
		if e.Command != nil {
			kept.Command = append([]byte(nil), e.Command...)
		}
		m.log = append(m.log, kept)
	}
}

// clone returns a deep-enough copy: the log slice (and each entry's
// command bytes) are duplicated so mutation of one side is invisible to
// the other.
func (s PersistentState) clone() PersistentState {
	out := s
	if s.Log != nil {
		out.Log = make([]LogEntry, len(s.Log))
		for i, e := range s.Log {
			out.Log[i] = LogEntry{Term: e.Term}
			if e.Command != nil {
				out.Log[i].Command = append([]byte(nil), e.Command...)
			}
		}
	}
	return out
}
