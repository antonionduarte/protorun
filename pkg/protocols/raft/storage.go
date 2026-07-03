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
// Persist is called synchronously from the protocol event loop before the
// corresponding reply is sent, so the ordering "persist, then reply"
// required by the paper is preserved by the caller; an implementation
// need only make Persist durable by the time it returns.
type Storage interface {
	// Load returns the persisted state at startup (the zero
	// PersistentState for a fresh server).
	Load() PersistentState
	// Persist durably records the full state. It is called on every
	// change to term, vote, or log.
	Persist(state PersistentState)
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
// Persist copies the log so the stored snapshot does not alias the
// caller's live slice; this is O(len(log)) per call, which is fine for a
// reference implementation but is exactly the kind of thing a real
// durable Storage would make incremental (append-only WAL).
type MemoryStorage struct {
	state PersistentState
	saved bool
}

// NewMemoryStorage returns an empty in-memory Storage.
func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (m *MemoryStorage) Load() PersistentState {
	if !m.saved {
		return PersistentState{}
	}
	return m.state.clone()
}

func (m *MemoryStorage) Persist(state PersistentState) {
	m.state = state.clone()
	m.saved = true
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
