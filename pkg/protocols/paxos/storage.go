package paxos

// PersistentState is the acceptor state that MUST survive a crash for
// Paxos safety to hold across restarts: the highest ballot promised and
// the highest (ballot, value) accepted. Lamport's proof assumes an
// acceptor that has promised or accepted has DURABLY recorded that fact
// before replying — an acceptor that forgets a promise could promise a
// lower ballot to a second proposer, and an acceptor that forgets an
// accept could let a chosen value be overwritten. Proposer and learner
// state (the current ballot, collected promises, per-ballot accept tallies)
// is volatile and rebuilt from live traffic after a restart.
type PersistentState struct {
	// Promised is the highest ballot this acceptor has promised (0 = the
	// null ballot, i.e. it has promised nothing).
	Promised uint64
	// AcceptedBallot / AcceptedValue are the highest-ballot proposal this
	// acceptor has accepted; valid only when HasAccepted is true. Modeled
	// as ballot + value + flag rather than a pointer so the zero
	// PersistentState is unambiguously "promised nothing, accepted nothing".
	AcceptedBallot uint64
	AcceptedValue  []byte
	HasAccepted    bool
}

// Storage is the durable persistence seam for acceptor state. A production
// deployment MUST supply a crash-durable implementation (fsync'd file,
// WAL, embedded KV): Paxos safety assumes an acceptor that acknowledges a
// promise or an accept has recorded it durably before the acknowledgement
// leaves the process.
//
// Persist is called synchronously from the protocol event loop before the
// corresponding message is sent (the Promise reply, or the Accepted
// announcement), so the "persist, then act" ordering the proof requires is
// preserved by the caller; an implementation need only make Persist
// durable by the time it returns.
type Storage interface {
	// Load returns the persisted state at startup (the zero
	// PersistentState for a fresh acceptor).
	Load() PersistentState
	// Persist durably records the full acceptor state. Called on every
	// change to the promised ballot or the accepted (ballot, value).
	Persist(state PersistentState)
}

// MemoryStorage is the default Storage: it keeps state in memory only.
//
// IMPORTANT: MemoryStorage is NOT durable. On process restart it returns
// the zero state, which means a restarted acceptor forgets what it
// promised and what it accepted. That VIOLATES Paxos's crash-recovery
// model and can break safety across restarts (a reborn acceptor could
// promise a lower ballot than one it already promised, or accept a value
// conflicting with one already chosen). It exists so tests and single-run
// demos need no disk; it makes Paxos's guarantees hold only for the
// lifetime of the process. Do not ship it.
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

// clone copies the accepted-value bytes so the stored snapshot never
// aliases the caller's live slice (or vice versa).
func (s PersistentState) clone() PersistentState {
	out := s
	if s.AcceptedValue != nil {
		out.AcceptedValue = append([]byte(nil), s.AcceptedValue...)
	}
	return out
}
