package protorun

import "sync"

// codecRegistry owns the runtime-wide wireID routing table: for each
// wire id, the owning protocol and the codec that (un)marshals its
// messages, registered together in one atomic step. It is the single
// lookup on both the send and receive hot paths — one guarded read
// yields everything needed to encode, decode, and route a message.
//
// Built up at Start time as protocols call RegisterCodec. Read on
// every send and every receive, written only at registration: hence
// sync.RWMutex. The concurrency shape is private to this module; if
// the read lock ever shows up in benchmarks, swap the internals
// (e.g. copy-on-write) without touching callers.
type codecRegistry struct {
	mu     sync.RWMutex
	lookup map[uint64]codecEntry
}

// codecEntry is everything the runtime needs to act on a wireID.
type codecEntry struct {
	proto *protoProtocol
	codec codec
}

func newCodecRegistry() *codecRegistry {
	return &codecRegistry{lookup: make(map[uint64]codecEntry)}
}

// Set records wireID's owning protocol and codec. Called from
// registerCodec while a protocol is in its Start phase. Last-writer-
// wins if two protocols claim the same wireID (strict mode catches
// that as a panic; non-strict logs a warning).
func (r *codecRegistry) Set(wireID uint64, proto *protoProtocol, c codec) {
	r.mu.Lock()
	r.lookup[wireID] = codecEntry{proto: proto, codec: c}
	r.mu.Unlock()
}

// Get returns the entry for wireID, or (zero, false) if no codec is
// registered.
func (r *codecRegistry) Get(wireID uint64) (codecEntry, bool) {
	r.mu.RLock()
	e, ok := r.lookup[wireID]
	r.mu.RUnlock()
	return e, ok
}
