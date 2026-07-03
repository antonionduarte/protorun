package raft

import (
	"bytes"
	"testing"

	"github.com/antonionduarte/protorun/pkg/transport"
)

// TestMemoryStorage_IncrementalSeam guards the Storage contract: appends
// persist only the changed suffix, conflict truncation discards from the
// given index, SaveTerm and AppendEntries are independent, and Load
// round-trips the accumulated state without aliasing.
func TestMemoryStorage_IncrementalSeam(t *testing.T) {
	st := NewMemoryStorage()
	host := transport.NewHost(1, "127.0.0.1")

	st.SaveTerm(3, host, true)
	st.AppendEntries(1, []LogEntry{
		{Term: 1, Command: []byte("a")},
		{Term: 2, Command: []byte("b")},
		{Term: 3, Command: []byte("c")},
	})
	// Conflict truncation: replace index 2..3 with a single entry.
	st.AppendEntries(2, []LogEntry{{Term: 3, Command: []byte("B")}})

	got := st.Load()
	if got.CurrentTerm != 3 || !got.HasVoted || got.VotedFor != host {
		t.Fatalf("term/vote did not round-trip: %+v", got)
	}
	if len(got.Log) != 2 {
		t.Fatalf("log length %d after truncating append, want 2", len(got.Log))
	}
	if !bytes.Equal(got.Log[1].Command, []byte("B")) || got.Log[1].Term != 3 {
		t.Fatalf("truncated suffix not replaced: %+v", got.Log[1])
	}

	// Load must not alias storage internals: mutating the returned state
	// must not affect a subsequent Load.
	got.Log[0].Command[0] = 'X'
	again := st.Load()
	if !bytes.Equal(again.Log[0].Command, []byte("a")) {
		t.Fatalf("Load aliases internal log: %q", again.Log[0].Command)
	}

	// A pure append after truncation extends the log.
	st.AppendEntries(3, []LogEntry{{Term: 3, Command: []byte("d")}})
	if final := st.Load(); len(final.Log) != 3 {
		t.Fatalf("log length %d after pure append, want 3", len(final.Log))
	}
}
