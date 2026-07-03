package raft

import "testing"

// These unit tests pin the pure Raft decision logic — log matching /
// truncation (§5.3), the up-to-date vote check (§5.4.1), and commit-index
// advancement counting only current-term entries (§5.4.2) — independently
// of the event loop, so a regression in the algorithm is caught without a
// full simulation.

func entries(terms ...uint64) []LogEntry {
	out := make([]LogEntry, len(terms))
	for i, tm := range terms {
		out[i] = LogEntry{Term: tm, Command: []byte{byte(i)}}
	}
	return out
}

func TestRaftLog_TermAndIndex(t *testing.T) {
	lg := newRaftLog(entries(1, 1, 2))
	if lg.lastIndex() != 3 {
		t.Fatalf("lastIndex = %d, want 3", lg.lastIndex())
	}
	if lg.lastTerm() != 2 {
		t.Fatalf("lastTerm = %d, want 2", lg.lastTerm())
	}
	if lg.termAt(0) != 0 {
		t.Fatalf("termAt(0) = %d, want 0 (implicit empty entry)", lg.termAt(0))
	}
	if lg.termAt(2) != 1 {
		t.Fatalf("termAt(2) = %d, want 1", lg.termAt(2))
	}
	if lg.termAt(99) != 0 {
		t.Fatalf("termAt past end = %d, want 0", lg.termAt(99))
	}

	empty := newRaftLog(nil)
	if empty.lastIndex() != 0 || empty.lastTerm() != 0 {
		t.Fatalf("empty log: lastIndex=%d lastTerm=%d, want 0/0", empty.lastIndex(), empty.lastTerm())
	}
}

func TestRaftLog_Truncation(t *testing.T) {
	lg := newRaftLog(entries(1, 1, 2, 3))
	lg.truncateFrom(3) // delete index 3 and 4
	if lg.lastIndex() != 2 {
		t.Fatalf("after truncateFrom(3), lastIndex = %d, want 2", lg.lastIndex())
	}
	if lg.termAt(2) != 1 {
		t.Fatalf("surviving entry term = %d, want 1", lg.termAt(2))
	}
	// Out-of-range truncations are no-ops.
	lg.truncateFrom(0)
	lg.truncateFrom(99)
	if lg.lastIndex() != 2 {
		t.Fatalf("out-of-range truncate changed the log: lastIndex = %d", lg.lastIndex())
	}
}

func TestRaftLog_SliceFromCopies(t *testing.T) {
	lg := newRaftLog(entries(1, 2, 3))
	s := lg.sliceFrom(2)
	if len(s) != 2 || s[0].Term != 2 || s[1].Term != 3 {
		t.Fatalf("sliceFrom(2) = %+v, want terms [2,3]", s)
	}
	// Mutating the slice must not touch the log.
	s[0].Term = 99
	if lg.termAt(2) != 2 {
		t.Fatalf("sliceFrom returned an aliasing slice; log mutated to term %d", lg.termAt(2))
	}
	if lg.sliceFrom(4) != nil {
		t.Fatalf("sliceFrom past end should be nil")
	}
}

func TestLogIsUpToDate(t *testing.T) {
	// §5.4.1: later last term wins; on equal terms the longer log wins.
	cases := []struct {
		name                               string
		candTerm, candIdx, ourTerm, ourIdx uint64
		want                               bool
	}{
		{"higher term beats longer log", 3, 1, 2, 10, true},
		{"lower term loses despite longer log", 1, 10, 2, 1, false},
		{"equal term, candidate longer", 2, 5, 2, 4, true},
		{"equal term, equal length", 2, 4, 2, 4, true},
		{"equal term, candidate shorter", 2, 3, 2, 4, false},
		{"both empty", 0, 0, 0, 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := logIsUpToDate(c.candTerm, c.candIdx, c.ourTerm, c.ourIdx); got != c.want {
				t.Fatalf("logIsUpToDate(%d,%d,%d,%d) = %v, want %v",
					c.candTerm, c.candIdx, c.ourTerm, c.ourIdx, got, c.want)
			}
		})
	}
}

func TestAdvanceCommitIndex_CurrentTermOnly(t *testing.T) {
	// Classic §5.4.2 scenario (Figure 8): an entry from an older term that
	// is replicated on a majority must NOT be committed by counting; only
	// once a current-term entry is replicated on a majority does
	// commitment sweep forward over the earlier entries.
	lg := newRaftLog(entries(1, 2)) // index1: term1, index2: term2
	const clusterSize = 3

	// Index 1 (term 1) replicated on a majority [self=2, peerA=1, peerB=0].
	// currentTerm is 2, so index 1 is a STALE-term entry: not committable
	// by count.
	got := advanceCommitIndex(0, 2, lg, []uint64{2, 1, 0}, clusterSize)
	if got != 0 {
		t.Fatalf("stale-term entry committed by counting: got commit %d, want 0", got)
	}

	// Now index 2 (term 2, the current term) is on a majority. Committing
	// it also carries index 1 forward implicitly (commitIndex jumps to 2).
	got = advanceCommitIndex(0, 2, lg, []uint64{2, 2, 0}, clusterSize)
	if got != 2 {
		t.Fatalf("current-term entry on a majority should commit index 2, got %d", got)
	}
}

func TestAdvanceCommitIndex_NoMajority(t *testing.T) {
	lg := newRaftLog(entries(1, 1, 1))
	// Only the leader has index 3; not a majority of 5.
	got := advanceCommitIndex(0, 1, lg, []uint64{3, 0, 0, 0, 0}, 5)
	if got != 0 {
		t.Fatalf("no majority yet, got commit %d, want 0", got)
	}
	// Three of five (incl. leader) have index 2: a majority.
	got = advanceCommitIndex(0, 1, lg, []uint64{3, 2, 2, 0, 0}, 5)
	if got != 2 {
		t.Fatalf("majority at index 2 should commit, got %d, want 2", got)
	}
}

// TestAppendConflictFree exercises the follower-side splice through a
// minimal Protocol value (no runtime): matching prefixes are kept,
// conflicting suffixes truncated, new entries appended, and an idempotent
// re-send changes nothing.
func TestAppendConflictFree(t *testing.T) {
	p := &Protocol{log: newRaftLog(entries(1, 1, 2)), cfg: Config{Storage: NewMemoryStorage()}}

	// Re-send of entries the follower already has (prevIndex 1, entries for
	// index 2 and 3 with matching terms): no change.
	p.appendConflictFree(1, entries(1, 2))
	if p.log.lastIndex() != 3 || p.log.termAt(3) != 2 {
		t.Fatalf("idempotent re-send altered the log: lastIndex=%d", p.log.lastIndex())
	}

	// Conflict at index 3 (leader says term 3 there): truncate index 3 and
	// append the new suffix [term3, term3].
	p.appendConflictFree(2, entries(3, 3))
	if p.log.lastIndex() != 4 {
		t.Fatalf("after conflict splice, lastIndex = %d, want 4", p.log.lastIndex())
	}
	if p.log.termAt(3) != 3 || p.log.termAt(4) != 3 {
		t.Fatalf("conflicting suffix not replaced: term3=%d term4=%d", p.log.termAt(3), p.log.termAt(4))
	}
}
