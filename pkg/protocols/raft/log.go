package raft

// raftLog is the in-memory replicated log with 1-based Raft indices. The
// backing slice entries[i-1] holds the entry at Raft index i; index 0 is
// the implicit empty entry (term 0), so termAt(0) == 0 and an empty log
// has lastIndex() == 0. All methods here are pure state manipulation with
// no framework dependency, which is what lets the log-matching, vote, and
// commit rules be unit-tested in isolation.
type raftLog struct {
	entries []LogEntry
}

func newRaftLog(entries []LogEntry) *raftLog {
	return &raftLog{entries: entries}
}

// lastIndex is the index of the last entry, or 0 for an empty log.
func (l *raftLog) lastIndex() uint64 { return uint64(len(l.entries)) }

// lastTerm is the term of the last entry, or 0 for an empty log.
func (l *raftLog) lastTerm() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

// termAt returns the term of the entry at index, 0 for index 0, and 0 for
// an index past the end (callers guard the past-the-end case before using
// the result in a match check).
func (l *raftLog) termAt(index uint64) uint64 {
	if index == 0 || index > l.lastIndex() {
		return 0
	}
	return l.entries[index-1].Term
}

// at returns the entry at a 1-based index; the caller must ensure
// 1 <= index <= lastIndex().
func (l *raftLog) at(index uint64) LogEntry { return l.entries[index-1] }

// append adds one entry to the end and returns its index.
func (l *raftLog) append(e LogEntry) uint64 {
	l.entries = append(l.entries, e)
	return l.lastIndex()
}

// truncateFrom deletes the entry at index and everything after it (§5.3:
// a follower deletes conflicting suffixes). index must be >= 1.
func (l *raftLog) truncateFrom(index uint64) {
	if index < 1 || index > l.lastIndex() {
		return
	}
	l.entries = l.entries[:index-1]
}

// sliceFrom returns a FRESH copy of entries from index to the end, so the
// caller (a leader building an AppendEntries) never aliases the live log.
// Returns nil when index is past the end.
func (l *raftLog) sliceFrom(index uint64) []LogEntry {
	if index < 1 || index > l.lastIndex() {
		return nil
	}
	out := make([]LogEntry, l.lastIndex()-index+1)
	copy(out, l.entries[index-1:])
	return out
}

// snapshot returns a copy of the whole log for persistence.
func (l *raftLog) snapshot() []LogEntry {
	if len(l.entries) == 0 {
		return nil
	}
	out := make([]LogEntry, len(l.entries))
	copy(out, l.entries)
	return out
}

// --- pure decision helpers (unit-tested directly) ----------------------

// logIsUpToDate implements the §5.4.1 "at least as up-to-date" comparison
// a voter applies to a candidate's log: a log with the later last term
// wins; on equal last terms the longer log wins. Returns true when the
// candidate's log (candTerm, candIndex) is at least as up-to-date as the
// voter's (ourTerm, ourIndex), which is the precondition for granting a
// vote.
func logIsUpToDate(candTerm, candIndex, ourTerm, ourIndex uint64) bool {
	if candTerm != ourTerm {
		return candTerm > ourTerm
	}
	return candIndex >= ourIndex
}

// advanceCommitIndex computes the new commit index for a leader (§5.3 +
// §5.4.2). matches must hold the match index of EVERY member of the
// cluster including the leader itself (the leader's match is its own last
// index). clusterSize is the total membership. Starting from the highest
// index and walking down to currentCommit+1, it returns the first index N
// such that (a) a majority of members have replicated N and (b) N belongs
// to the current term — the §5.4.2 rule that a leader may only advance
// commitment over an entry from its OWN term, never by counting replicas
// of a stale-term entry. When no such N exists it returns currentCommit
// unchanged.
func advanceCommitIndex(currentCommit, currentTerm uint64, lg *raftLog, matches []uint64, clusterSize int) uint64 {
	majority := clusterSize/2 + 1
	for n := lg.lastIndex(); n > currentCommit; n-- {
		if lg.termAt(n) != currentTerm {
			// Entries below this are from strictly older terms too, so no
			// current-term entry remains to commit: stop.
			break
		}
		count := 0
		for _, m := range matches {
			if m >= n {
				count++
			}
		}
		if count >= majority {
			return n
		}
	}
	return currentCommit
}
