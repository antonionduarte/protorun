// Package raft implements the Raft consensus algorithm of Ongaro &
// Ousterhout, "In Search of an Understandable Consensus Algorithm" (the
// condensed 2014 paper), as a protorun protocol. It provides a
// replicated, linearizable log over a fixed set of servers: leader
// election with randomized timeouts (§5.2), log replication and the
// AppendEntries consistency check (§5.3), commitment by majority counting
// restricted to current-term entries (§5.4.2), the up-to-date vote
// restriction (§5.4.1), and higher-term stepdown everywhere (§5.1).
//
// # Faithful to the paper
//
//   - Leader election: a follower that hears nothing from a leader within
//     a randomized election timeout becomes a candidate, increments its
//     term, votes for itself, and solicits votes; a candidate that wins a
//     majority becomes leader. Randomized, per-node-seeded timeouts break
//     split votes (§5.2).
//   - Log replication: the leader appends locally, then drives each
//     follower's log to match via AppendEntries with a prev-log
//     consistency check and nextIndex backoff on mismatch (§5.3).
//   - Safety: a server only grants a vote to a candidate whose log is at
//     least as up-to-date as its own (§5.4.1); a leader only advances the
//     commit index over an entry from its OWN term, counting replicas
//     (§5.4.2); any message bearing a higher term forces an immediate
//     stepdown and term update (§5.1).
//   - Persistence: currentTerm, votedFor, and the log are written through
//     the Storage seam before the triggering reply is sent, and reloaded
//     at Init.
//
// # Out of scope (deliberately)
//
//   - Cluster membership changes (§6, joint consensus): membership is
//     static, taken from Config.Peers. See Config for why a partial-view
//     membership protocol is the wrong substrate for consensus.
//   - Log compaction / snapshots (§7): the log grows without bound.
//   - Client session dedup / linearizable client semantics: Propose is
//     at-least-once from the client's point of view; the application must
//     dedup if it needs exactly-once.
//   - Read-index / lease reads: there is no optimized read path; reads
//     would have to go through the log.
//
// # Determinism
//
// All state lives on the single event loop and is touched only inside
// handlers, timer callbacks, and session-event callbacks. The only
// randomness is the election timeout, drawn from a per-node RNG seeded
// from the node's Host, so the protocol is fully reproducible under
// prototest.Sim.
package raft

import (
	"hash/fnv"
	"math/rand/v2"
	"sort"
	"time"

	"github.com/antonionduarte/protorun/pkg/protorun"
	"github.com/antonionduarte/protorun/pkg/transport"
)

// Protocol is a Raft server. Construct with New and Register it with a
// runtime. Every field is owned by the event loop and touched only inside
// handlers, timer callbacks, and session-event callbacks — no locking.
type Protocol struct {
	cfg   Config
	ctx   protorun.ProtocolContext
	self  transport.Host
	peers []transport.Host // sorted; the other members (never includes self)
	rng   *rand.Rand

	// Persistent state (mirrored to cfg.Storage on every change).
	currentTerm uint64
	votedFor    transport.Host
	hasVoted    bool
	log         *raftLog

	// Volatile state on all servers.
	role        Role
	commitIndex uint64
	lastApplied uint64

	// Leader belief (for redirect + LeaderChanged); cleared on stepdown.
	leader    transport.Host
	hasLeader bool

	// Volatile leader state, reinitialized on election (§5.3). Keyed by
	// peer Host; iterated only via the sorted peers slice.
	nextIndex  map[transport.Host]uint64
	matchIndex map[transport.Host]uint64

	// Election bookkeeping while a candidate.
	votesGranted map[transport.Host]bool

	// Session liveness, for reconnect-after-heal.
	sessions map[transport.Host]bool

	electionTimer  protorun.TimerHandle
	heartbeatTimer protorun.TimerHandle
}

// New returns a Raft server for self configured by cfg. cfg.Peers must
// list the other members of the group (and must not include self); the
// zero timing fields are filled with defaults and a MemoryStorage is
// installed when none is supplied.
func New(self transport.Host, cfg Config) *Protocol {
	cfg.fillDefaults()
	peers := make([]transport.Host, 0, len(cfg.Peers))
	for _, p := range cfg.Peers {
		if p != self {
			peers = append(peers, p)
		}
	}
	sort.Slice(peers, func(i, j int) bool { return hostLess(peers[i], peers[j]) })
	return &Protocol{
		cfg:          cfg,
		self:         self,
		peers:        peers,
		role:         Follower,
		nextIndex:    make(map[transport.Host]uint64),
		matchIndex:   make(map[transport.Host]uint64),
		votesGranted: make(map[transport.Host]bool),
		sessions:     make(map[transport.Host]bool),
	}
}

func (p *Protocol) Start(ctx protorun.ProtocolContext) {
	p.ctx = ctx
	protorun.Handle(ctx, p.onRequestVote)
	protorun.Handle(ctx, p.onRequestVoteReply)
	protorun.Handle(ctx, p.onAppendEntries)
	protorun.Handle(ctx, p.onAppendEntriesReply)
	protorun.RegisterRequestHandler(ctx, p.handlePropose)
	protorun.RegisterRequestHandler(ctx, p.handleDebugState)
}

func (p *Protocol) Init(ctx protorun.ProtocolContext) {
	// Per-node deterministic RNG seeded from self: election timeouts are
	// reproducible under the sim (which fixes handler order) while
	// different nodes still draw different timeouts, which is what breaks
	// symmetric split votes.
	s1, s2 := seedFromHost(p.self)
	p.rng = rand.New(rand.NewPCG(s1, s2)) //nolint:gosec // seeded PRNG is intentional for determinism, not security

	// Load durable state (empty for a fresh MemoryStorage).
	st := p.cfg.Storage.Load()
	p.currentTerm = st.CurrentTerm
	p.votedFor, p.hasVoted = st.VotedFor, st.HasVoted
	p.log = newRaftLog(st.Log)

	// Dial every peer and keep the sessions up. Sessions are symmetric, so
	// a redundant dial from the other side is a no-op; the periodic
	// reconnect is the sole recovery path after a partition heals.
	p.ensureConnections()
	ctx.Every(p.cfg.ReconnectInterval, p.ensureConnections)

	// Start as a follower waiting for a leader; if none appears within the
	// randomized timeout we stand for election.
	p.resetElectionTimer()
}

// --- session lifecycle -------------------------------------------------

func (p *Protocol) OnSessionConnected(host transport.Host) { p.sessions[host] = true }

func (p *Protocol) OnSessionDisconnected(host transport.Host) { delete(p.sessions, host) }

// OnSessionGivenUp treats an exhausted dial like a disconnect: the peer is
// simply not reachable right now; the reconnect timer will retry.
func (p *Protocol) OnSessionGivenUp(host transport.Host, _ int) { delete(p.sessions, host) }

// ensureConnections dials any peer we do not currently hold a session
// with. Run at Init and on the reconnect timer; iterates peers in sorted
// order for determinism.
func (p *Protocol) ensureConnections() {
	for _, peer := range p.peers {
		if !p.sessions[peer] {
			if err := p.ctx.Connect(peer); err != nil {
				p.ctx.Logger().Debug("raft: connect failed", "peer", peer.String(), "err", err)
			}
		}
	}
}

// --- election ----------------------------------------------------------

// resetElectionTimer cancels any pending election timeout and arms a fresh
// randomized one. Called whenever we hear from a current leader, grant a
// vote, or (re)start an election — every event that means "a leader or a
// candidate is making progress, so do not time out yet".
func (p *Protocol) resetElectionTimer() {
	p.electionTimer.Cancel()
	d := p.randomElectionTimeout()
	p.electionTimer = p.ctx.After(d, p.onElectionTimeout)
}

func (p *Protocol) randomElectionTimeout() time.Duration {
	span := p.cfg.ElectionTimeoutMax - p.cfg.ElectionTimeoutMin
	return p.cfg.ElectionTimeoutMin + time.Duration(p.rng.Int64N(int64(span)))
}

// onElectionTimeout fires when we have heard from no leader in time. A
// leader never arms this timer, so reaching here means follower or
// candidate: stand for (a new) election.
func (p *Protocol) onElectionTimeout() {
	if p.role == Leader {
		return
	}
	p.startElection()
}

// startElection begins a new term with this node as candidate (§5.2):
// bump the term, vote for self, persist, reset the timer, and solicit
// votes from every peer with our log summary attached.
func (p *Protocol) startElection() {
	p.role = Candidate
	p.currentTerm++
	p.votedFor, p.hasVoted = p.self, true
	p.clearLeader()
	p.votesGranted = map[transport.Host]bool{p.self: true}
	p.persistTerm()
	p.resetElectionTimer()

	rv := &RequestVote{
		Term:         p.currentTerm,
		LastLogIndex: p.log.lastIndex(),
		LastLogTerm:  p.log.lastTerm(),
	}
	for _, peer := range p.peers {
		_ = p.ctx.Send(rv, peer)
	}

	// Single-node group (no peers): a self-vote is already a majority.
	if p.wonElection() {
		p.becomeLeader()
	}
}

func (p *Protocol) onRequestVote(msg *RequestVote, from transport.Host) {
	// Higher term: adopt it and step down before deciding the vote, so a
	// fresh term starts with no vote cast.
	if msg.Term > p.currentTerm {
		p.stepDown(msg.Term)
	}

	grant := false
	if msg.Term == p.currentTerm &&
		(!p.hasVoted || p.votedFor == from) &&
		logIsUpToDate(msg.LastLogTerm, msg.LastLogIndex, p.log.lastTerm(), p.log.lastIndex()) {
		grant = true
		p.votedFor, p.hasVoted = from, true
		p.persistTerm()
		// Granting a vote is progress by a candidate; give it time to win.
		p.resetElectionTimer()
	}
	_ = p.ctx.Send(&RequestVoteReply{Term: p.currentTerm, VoteGranted: grant}, from)
}

func (p *Protocol) onRequestVoteReply(msg *RequestVoteReply, from transport.Host) {
	if msg.Term > p.currentTerm {
		p.stepDown(msg.Term)
		return
	}
	// Ignore stale replies (from an earlier term, or once we are no longer
	// collecting votes).
	if p.role != Candidate || msg.Term != p.currentTerm {
		return
	}
	if msg.VoteGranted {
		p.votesGranted[from] = true
		if p.wonElection() {
			p.becomeLeader()
		}
	}
}

// wonElection reports whether the votes granted so far are a majority of
// the whole cluster (self + peers).
func (p *Protocol) wonElection() bool {
	majority := (len(p.peers)+1)/2 + 1
	return len(p.votesGranted) >= majority
}

// becomeLeader takes leadership for the current term (§5.3): reset each
// follower's nextIndex to our last index + 1, stop the election timer,
// start heartbeating, announce leadership, and send an immediate round of
// AppendEntries to assert authority and begin replication.
func (p *Protocol) becomeLeader() {
	p.role = Leader
	p.electionTimer.Cancel()

	last := p.log.lastIndex()
	p.nextIndex = make(map[transport.Host]uint64, len(p.peers))
	p.matchIndex = make(map[transport.Host]uint64, len(p.peers))
	for _, peer := range p.peers {
		p.nextIndex[peer] = last + 1
		p.matchIndex[peer] = 0
	}

	p.setLeader(p.self)
	p.heartbeatTimer = p.ctx.Every(p.cfg.HeartbeatInterval, p.broadcastAppendEntries)
	p.broadcastAppendEntries()
}

// stepDown reverts to follower under a newly-observed higher term (§5.1):
// adopt the term, discard any vote, stop leading, and re-arm the election
// timer. The new leader (if any) is learned from a subsequent
// AppendEntries, so leader belief is cleared here.
func (p *Protocol) stepDown(term uint64) {
	wasLeader := p.role == Leader
	p.currentTerm = term
	p.votedFor, p.hasVoted = transport.Host{}, false
	p.role = Follower
	p.clearLeader()
	if wasLeader {
		p.heartbeatTimer.Cancel()
	}
	p.persistTerm()
	p.resetElectionTimer()
}

// --- replication -------------------------------------------------------

// broadcastAppendEntries sends an AppendEntries to every peer (the
// heartbeat, and the replication driver). Iterates peers in sorted order.
func (p *Protocol) broadcastAppendEntries() {
	for _, peer := range p.peers {
		p.sendAppendEntries(peer)
	}
}

// sendAppendEntries sends one peer the entries it is missing, anchored by
// the entry just before nextIndex[peer] for the consistency check.
func (p *Protocol) sendAppendEntries(peer transport.Host) {
	ni := max(p.nextIndex[peer], 1)
	prevIndex := ni - 1
	msg := &AppendEntries{
		Term:         p.currentTerm,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  p.log.termAt(prevIndex),
		LeaderCommit: p.commitIndex,
		Entries:      p.log.sliceFrom(ni),
	}
	_ = p.ctx.Send(msg, peer)
}

func (p *Protocol) onAppendEntries(msg *AppendEntries, from transport.Host) {
	// Reject an AppendEntries from a stale term outright (§5.1).
	if msg.Term < p.currentTerm {
		_ = p.ctx.Send(&AppendEntriesReply{Term: p.currentTerm, Success: false}, from)
		return
	}
	// Equal or higher term: recognize `from` as the current leader. A
	// higher term also adopts the term and clears any vote; an equal term
	// while we were candidate means we lost the election.
	if msg.Term > p.currentTerm {
		p.stepDown(msg.Term)
	}
	p.role = Follower
	p.setLeader(from)
	p.resetElectionTimer()

	// Log consistency check (§5.3): our log must contain PrevLogIndex with
	// a matching term, else we cannot safely append.
	if msg.PrevLogIndex > p.log.lastIndex() ||
		p.log.termAt(msg.PrevLogIndex) != msg.PrevLogTerm {
		_ = p.ctx.Send(&AppendEntriesReply{Term: p.currentTerm, Success: false}, from)
		return
	}

	p.appendConflictFree(msg.PrevLogIndex, msg.Entries)
	matchIndex := msg.PrevLogIndex + uint64(len(msg.Entries))

	// Advance the commit index to the leader's, capped at the last entry
	// we actually hold from this message (§5.3).
	if msg.LeaderCommit > p.commitIndex {
		p.commitIndex = min(msg.LeaderCommit, matchIndex)
		p.applyCommitted()
	}
	_ = p.ctx.Send(&AppendEntriesReply{Term: p.currentTerm, MatchIndex: matchIndex, Success: true}, from)
}

// appendConflictFree splices the leader's entries onto our log starting
// after prevIndex, truncating the first conflicting suffix (§5.3) and
// appending only genuinely new entries. It avoids truncating on an
// idempotent re-send (a duplicate/stale heartbeat carrying entries we
// already have), which would needlessly churn the log.
func (p *Protocol) appendConflictFree(prevIndex uint64, entries []LogEntry) {
	for i, e := range entries {
		idx := prevIndex + 1 + uint64(i)
		if idx <= p.log.lastIndex() && p.log.termAt(idx) == e.Term {
			// Already have this entry with the matching term: skip it
			// (idempotent re-send).
			continue
		}
		// Either idx is past our end (all remaining entries are new), or it
		// conflicts (different term). Truncate any conflicting suffix, then
		// append the incoming entries from i onward.
		if idx <= p.log.lastIndex() {
			p.log.truncateFrom(idx)
		}
		for _, ne := range entries[i:] {
			p.log.append(LogEntry{Term: ne.Term, Command: append([]byte(nil), ne.Command...)})
		}
		// Persist exactly the changed suffix: idx is the first index that
		// was truncated or newly appended.
		p.cfg.Storage.AppendEntries(idx, entries[i:])
		return
	}
}

func (p *Protocol) onAppendEntriesReply(msg *AppendEntriesReply, from transport.Host) {
	if msg.Term > p.currentTerm {
		p.stepDown(msg.Term)
		return
	}
	// Ignore stale replies or replies that arrive when we are no longer
	// the leader for this term.
	if p.role != Leader || msg.Term != p.currentTerm {
		return
	}
	if msg.Success {
		// matchIndex is monotonic: never regress it on a reordered reply.
		p.matchIndex[from] = max(p.matchIndex[from], msg.MatchIndex)
		p.nextIndex[from] = p.matchIndex[from] + 1
		p.maybeAdvanceCommit()
		return
	}
	// Consistency check failed: back off nextIndex and retry immediately
	// (§5.3). This walks the follower's log back until a match is found.
	if p.nextIndex[from] > 1 {
		p.nextIndex[from]--
	}
	p.sendAppendEntries(from)
}

// maybeAdvanceCommit recomputes the commit index from the replicated set
// and applies anything newly committed. Only current-term entries can be
// committed by counting (§5.4.2), enforced inside advanceCommitIndex.
func (p *Protocol) maybeAdvanceCommit() {
	matches := make([]uint64, 0, len(p.peers)+1)
	matches = append(matches, p.log.lastIndex()) // the leader itself
	for _, peer := range p.peers {
		matches = append(matches, p.matchIndex[peer])
	}
	newCommit := advanceCommitIndex(p.commitIndex, p.currentTerm, p.log, matches, len(p.peers)+1)
	if newCommit > p.commitIndex {
		p.commitIndex = newCommit
		p.applyCommitted()
	}
}

// applyCommitted publishes an Applied notification for every entry now
// committed but not yet applied, in strict log order (§5.3).
func (p *Protocol) applyCommitted() {
	for p.lastApplied < p.commitIndex {
		p.lastApplied++
		e := p.log.at(p.lastApplied)
		protorun.PublishNotification(p.ctx, Applied{
			Index:   p.lastApplied,
			Term:    e.Term,
			Command: append([]byte(nil), e.Command...),
		})
	}
}

// --- IPC ---------------------------------------------------------------

func (p *Protocol) handlePropose(req *Propose, r protorun.Responder[*ProposeReply]) {
	if p.role != Leader {
		r.Fail(&NotLeaderError{Leader: p.leader, HasLeader: p.hasLeader})
		return
	}
	entry := LogEntry{Term: p.currentTerm, Command: append([]byte(nil), req.Command...)}
	idx := p.log.append(entry)
	p.cfg.Storage.AppendEntries(idx, []LogEntry{entry})
	// The leader's own match index is its last log index; commitment for a
	// single-node group can advance immediately.
	p.maybeAdvanceCommit()
	r.Reply(&ProposeReply{Index: idx, Term: p.currentTerm})
	// Replicate now rather than waiting for the next heartbeat.
	p.broadcastAppendEntries()
}

func (p *Protocol) handleDebugState(_ *DebugState, r protorun.Responder[*DebugStateReply]) {
	r.Reply(&DebugStateReply{
		Role:         p.role,
		Term:         p.currentTerm,
		CommitIndex:  p.commitIndex,
		LastApplied:  p.lastApplied,
		LastLogIndex: p.log.lastIndex(),
		LastLogTerm:  p.log.lastTerm(),
		Leader:       p.leader,
		HasLeader:    p.hasLeader,
	})
}

// --- leader belief + persistence ---------------------------------------

// setLeader records (host, currentTerm) as the believed leadership and
// publishes LeaderChanged when that belief actually changes, so the
// notification stream has one entry per genuine transition.
func (p *Protocol) setLeader(host transport.Host) {
	if p.hasLeader && p.leader == host {
		return
	}
	p.leader, p.hasLeader = host, true
	protorun.PublishNotification(p.ctx, LeaderChanged{Leader: host, Term: p.currentTerm})
}

func (p *Protocol) clearLeader() { p.leader, p.hasLeader = transport.Host{}, false }

// persistTerm writes the term/vote pair through the Storage seam. The
// caller is responsible for calling it before sending any reply that the
// paper requires to be preceded by a durable write (a vote). Log appends
// persist separately and incrementally via Storage.AppendEntries at the
// two log-mutation sites (handlePropose, appendConflictFree).
func (p *Protocol) persistTerm() {
	p.cfg.Storage.SaveTerm(p.currentTerm, p.votedFor, p.hasVoted)
}

// --- helpers -----------------------------------------------------------

// seedFromHost derives a deterministic 128-bit PCG seed from a host, so a
// node's random stream is a stable function of its identity (matches the
// hyparview approach).
func seedFromHost(h transport.Host) (uint64, uint64) {
	f := fnv.New64a()
	_, _ = f.Write([]byte(h.String()))
	s1 := f.Sum64()
	_, _ = f.Write([]byte{0x5a})
	s2 := f.Sum64()
	return s1, s2
}

// hostLess is a total order on hosts (IP then port) for deterministic
// sorted iteration.
func hostLess(a, b transport.Host) bool {
	if a.IP != b.IP {
		return a.IP < b.IP
	}
	return a.Port < b.Port
}
