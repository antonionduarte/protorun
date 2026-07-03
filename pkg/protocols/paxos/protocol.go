// Package paxos implements classic single-decree Paxos — Lamport's synod
// protocol from "Paxos Made Simple" (2001) — as a protorun protocol. A
// fixed set of nodes agree on one immutable value: the three roles
// (proposer, acceptor, learner) all live in a single Protocol type, and
// every node plays all three at once.
//
// # Faithful to the paper
//
//   - Phase 1 (prepare/promise): a proposer picks a ballot from its own
//     disjoint sequence (round*N + nodeIndex, so ballots never collide
//     across nodes) and sends Prepare(n); an acceptor promises iff n is
//     strictly greater than every ballot it has promised, returning the
//     highest (ballot, value) it has already accepted, if any.
//   - Phase 2 (accept/accepted): on promises from a majority the proposer
//     MUST adopt the highest-ballot accepted value among those promises
//     (its own value only if none was accepted), then sends Accept(n, v);
//     an acceptor accepts iff n is at least its promised ballot.
//   - Learning: an acceptor that accepts announces Accepted(n, v) to every
//     learner; a value is CHOSEN once a learner sees a majority of
//     acceptors accept the same ballot, and the decree is then decided
//     forever (published once as a Decided notification).
//   - Liveness: a proposer whose round stalls retries with a higher ballot
//     after a randomized backoff drawn from a per-node seeded RNG, which is
//     what keeps dueling proposers from livelocking. Lamport's distinguished-
//     proposer (leader) optimization is deliberately NOT implemented —
//     randomized backoff alone drives progress under the simulation.
//
// # Safety, informally
//
// Once a value v is chosen at ballot b (a majority accepted b), every
// higher ballot's promise quorum intersects that majority in at least one
// acceptor, which reports (b, v); the adoption rule then forces every
// ballot above b to carry v too. So the decree, once chosen, can never
// change — Agreement holds across all nodes and across all future rounds.
//
// # Out of scope (deliberately)
//
//   - Multi-decree / a replicated log: this is ONE decree. Log replication
//     (Multi-Paxos) is what pkg/protocols/raft is for in this tree.
//   - Dynamic membership / reconfiguration: the acceptor set is static, from
//     Config.Peers (see Config for why partial-view membership is the wrong
//     substrate).
//   - Durability across restarts beyond what the Storage seam provides: the
//     default MemoryStorage is non-durable (see storage.go).
//
// # Determinism
//
// All state lives on the single event loop and is touched only inside
// handlers, timer callbacks, and session-event callbacks. The only
// randomness is the retry backoff, drawn from a per-node RNG seeded from
// the node's Host, so the protocol is fully reproducible under
// prototest.Sim.
package paxos

import (
	"hash/fnv"
	"math/rand/v2"
	"sort"
	"time"

	"github.com/antonionduarte/protorun/pkg/protorun"
	"github.com/antonionduarte/protorun/pkg/transport"
)

// Protocol is a single Paxos synod participant: proposer, acceptor, and
// learner in one. Construct with New and Register it with a runtime. Every
// field is owned by the event loop and touched only inside handlers, timer
// callbacks, and session-event callbacks — no locking.
type Protocol struct {
	cfg   Config
	ctx   protorun.ProtocolContext
	self  transport.Host
	peers []transport.Host // sorted; the other members (never includes self)
	index int              // this node's position in the sorted full roster
	size  int              // total membership (len(peers)+1)
	rng   *rand.Rand

	// Acceptor state (mirrored to cfg.Storage on every change).
	promised       uint64
	acceptedBallot uint64
	acceptedValue  []byte
	hasAccepted    bool

	// maxSeen is the highest ballot this node has observed anywhere (its own
	// promises, NACKs, and Accepted announcements). A new round's ballot is
	// chosen strictly above it, so a proposer never wastes a round on a
	// ballot already known to be too low.
	maxSeen uint64

	// Proposer state (volatile).
	proposing  bool
	phase      int // 1 = collecting promises, 2 = driving accepts
	myBallot   uint64
	myValue    []byte
	promises   map[transport.Host]promiseInfo
	retryTimer protorun.TimerHandle

	// Learner state (volatile). accepts[ballot] is the set of acceptors seen
	// to accept that ballot; valueAt[ballot] is the (unique) value carried by
	// it. A ballot reaching a majority in accepts decides the decree.
	accepts map[uint64]map[transport.Host]bool
	valueAt map[uint64][]byte

	decided       bool
	decidedBallot uint64
	decidedValue  []byte
	announced     bool // Decided published exactly once

	// Session liveness, for reconnect-after-heal catch-up.
	sessions map[transport.Host]bool
}

// New returns a Paxos participant for self configured by cfg. cfg.Peers
// must list the other members of the group (and must not include self);
// the zero timing fields are filled with defaults and a MemoryStorage is
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

	// This node's index in the full sorted roster (self + peers). The index
	// is what makes each node's ballot sequence disjoint from every other's.
	roster := append(append([]transport.Host(nil), peers...), self)
	sort.Slice(roster, func(i, j int) bool { return hostLess(roster[i], roster[j]) })
	index := 0
	for i, h := range roster {
		if h == self {
			index = i
			break
		}
	}

	return &Protocol{
		cfg:      cfg,
		self:     self,
		peers:    peers,
		index:    index,
		size:     len(peers) + 1,
		promises: make(map[transport.Host]promiseInfo),
		accepts:  make(map[uint64]map[transport.Host]bool),
		valueAt:  make(map[uint64][]byte),
		sessions: make(map[transport.Host]bool),
	}
}

func (p *Protocol) Start(ctx protorun.ProtocolContext) {
	p.ctx = ctx
	protorun.Handle(ctx, p.onPrepare)
	protorun.Handle(ctx, p.onPromise)
	protorun.Handle(ctx, p.onAccept)
	protorun.Handle(ctx, p.onAccepted)
	protorun.Handle(ctx, p.onAcceptNack)
	protorun.RegisterRequestHandler(ctx, p.handlePropose)
	protorun.RegisterRequestHandler(ctx, p.handleDebugState)
}

func (p *Protocol) Init(ctx protorun.ProtocolContext) {
	// Per-node deterministic RNG seeded from self: retry backoffs are
	// reproducible under the sim (which fixes handler order) while different
	// nodes still draw different backoffs, which is what desynchronizes
	// dueling proposers.
	s1, s2 := seedFromHost(p.self)
	p.rng = rand.New(rand.NewPCG(s1, s2)) //nolint:gosec // seeded PRNG is intentional for determinism, not security

	// Load durable acceptor state (empty for a fresh MemoryStorage).
	st := p.cfg.Storage.Load()
	p.promised = st.Promised
	p.acceptedBallot, p.acceptedValue, p.hasAccepted = st.AcceptedBallot, st.AcceptedValue, st.HasAccepted
	p.maxSeen = st.Promised

	// Dial every peer and keep the sessions up. Sessions are symmetric, so a
	// redundant dial from the other side is a no-op; the periodic reconnect
	// is the sole recovery path after a partition heals.
	p.ensureConnections()
	ctx.Every(p.cfg.ReconnectInterval, p.ensureConnections)
}

// --- session lifecycle -------------------------------------------------

// OnSessionConnected records the session and, if this node has accepted a
// value, re-announces it to the freshly-connected peer. That re-announce is
// the partition-heal catch-up path: a node stranded in a minority that
// could never learn the decision collects a majority of these on reconnect
// and decides. See the package doc's learning note.
func (p *Protocol) OnSessionConnected(host transport.Host) {
	p.sessions[host] = true
	if p.hasAccepted {
		_ = p.ctx.Send(&Accepted{Ballot: p.acceptedBallot, Value: p.acceptedValue}, host)
	}
}

func (p *Protocol) OnSessionDisconnected(host transport.Host) { delete(p.sessions, host) }

// OnSessionGivenUp treats an exhausted dial like a disconnect: the peer is
// simply not reachable right now; the reconnect timer will retry.
func (p *Protocol) OnSessionGivenUp(host transport.Host, _ int) { delete(p.sessions, host) }

// ensureConnections dials any peer we do not currently hold a session with.
// Run at Init and on the reconnect timer; iterates peers in sorted order
// for determinism.
func (p *Protocol) ensureConnections() {
	for _, peer := range p.peers {
		if !p.sessions[peer] {
			if err := p.ctx.Connect(peer); err != nil {
				p.ctx.Logger().Debug("paxos: connect failed", "peer", peer.String(), "err", err)
			}
		}
	}
}

// --- proposer ----------------------------------------------------------

// startRound begins (or restarts) a proposal round: pick the next ballot
// strictly above every ballot seen so far, promise to it locally (this node
// is an acceptor too), solicit promises from the peers, and arm the
// randomized retry timer. A single-node group reaches a majority from the
// self-promise alone and proceeds to Phase 2 immediately.
func (p *Protocol) startRound() {
	if p.decided {
		return
	}
	p.proposing = true
	p.phase = 1
	p.myBallot = nextBallot(p.maxSeen, p.size, p.index)
	p.observeBallot(p.myBallot)
	p.promises = make(map[transport.Host]promiseInfo)

	// Fold self into the promise quorum: there is no network loopback, so a
	// node must run its own acceptor logic in-process for every quorum it
	// participates in. myBallot is strictly above our promised ballot, so
	// this promise always succeeds.
	if ok, _, info := p.acceptPrepare(p.myBallot); ok {
		p.promises[p.self] = info
	}

	prep := &Prepare{Ballot: p.myBallot}
	for _, peer := range p.peers {
		_ = p.ctx.Send(prep, peer)
	}

	p.retryTimer.Cancel()
	p.retryTimer = p.ctx.After(p.randomBackoff(), p.onRetryTimeout)

	if len(p.promises) >= majoritySize(p.size) {
		p.beginPhase2()
	}
}

// beginPhase2 fires once a promise quorum is in hand: apply the adoption
// rule to pick the value, then drive Accept to every acceptor (self
// included, in-process).
func (p *Protocol) beginPhase2() {
	if p.phase != 1 {
		return
	}
	p.phase = 2

	infos := make([]promiseInfo, 0, len(p.promises))
	for _, peer := range p.peers { // deterministic order (value is order-independent, but keep it stable)
		if in, ok := p.promises[peer]; ok {
			infos = append(infos, in)
		}
	}
	if in, ok := p.promises[p.self]; ok {
		infos = append(infos, in)
	}
	value, _ := chooseValue(infos, p.myValue)

	// Self-accept in-process (again: no loopback), then announce, then ask
	// the peers to accept.
	if ok, _ := p.acceptAccept(p.myBallot, value); ok {
		p.broadcastAccepted(p.myBallot, value)
	}
	acc := &Accept{Ballot: p.myBallot, Value: value}
	for _, peer := range p.peers {
		_ = p.ctx.Send(acc, peer)
	}
}

// onRetryTimeout abandons a stalled round and starts a fresh one with a
// higher ballot. A decided or no-longer-proposing node lets it lapse.
func (p *Protocol) onRetryTimeout() {
	if p.decided || !p.proposing {
		return
	}
	p.startRound()
}

func (p *Protocol) randomBackoff() time.Duration {
	span := p.cfg.RetryTimeoutMax - p.cfg.RetryTimeoutMin
	return p.cfg.RetryTimeoutMin + time.Duration(p.rng.Int64N(int64(span)))
}

// --- acceptor ----------------------------------------------------------

// acceptPrepare applies the Phase 1b rule for ballot: promise (raising the
// promised ballot and persisting) iff ballot is strictly above every ballot
// promised so far. Returns whether it promised, the current promised ballot
// (for a NACK), and the accepted-value summary the proposer needs.
func (p *Protocol) acceptPrepare(ballot uint64) (bool, uint64, promiseInfo) {
	p.observeBallot(ballot)
	if !canPromise(ballot, p.promised) {
		return false, p.promised, promiseInfo{}
	}
	p.promised = ballot
	p.persist()
	return true, p.promised, promiseInfo{
		acceptedBallot: p.acceptedBallot,
		acceptedValue:  p.acceptedValue,
		hasAccepted:    p.hasAccepted,
	}
}

// acceptAccept applies the Phase 2b rule for (ballot, value): accept
// (recording it, raising promised, and persisting) iff ballot is at least
// the promised ballot. Returns whether it accepted and the current promised
// ballot (for a NACK).
func (p *Protocol) acceptAccept(ballot uint64, value []byte) (bool, uint64) {
	p.observeBallot(ballot)
	if !canAccept(ballot, p.promised) {
		return false, p.promised
	}
	p.promised = ballot
	p.acceptedBallot = ballot
	p.acceptedValue = append([]byte(nil), value...)
	p.hasAccepted = true
	p.persist()
	return true, p.promised
}

func (p *Protocol) onPrepare(msg *Prepare, from transport.Host) {
	ok, maxBallot, info := p.acceptPrepare(msg.Ballot)
	_ = p.ctx.Send(&Promise{
		Ballot:         msg.Ballot,
		OK:             ok,
		MaxBallot:      maxBallot,
		AcceptedBallot: info.acceptedBallot,
		AcceptedValue:  info.acceptedValue,
		HasAccepted:    info.hasAccepted,
	}, from)
}

func (p *Protocol) onAccept(msg *Accept, from transport.Host) {
	if ok, maxBallot := p.acceptAccept(msg.Ballot, msg.Value); ok {
		p.broadcastAccepted(msg.Ballot, msg.Value)
	} else {
		_ = p.ctx.Send(&AcceptNack{MaxBallot: maxBallot}, from)
	}
}

// --- proposer reply handlers -------------------------------------------

func (p *Protocol) onPromise(msg *Promise, from transport.Host) {
	if !msg.OK {
		// Lost to a higher promise: learn the ballot so the next round jumps
		// past it. The retry timer drives the actual retry.
		p.observeBallot(msg.MaxBallot)
		return
	}
	// Ignore promises for a ballot we are no longer collecting (stale round,
	// or we already moved to Phase 2 / decided).
	if p.decided || !p.proposing || p.phase != 1 || msg.Ballot != p.myBallot {
		return
	}
	p.promises[from] = promiseInfo{
		acceptedBallot: msg.AcceptedBallot,
		acceptedValue:  msg.AcceptedValue,
		hasAccepted:    msg.HasAccepted,
	}
	if len(p.promises) >= majoritySize(p.size) {
		p.beginPhase2()
	}
}

func (p *Protocol) onAcceptNack(msg *AcceptNack, _ transport.Host) {
	// Liveness only: learn the higher ballot; the retry timer retries.
	p.observeBallot(msg.MaxBallot)
}

// --- learner -----------------------------------------------------------

func (p *Protocol) onAccepted(msg *Accepted, from transport.Host) {
	p.recordAccepted(msg.Ballot, msg.Value, from)
}

// broadcastAccepted announces our own acceptance of (ballot, value) to
// every learner: it records the acceptance locally (self is a learner and
// there is no loopback) and sends Accepted to each peer.
func (p *Protocol) broadcastAccepted(ballot uint64, value []byte) {
	p.recordAccepted(ballot, value, p.self)
	msg := &Accepted{Ballot: ballot, Value: value}
	for _, peer := range p.peers {
		_ = p.ctx.Send(msg, peer)
	}
}

// recordAccepted tallies one acceptor's acceptance of a ballot and decides
// the decree once a majority of DISTINCT acceptors have accepted the same
// ballot. A ballot carries a unique value (ballots are proposer-disjoint
// and a proposer sends one value per ballot), so counting per ballot is
// sound.
func (p *Protocol) recordAccepted(ballot uint64, value []byte, from transport.Host) {
	p.observeBallot(ballot)
	if p.decided {
		return
	}
	set := p.accepts[ballot]
	if set == nil {
		set = make(map[transport.Host]bool)
		p.accepts[ballot] = set
	}
	if set[from] {
		return // idempotent: duplicate/reconnect re-announce
	}
	set[from] = true
	if _, ok := p.valueAt[ballot]; !ok {
		p.valueAt[ballot] = append([]byte(nil), value...)
	}
	if len(set) >= majoritySize(p.size) {
		p.decide(ballot, p.valueAt[ballot])
	}
}

// decide marks the decree chosen forever and publishes Decided exactly
// once. It also stops this node's proposer: there is nothing left to
// propose.
func (p *Protocol) decide(ballot uint64, value []byte) {
	if p.decided {
		return
	}
	p.decided = true
	p.decidedBallot = ballot
	p.decidedValue = append([]byte(nil), value...)
	p.proposing = false
	p.retryTimer.Cancel()
	if !p.announced {
		p.announced = true
		protorun.PublishNotification(p.ctx, Decided{
			Value:  append([]byte(nil), p.decidedValue...),
			Ballot: ballot,
		})
	}
}

// --- IPC ---------------------------------------------------------------

func (p *Protocol) handlePropose(req *Propose, r protorun.Responder[*ProposeReply]) {
	if p.decided {
		r.Fail(&AlreadyDecidedError{
			Value:  append([]byte(nil), p.decidedValue...),
			Ballot: p.decidedBallot,
		})
		return
	}
	// Nominate this value and kick a round if idle. An already-running round
	// keeps its in-flight ballot; the value may still change under adoption.
	if !p.proposing {
		p.myValue = append([]byte(nil), req.Value...)
		p.startRound()
	}
	r.Reply(&ProposeReply{})
}

func (p *Protocol) handleDebugState(_ *DebugState, r protorun.Responder[*DebugStateReply]) {
	r.Reply(&DebugStateReply{
		Promised:       p.promised,
		AcceptedBallot: p.acceptedBallot,
		AcceptedValue:  append([]byte(nil), p.acceptedValue...),
		HasAccepted:    p.hasAccepted,
		Decided:        p.decided,
		DecidedValue:   append([]byte(nil), p.decidedValue...),
		DecidedBallot:  p.decidedBallot,
		Proposing:      p.proposing,
		MyBallot:       p.myBallot,
	})
}

// --- helpers -----------------------------------------------------------

// observeBallot raises maxSeen to b if b is higher, so the next round's
// ballot clears every ballot this node has ever heard of.
func (p *Protocol) observeBallot(b uint64) {
	if b > p.maxSeen {
		p.maxSeen = b
	}
}

// persist writes the durable acceptor state through the Storage seam. The
// caller is responsible for calling it before any message the paper
// requires to be preceded by a durable write (a promise, an accept) — both
// acceptPrepare and acceptAccept do exactly that.
func (p *Protocol) persist() {
	p.cfg.Storage.Persist(PersistentState{
		Promised:       p.promised,
		AcceptedBallot: p.acceptedBallot,
		AcceptedValue:  p.acceptedValue,
		HasAccepted:    p.hasAccepted,
	})
}

// seedFromHost derives a deterministic 128-bit PCG seed from a host, so a
// node's random stream is a stable function of its identity (matches the
// raft/hyparview approach).
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
