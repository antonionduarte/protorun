// Package hyparview implements HyParView, the partial-view membership
// protocol of Leitão, Pereira and Rodrigues (2007). It maintains a
// small, symmetric, session-backed active view and a larger unconnected
// passive view, and publishes the protocols/membership contract
// (NeighborUp/NeighborDown notifications + a GetView reply) so any
// dissemination protocol written against that contract — Plumtree, the
// eager-push gossip in cmd/gossip — can run on top of it unchanged.
//
// # Mapping HyParView onto protorun sessions
//
// The active view is backed one-to-one by protorun sessions: a peer is
// in the active view iff we hold a live session with it and an
// application-level handshake (Join or Neighbor/NeighborReply) has
// admitted it. protorun sessions are symmetric — Connect yields
// OnSessionConnected on both ends — which matches HyParView's symmetric
// active view, but a raw session coming up is only the *transport*; the
// Join/Neighbor control messages decide membership. This is why an
// inbound session with no accompanying control message is left dormant.
//
// # Failure detection
//
// The session layer is the failure detector — there are no extra
// heartbeats. A dropped active-view session surfaces as
// OnSessionDisconnected (or OnSessionGivenUp for a retryable dial that
// exhausted its budget); either way the peer leaves the active view,
// NeighborDown is published, and the passive view is promoted to refill.
//
// # Shuffle transport (resolves the roadmap open question)
//
// The periodic shuffle is routed entirely over active-view links and
// never opens a transient session to a passive peer. The Shuffle request
// is a TTL random walk over active views exactly as in the paper; the
// twist is the reply: the walk records its path (each forwarder appends
// itself), and the accepting node returns its ShuffleReply by retracing
// that path hop-by-hop back to the origin. Every hop — request and reply
// — is an existing active-view session, so no change to the session
// layer is needed. A passive peer is contacted only during promotion,
// which legitimately opens a session because the peer is *becoming* an
// active-view member. This is option (a) from the roadmap; it needs no
// framework changes, which is why it is preferred over transient
// sessions to passive peers.
package hyparview

import (
	"hash/fnv"
	"math/rand/v2"

	"github.com/antonionduarte/protorun"
	"github.com/antonionduarte/protorun/protocols/membership"
	"github.com/antonionduarte/protorun/transport"
)

// intentKind tags why we opened an outgoing session to a peer, so the
// OnSessionConnected callback knows which control message to send.
type intentKind int

const (
	intentJoin     intentKind = iota // JOIN a bootstrap contact
	intentNeighbor                   // promote / forward-join a peer via Neighbor
)

// outIntent is an in-flight outgoing active-view attempt: a session we
// opened (or reused) and the control handshake we are driving over it.
type outIntent struct {
	kind     intentKind
	priority bool
	timer    protorun.TimerHandle // abandons the attempt if not accepted in time; Cancel is nil-safe
}

// Protocol is a HyParView instance. Construct with New and Register (or
// RegisterFactory) it with a runtime. All state is owned by the event
// loop; every field below is touched only inside handlers, timer
// callbacks, and session-event callbacks.
type Protocol struct {
	cfg  Config
	ctx  protorun.ProtocolContext
	self transport.Host
	rng  *rand.Rand

	active  *hostSet // session-backed, symmetric, target size cfg.ActiveSize
	passive *hostSet // unconnected sample, max cfg.PassiveSize

	// sessions is every live session, active-view or not. Tracked so an
	// outgoing attempt can send its control message immediately when the
	// session is already up (the peer dialed us first).
	sessions *hostSet

	// pending are outgoing active-view attempts awaiting acceptance,
	// keyed by target host.
	pending map[transport.Host]*outIntent
}

// New returns a HyParView protocol configured by cfg. The zero Config is
// valid — see Config for the defaults New fills in.
//
//nolint:gocritic // hugeParam: New(cfg Config) by value is the mandated, idiomatic constructor shape.
func New(self transport.Host, cfg Config) *Protocol {
	cfg.fillDefaults()
	return &Protocol{
		cfg:      cfg,
		self:     self,
		active:   newHostSet(),
		passive:  newHostSet(),
		sessions: newHostSet(),
		pending:  make(map[transport.Host]*outIntent),
	}
}

func (p *Protocol) Start(ctx protorun.ProtocolContext) {
	p.ctx = ctx
	protorun.Handle(ctx, p.onJoin)
	protorun.Handle(ctx, p.onForwardJoin)
	protorun.Handle(ctx, p.onNeighbor)
	protorun.Handle(ctx, p.onNeighborReply)
	protorun.Handle(ctx, p.onDisconnect)
	protorun.Handle(ctx, p.onShuffle)
	protorun.Handle(ctx, p.onShuffleReply)
	protorun.RegisterRequestHandler(ctx, p.handleGetView)
	protorun.RegisterRequestHandler(ctx, p.handleDebugState)
}

func (p *Protocol) Init(ctx protorun.ProtocolContext) {
	// Per-node deterministic RNG: seeded from self, so every random walk
	// and view pick is reproducible under the sim (which fixes handler
	// order) while different nodes still make different choices.
	s1, s2 := seedFromHost(p.self)
	p.rng = rand.New(rand.NewPCG(s1, s2)) //nolint:gosec // seeded PRNG is intentional for determinism, not security

	for _, c := range p.cfg.Contacts {
		p.openActive(c, intentJoin, false)
	}
	ctx.Every(p.cfg.ShuffleInterval, p.onShuffleTick)
	ctx.Every(p.cfg.JoinInterval, p.onMaintenanceTick)
}

// --- session lifecycle -------------------------------------------------

func (p *Protocol) OnSessionConnected(host transport.Host) {
	p.sessions.add(host)
	if intent, ok := p.pending[host]; ok {
		p.driveIntent(host, intent)
	}
}

func (p *Protocol) OnSessionDisconnected(host transport.Host) {
	p.onSessionDown(host)
}

// OnSessionGivenUp treats an exhausted retryable dial the same as a
// disconnect: the peer is gone.
func (p *Protocol) OnSessionGivenUp(host transport.Host, _ int) {
	p.onSessionDown(host)
}

func (p *Protocol) onSessionDown(host transport.Host) {
	p.sessions.remove(host)
	if intent, ok := p.pending[host]; ok {
		// An outgoing attempt's session dropped before acceptance; abandon
		// it. The candidate may be dead, so it is not filed into passive.
		p.clearPending(host, intent)
	}
	if p.active.contains(host) {
		// A live active-view session failed (a graceful Disconnect would
		// have removed it from the active view already). Presume the peer
		// dead: drop it, tell the layer above, and refill.
		p.removeActive(host)
		p.promote()
	}
}

// --- outgoing active-view attempts -------------------------------------

// openActive begins an attempt to bring host into the active view via the
// given handshake. It reuses an existing session when one is already up
// (symmetric sessions mean the peer may have dialed us first), otherwise
// it dials. A timeout abandons the attempt if it is never accepted, which
// also covers a silent connect failure (a plain Connect that fails is not
// surfaced to protocols).
func (p *Protocol) openActive(host transport.Host, kind intentKind, priority bool) {
	if host == p.self || p.active.contains(host) || p.pending[host] != nil {
		return
	}
	// The candidate stays in the passive view for the duration of the
	// attempt (promote excludes pending hosts, so it is not re-picked). It
	// is removed from passive only on success, by addActive. Keeping it
	// here means a *failed* attempt does not forget the peer — which is
	// what lets a healed partition reconnect: the far-side peers a node
	// tried and failed during the partition are still in its passive view
	// to retry once the link is reachable again.
	timer := p.ctx.After(p.cfg.NeighborTimeout, func() { p.onAttemptTimeout(host) })
	intent := &outIntent{kind: kind, priority: priority, timer: timer}
	p.pending[host] = intent
	if p.sessions.contains(host) {
		p.driveIntent(host, intent)
		return
	}
	if err := p.ctx.Connect(host); err != nil {
		p.ctx.Logger().Debug("hyparview: connect failed", "host", host.String(), "err", err)
		p.clearPending(host, intent)
	}
}

// driveIntent sends the control message for an attempt whose session is
// up. JOIN is always accepted by the contact, so we optimistically admit
// the contact locally and finish. A Neighbor request must wait for the
// NeighborReply, so its intent stays pending.
func (p *Protocol) driveIntent(host transport.Host, intent *outIntent) {
	switch intent.kind {
	case intentJoin:
		_ = p.ctx.Send(&Join{}, host)
		p.clearPending(host, intent)
		p.addActive(host)
	case intentNeighbor:
		_ = p.ctx.Send(&Neighbor{Priority: intent.priority}, host)
	}
}

// onAttemptTimeout fires when a pending attempt was not accepted in time.
// It abandons the candidate, tearing down any half-open session, and
// tries to refill from the passive view for a neighbour attempt.
func (p *Protocol) onAttemptTimeout(host transport.Host) {
	intent, ok := p.pending[host]
	if !ok || p.active.contains(host) {
		return // already resolved
	}
	delete(p.pending, host)
	_ = p.ctx.Disconnect(host)
	if intent.kind == intentNeighbor {
		p.promote()
	}
}

func (p *Protocol) clearPending(host transport.Host, intent *outIntent) {
	intent.timer.Cancel() // nil-safe on the zero handle
	delete(p.pending, host)
}

// --- message handlers --------------------------------------------------

// onJoin: a node bootstrapped into the overlay by dialing us. Admit it
// unconditionally (the contact always accepts) and disseminate a
// ForwardJoin random walk to seed the newcomer into other active views.
func (p *Protocol) onJoin(_ *Join, from transport.Host) {
	p.addActive(from)
	for _, n := range p.active.sorted() {
		if n == from {
			continue
		}
		_ = p.ctx.Send(&ForwardJoin{NewNode: from, TTL: uint32(p.cfg.ARWL)}, n)
	}
}

// onForwardJoin walks a join outward. It terminates (adding NewNode to
// this node's active view) at TTL 0, when this node has at most one
// active peer, or when there is no other active peer to forward to.
func (p *Protocol) onForwardJoin(msg *ForwardJoin, from transport.Host) {
	newNode := msg.NewNode
	if newNode == p.self {
		return
	}
	if msg.TTL == uint32(p.cfg.PRWL) {
		p.addPassive(newNode)
	}
	if msg.TTL == 0 || p.active.len() <= 1 {
		p.openActive(newNode, intentNeighbor, false)
		return
	}
	n, ok := p.active.randomExcept(p.rng, from, newNode)
	if !ok {
		p.openActive(newNode, intentNeighbor, false)
		return
	}
	_ = p.ctx.Send(&ForwardJoin{NewNode: newNode, TTL: msg.TTL - 1}, n)
}

// onNeighbor decides whether to admit a peer that asked to join our
// active view. A high-priority request (the sender has an empty active
// view) is always accepted, evicting an existing peer if necessary; a
// low-priority request is rejected when the active view is full.
func (p *Protocol) onNeighbor(msg *Neighbor, from transport.Host) {
	if p.active.contains(from) {
		_ = p.ctx.Send(&NeighborReply{Accepted: true}, from)
		return
	}
	if msg.Priority || p.active.len() < p.cfg.ActiveSize {
		p.addActive(from)
		_ = p.ctx.Send(&NeighborReply{Accepted: true}, from)
		return
	}
	_ = p.ctx.Send(&NeighborReply{Accepted: false}, from)
	p.addPassive(from)
}

// onNeighborReply resolves a pending Neighbor attempt: admit on accept,
// or file the peer back into the passive view and try another candidate
// on rejection.
func (p *Protocol) onNeighborReply(msg *NeighborReply, from transport.Host) {
	intent, ok := p.pending[from]
	if !ok {
		return
	}
	p.clearPending(from, intent)
	if msg.Accepted {
		p.addActive(from)
		return
	}
	p.addPassive(from)
	_ = p.ctx.Disconnect(from)
	p.promote()
}

// onDisconnect handles a graceful "I dropped you" notice: move the sender
// to the passive view (it is alive, just not our neighbour any more) and
// refill.
func (p *Protocol) onDisconnect(_ *Disconnect, from transport.Host) {
	if !p.active.contains(from) {
		return
	}
	p.removeActive(from)
	p.addPassive(from)
	p.promote()
}

// --- shuffle -----------------------------------------------------------

// onShuffleTick initiates a shuffle: pick a random active neighbour and
// send it a walk carrying a sample of our active and passive views.
func (p *Protocol) onShuffleTick() {
	q, ok := p.active.randomExcept(p.rng)
	if !ok {
		return
	}
	msg := &Shuffle{
		Origin:  p.self,
		TTL:     uint32(p.cfg.ARWL),
		Active:  p.active.randomSample(p.rng, p.cfg.ShuffleActive, q),
		Passive: p.passive.randomSample(p.rng, p.cfg.ShufflePassive),
		Path:    []transport.Host{p.self},
	}
	_ = p.ctx.Send(msg, q)
}

// onShuffle forwards the walk while it has budget and more than one
// active peer, else accepts it: integrate the offered sample into the
// passive view and return a sample of our own passive view back along the
// recorded path.
func (p *Protocol) onShuffle(msg *Shuffle, from transport.Host) {
	if msg.Origin != p.self && msg.TTL > 0 && p.active.len() > 1 {
		if n, ok := p.active.randomExcept(p.rng, from, msg.Origin); ok {
			fwd := &Shuffle{
				Origin:  msg.Origin,
				TTL:     msg.TTL - 1,
				Active:  msg.Active,
				Passive: msg.Passive,
				Path:    appendHost(msg.Path, p.self),
			}
			_ = p.ctx.Send(fwd, n)
			return
		}
	}
	// Accept: build the reply sample first so we can prefer to keep the
	// nodes we are about to hand out when making room.
	offered := combineHosts(msg.Origin, msg.Active, msg.Passive)
	reply := p.passive.randomSample(p.rng, len(offered))
	p.integratePassive(offered)
	if len(msg.Path) == 0 {
		return // malformed; origin unreachable
	}
	target := msg.Path[len(msg.Path)-1]
	_ = p.ctx.Send(&ShuffleReply{Nodes: reply, Route: msg.Path}, target)
}

// onShuffleReply retraces the recorded path back to the origin, which
// then integrates the returned sample into its passive view.
func (p *Protocol) onShuffleReply(msg *ShuffleReply, _ transport.Host) {
	route := msg.Route
	if len(route) > 0 && route[len(route)-1] == p.self {
		route = route[:len(route)-1]
	}
	if len(route) == 0 {
		p.integratePassive(msg.Nodes) // we are the origin
		return
	}
	target := route[len(route)-1]
	_ = p.ctx.Send(&ShuffleReply{Nodes: msg.Nodes, Route: route}, target)
}

// --- periodic maintenance ----------------------------------------------

// onMaintenanceTick keeps the active view healthy. A node that has fallen
// entirely out of the overlay re-JOINs its contacts (bootstrap and full-
// isolation recovery). Otherwise, if the active view is below target, it
// promotes from the passive view — this is what reconnects a node to
// peers it lost (e.g. across a healed partition), since promotion is the
// only path that opens a session to a passive peer.
func (p *Protocol) onMaintenanceTick() {
	if p.active.len() == 0 && len(p.pending) == 0 {
		for _, c := range p.cfg.Contacts {
			p.openActive(c, intentJoin, false)
		}
		return
	}
	if p.active.len() < p.cfg.ActiveSize {
		p.promote()
	}
}

// --- view manipulation -------------------------------------------------

// addActive admits host into the active view (evicting a random peer if
// it is full) and publishes NeighborUp. Idempotent for a peer already in
// the view.
func (p *Protocol) addActive(host transport.Host) {
	if host == p.self || p.active.contains(host) {
		return
	}
	if p.active.len() >= p.cfg.ActiveSize {
		p.dropRandomActive(host)
	}
	p.passive.remove(host)
	p.active.add(host)
	protorun.PublishNotification(p.ctx, membership.NeighborUp{Peer: host})
}

// removeActive drops host from the active view and publishes
// NeighborDown. Idempotent.
func (p *Protocol) removeActive(host transport.Host) {
	if !p.active.contains(host) {
		return
	}
	p.active.remove(host)
	protorun.PublishNotification(p.ctx, membership.NeighborDown{Peer: host})
}

// dropRandomActive evicts a random active peer (other than keep) to make
// room, filing it into the passive view and notifying it with a graceful
// Disconnect.
func (p *Protocol) dropRandomActive(keep transport.Host) {
	victim, ok := p.active.randomExcept(p.rng, keep)
	if !ok {
		return
	}
	p.removeActive(victim)
	p.addPassive(victim)
	_ = p.ctx.Send(&Disconnect{}, victim)
	_ = p.ctx.Disconnect(victim)
}

// addPassive files host into the passive view (evicting a random member
// if it is full). Never adds self or a current active-view peer.
func (p *Protocol) addPassive(host transport.Host) {
	if host == p.self || p.active.contains(host) || p.passive.contains(host) {
		return
	}
	if p.passive.len() >= p.cfg.PassiveSize {
		if v, ok := p.passive.randomExcept(p.rng); ok {
			p.passive.remove(v)
		}
	}
	p.passive.add(host)
}

// integratePassive folds a shuffle's offered nodes into the passive view.
func (p *Protocol) integratePassive(nodes []transport.Host) {
	for _, h := range nodes {
		p.addPassive(h)
	}
}

// promote tries to fill one active-view slot from the passive view. The
// candidate is admitted with high priority when our active view is empty
// (a node with no neighbours must be admitted by whoever it reaches).
func (p *Protocol) promote() {
	if p.active.len()+p.pendingCount() >= p.cfg.ActiveSize {
		return
	}
	cand, ok := p.passive.randomExcept(p.rng, p.pendingHosts()...)
	if !ok {
		return
	}
	p.openActive(cand, intentNeighbor, p.active.len() == 0)
}

func (p *Protocol) pendingCount() int { return len(p.pending) }

func (p *Protocol) pendingHosts() []transport.Host {
	out := make([]transport.Host, 0, len(p.pending))
	for h := range p.pending {
		out = append(out, h)
	}
	return out
}

// --- IPC contract ------------------------------------------------------

func (p *Protocol) handleGetView(_ *membership.GetView, r protorun.Responder[*membership.View]) {
	r.Reply(&membership.View{Active: p.active.sorted()})
}

// DebugState is a HyParView-specific introspection request that returns
// both views. It goes beyond the generic membership contract (which
// exposes only the active view) to surface the passive view for tests
// and operational tooling. Being IPC, reading a protocol's private views
// from off the event loop stays on the framework's supported path (a
// runtime-routed request), rather than a data race on protocol state.
type DebugState struct{ protorun.BaseRequest }

// DebugStateReply carries snapshots of both views. Both slices are fresh
// and owned by the caller.
type DebugStateReply struct {
	protorun.BaseReply
	Active  []transport.Host
	Passive []transport.Host
}

func (p *Protocol) handleDebugState(_ *DebugState, r protorun.Responder[*DebugStateReply]) {
	r.Reply(&DebugStateReply{Active: p.active.sorted(), Passive: p.passive.sorted()})
}

// --- helpers -----------------------------------------------------------

// seedFromHost derives a deterministic 128-bit PCG seed from a host, so a
// node's random stream is a stable function of its identity.
func seedFromHost(h transport.Host) (uint64, uint64) {
	f := fnv.New64a()
	_, _ = f.Write([]byte(h.String()))
	s1 := f.Sum64()
	_, _ = f.Write([]byte{0x5a})
	s2 := f.Sum64()
	return s1, s2
}

// appendHost returns a fresh slice with host appended, never aliasing the
// input (the input rides on an inbound message we must not mutate).
func appendHost(path []transport.Host, host transport.Host) []transport.Host {
	out := make([]transport.Host, len(path)+1)
	copy(out, path)
	out[len(path)] = host
	return out
}

// combineHosts flattens a shuffle's origin and its two samples into one
// slice.
func combineHosts(origin transport.Host, active, passive []transport.Host) []transport.Host {
	out := make([]transport.Host, 0, 1+len(active)+len(passive))
	out = append(out, origin)
	out = append(out, active...)
	out = append(out, passive...)
	return out
}
