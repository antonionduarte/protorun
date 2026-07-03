// Package prototest lets protocol authors test their protorun protocols
// without touching a real network. An in-memory mesh of nodes stands in
// for the TCP + handshake stack at the runtime's Sessions seam (no wire,
// no handshake, in-process delivery), NewRuntime stands up a runnable
// runtime around a mesh node in one call, and — the headline — Sim runs a
// whole protocol stack under seeded, virtual-time simulation:
// reproducible schedules, injectable network faults, and convergence
// tests that finish in milliseconds of real time.
//
// Three layers of use, smallest first:
//
//	// 1. A bare mesh for exercising the Sessions contract directly.
//	mesh := prototest.NewMesh(t)
//	a := mesh.Node(transport.NewHost(1, "10.0.0.1"))
//
//	// 2. Full runtimes over a mesh, virtual time by default.
//	rtA := prototest.NewRuntime(t, mesh, hostA, []protorun.Protocol{pa})
//	rtB := prototest.NewRuntime(t, mesh, hostB, []protorun.Protocol{pb})
//
//	// 3. A seeded, virtual-time simulation of the whole stack.
//	sim := prototest.NewSim(t, prototest.WithSeed(42))
//	rtA := sim.Node(hostA, pa)
//	rtB := sim.Node(hostB, pb)
//	sim.Run(30 * time.Second) // completes in ms; deterministic per seed
package prototest

import (
	"bytes"
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/antonionduarte/protorun/pkg/protorun"
	"github.com/antonionduarte/protorun/pkg/transport"
)

// meshChannelBuffer sizes each node's outbound message and event
// channels in async (non-Sim) mode. Matches the session layer's
// defaults.
const meshChannelBuffer = 16

// simEpoch is the wall-clock instant a virtual clock starts from, so
// Now() is a stable, readable value across runs.
var simEpoch = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

// pcgStream is the second PCG seed word; keeping it fixed means the whole
// random stream is a pure function of the user-visible seed.
const pcgStream = 0x9E3779B97F4A7C15

// link is a directed edge between two hosts. Fault tables key on it so a
// fault can be asymmetric internally even though the public setters
// (Cut/SetLoss/SetDelay) apply to both directions.
type link struct{ from, to transport.Host }

// delaySpec is a per-link delivery delay: base ± jitter, sampled from the
// mesh RNG at send time.
type delaySpec struct {
	base   time.Duration
	jitter time.Duration
}

func (d delaySpec) sample(rng *rand.Rand) time.Duration {
	if d.jitter <= 0 {
		return d.base
	}
	// Uniform in [-jitter, +jitter], clamped so a delivery never travels
	// backwards in virtual time.
	span := int64(2*d.jitter) + 1
	off := time.Duration(rng.Int64N(span)) - d.jitter
	return max(d.base+off, 0)
}

// Mesh is a set of in-process nodes addressable by Host. Nodes join via
// Node (usually indirectly, through NewRuntime or Sim.Node) and reach
// each other with the semantics of the real stack: Connect yields
// SessionConnected on both sides, Disconnect/Cancel yields
// SessionDisconnected on both sides, dialing an absent Host yields
// SessionFailed, and Send without a session is dropped.
//
// A Mesh owns a single virtual clock shared by every node (Clock), so all
// nodes advance on one timeline, and a single seeded RNG behind every
// random decision (loss, jitter, delivery interleaving under a Sim).
// Fault injection (Cut/Heal/Isolate/SetLoss/SetDelay) is applied here and
// consulted on every send.
type Mesh struct {
	t TB

	mu    sync.Mutex
	nodes map[transport.Host]*Node

	// clock is the shared virtual clock, or nil under WithRealClock.
	clock *FakeClock

	// seed and rng: one source for every random decision on the mesh,
	// guarded by mu so concurrent node goroutines can Send safely.
	seed int64
	rng  *rand.Rand

	// Fault tables, keyed by directed link. Guarded by mu.
	cut   map[link]struct{}
	loss  map[link]float64
	delay map[link]delaySpec

	// isolated tracks hosts taken down by Isolate, so a drop across one of
	// their (cut) links is labelled "isolated" rather than "cut" in a
	// trace. Purely a diagnostic distinction — the drop happens either way.
	// Guarded by mu.
	isolated map[transport.Host]struct{}

	// recorder is non-nil when the mesh is built with a trace option
	// (WithTrace / WithTraceOnFailure). It observes the schedule the mesh
	// and Sim scheduler already produce and never perturbs it; see trace.go.
	recorder *recorder

	// sched is non-nil only under a Sim; when set, Node send/connect/
	// disconnect route through it instead of the async channels.
	sched *scheduler

	// pending is the Sim scheduler's set of undelivered events. Held on
	// the mesh (rather than the scheduler) so it shares mesh.mu with the
	// fault tables and RNG that a send touches in the same critical
	// section. Empty and unused on a bare mesh.
	pending []*delivery
}

// NewMesh builds a bare mesh. It logs (and returns behind Clock) a shared
// virtual clock by default; pass WithRealClock for wall time. The seed —
// pinned with WithSeed or derived from t.Name() — is always logged so any
// run is reproducible.
func NewMesh(t TB, opts ...Option) *Mesh {
	t.Helper()
	return newMesh(t, opts...)
}

func newMesh(t TB, opts ...Option) *Mesh {
	cfg := meshConfig{seed: defaultSeed(t.Name())}
	for _, o := range opts {
		o(&cfg)
	}
	m := &Mesh{
		t:     t,
		nodes: make(map[transport.Host]*Node),
		seed:  cfg.seed,
		//nolint:gosec // A simulation needs a seeded, reproducible PRNG; crypto randomness would defeat determinism.
		rng:      rand.New(rand.NewPCG(uint64(cfg.seed), pcgStream)),
		cut:      make(map[link]struct{}),
		loss:     make(map[link]float64),
		delay:    make(map[link]delaySpec),
		isolated: make(map[transport.Host]struct{}),
	}
	if !cfg.realClock {
		m.clock = NewFakeClock(simEpoch)
	}
	m.recorder = newRecorder(t, m, cfg)
	t.Logf("prototest: seed %d — re-run with prototest.WithSeed(%d) to reproduce", cfg.seed, cfg.seed)
	return m
}

// Clock returns the mesh's shared virtual clock, or nil if the mesh was
// built WithRealClock. Under a Sim the scheduler drives it; for a bare
// mesh a test may Advance it directly.
func (m *Mesh) Clock() *FakeClock { return m.clock }

// Seed returns the seed behind this mesh's RNG, for logging or asserting.
func (m *Mesh) Seed() int64 { return m.seed }

// Node returns the mesh node for self, creating it on first use. The
// returned Node implements protorun.Sessions and is what NewRuntime wires
// into the runtime via WithTransport.
func (m *Mesh) Node(self transport.Host) *Node {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n, ok := m.nodes[self]; ok {
		return n
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		mesh:        m,
		self:        self,
		ctx:         ctx,
		cancelFunc:  cancel,
		outMessages: make(chan transport.SessionMessage, meshChannelBuffer),
		outEvents:   make(chan transport.SessionEvent, meshChannelBuffer),
		peers:       make(map[transport.Host]struct{}),
	}
	m.nodes[self] = n
	return n
}

// --- Fault injection ---------------------------------------------------

// Cut drops the link between a and b in both directions. Any established
// session is torn down (both endpoints see SessionDisconnected, as they
// would on a dead connection) and any messages already in flight between
// the two are lost. The cut persists until Heal: further sends are
// dropped and Connect across it fails with SessionFailed.
func (m *Mesh) Cut(a, b transport.Host) {
	m.mu.Lock()
	na, nb, teardown := m.cutLinkLocked(a, b)
	m.mu.Unlock()

	m.recorder.fault("cut", []transport.Host{a, b}, nil)
	if teardown {
		m.surface(na, transport.NewSessionDisconnected(b))
		m.surface(nb, transport.NewSessionDisconnected(a))
	}
}

// cutLinkLocked severs the a<->b link in both directions and purges any
// in-flight messages, without emitting a fault event or surfacing the
// disconnects — so Cut and Isolate can label the mutation differently and
// surface after releasing the lock. It reports the two endpoints and
// whether a live session was actually torn down. Caller holds m.mu.
func (m *Mesh) cutLinkLocked(a, b transport.Host) (na, nb *Node, teardown bool) {
	m.cut[link{a, b}] = struct{}{}
	m.cut[link{b, a}] = struct{}{}
	na, aok := m.nodes[a]
	nb, bok := m.nodes[b]
	if aok && bok {
		if _, connected := na.peers[b]; connected {
			delete(na.peers, b)
			delete(nb.peers, a)
			teardown = true
		}
	}
	if m.sched != nil {
		m.sched.purgeLinkLocked(a, b)
	}
	return na, nb, teardown
}

// Heal restores the link between a and b. It does NOT reconnect anything:
// a real partition heal only makes the network reachable again — the
// protocols must re-establish their own sessions (typically via their
// reconnect timers or session-event handlers). This mirrors production,
// where the framework never silently reopens a session an operator or a
// fault tore down.
func (m *Mesh) Heal(a, b transport.Host) {
	m.mu.Lock()
	delete(m.cut, link{a, b})
	delete(m.cut, link{b, a})
	// A heal makes both endpoints reachable again, so neither is fully
	// isolated any more (a re-isolate re-adds them).
	delete(m.isolated, a)
	delete(m.isolated, b)
	m.mu.Unlock()

	m.recorder.fault("heal", []transport.Host{a, b}, nil)
}

// Isolate cuts every link of h, partitioning it off from the rest of the
// mesh. Equivalent to Cut(h, x) for every other node x, but it records a
// single "isolate" fault (not one "cut" per link) and marks h isolated so
// drops across its severed links are labelled "isolated" in a trace.
func (m *Mesh) Isolate(h transport.Host) {
	m.mu.Lock()
	m.isolated[h] = struct{}{}
	others := make([]transport.Host, 0, len(m.nodes))
	for host := range m.nodes {
		if host != h {
			others = append(others, host)
		}
	}
	type torn struct{ na, nb *Node }
	var teardowns []torn
	peers := make([]transport.Host, 0, len(others))
	for _, x := range others {
		na, nb, teardown := m.cutLinkLocked(h, x)
		if teardown {
			teardowns = append(teardowns, torn{na, nb})
			peers = append(peers, x)
		}
	}
	m.mu.Unlock()

	m.recorder.fault("isolate", []transport.Host{h}, nil)
	for i, td := range teardowns {
		m.surface(td.na, transport.NewSessionDisconnected(peers[i]))
		m.surface(td.nb, transport.NewSessionDisconnected(h))
	}
}

// SetLoss sets the per-message drop probability p (in [0,1]) on the link
// between a and b, both directions. On each send the mesh RNG decides
// whether that message is dropped. p<=0 clears the loss.
func (m *Mesh) SetLoss(a, b transport.Host, p float64) {
	m.mu.Lock()
	if p <= 0 {
		delete(m.loss, link{a, b})
		delete(m.loss, link{b, a})
	} else {
		if p > 1 {
			p = 1
		}
		m.loss[link{a, b}] = p
		m.loss[link{b, a}] = p
	}
	m.mu.Unlock()

	m.recorder.fault("loss", []transport.Host{a, b}, map[string]any{"p": p})
}

// SetDelay sets a per-message delivery delay of d ± jitter on the link
// between a and b, both directions. The jitter is sampled from the mesh
// RNG at send time. Delays only take effect under a Sim, where the
// scheduler holds each message until its (virtual) delivery time; on a
// bare async mesh they are ignored. d<=0 with jitter<=0 clears the delay.
func (m *Mesh) SetDelay(a, b transport.Host, d, jitter time.Duration) {
	m.mu.Lock()
	if d <= 0 && jitter <= 0 {
		delete(m.delay, link{a, b})
		delete(m.delay, link{b, a})
	} else {
		spec := delaySpec{base: d, jitter: jitter}
		m.delay[link{a, b}] = spec
		m.delay[link{b, a}] = spec
	}
	m.mu.Unlock()

	m.recorder.fault("delay", []transport.Host{a, b},
		map[string]any{"base": d.String(), "jitter": jitter.String()})
}

// isCutLocked reports whether the from->to link is cut. Caller holds
// m.mu.
func (m *Mesh) isCutLocked(from, to transport.Host) bool {
	_, cut := m.cut[link{from, to}]
	return cut
}

// dropLocked reports whether a message from->to should be dropped by the
// fault policy: a cut link, or a seeded loss roll landing under the
// configured probability. Caller holds m.mu.
func (m *Mesh) dropLocked(from, to transport.Host) bool {
	drop, _ := m.dropDecisionLocked(from, to)
	return drop
}

// dropDecisionLocked is dropLocked plus the reason a drop happened, for the
// trace recorder: "cut" for a severed link, "isolated" when either
// endpoint was taken down by Isolate, or "loss" for a seeded loss roll.
// It consumes exactly the same RNG as dropLocked (one Float64 only on the
// not-cut, loss-configured path), so recording a run's drops never
// perturbs its schedule. Caller holds m.mu.
func (m *Mesh) dropDecisionLocked(from, to transport.Host) (bool, string) {
	if m.isCutLocked(from, to) {
		if _, iso := m.isolated[from]; iso {
			return true, "isolated"
		}
		if _, iso := m.isolated[to]; iso {
			return true, "isolated"
		}
		return true, "cut"
	}
	if p, ok := m.loss[link{from, to}]; ok && m.rng.Float64() < p {
		return true, "loss"
	}
	return false, ""
}

// surface delivers a session event to n: scheduled through the Sim (at
// current virtual time) or emitted on the async channel for a bare mesh.
func (m *Mesh) surface(n *Node, ev transport.SessionEvent) {
	if m.sched != nil {
		m.sched.enqueue(&delivery{at: m.clock.Now(), node: n, ev: ev})
		return
	}
	n.emit(ev)
}

// Node is one Host's endpoint on a Mesh. It satisfies protorun.Sessions
// (the runtime drives it like the real session layer) and, under a Sim,
// protorun.SyncDeliverer (it takes over inbound delivery so the scheduler
// can detect quiescence).
type Node struct {
	mesh *Mesh
	self transport.Host

	ctx        context.Context
	cancelFunc context.CancelFunc

	// Async-mode delivery channels (unused under a Sim).
	outMessages chan transport.SessionMessage
	outEvents   chan transport.SessionEvent

	// peers holds the Hosts this node has a live session with. Guarded by
	// mesh.mu so both endpoints mutate under one lock.
	peers map[transport.Host]struct{}

	// sink and rt are set under a Sim: sink is the runtime's synchronous
	// inbound path (installed via UseSyncInbound), rt is the runtime the
	// scheduler polls for quiescence.
	sink protorun.InboundSink
	rt   *protorun.Runtime
}

var (
	_ protorun.Sessions      = (*Node)(nil)
	_ protorun.SyncDeliverer = (*Node)(nil)
)

// UseSyncInbound satisfies protorun.SyncDeliverer. It stashes the
// runtime's inbound sink and reports whether this node runs under a Sim
// (mesh has a scheduler). When true the runtime routes inbound traffic
// through the sink and skips its async pump goroutines, which is what
// lets the scheduler deliver synchronously and observe quiescence.
func (n *Node) UseSyncInbound(sink protorun.InboundSink) bool {
	n.mesh.mu.Lock()
	n.sink = sink
	sched := n.mesh.sched
	n.mesh.mu.Unlock()
	return sched != nil
}

// Connect establishes a session with host: both endpoints see
// SessionConnected, mirroring the real handshake where the dialer is
// Established on Ack and the listener on Hello. Dialing a Host that isn't
// on the mesh (or has been cancelled), or one across a cut link, yields
// SessionFailed. Connecting to an already-connected peer is a no-op.
func (n *Node) Connect(host transport.Host) {
	if n.mesh.sched != nil {
		n.simConnect(host)
		return
	}
	n.mesh.mu.Lock()
	peer, ok := n.mesh.nodes[host]
	if ok && peer.ctx.Err() != nil {
		ok = false
	}
	if !ok || n.mesh.isCutLocked(n.self, host) {
		n.mesh.mu.Unlock()
		n.emit(transport.NewSessionFailed(host))
		return
	}
	if _, already := n.peers[host]; already {
		n.mesh.mu.Unlock()
		return
	}
	n.peers[host] = struct{}{}
	peer.peers[n.self] = struct{}{}
	n.mesh.mu.Unlock()

	n.emit(transport.NewSessionConnected(host))
	peer.emit(transport.NewSessionConnected(n.self))
}

// simConnect is Connect under a Sim: session state changes immediately
// (as in the async path) but the SessionConnected/SessionFailed events
// are scheduled so the harness controls when protocols observe them.
func (n *Node) simConnect(host transport.Host) {
	m := n.mesh
	m.mu.Lock()
	peer, ok := m.nodes[host]
	if ok && peer.ctx.Err() != nil {
		ok = false
	}
	if !ok || m.isCutLocked(n.self, host) {
		at := m.clock.Now()
		m.mu.Unlock()
		m.sched.enqueue(&delivery{at: at, node: n, ev: transport.NewSessionFailed(host)})
		return
	}
	if _, already := n.peers[host]; already {
		m.mu.Unlock()
		return
	}
	n.peers[host] = struct{}{}
	peer.peers[n.self] = struct{}{}
	at := m.clock.Now()
	m.mu.Unlock()

	m.sched.enqueue(&delivery{at: at, node: n, ev: transport.NewSessionConnected(host)})
	m.sched.enqueue(&delivery{at: at, node: peer, ev: transport.NewSessionConnected(n.self)})
}

// Disconnect tears down the session with host; both endpoints see
// SessionDisconnected. A no-op if no session exists.
func (n *Node) Disconnect(host transport.Host) {
	m := n.mesh
	m.mu.Lock()
	if _, connected := n.peers[host]; !connected {
		m.mu.Unlock()
		return
	}
	delete(n.peers, host)
	peer, peerExists := m.nodes[host]
	if peerExists {
		delete(peer.peers, n.self)
	}
	if m.sched != nil {
		m.sched.purgeLinkLocked(n.self, host)
	}
	m.mu.Unlock()

	m.surface(n, transport.NewSessionDisconnected(host))
	if peerExists {
		m.surface(peer, transport.NewSessionDisconnected(n.self))
	}
}

// Send delivers an application payload to sendTo's inbound stream. Like
// the real transport, sending without a live session (or across a cut
// link, or when a loss roll drops it) is a silent drop — failures surface
// as session events, not here. Under a Sim the message is handed to the
// scheduler with its (virtual) delivery time; on a bare mesh it goes
// straight to the peer's inbound channel.
func (n *Node) Send(msg bytes.Buffer, sendTo transport.Host) {
	if n.mesh.sched != nil {
		n.mesh.sched.send(n, sendTo, msg)
		return
	}
	m := n.mesh
	m.mu.Lock()
	_, connected := n.peers[sendTo]
	peer, peerExists := m.nodes[sendTo]
	drop := !connected || !peerExists || m.dropLocked(n.self, sendTo)
	m.mu.Unlock()
	if drop {
		return
	}
	sessionMsg := transport.NewSessionMessage(msg, n.self)
	select {
	case peer.outMessages <- sessionMsg:
	case <-peer.ctx.Done():
	case <-n.ctx.Done():
	}
}

func (n *Node) OutMessages() chan transport.SessionMessage    { return n.outMessages }
func (n *Node) OutChannelEvents() chan transport.SessionEvent { return n.outEvents }

// Cancel takes the node off the mesh: every live session is torn down
// (peers see SessionDisconnected, as on a closed connection) and further
// Connect attempts against this Host fail. Called by runtime teardown.
func (n *Node) Cancel() {
	n.cancelFunc()

	m := n.mesh
	m.mu.Lock()
	peers := make([]*Node, 0, len(n.peers))
	for host := range n.peers {
		delete(n.peers, host)
		if peer, ok := m.nodes[host]; ok {
			delete(peer.peers, n.self)
			peers = append(peers, peer)
		}
	}
	m.mu.Unlock()

	// At shutdown under a Sim there is no more scheduling, so peers are
	// simply dropped; on a bare mesh, mirror a closed connection.
	if m.sched == nil {
		for _, peer := range peers {
			peer.emit(transport.NewSessionDisconnected(n.self))
		}
	}
}

// emit delivers a session event on the async channel unless the node is
// cancelled — the same ctx-guarded discipline as the real session layer's
// emitEvent. Used only in async (non-Sim) mode.
func (n *Node) emit(ev transport.SessionEvent) {
	select {
	case n.outEvents <- ev:
	case <-n.ctx.Done():
	}
}
