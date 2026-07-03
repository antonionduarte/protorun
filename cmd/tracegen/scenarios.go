package main

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/antonionduarte/protorun/pkg/protocols/hyparview"
	"github.com/antonionduarte/protorun/pkg/protocols/paxos"
	"github.com/antonionduarte/protorun/pkg/protocols/plumtree"
	"github.com/antonionduarte/protorun/pkg/protocols/raft"
	"github.com/antonionduarte/protorun/pkg/protorun"
	"github.com/antonionduarte/protorun/pkg/prototest"
	"github.com/antonionduarte/protorun/pkg/transport"
)

// nodeHost builds the i-th scenario node with a readable address.
func nodeHost(i int) transport.Host {
	return transport.NewHost(6000+i, fmt.Sprintf("10.0.0.%d", i+1))
}

// otherHosts returns every node index's host except self, for a static
// consensus peer list.
func otherHosts(n, self int) []transport.Host {
	peers := make([]transport.Host, 0, n-1)
	for j := range n {
		if j != self {
			peers = append(peers, nodeHost(j))
		}
	}
	return peers
}

// --- state probes ------------------------------------------------------

// statePoller is the reusable probe protocol behind state sampling: it
// polls a protocol's DebugState over IPC (the established pattern — no new
// protocol-side API) and caches the latest reply, which the trace sampler
// reads at quiescent points. Rep is the protocol's DebugState reply type.
type statePoller[Rep protorun.Reply] struct {
	self     transport.Host
	name     string
	interval time.Duration
	send     func(ctx protorun.ProtocolContext, cb func(Rep, error))

	mu   sync.Mutex
	last Rep
	have bool
}

func (p *statePoller[Rep]) Start(protorun.ProtocolContext) {}

func (p *statePoller[Rep]) Init(ctx protorun.ProtocolContext) {
	poll := func() {
		p.send(ctx, func(rep Rep, err error) {
			if err != nil {
				return
			}
			p.mu.Lock()
			p.last, p.have = rep, true
			p.mu.Unlock()
		})
	}
	poll()
	ctx.Every(p.interval, poll)
}

func (p *statePoller[Rep]) host() transport.Host { return p.self }
func (p *statePoller[Rep]) label() string        { return p.name }

func (p *statePoller[Rep]) snapshot() (any, bool) {
	rep, ok := p.typed()
	if !ok {
		return nil, false
	}
	return rep, true
}

func (p *statePoller[Rep]) typed() (Rep, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.last, p.have
}

// stateSource is the sampler's view of a probe: where it lives and its
// most recent snapshot.
type stateSource interface {
	host() transport.Host
	label() string
	snapshot() (any, bool)
}

// sampleSet collects a scenario's probes and produces the WithTraceSampler
// callback that emits each probe's cached state every N steps.
type sampleSet struct {
	probes []stateSource
}

func (s *sampleSet) add(p stateSource) { s.probes = append(s.probes, p) }

func (s *sampleSet) sampler() func(*prototest.Sim, func(transport.Host, string, any)) {
	return func(_ *prototest.Sim, emit func(transport.Host, string, any)) {
		for _, p := range s.probes {
			if st, ok := p.snapshot(); ok {
				emit(p.host(), p.label(), st)
			}
		}
	}
}

// driver is a probe that holds a ProtocolContext so the scenario goroutine
// can issue requests (Propose, Broadcast) into a node's event loop through
// supported IPC, mirroring the harness controls the protocol test suites
// use.
type driver struct {
	mu  sync.Mutex
	ctx protorun.ProtocolContext
}

func (d *driver) Start(ctx protorun.ProtocolContext) {
	d.mu.Lock()
	d.ctx = ctx
	d.mu.Unlock()
}
func (d *driver) Init(protorun.ProtocolContext) {}

func (d *driver) context() protorun.ProtocolContext {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.ctx
}

// --- probe constructors ------------------------------------------------

func raftProbe(self transport.Host, every time.Duration) *statePoller[*raft.DebugStateReply] {
	return &statePoller[*raft.DebugStateReply]{
		self: self, name: "raft", interval: every,
		send: func(ctx protorun.ProtocolContext, cb func(*raft.DebugStateReply, error)) {
			protorun.SendRequest(ctx, &raft.DebugState{}, cb)
		},
	}
}

func paxosProbe(self transport.Host, every time.Duration) *statePoller[*paxos.DebugStateReply] {
	return &statePoller[*paxos.DebugStateReply]{
		self: self, name: "paxos", interval: every,
		send: func(ctx protorun.ProtocolContext, cb func(*paxos.DebugStateReply, error)) {
			protorun.SendRequest(ctx, &paxos.DebugState{}, cb)
		},
	}
}

func hyparviewProbe(self transport.Host, every time.Duration) *statePoller[*hyparview.DebugStateReply] {
	return &statePoller[*hyparview.DebugStateReply]{
		self: self, name: "hyparview", interval: every,
		send: func(ctx protorun.ProtocolContext, cb func(*hyparview.DebugStateReply, error)) {
			protorun.SendRequest(ctx, &hyparview.DebugState{}, cb)
		},
	}
}

func plumtreeProbe(self transport.Host, every time.Duration) *statePoller[*plumtree.DebugStatsReply] {
	return &statePoller[*plumtree.DebugStatsReply]{
		self: self, name: "plumtree", interval: every,
		send: func(ctx protorun.ProtocolContext, cb func(*plumtree.DebugStatsReply, error)) {
			protorun.SendRequest(ctx, &plumtree.DebugStats{}, cb)
		},
	}
}

// --- scenarios ---------------------------------------------------------

// runRaftPartition elects a leader in a 5-node Raft group, replicates a
// batch of commands, cuts the leader off from the majority (forcing a new
// election on the majority side), replicates more, then heals and lets the
// cluster reconverge. Exercises session events, message deliveries,
// election churn, and cut/heal faults.
func runRaftPartition(t *cliTB, out io.Writer, seed int64) {
	const n = 5
	set := &sampleSet{}
	sim := prototest.NewSim(t,
		prototest.WithSeed(seed),
		prototest.WithTrace(out),
		prototest.WithTraceStateEvery(40),
		prototest.WithTraceSampler(set.sampler()),
	)

	drivers := make([]*driver, n)
	probes := make([]*statePoller[*raft.DebugStateReply], n)
	for i := range n {
		self := nodeHost(i)
		rp := raft.New(self, raft.Config{
			Peers:              otherHosts(n, i),
			HeartbeatInterval:  30 * time.Millisecond,
			ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
			ReconnectInterval:  50 * time.Millisecond,
		})
		drivers[i] = &driver{}
		probes[i] = raftProbe(self, 20*time.Millisecond)
		set.add(probes[i])
		sim.Node(self, rp, drivers[i], probes[i])
	}

	// Converge to a first leader, then replicate a batch to it.
	sim.RunUntil(func() bool { return raftLeader(probes, -1) >= 0 }, 10*time.Second)
	leader := raftLeader(probes, -1)
	for i := range 6 {
		raftPropose(drivers[leader], fmt.Sprintf("term1-cmd%d", i))
		sim.Run(30 * time.Millisecond)
	}
	sim.Run(400 * time.Millisecond)

	// Partition the leader off the majority: a new leader must emerge.
	for i := range n {
		if i != leader {
			sim.Mesh().Cut(nodeHost(leader), nodeHost(i))
		}
	}
	sim.RunUntil(func() bool { return raftLeader(probes, leader) >= 0 }, 5*time.Second)
	if newLeader := raftLeader(probes, leader); newLeader >= 0 {
		for i := range 4 {
			raftPropose(drivers[newLeader], fmt.Sprintf("term2-cmd%d", i))
			sim.Run(30 * time.Millisecond)
		}
	}
	sim.Run(500 * time.Millisecond)

	// Heal and let the old leader rejoin and catch up.
	for i := range n {
		if i != leader {
			sim.Mesh().Heal(nodeHost(leader), nodeHost(i))
		}
	}
	sim.Run(3 * time.Second)
}

// raftLeader returns the index of a probe whose node currently believes it
// is leader, skipping the except index (-1 to skip none), or -1 if none.
func raftLeader(probes []*statePoller[*raft.DebugStateReply], except int) int {
	for i, p := range probes {
		if i == except {
			continue
		}
		if rep, ok := p.typed(); ok && rep.Role == raft.Leader {
			return i
		}
	}
	return -1
}

func raftPropose(d *driver, cmd string) {
	protorun.SendRequest(d.context(), &raft.Propose{Command: []byte(cmd)},
		func(*raft.ProposeReply, error) {})
}

// runPaxosDuel stands up a 5-node synod and has two different proposers
// nominate different values at nearly the same time, so the trace shows the
// prepare/accept duel converging on a single decided value. Exercises the
// consensus lens's ballot/promise state.
func runPaxosDuel(t *cliTB, out io.Writer, seed int64) {
	const n = 5
	set := &sampleSet{}
	sim := prototest.NewSim(t,
		prototest.WithSeed(seed),
		prototest.WithTrace(out),
		prototest.WithTraceStateEvery(30),
		prototest.WithTraceSampler(set.sampler()),
	)

	drivers := make([]*driver, n)
	probes := make([]*statePoller[*paxos.DebugStateReply], n)
	for i := range n {
		self := nodeHost(i)
		pp := paxos.New(self, paxos.Config{
			Peers:             otherHosts(n, i),
			RetryTimeoutMin:   120 * time.Millisecond,
			RetryTimeoutMax:   260 * time.Millisecond,
			ReconnectInterval: 50 * time.Millisecond,
		})
		drivers[i] = &driver{}
		probes[i] = paxosProbe(self, 20*time.Millisecond)
		set.add(probes[i])
		sim.Node(self, pp, drivers[i], probes[i])
	}

	// Let sessions form, then two proposers duel with different values.
	sim.Run(300 * time.Millisecond)
	paxosPropose(drivers[0], "alpha")
	paxosPropose(drivers[1], "bravo")

	// Run to a decision on every node (single-decree agreement).
	sim.RunUntil(func() bool {
		for _, p := range probes {
			rep, ok := p.typed()
			if !ok || !rep.Decided {
				return false
			}
		}
		return true
	}, 15*time.Second)
	sim.Run(500 * time.Millisecond)
}

func paxosPropose(d *driver, value string) {
	protorun.SendRequest(d.context(), &paxos.Propose{Value: []byte(value)},
		func(*paxos.ProposeReply, error) {})
}

// runHyParViewChurn brings a 12-node HyParView overlay to convergence,
// isolates a quarter of the nodes (the sim's "kill"), and lets the
// survivors detect the failures and reconverge. Exercises the membership
// lens: active/passive views, shuffles, and isolate faults.
func runHyParViewChurn(t *cliTB, out io.Writer, seed int64) {
	const n = 12
	const survivors = 9
	set := &sampleSet{}
	sim := prototest.NewSim(t,
		prototest.WithSeed(seed),
		prototest.WithTrace(out),
		prototest.WithTraceStateEvery(60),
		prototest.WithTraceSampler(set.sampler()),
	)

	for i := range n {
		self := nodeHost(i)
		// Chain the bootstrap contacts so the overlay has to grow itself.
		var contacts []transport.Host
		if i > 0 {
			contacts = []transport.Host{nodeHost(i - 1)}
		}
		hv := hyparview.New(self, hyparview.Config{
			Contacts:        contacts,
			ShuffleInterval: 2 * time.Second,
			JoinInterval:    1 * time.Second,
		})
		probe := hyparviewProbe(self, 200*time.Millisecond)
		set.add(probe)
		sim.Node(self, hv, probe)
	}

	// Converge the healthy overlay.
	sim.Run(20 * time.Second)
	// Kill the last quarter.
	for i := survivors; i < n; i++ {
		sim.Mesh().Isolate(nodeHost(i))
	}
	// Let the survivors reconverge without the dead nodes.
	sim.Run(30 * time.Second)
}

// runBroadcast stacks Plumtree over HyParView on 8 nodes (the flagship
// demo's protocol pair), grows the overlay, then originates a handful of
// broadcasts from one node and lets them disseminate over the epidemic
// tree. Exercises the topology and broadcast-tree lenses: eager/lazy peer
// sets and message fan-out.
func runBroadcast(t *cliTB, out io.Writer, seed int64) {
	const n = 8
	set := &sampleSet{}
	sim := prototest.NewSim(t,
		prototest.WithSeed(seed),
		prototest.WithTrace(out),
		prototest.WithTraceStateEvery(80),
		prototest.WithTraceSampler(set.sampler()),
	)

	drivers := make([]*driver, n)
	for i := range n {
		self := nodeHost(i)
		var contacts []transport.Host
		if i > 0 {
			contacts = []transport.Host{nodeHost(i - 1)}
		}
		hv := hyparview.New(self, hyparview.Config{
			Contacts:        contacts,
			ShuffleInterval: 2 * time.Second,
			JoinInterval:    1 * time.Second,
		})
		pt := plumtree.New(self, plumtree.Config{})
		drivers[i] = &driver{}
		probe := plumtreeProbe(self, 200*time.Millisecond)
		set.add(probe)
		sim.Node(self, hv, pt, drivers[i], probe)
	}

	// Act 1 — grow the overlay, then converge the tree: Plumtree starts
	// all-eager and only PRUNEs on duplicate receipt, so the spanning
	// tree emerges over a warmup batch of broadcasts. Without this the
	// trace ends mid-convergence and the tree lens (truthfully) shows a
	// mesh. A converged tree delivers each broadcast exactly n-1 times.
	sim.Run(15 * time.Second)
	for i := range 14 {
		broadcast(drivers[0], fmt.Sprintf("warmup-%d", i))
		sim.Run(400 * time.Millisecond)
	}

	// Act 2 — one lossy link so the trace shows drops and GRAFT repair.
	sim.Mesh().SetLoss(nodeHost(0), nodeHost(1), 0.30)
	for i := range 5 {
		broadcast(drivers[0], fmt.Sprintf("payload-%d", i))
		sim.Run(500 * time.Millisecond)
	}
	sim.Mesh().SetLoss(nodeHost(0), nodeHost(1), 0) // clear

	// Act 3 — clean broadcasts over the converged (possibly re-grafted)
	// tree, so the trace ends on the steady state the lens should show.
	for i := range 3 {
		broadcast(drivers[0], fmt.Sprintf("steady-%d", i))
		sim.Run(500 * time.Millisecond)
	}
	sim.Run(2 * time.Second)
}

func broadcast(d *driver, payload string) {
	protorun.SendRequest(d.context(), &plumtree.Broadcast{Payload: []byte(payload)},
		func(*plumtree.BroadcastAck, error) {})
}
