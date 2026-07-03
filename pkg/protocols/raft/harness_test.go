package raft

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/protorun"
	"github.com/antonionduarte/protorun/pkg/prototest"
	"github.com/antonionduarte/protorun/pkg/transport"
)

// raftHost builds the i-th cluster member with a readable address. Ports
// stay below 7400 so the package can run in parallel with the multi-
// runtime suites (see CLAUDE.md testing notes).
func raftHost(i int) transport.Host { return transport.NewHost(7100+i, fmt.Sprintf("10.7.0.%d", i)) }

// clusterConfig builds the Config for member `self` of an n-node cluster.
// Timings are compressed relative to the paper's real-world numbers (they
// run on the virtual clock) but keep heartbeat << election timeout so a
// live leader is stable.
func clusterConfig(self, n int) Config {
	var peers []transport.Host
	for j := range n {
		if j != self {
			peers = append(peers, raftHost(j))
		}
	}
	return Config{
		Peers:              peers,
		HeartbeatInterval:  30 * time.Millisecond,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		ReconnectInterval:  50 * time.Millisecond,
	}
}

// appliedRec subscribes to raft.Applied on its own event loop and records
// the ordered sequence of applied entries for its node. Read by tests at
// quiescent points to assert State Machine Safety.
type appliedRec struct {
	mu      sync.Mutex
	applied []Applied
}

func (a *appliedRec) Start(ctx protorun.ProtocolContext) {
	protorun.SubscribeNotification(ctx, func(ev Applied) {
		a.mu.Lock()
		a.applied = append(a.applied, ev)
		a.mu.Unlock()
	})
}
func (*appliedRec) Init(protorun.ProtocolContext) {}

// sequence renders the applied commands as comparable strings.
func (a *appliedRec) sequence() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, len(a.applied))
	for i, e := range a.applied {
		out[i] = string(e.Command)
	}
	return out
}

func (a *appliedRec) len() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.applied)
}

// contains reports whether cmd was ever applied on this node.
func (a *appliedRec) contains(cmd string) bool {
	return slices.Contains(a.sequence(), cmd)
}

// leaderRec subscribes to raft.LeaderChanged and records every observed
// (term, leader) pair for this node. Collected across nodes, it witnesses
// Election Safety.
type leaderRec struct {
	mu  sync.Mutex
	obs []LeaderChanged
}

func (l *leaderRec) Start(ctx protorun.ProtocolContext) {
	protorun.SubscribeNotification(ctx, func(ev LeaderChanged) {
		l.mu.Lock()
		l.obs = append(l.obs, ev)
		l.mu.Unlock()
	})
}
func (*leaderRec) Init(protorun.ProtocolContext) {}

func (l *leaderRec) observations() []LeaderChanged {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]LeaderChanged(nil), l.obs...)
}

// control is a probe protocol that drives Propose and polls DebugState so
// tests can act on and observe the Raft node from the test goroutine
// (both are supported cross-protocol IPC paths). It caches the most
// recent DebugState, refreshed on a poll timer.
type control struct {
	ctx protorun.ProtocolContext

	mu        sync.Mutex
	last      DebugStateReply
	haveState bool
	lastErr   error
}

func (c *control) Start(ctx protorun.ProtocolContext) { c.ctx = ctx }
func (c *control) Init(ctx protorun.ProtocolContext) {
	poll := func() {
		protorun.SendRequest(ctx, &DebugState{}, func(rep *DebugStateReply, err error) {
			c.mu.Lock()
			if err == nil {
				c.last, c.haveState = *rep, true
			}
			c.mu.Unlock()
		})
	}
	poll()
	ctx.Every(20*time.Millisecond, poll)
}

// propose issues a Propose and records the resulting reply/error. Call
// from the test goroutine, then advance the sim.
func (c *control) propose(cmd string) {
	protorun.SendRequest(c.ctx, &Propose{Command: []byte(cmd)}, func(_ *ProposeReply, err error) {
		c.mu.Lock()
		c.lastErr = err
		c.mu.Unlock()
	})
}

func (c *control) state() (DebugStateReply, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last, c.haveState
}

func (c *control) proposeErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastErr
}

// raftNode bundles the per-node test handles.
type raftNode struct {
	host    transport.Host
	proto   *Protocol
	applied *appliedRec
	leaders *leaderRec
	ctrl    *control
}

// buildCluster wires an n-node Raft cluster onto sim, each node running
// the raft protocol plus the three test probes.
func buildCluster(t *testing.T, sim *prototest.Sim, n int) []*raftNode {
	t.Helper()
	nodes := make([]*raftNode, n)
	for i := range n {
		pr := New(raftHost(i), clusterConfig(i, n))
		ar := &appliedRec{}
		lr := &leaderRec{}
		ct := &control{}
		nodes[i] = &raftNode{host: raftHost(i), proto: pr, applied: ar, leaders: lr, ctrl: ct}
		sim.Node(raftHost(i), pr, ar, lr, ct)
	}
	return nodes
}

// --- observation helpers -----------------------------------------------

// leaderIndex returns the index of the node currently reporting itself as
// leader with the highest term, or -1 if none does.
func leaderIndex(nodes []*raftNode) int {
	best, bestTerm := -1, uint64(0)
	for i, nd := range nodes {
		st, ok := nd.ctrl.state()
		if !ok || st.Role != Leader {
			continue
		}
		if best == -1 || st.Term > bestTerm {
			best, bestTerm = i, st.Term
		}
	}
	return best
}

// waitForLeader runs the sim until some reachable node reports itself
// leader, failing the test if none appears within the horizon.
func waitForLeader(t *testing.T, sim *prototest.Sim, nodes []*raftNode, among []int) int {
	t.Helper()
	ok := sim.RunUntil(func() bool { return leaderAmong(nodes, among) != -1 }, 15*time.Second)
	if !ok {
		t.Fatalf("no leader elected within horizon")
	}
	return leaderAmong(nodes, among)
}

// leaderAmong returns the index (restricted to the given subset, or all
// nodes if among is nil) of a node reporting itself leader, else -1.
func leaderAmong(nodes []*raftNode, among []int) int {
	inSet := func(i int) bool {
		return among == nil || slices.Contains(among, i)
	}
	best, bestTerm := -1, uint64(0)
	for i, nd := range nodes {
		if !inSet(i) {
			continue
		}
		st, ok := nd.ctrl.state()
		if !ok || st.Role != Leader {
			continue
		}
		if best == -1 || st.Term > bestTerm {
			best, bestTerm = i, st.Term
		}
	}
	return best
}

// assertElectionSafety checks the global Election Safety invariant across
// every node's LeaderChanged stream: for any term, all observers that saw
// a leader for that term saw the SAME leader.
func assertElectionSafety(t *testing.T, nodes []*raftNode) {
	t.Helper()
	leaderByTerm := make(map[uint64]transport.Host)
	for _, nd := range nodes {
		for _, ev := range nd.leaders.observations() {
			if prev, ok := leaderByTerm[ev.Term]; ok && prev != ev.Leader {
				t.Fatalf("Election Safety violated: term %d had leaders %s and %s",
					ev.Term, prev.String(), ev.Leader.String())
			}
			leaderByTerm[ev.Term] = ev.Leader
		}
	}
}

// assertConsistentApplied checks that no two nodes have divergent applied
// sequences: for every pair, one sequence is a prefix of the other (State
// Machine Safety — nodes may lag but never disagree on applied order).
func assertConsistentApplied(t *testing.T, nodes []*raftNode) {
	t.Helper()
	for i := range nodes {
		for j := i + 1; j < len(nodes); j++ {
			a, b := nodes[i].applied.sequence(), nodes[j].applied.sequence()
			n := min(len(a), len(b))
			for k := range n {
				if a[k] != b[k] {
					t.Fatalf("applied divergence at index %d between node %d (%q) and node %d (%q)",
						k, i, a[k], j, b[k])
				}
			}
		}
	}
}

// healAll restores every cut link touching host h (the inverse of
// Mesh.Isolate).
func healAll(sim *prototest.Sim, nodes []*raftNode, h transport.Host) {
	for _, nd := range nodes {
		if nd.host != h {
			sim.Mesh().Heal(h, nd.host)
		}
	}
}
