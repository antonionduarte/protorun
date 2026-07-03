package paxos

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/protorun"
	"github.com/antonionduarte/protorun/pkg/prototest"
	"github.com/antonionduarte/protorun/pkg/transport"
)

// paxosHost builds the i-th synod member with a readable address. Ports
// stay below 7400 so the package can run in parallel with the multi-runtime
// suites (see CLAUDE.md testing notes).
func paxosHost(i int) transport.Host { return transport.NewHost(7200+i, fmt.Sprintf("10.8.0.%d", i)) }

// clusterConfig builds the Config for member `self` of an n-node synod.
// Retry timings are compressed relative to real-world numbers (they run on
// the virtual clock) but stay comfortably above a message round-trip so a
// healthy round is not cut short.
func clusterConfig(self, n int) Config {
	var peers []transport.Host
	for j := range n {
		if j != self {
			peers = append(peers, paxosHost(j))
		}
	}
	return Config{
		Peers:             peers,
		RetryTimeoutMin:   120 * time.Millisecond,
		RetryTimeoutMax:   260 * time.Millisecond,
		ReconnectInterval: 50 * time.Millisecond,
	}
}

// decidedRec subscribes to paxos.Decided on its own event loop and records
// the ordered sequence of decisions for its node. Read by tests at
// quiescent points to assert Agreement and Integrity.
type decidedRec struct {
	mu  sync.Mutex
	obs []Decided
}

func (d *decidedRec) Start(ctx protorun.ProtocolContext) {
	protorun.SubscribeNotification(ctx, func(ev Decided) {
		d.mu.Lock()
		d.obs = append(d.obs, ev)
		d.mu.Unlock()
	})
}
func (*decidedRec) Init(protorun.ProtocolContext) {}

func (d *decidedRec) decisions() []Decided {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]Decided(nil), d.obs...)
}

// value returns the decided value (or "" and false if this node has not
// decided). Also reports whether MORE than one Decided was seen, which is
// an Integrity violation.
func (d *decidedRec) value() (string, bool, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.obs) == 0 {
		return "", false, false
	}
	return string(d.obs[0].Value), true, len(d.obs) > 1
}

func (d *decidedRec) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.obs)
}

// control is a probe protocol that drives Propose and polls DebugState so
// tests can act on and observe a node from the test goroutine (both are
// supported cross-protocol IPC paths). It caches the most recent DebugState,
// refreshed on a poll timer.
type control struct {
	ctx protorun.ProtocolContext

	mu        sync.Mutex
	last      DebugStateReply
	haveState bool
	lastErr   error
	haveErr   bool
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

// propose issues a Propose and records the resulting reply/error. Call from
// the test goroutine, then advance the sim.
func (c *control) propose(value string) {
	protorun.SendRequest(c.ctx, &Propose{Value: []byte(value)}, func(_ *ProposeReply, err error) {
		c.mu.Lock()
		c.lastErr, c.haveErr = err, true
		c.mu.Unlock()
	})
}

func (c *control) state() (DebugStateReply, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last, c.haveState
}

func (c *control) proposeErr() (error, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastErr, c.haveErr
}

// paxosNode bundles the per-node test handles.
type paxosNode struct {
	host    transport.Host
	proto   *Protocol
	decided *decidedRec
	ctrl    *control
}

// buildCluster wires an n-node Paxos synod onto sim, each node running the
// paxos protocol plus the two test probes.
func buildCluster(t *testing.T, sim *prototest.Sim, n int) []*paxosNode {
	t.Helper()
	nodes := make([]*paxosNode, n)
	for i := range n {
		pr := New(paxosHost(i), clusterConfig(i, n))
		dr := &decidedRec{}
		ct := &control{}
		nodes[i] = &paxosNode{host: paxosHost(i), proto: pr, decided: dr, ctrl: ct}
		sim.Node(paxosHost(i), pr, dr, ct)
	}
	return nodes
}

// --- observation helpers -----------------------------------------------

// allDecided reports whether every node has published a Decided.
func allDecided(nodes []*paxosNode) bool {
	for _, nd := range nodes {
		if nd.decided.count() == 0 {
			return false
		}
	}
	return true
}

// assertAgreement checks the global Agreement + Integrity invariants: every
// node that decided agrees on the value, and no node decided more than
// once. Returns the agreed value (empty if nobody decided).
func assertAgreement(t *testing.T, nodes []*paxosNode) string {
	t.Helper()
	var agreed string
	haveAgreed := false
	for i, nd := range nodes {
		v, decided, dup := nd.decided.value()
		if dup {
			t.Fatalf("Integrity violated: node %d published Decided more than once (%d times)",
				i, nd.decided.count())
		}
		if !decided {
			continue
		}
		if !haveAgreed {
			agreed, haveAgreed = v, true
			continue
		}
		if v != agreed {
			t.Fatalf("Agreement violated: node %d decided %q, another node decided %q", i, v, agreed)
		}
	}
	return agreed
}

// proposedValues is the set of values legally proposed in a scenario; used
// to assert Integrity (a decided value must have been proposed by someone).
func assertDecidedWasProposed(t *testing.T, nodes []*paxosNode, proposed map[string]bool) {
	t.Helper()
	for i, nd := range nodes {
		if v, decided, _ := nd.decided.value(); decided && !proposed[v] {
			t.Fatalf("Integrity violated: node %d decided %q, which was never proposed", i, v)
		}
	}
}
