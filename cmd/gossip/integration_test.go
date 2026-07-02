package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antonionduarte/protorun"
	"github.com/antonionduarte/protorun/cmd/gossip/gossip"
	staticmembership "github.com/antonionduarte/protorun/cmd/gossip/membership"
	"github.com/antonionduarte/protorun/protocols/membership"
	"github.com/antonionduarte/protorun/transport"
)

const (
	nodeCount        = 10
	expectedViewSize = 3 // ring (i+/-1) + chord (i+/-5) -> 3 unique neighbors per node
	convergeTimeout  = 5 * time.Second
	deliverTimeout   = 2 * time.Second
)

// nextBasePort hands out fresh port ranges so concurrent / repeated
// test runs don't collide with TCP TIME_WAIT.
var nextBasePort int32 = 7400

func reservePorts(n int) int {
	return int(atomic.AddInt32(&nextBasePort, int32(n))) - n
}

// recorder accumulates per-node delivery counts keyed by payload string.
type recorder struct {
	mu     sync.Mutex
	counts map[string]int
}

func newRecorder() *recorder { return &recorder{counts: make(map[string]int)} }

func (r *recorder) record(b []byte) {
	r.mu.Lock()
	r.counts[string(b)]++
	r.mu.Unlock()
}

func (r *recorder) count(payload string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[payload]
}

// viewWatcher is a test harness protocol that subscribes to
// membership.ViewChanged and tracks the running peer count for
// convergence assertions. The lock lives here at the test boundary,
// so the production membership and gossip protocols can stay
// lock-free with all state owned by their event loops.
type viewWatcher struct {
	mu   sync.Mutex
	size int
}

func (w *viewWatcher) Start(ctx protorun.ProtocolContext) {
	protorun.SubscribeNotification(ctx, func(membership.NeighborUp) {
		w.mu.Lock()
		w.size++
		w.mu.Unlock()
	})
	protorun.SubscribeNotification(ctx, func(membership.NeighborDown) {
		w.mu.Lock()
		w.size--
		w.mu.Unlock()
	})
}

func (*viewWatcher) Init(_ protorun.ProtocolContext) {}

func (w *viewWatcher) Size() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

// triggerer is a test harness protocol whose only job is to expose a
// goroutine-safe Trigger method that the test goroutine can call.
// Trigger uses protorun.SendRequest under the hood, which is the same
// path peer protocols on the runtime would use to originate a
// broadcast. This keeps "cross-protocol coordination is IPC" as the
// single rule, including for the test driver.
type triggerer struct {
	ctx protorun.ProtocolContext
}

func (t *triggerer) Start(ctx protorun.ProtocolContext) { t.ctx = ctx }
func (*triggerer) Init(_ protorun.ProtocolContext)      {}

func (t *triggerer) Trigger(payload []byte) {
	protorun.SendRequest(t.ctx, &gossip.TriggerBroadcast{Payload: payload},
		func(_ *gossip.BroadcastAck, _ error) {})
}

// node bundles the per-instance handles a test wants to interact with.
type node struct {
	rt      *protorun.Runtime
	trigger *triggerer
	watcher *viewWatcher
	rec     *recorder
}

// startCluster builds nodeCount runtimes on a fresh port range, wires
// each with membership(contacts(i+1, i+5)) + gossip + viewWatcher,
// starts them in parallel, waits for membership convergence, and
// registers Cancel for cleanup.
func startCluster(t *testing.T) []*node {
	t.Helper()
	base := reservePorts(nodeCount)
	nodes := make([]*node, nodeCount)
	for i := range nodeCount {
		nodes[i] = buildNode(t, base, i)
	}
	for _, n := range nodes {
		go func() { _ = n.rt.Run() }()
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.rt.Cancel()
		}
	})
	waitForConvergence(t, nodes)
	return nodes
}

func buildNode(t *testing.T, basePort, i int) *node {
	return buildNodeOf(t, basePort, i, nodeCount)
}

func buildNodeOf(t *testing.T, basePort, i, total int) *node {
	t.Helper()
	self := transport.NewHost(basePort+i, "127.0.0.1")
	contacts := []transport.Host{
		transport.NewHost(basePort+(i+1)%total, "127.0.0.1"),
		transport.NewHost(basePort+(i+5)%total, "127.0.0.1"),
	}

	rec := newRecorder()
	watcher := &viewWatcher{}
	trigger := &triggerer{}
	m := staticmembership.New(contacts)
	g := gossip.New(rec.record)

	rt := protorun.New(self, protorun.WithTCPTransport(context.Background()))
	rt.Register(m)
	rt.Register(g)
	rt.Register(watcher)
	rt.Register(trigger)

	return &node{rt: rt, trigger: trigger, watcher: watcher, rec: rec}
}

func waitForConvergence(t *testing.T, nodes []*node) {
	t.Helper()
	deadline := time.Now().Add(convergeTimeout)
	for time.Now().Before(deadline) {
		if everyNodeHasView(nodes, expectedViewSize) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("cluster did not converge to view size %d within %v: %s",
		expectedViewSize, convergeTimeout, viewReport(nodes))
}

func everyNodeHasView(nodes []*node, minSize int) bool {
	for _, n := range nodes {
		if n.watcher.Size() < minSize {
			return false
		}
	}
	return true
}

func viewReport(nodes []*node) string {
	var b strings.Builder
	for i, n := range nodes {
		fmt.Fprintf(&b, "node%d=%d ", i, n.watcher.Size())
	}
	return strings.TrimSpace(b.String())
}

// waitForDelivery polls until every node's recorder has delivered
// each expected payload exactly once, or the deadline expires.
func waitForDelivery(nodes []*node, payloads []string) bool {
	deadline := time.Now().Add(deliverTimeout)
	for time.Now().Before(deadline) {
		if allDeliveredOnce(nodes, payloads) {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func allDeliveredOnce(nodes []*node, payloads []string) bool {
	for _, n := range nodes {
		for _, p := range payloads {
			if n.rec.count(p) != 1 {
				return false
			}
		}
	}
	return true
}

func nodesMissing(nodes []*node, payload string, expected int) []int {
	var missing []int
	for i, n := range nodes {
		if n.rec.count(payload) != expected {
			missing = append(missing, i)
		}
	}
	return missing
}

// TestGossip_TenNodes_SingleBroadcast asserts that one Broadcast from
// any node reaches every node in the 10-node cluster exactly once.
func TestGossip_TenNodes_SingleBroadcast(t *testing.T) {
	nodes := startCluster(t)

	nodes[0].trigger.Trigger([]byte("hello"))

	if !waitForDelivery(nodes, []string{"hello"}) {
		t.Fatalf("nodes %v never received \"hello\" exactly once",
			nodesMissing(nodes, "hello", 1))
	}
}

// TestGossip_TenNodes_ConcurrentBroadcasts asserts that two
// near-simultaneous Broadcasts from different nodes each reach every
// node exactly once. Catches seen-set races and cross-broadcast
// interference.
func TestGossip_TenNodes_ConcurrentBroadcasts(t *testing.T) {
	nodes := startCluster(t)

	var wg, ready sync.WaitGroup
	start := make(chan struct{})

	wg.Add(2)
	ready.Add(2)
	go func() {
		defer wg.Done()
		ready.Done()
		<-start
		nodes[0].trigger.Trigger([]byte("alpha"))
	}()
	go func() {
		defer wg.Done()
		ready.Done()
		<-start
		nodes[5].trigger.Trigger([]byte("beta"))
	}()

	ready.Wait()
	close(start)
	wg.Wait()

	if !waitForDelivery(nodes, []string{"alpha", "beta"}) {
		t.Fatalf("incomplete delivery: missing alpha=%v missing beta=%v",
			nodesMissing(nodes, "alpha", 1), nodesMissing(nodes, "beta", 1))
	}
}
