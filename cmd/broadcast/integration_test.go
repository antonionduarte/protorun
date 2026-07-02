package main

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/protocols/hyparview"
	"github.com/antonionduarte/protorun/pkg/protocols/membership"
	"github.com/antonionduarte/protorun/pkg/protocols/plumtree"
	"github.com/antonionduarte/protorun/pkg/protorun"
	"github.com/antonionduarte/protorun/pkg/transport"
)

// nextBasePort hands out fresh port ranges for this package's runs
// (-count=N). Per the repo convention it stays BELOW 7400: the 7400+ band
// is cmd/gossip's growing atomic, and because packages run in parallel
// under `go test ./...`, fixed ports elsewhere must avoid it. 7150-7199 is
// an unused gap.
var nextBasePort int32 = 7150

func reservePorts(n int) int { return int(atomic.AddInt32(&nextBasePort, int32(n))) - n }

// recorder subscribes to plumtree.Delivered and to the membership
// contract, so a test can wait for the overlay to form and then assert on
// deliveries. All reads happen from the test goroutine under mu.
type recorder struct {
	mu        sync.Mutex
	delivered map[string]int
	neighbors int
}

func (r *recorder) Start(ctx protorun.ProtocolContext) {
	r.delivered = make(map[string]int)
	protorun.SubscribeNotification(ctx, func(ev plumtree.Delivered) {
		r.mu.Lock()
		r.delivered[string(ev.Payload)]++
		r.mu.Unlock()
	})
	protorun.SubscribeNotification(ctx, func(membership.NeighborUp) {
		r.mu.Lock()
		r.neighbors++
		r.mu.Unlock()
	})
	protorun.SubscribeNotification(ctx, func(membership.NeighborDown) {
		r.mu.Lock()
		r.neighbors--
		r.mu.Unlock()
	})
}
func (*recorder) Init(protorun.ProtocolContext) {}

func (r *recorder) neighborCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.neighbors
}

func (r *recorder) count(payload string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.delivered[payload]
}

type testNode struct {
	rt    *protorun.Runtime
	bcast *broadcaster
	rec   *recorder
}

// tcpConfig is a HyParView tuning with short periods so the overlay forms
// quickly over real TCP.
func tcpConfig(contacts ...transport.Host) hyparview.Config {
	return hyparview.Config{
		Contacts:        contacts,
		ActiveSize:      4,
		PassiveSize:     12,
		ShuffleInterval: 500 * time.Millisecond,
		JoinInterval:    500 * time.Millisecond,
		NeighborTimeout: time.Second,
	}
}

// TestBroadcast_ClusterDelivers stands up a 5-node plumtree/hyparview/TCP
// cluster on real sockets, waits for the overlay to form, broadcasts from
// one node, and asserts every node delivers the message exactly once.
func TestBroadcast_ClusterDelivers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real-TCP integration test in -short mode")
	}
	const n = 5
	base := reservePorts(n)

	nodes := make([]*testNode, n)
	for i := range n {
		self := transport.NewHost(base+i, "127.0.0.1")
		var contacts []transport.Host
		if i > 0 {
			contacts = append(contacts, transport.NewHost(base+i-1, "127.0.0.1"))
		}
		bc := &broadcaster{}
		rec := &recorder{}
		rt := protorun.New(self, protorun.WithTCPTransport(context.Background()))
		rt.Register(hyparview.New(self, tcpConfig(contacts...)))
		rt.Register(plumtree.New(self, plumtree.Config{}))
		rt.Register(rec)
		rt.Register(bc)
		nodes[i] = &testNode{rt: rt, bcast: bc, rec: rec}
	}
	for _, nd := range nodes {
		go func() { _ = nd.rt.Run() }()
	}
	t.Cleanup(func() {
		var wg sync.WaitGroup
		for _, nd := range nodes {
			wg.Go(nd.rt.Cancel)
		}
		wg.Wait()
	})

	// Wait for every node to have at least one active-view neighbour. The
	// timeout is generous because under `make test-race` this runs in
	// parallel with the CPU-heavy gossip scale tests.
	if !waitFor(20*time.Second, func() bool {
		for _, nd := range nodes {
			if nd.rec.neighborCount() < 1 {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("overlay did not form: neighbour counts %v", neighborCounts(nodes))
	}

	nodes[0].bcast.broadcast([]byte("hello cluster"))

	if !waitFor(20*time.Second, func() bool {
		for _, nd := range nodes {
			if nd.rec.count("hello cluster") != 1 {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("broadcast not delivered exactly once everywhere: counts %v",
			deliverCounts(nodes, "hello cluster"))
	}
}

func waitFor(timeout time.Duration, pred func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return pred()
}

func neighborCounts(nodes []*testNode) []int {
	out := make([]int, len(nodes))
	for i, nd := range nodes {
		out[i] = nd.rec.neighborCount()
	}
	return out
}

func deliverCounts(nodes []*testNode, payload string) []int {
	out := make([]int, len(nodes))
	for i, nd := range nodes {
		out[i] = nd.rec.count(payload)
	}
	return out
}
