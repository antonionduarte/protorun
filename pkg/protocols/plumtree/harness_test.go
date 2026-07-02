package plumtree

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/protocols/hyparview"
	"github.com/antonionduarte/protorun/pkg/protorun"
	"github.com/antonionduarte/protorun/pkg/prototest"
	"github.com/antonionduarte/protorun/pkg/transport"
)

func ptHost(i int) transport.Host { return transport.NewHost(6500+i, fmt.Sprintf("10.2.0.%d", i)) }

func hvConfig(contacts ...transport.Host) hyparview.Config {
	return hyparview.Config{
		Contacts:        contacts,
		ActiveSize:      4,
		PassiveSize:     12,
		ARWL:            6,
		PRWL:            3,
		ShuffleActive:   3,
		ShufflePassive:  4,
		ShuffleInterval: 1 * time.Second,
		JoinInterval:    1 * time.Second,
		NeighborTimeout: 2 * time.Second,
	}
}

func ptConfig() Config {
	return Config{
		MissingTimeout:    1 * time.Second,
		GraftRetryTimeout: 500 * time.Millisecond,
		LazyInterval:      100 * time.Millisecond,
		CacheSize:         256,
	}
}

// deliverRec records, per node, how many times each broadcast id was
// Delivered to the app layer. It subscribes to plumtree.Delivered on its
// own event loop and is read by the test at quiescent points.
type deliverRec struct {
	mu    sync.Mutex
	count map[MessageID]int
}

func (d *deliverRec) Start(ctx protorun.ProtocolContext) {
	d.count = make(map[MessageID]int)
	protorun.SubscribeNotification(ctx, func(ev Delivered) {
		d.mu.Lock()
		d.count[ev.ID]++
		d.mu.Unlock()
	})
}
func (*deliverRec) Init(protorun.ProtocolContext) {}

func (d *deliverRec) deliveries(id MessageID) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.count[id]
}

func (d *deliverRec) total() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, c := range d.count {
		n += c
	}
	return n
}

// broadcaster exposes a goroutine-safe way to originate a broadcast via
// the plumtree.Broadcast IPC request (the supported trigger path).
type broadcaster struct {
	ctx  protorun.ProtocolContext
	self transport.Host
}

func (b *broadcaster) Start(ctx protorun.ProtocolContext) { b.ctx = ctx }
func (*broadcaster) Init(protorun.ProtocolContext)        {}

// broadcast originates payload and returns the MessageID it will carry
// (origin = this node, seq = 1-based call count). Call from the test
// goroutine, then advance the sim.
func (b *broadcaster) broadcast(payload []byte) {
	protorun.SendRequest(b.ctx, &Broadcast{Payload: payload}, func(*BroadcastAck, error) {})
}

// statsProbe polls plumtree.DebugStats so a test can read duplicate
// counters at quiescent points.
type statsProbe struct {
	ctx protorun.ProtocolContext

	mu   sync.Mutex
	last DebugStatsReply
}

func (s *statsProbe) Start(ctx protorun.ProtocolContext) { s.ctx = ctx }
func (s *statsProbe) Init(ctx protorun.ProtocolContext) {
	poll := func() {
		protorun.SendRequest(ctx, &DebugStats{}, func(rep *DebugStatsReply, err error) {
			if err != nil {
				return
			}
			s.mu.Lock()
			s.last = *rep
			s.mu.Unlock()
		})
	}
	poll()
	ctx.Every(200*time.Millisecond, poll)
}

func (s *statsProbe) stats() DebugStatsReply {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}

// ptNode bundles the per-node test handles.
type ptNode struct {
	bcast *broadcaster
	rec   *deliverRec
	stats *statsProbe
}

// buildStack wires n nodes, each running hyparview + plumtree + the test
// probes, as a contact chain rooted at node 0.
func buildStack(t *testing.T, sim *prototest.Sim, n int) []*ptNode {
	t.Helper()
	nodes := make([]*ptNode, n)
	for i := range n {
		var cfg hyparview.Config
		if i == 0 {
			cfg = hvConfig()
		} else {
			cfg = hvConfig(ptHost(i - 1))
		}
		hv := hyparview.New(ptHost(i), cfg)
		pt := New(ptHost(i), ptConfig())
		bc := &broadcaster{self: ptHost(i)}
		rec := &deliverRec{}
		sp := &statsProbe{}
		nodes[i] = &ptNode{bcast: bc, rec: rec, stats: sp}
		sim.Node(ptHost(i), hv, pt, rec, bc, sp)
	}
	return nodes
}

// allDelivered reports whether every node delivered id exactly once.
func allDelivered(nodes []*ptNode, id MessageID) bool {
	for _, n := range nodes {
		if n.rec.deliveries(id) != 1 {
			return false
		}
	}
	return true
}
