package plumtree

import (
	"testing"
	"time"

	"github.com/antonionduarte/protorun/protocols/hyparview"
	"github.com/antonionduarte/protorun/prototest"
	"github.com/antonionduarte/protorun/transport"
)

// buildTwoHalves wires 6 nodes as two triangles A={0,1,2} and B={3,4,5},
// initially joined by a single cross contact (node 3 -> node 0). Each half
// stays internally connected when the cross links are cut. ActiveSize (4)
// exceeds the per-half size (3), so every node always wants more active
// peers and keeps promoting from its passive view — which is what
// reconnects the halves once a partition heals.
func buildTwoHalves(t *testing.T, sim *prototest.Sim) []*ptNode {
	t.Helper()
	contacts := map[int][]transport.Host{
		0: nil,
		1: {ptHost(0)},
		2: {ptHost(0)},
		3: {ptHost(0)}, // cross-link A<->B, plus root of B
		4: {ptHost(3)},
		5: {ptHost(3)},
	}
	nodes := make([]*ptNode, 6)
	for i := range 6 {
		cfg := hvConfig(contacts[i]...)
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

// TestPlumtree_PartitionHeal checks that broadcasts during a partition
// reach only their side, and that after the partition heals and HyParView
// re-establishes the cross links, GRAFT repairs the tree so a SUBSEQUENT
// broadcast is delivered everywhere. Messages sent during the partition
// are NOT replayed to the far side (documented Plumtree behaviour: it is
// not a full anti-entropy protocol).
func TestPlumtree_PartitionHeal(t *testing.T) {
	sim := prototest.NewSim(t, prototest.WithSeed(0x9A17))
	nodes := buildTwoHalves(t, sim)
	sideA := []int{0, 1, 2}
	sideB := []int{3, 4, 5}

	// Converge the joined overlay.
	sim.Run(40 * time.Second)

	// Partition A | B: cut every cross link.
	for _, a := range sideA {
		for _, b := range sideB {
			sim.Mesh().Cut(ptHost(a), ptHost(b))
		}
	}
	sim.Run(20 * time.Second) // disconnects propagate; each side re-forms its tree

	// A broadcast on side A during the partition.
	nodes[0].bcast.broadcast([]byte("A-only"))
	idA := MessageID{Origin: ptHost(0), Seq: 1}
	sim.Run(10 * time.Second)
	for _, i := range sideA {
		if nodes[i].rec.deliveries(idA) != 1 {
			t.Errorf("side-A node %d should have delivered the A-only broadcast", i)
		}
	}
	for _, i := range sideB {
		if nodes[i].rec.deliveries(idA) != 0 {
			t.Errorf("side-B node %d must NOT receive a broadcast across the partition", i)
		}
	}

	// A broadcast on side B during the partition, symmetric check.
	nodes[3].bcast.broadcast([]byte("B-only"))
	idB := MessageID{Origin: ptHost(3), Seq: 1}
	sim.Run(10 * time.Second)
	for _, i := range sideB {
		if nodes[i].rec.deliveries(idB) != 1 {
			t.Errorf("side-B node %d should have delivered the B-only broadcast", i)
		}
	}
	for _, i := range sideA {
		if nodes[i].rec.deliveries(idB) != 0 {
			t.Errorf("side-A node %d must NOT receive the B-only broadcast", i)
		}
	}

	// Heal: cross links reachable again. HyParView reconnects via its
	// promotion loop (no auto-reconnect from the mesh).
	for _, a := range sideA {
		for _, b := range sideB {
			sim.Mesh().Heal(ptHost(a), ptHost(b))
		}
	}
	// Give the overlay time to rebuild cross sessions -> NeighborUp ->
	// eager links.
	sim.Run(40 * time.Second)

	// A SUBSEQUENT broadcast must now reach everyone (tree repaired).
	nodes[0].bcast.broadcast([]byte("after-heal"))
	idZ := MessageID{Origin: ptHost(0), Seq: 2}
	ok := sim.RunUntil(func() bool { return allDelivered(nodes, idZ) }, 40*time.Second)
	if !ok {
		for i, nd := range nodes {
			t.Logf("node %d delivered after-heal %d times", i, nd.rec.deliveries(idZ))
		}
		t.Fatalf("post-heal broadcast did not reach every node")
	}

	// The mid-partition broadcasts were never replayed across the heal.
	for _, i := range sideB {
		if nodes[i].rec.deliveries(idA) != 0 {
			t.Errorf("mid-partition A-only must not be replayed to side-B node %d after heal", i)
		}
	}
}
