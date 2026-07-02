package plumtree

import (
	"fmt"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/prototest"
)

// TestPlumtree_BroadcastDeliversOnce brings up a 20-node plumtree-over-
// hyparview stack and asserts a broadcast reaches every node exactly once
// (no duplicate deliveries).
func TestPlumtree_BroadcastDeliversOnce(t *testing.T) {
	const n = 20
	sim := prototest.NewSim(t, prototest.WithSeed(0x71EE))
	nodes := buildStack(t, sim, n)

	// Let the membership overlay converge and the tree form.
	sim.Run(40 * time.Second)

	nodes[0].bcast.broadcast([]byte("hello tree"))
	id := MessageID{Origin: ptHost(0), Seq: 1}
	ok := sim.RunUntil(func() bool { return allDelivered(nodes, id) }, 30*time.Second)
	if !ok {
		missing := 0
		for i, nd := range nodes {
			if d := nd.rec.deliveries(id); d != 1 {
				t.Logf("node %d delivered %d times", i, d)
				missing++
			}
		}
		t.Fatalf("broadcast not delivered exactly once everywhere (%d nodes off)", missing)
	}

	// Exactly-once: no node delivered it more than once, and totals match.
	for i, nd := range nodes {
		if d := nd.rec.deliveries(id); d != 1 {
			t.Errorf("node %d delivered %d times, want 1", i, d)
		}
		if tot := nd.rec.total(); tot != 1 {
			t.Errorf("node %d total deliveries %d, want 1", i, tot)
		}
	}
}

// TestPlumtree_TreeConvergesLowDuplicates asserts the eager-link graph
// converges toward a spanning tree: after a warmup of broadcasts prunes
// the redundant edges, a subsequent batch of broadcasts produces a
// bounded (low) number of duplicate receipts across the whole cluster.
func TestPlumtree_TreeConvergesLowDuplicates(t *testing.T) {
	const n = 20
	sim := prototest.NewSim(t, prototest.WithSeed(0x7EE2))
	nodes := buildStack(t, sim, n)

	sim.Run(40 * time.Second)

	// Warmup: several broadcasts to let PRUNE collapse redundant edges.
	const warmup = 5
	for k := range warmup {
		nodes[k%n].bcast.broadcast([]byte(fmt.Sprintf("warmup-%d", k)))
		sim.Run(5 * time.Second)
	}

	dupBefore := totalDuplicates(nodes)

	// Measured batch: broadcasts after the tree has settled.
	const batch = 10
	for k := range batch {
		src := nodes[k%n]
		src.bcast.broadcast([]byte(fmt.Sprintf("measured-%d", k)))
		sim.Run(5 * time.Second)
	}
	dupAfter := totalDuplicates(nodes)

	dupsInBatch := dupAfter - dupBefore
	// On a converged spanning tree a broadcast produces ~0 duplicates. We
	// allow a small slack (< 1 duplicate per broadcast per node would be
	// n*batch = 200; a converged tree is far below that). The bound proves
	// PRUNE is doing its job versus an un-pruned eager graph.
	limit := batch // < 1 duplicate per broadcast on average across the cluster
	if dupsInBatch > limit {
		t.Fatalf("too many duplicates after tree convergence: %d in %d broadcasts (limit %d)",
			dupsInBatch, batch, limit)
	}
	t.Logf("duplicates in %d post-convergence broadcasts across %d nodes: %d", batch, n, dupsInBatch)

	// Sanity: every measured broadcast still reached everyone exactly once.
	for k := range batch {
		id := MessageID{Origin: ptHost(k % n), Seq: seqFor(k, n)}
		for i, nd := range nodes {
			if d := nd.rec.deliveries(id); d != 1 {
				t.Errorf("measured broadcast %v: node %d delivered %d times", id, i, d)
			}
		}
	}
}

// seqFor computes the plumtree sequence number of the k-th measured
// broadcast (0-based) given round-robin origination over n nodes with a
// preceding warmup of 5. Node k%n originates; its seq is its total
// broadcast count so far (warmup uses k%n too).
func seqFor(k, n int) uint64 {
	origin := k % n
	seq := uint64(0)
	// Warmup broadcasts 0..4 used node (w % n) as origin.
	for w := range 5 {
		if w%n == origin {
			seq++
		}
	}
	for w := 0; w <= k; w++ {
		if w%n == origin {
			seq++
		}
	}
	return seq
}

func totalDuplicates(nodes []*ptNode) int {
	sum := 0
	for _, n := range nodes {
		sum += n.stats.stats().Duplicates
	}
	return sum
}
