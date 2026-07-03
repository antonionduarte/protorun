package raft

import (
	"fmt"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/prototest"
)

// Invariant guarded here (the big one): a minority partition commits
// nothing, and a heal never produces applied-sequence divergence at any
// node. An old leader stranded in a 2-node minority of a 5-node cluster
// accepts proposals but can never commit them; the 3-node majority elects
// a new leader and makes progress; on heal the minority rolls back its
// uncommitted tail and converges. State Machine Safety must hold at every
// observation point.

func TestRaft_Partition_MinorityCommitsNothing(t *testing.T) {
	sim := prototest.NewSim(t, prototest.WithSeed(0x9A27))
	nodes := buildCluster(t, sim, 5)

	oldLeader := waitForLeader(t, sim, nodes, nil)

	// Commit a baseline batch everywhere.
	var base []string
	for i := range 3 {
		c := fmt.Sprintf("base-%d", i)
		base = append(base, c)
		nodes[oldLeader].ctrl.propose(c)
		sim.Run(30 * time.Millisecond)
	}
	ok := sim.RunUntil(func() bool {
		for _, nd := range nodes {
			if nd.applied.len() < len(base) {
				return false
			}
		}
		return true
	}, 10*time.Second)
	if !ok {
		t.Fatalf("baseline batch did not commit everywhere")
	}
	assertConsistentApplied(t, nodes)

	// Partition: old leader + one buddy on the minority side, the other
	// three on the majority side.
	buddy := (oldLeader + 1) % 5
	minoritySet := map[int]bool{oldLeader: true, buddy: true}
	var minority, majority []int
	for i := range nodes {
		if minoritySet[i] {
			minority = append(minority, i)
		} else {
			majority = append(majority, i)
		}
	}
	for _, a := range minority {
		for _, b := range majority {
			sim.Mesh().Cut(nodes[a].host, nodes[b].host)
		}
	}
	sim.Run(1 * time.Second) // disconnects propagate

	// The majority elects a new leader and commits.
	newLeader := waitForLeader(t, sim, nodes, majority)
	t.Logf("majority leader: node %d (old leader %d is in the minority)", newLeader, oldLeader)

	// Proposals to the stranded old leader: it appends optimistically but
	// can never reach a majority, so these must NEVER commit or apply.
	ghosts := []string{"ghost-0", "ghost-1", "ghost-2"}
	for _, g := range ghosts {
		nodes[oldLeader].ctrl.propose(g)
		sim.Run(30 * time.Millisecond)
	}

	// The majority makes real progress.
	var maj []string
	for i := range 4 {
		c := fmt.Sprintf("maj-%d", i)
		maj = append(maj, c)
		nodes[newLeader].ctrl.propose(c)
		sim.Run(30 * time.Millisecond)
	}
	ok = sim.RunUntil(func() bool {
		for _, i := range majority {
			if nodes[i].applied.len() < len(base)+len(maj) {
				return false
			}
		}
		return true
	}, 10*time.Second)
	if !ok {
		t.Fatalf("majority did not commit its batch")
	}

	// During the partition: no ghost has been applied anywhere, and no node
	// diverges.
	assertNoGhostApplied(t, nodes, ghosts)
	assertConsistentApplied(t, nodes)

	// Heal every cut link. The minority reconnects, learns the higher term,
	// rolls back its uncommitted ghost tail, and converges.
	for _, a := range minority {
		for _, b := range majority {
			sim.Mesh().Heal(nodes[a].host, nodes[b].host)
		}
	}
	want := len(base) + len(maj)
	ok = sim.RunUntil(func() bool {
		for _, nd := range nodes {
			if nd.applied.len() < want {
				return false
			}
		}
		return true
	}, 20*time.Second)
	if !ok {
		for i, nd := range nodes {
			t.Logf("node %d applied %d/%d", i, nd.applied.len(), want)
		}
		t.Fatalf("cluster did not converge after heal")
	}
	sim.Run(1 * time.Second)

	// Ghosts were never applied — not before, not during, not after heal.
	assertNoGhostApplied(t, nodes, ghosts)
	assertConsistentApplied(t, nodes)
	assertElectionSafety(t, nodes)

	// Every node ends on the same applied sequence: base then maj.
	wantSeq := append(append([]string{}, base...), maj...)
	for i, nd := range nodes {
		seq := nd.applied.sequence()
		for k, c := range wantSeq {
			if k >= len(seq) || seq[k] != c {
				t.Fatalf("node %d applied[%d]=%v, want %q", i, k, safeIdx(seq, k), c)
			}
		}
	}
}

func assertNoGhostApplied(t *testing.T, nodes []*raftNode, ghosts []string) {
	t.Helper()
	for i, nd := range nodes {
		for _, g := range ghosts {
			if nd.applied.contains(g) {
				t.Fatalf("uncommitted minority entry %q was applied on node %d", g, i)
			}
		}
	}
}

func safeIdx(s []string, k int) string {
	if k < len(s) {
		return s[k]
	}
	return "<missing>"
}
