package raft

import (
	"fmt"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/prototest"
)

// Invariant guarded here: Leader Completeness (§5.4) — an entry committed
// under one leader is present in every future leader's log — plus recovery
// convergence: a deposed leader that rejoins steps down and converges to
// the same applied sequence.

func TestRaft_LeaderCrash_NewLeaderKeepsCommitted(t *testing.T) {
	sim := prototest.NewSim(t, prototest.WithSeed(0xC7A54))
	nodes := buildCluster(t, sim, 5)

	oldLeader := waitForLeader(t, sim, nodes, nil)

	// Commit a batch under the original leader and let it apply everywhere.
	var committed []string
	for i := range 5 {
		c := fmt.Sprintf("pre-%d", i)
		committed = append(committed, c)
		nodes[oldLeader].ctrl.propose(c)
		sim.Run(30 * time.Millisecond)
	}
	ok := sim.RunUntil(func() bool {
		for _, nd := range nodes {
			if nd.applied.len() < len(committed) {
				return false
			}
		}
		return true
	}, 10*time.Second)
	if !ok {
		t.Fatalf("pre-crash batch did not commit everywhere")
	}

	// Crash the leader: isolate it from the whole cluster.
	sim.Mesh().Isolate(nodes[oldLeader].host)

	// The remaining four must elect a new leader.
	var rest []int
	for i := range nodes {
		if i != oldLeader {
			rest = append(rest, i)
		}
	}
	sim.Run(1 * time.Second) // let the old leader's followers time out
	newLeader := waitForLeader(t, sim, nodes, rest)
	if newLeader == oldLeader {
		t.Fatalf("isolated leader %d still reported as leader", oldLeader)
	}
	t.Logf("new leader after crash: node %d (was %d)", newLeader, oldLeader)

	// Every committed-before-crash entry must survive on the new leader
	// (Leader Completeness): it won the election, so its log contains them.
	for _, c := range committed {
		if !nodes[newLeader].applied.contains(c) {
			t.Fatalf("new leader %d lost committed entry %q", newLeader, c)
		}
	}

	// Proposals continue on the new leader and commit across the majority.
	var post []string
	for i := range 4 {
		c := fmt.Sprintf("post-%d", i)
		post = append(post, c)
		nodes[newLeader].ctrl.propose(c)
		sim.Run(30 * time.Millisecond)
	}
	ok = sim.RunUntil(func() bool {
		for _, i := range rest {
			if nodes[i].applied.len() < len(committed)+len(post) {
				return false
			}
		}
		return true
	}, 10*time.Second)
	if !ok {
		t.Fatalf("post-crash proposals did not commit across the majority")
	}

	// Heal the old leader: it rejoins, gives up its stale term, and
	// converges to the same applied sequence as the rest. (Leadership may
	// legitimately move again — a rejoining node with an up-to-date log can
	// win a later election — so we assert convergence and single
	// leadership, not that this specific node stays a follower.)
	healAll(sim, nodes, nodes[oldLeader].host)
	want := len(committed) + len(post)
	ok = sim.RunUntil(func() bool {
		return nodes[oldLeader].applied.len() >= want
	}, 15*time.Second)
	if !ok {
		t.Fatalf("recovered node did not catch up: applied %d, want %d",
			nodes[oldLeader].applied.len(), want)
	}
	sim.Run(2 * time.Second) // let leadership settle

	leaders := 0
	for _, nd := range nodes {
		if st, ok := nd.ctrl.state(); ok && st.Role == Leader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("expected exactly one leader after heal, found %d", leaders)
	}

	assertConsistentApplied(t, nodes)
	assertElectionSafety(t, nodes)

	// The full expected sequence, identical on every node.
	wantSeq := append(append([]string{}, committed...), post...)
	for i, nd := range nodes {
		seq := nd.applied.sequence()
		if len(seq) < len(wantSeq) {
			t.Fatalf("node %d applied %d entries, want >= %d", i, len(seq), len(wantSeq))
		}
		for k, c := range wantSeq {
			if seq[k] != c {
				t.Fatalf("node %d applied[%d]=%q, want %q", i, k, seq[k], c)
			}
		}
	}
}
