package raft

import (
	"errors"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/prototest"
)

// Invariant guarded here: Election Safety (§5.2) — at most one leader is
// elected per term — plus leader stability: once a leader is established,
// a long quiet period produces no spurious re-elections.

func TestRaft_Election_SingleLeaderPerTerm(t *testing.T) {
	sim := prototest.NewSim(t, prototest.WithSeed(0xE1EC7))
	nodes := buildCluster(t, sim, 5)

	// From cold start, a leader must emerge.
	leader := waitForLeader(t, sim, nodes, nil)
	t.Logf("elected leader: node %d", leader)

	// Let leadership settle and run a long quiet period. A stable leader
	// heartbeats faster than any follower's election timeout, so no new
	// term should be started.
	sim.Run(2 * time.Second)
	stTerm := func() uint64 {
		st, _ := nodes[leader].ctrl.state()
		return st.Term
	}
	termAfterSettle := stTerm()

	sim.Run(10 * time.Second) // long quiet period

	// Leadership is unchanged and no re-election bumped the term.
	if got := leaderIndex(nodes); got != leader {
		t.Fatalf("leadership changed during a quiet period: was node %d, now node %d", leader, got)
	}
	if got := stTerm(); got != termAfterSettle {
		t.Fatalf("spurious re-election during quiet period: term %d -> %d", termAfterSettle, got)
	}

	// The global Election Safety invariant over the whole run.
	assertElectionSafety(t, nodes)

	// Exactly one node believes itself leader for the final term.
	leaders := 0
	for _, nd := range nodes {
		if st, ok := nd.ctrl.state(); ok && st.Role == Leader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("expected exactly one leader, found %d", leaders)
	}
}

// TestRaft_Propose_NotLeaderRedirect checks the public not-leader path: a
// Propose to a follower fails with a *NotLeaderError naming the leader the
// follower currently believes in, so a client can redirect.
func TestRaft_Propose_NotLeaderRedirect(t *testing.T) {
	sim := prototest.NewSim(t, prototest.WithSeed(0x2ED17EC7))
	nodes := buildCluster(t, sim, 5)

	leader := waitForLeader(t, sim, nodes, nil)
	sim.Run(300 * time.Millisecond) // let followers learn the leader

	// Pick a follower and propose to it.
	follower := (leader + 1) % 5
	nodes[follower].ctrl.propose("should-be-rejected")
	sim.Run(200 * time.Millisecond)

	err := nodes[follower].ctrl.proposeErr()
	if err == nil {
		t.Fatalf("propose to a follower should fail with not-leader")
	}
	var nle *NotLeaderError
	if !errors.As(err, &nle) {
		t.Fatalf("expected *NotLeaderError, got %v", err)
	}
	if !nle.HasLeader || nle.Leader != nodes[leader].host {
		t.Fatalf("not-leader error should name leader %s, got %+v", nodes[leader].host.String(), nle)
	}
	// The rejected command must never be applied anywhere.
	for i, nd := range nodes {
		if nd.applied.contains("should-be-rejected") {
			t.Fatalf("rejected command was applied on node %d", i)
		}
	}
}
