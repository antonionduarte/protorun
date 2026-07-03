package raft

import (
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/prototest"
	"github.com/antonionduarte/protorun/pkg/transport"
)

// Invariant guarded here: split votes always resolve. With a narrow
// election-timeout window and a network delay on every link, multiple
// candidates repeatedly time out together and split the vote; randomized
// timeouts must nonetheless drive the cluster to exactly one winner, with
// the term strictly increasing across the failed rounds and Election
// Safety never violated.

func buildDuelingCluster(t *testing.T, sim *prototest.Sim, n int) []*raftNode {
	t.Helper()
	nodes := make([]*raftNode, n)
	for i := range n {
		var peers []transport.Host
		for j := range n {
			if j != i {
				peers = append(peers, raftHost(j))
			}
		}
		// Narrow election window => nodes tend to time out together and
		// split the vote; a link delay makes vote collection lag a round.
		cfg := Config{
			Peers:              peers,
			HeartbeatInterval:  20 * time.Millisecond,
			ElectionTimeoutMin: 100 * time.Millisecond,
			ElectionTimeoutMax: 115 * time.Millisecond,
			ReconnectInterval:  40 * time.Millisecond,
		}
		pr := New(raftHost(i), cfg)
		nodes[i] = &raftNode{host: raftHost(i), proto: pr, applied: &appliedRec{}, leaders: &leaderRec{}, ctrl: &control{}}
		sim.Node(raftHost(i), pr, nodes[i].applied, nodes[i].leaders, nodes[i].ctrl)
	}
	return nodes
}

func TestRaft_DuelingCandidates_Resolves(t *testing.T) {
	sim := prototest.NewSim(t, prototest.WithSeed(0xD0E1))
	const n = 5
	nodes := buildDuelingCluster(t, sim, n)

	// A small delay on every link: enough that a full vote round-trip lags
	// (encouraging the narrow-window candidates to split), but well under
	// the election timeout so a candidate that does get ahead still wins
	// within one timeout rather than re-splitting forever.
	for i := range n {
		for j := i + 1; j < n; j++ {
			sim.Mesh().SetDelay(raftHost(i), raftHost(j), 8*time.Millisecond, 4*time.Millisecond)
		}
	}

	// Let the cluster establish sessions and then fight it out.
	leader := waitForLeader(t, sim, nodes, nil)

	// Give leadership time to stabilize under the delayed network.
	sim.Run(3 * time.Second)

	// Exactly one leader in the end.
	leaders := 0
	final := -1
	for i, nd := range nodes {
		if st, ok := nd.ctrl.state(); ok && st.Role == Leader {
			leaders++
			final = i
		}
	}
	if leaders != 1 {
		t.Fatalf("dueling did not resolve to a single leader: found %d leaders", leaders)
	}
	t.Logf("resolved to leader node %d (first observed leader was node %d)", final, leader)

	// Terms strictly increased across the contested rounds: the winning
	// term should be well past 1 given the forced split votes.
	st, _ := nodes[final].ctrl.state()
	if st.Term < 2 {
		t.Fatalf("expected the contested election to advance the term past 1, got term %d", st.Term)
	}

	// Election Safety held throughout every contested term.
	assertElectionSafety(t, nodes)

	// The winner is stable: a further quiet period keeps the same leader
	// and term.
	sim.Run(2 * time.Second)
	if got := leaderIndex(nodes); got != final {
		t.Fatalf("leader changed after resolution: was %d, now %d", final, got)
	}
	if st2, _ := nodes[final].ctrl.state(); st2.Term != st.Term {
		t.Fatalf("term kept advancing after resolution: %d -> %d", st.Term, st2.Term)
	}
}
