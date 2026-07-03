package raft

import (
	"fmt"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/prototest"
)

// Invariant guarded here: State Machine Safety (§5.4.3) — every node
// applies the same commands in the same order. Asserted by comparing the
// full applied sequence at every node after replicating N proposals
// through the leader.

func TestRaft_Replication_SameOrderEverywhere(t *testing.T) {
	sim := prototest.NewSim(t, prototest.WithSeed(0x8EEF))
	nodes := buildCluster(t, sim, 5)

	leader := waitForLeader(t, sim, nodes, nil)

	const numCmds = 12
	cmds := make([]string, numCmds)
	for i := range numCmds {
		cmds[i] = fmt.Sprintf("cmd-%02d", i)
		nodes[leader].ctrl.propose(cmds[i])
		sim.Run(20 * time.Millisecond) // let each proposal replicate a bit
	}

	// Run until every node has applied all commands.
	ok := sim.RunUntil(func() bool {
		for _, nd := range nodes {
			if nd.applied.len() < numCmds {
				return false
			}
		}
		return true
	}, 15*time.Second)
	if !ok {
		for i, nd := range nodes {
			t.Logf("node %d applied %d/%d", i, nd.applied.len(), numCmds)
		}
		t.Fatalf("not all nodes applied all commands")
	}

	// Every node's applied sequence must equal the proposed order exactly.
	for i, nd := range nodes {
		seq := nd.applied.sequence()
		if len(seq) != numCmds {
			t.Fatalf("node %d applied %d commands, want %d", i, len(seq), numCmds)
		}
		for k, c := range cmds {
			if seq[k] != c {
				t.Fatalf("node %d applied[%d] = %q, want %q", i, k, seq[k], c)
			}
		}
	}
	assertConsistentApplied(t, nodes)
	assertElectionSafety(t, nodes)
}
