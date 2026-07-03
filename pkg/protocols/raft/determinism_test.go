package raft

import (
	"fmt"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/prototest"
)

// Invariant guarded here: full-stack determinism — the same seed yields
// byte-identical applied traces across the whole cluster. Mirrors
// prototest.TestSim_DeterministicTrace, and is only possible if the Raft
// protocol follows the authoring contract (all state on the loop, the
// only randomness a per-node seeded RNG, sorted peer iteration).

func runElectAndReplicate(t *testing.T, seed int64) string {
	t.Helper()
	sim := prototest.NewSim(t, prototest.WithSeed(seed))
	nodes := buildCluster(t, sim, 5)
	leader := waitForLeader(t, sim, nodes, nil)

	const numCmds = 8
	for i := range numCmds {
		nodes[leader].ctrl.propose(fmt.Sprintf("v%d", i))
		sim.Run(15 * time.Millisecond)
	}
	sim.RunUntil(func() bool {
		for _, nd := range nodes {
			if nd.applied.len() < numCmds {
				return false
			}
		}
		return true
	}, 15*time.Second)
	sim.Run(500 * time.Millisecond) // settle any trailing replication

	var out string
	for i, nd := range nodes {
		out += fmt.Sprintf("=== node %d ===\n", i)
		nd.applied.mu.Lock()
		for _, e := range nd.applied.applied {
			out += fmt.Sprintf("%d:%d:%s\n", e.Index, e.Term, e.Command)
		}
		nd.applied.mu.Unlock()
	}
	return out
}

func TestRaft_DeterministicTrace(t *testing.T) {
	const seed = 0xD37E824
	first := runElectAndReplicate(t, seed)
	second := runElectAndReplicate(t, seed)
	if first != second {
		t.Fatalf("same-seed applied traces differ:\n--- first ---\n%s\n--- second ---\n%s", first, second)
	}
	if len(first) == 0 {
		t.Fatalf("expected a non-empty trace")
	}

	// A different seed must itself be reproducible (it may or may not
	// differ from the first).
	other := runElectAndReplicate(t, seed+1)
	if other != runElectAndReplicate(t, seed+1) {
		t.Fatalf("a fixed seed must reproduce regardless of its value")
	}
}

func TestRaft_DeterministicTrace_TimeBudget(t *testing.T) {
	start := time.Now()
	_ = runElectAndReplicate(t, 0x5EED)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("virtual-time scenario took %v of real time, expected well under 5s", elapsed)
	}
}
