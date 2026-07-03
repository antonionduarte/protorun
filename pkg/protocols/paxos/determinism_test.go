package paxos

import (
	"fmt"
	"testing"

	"github.com/antonionduarte/protorun/pkg/prototest"
)

// Invariant guarded here: full-stack determinism — the same seed yields a
// byte-identical decision trace across the whole synod. This is only
// possible if the Paxos protocol follows the authoring contract (all state
// on the loop, the only randomness a per-node seeded RNG, sorted peer
// iteration). Mirrors raft's determinism test.

// runDuelAndTrace runs a dueling-proposer scenario under seed and returns a
// stable textual trace of every node's decision (value + ballot).
func runDuelAndTrace(t *testing.T, seed int64) string {
	t.Helper()
	const n = 5
	sim := prototest.NewSim(t, prototest.WithSeed(seed))
	nodes := buildCluster(t, sim, n)

	sim.Run(1_000_000_000) // 1s: establish sessions
	nodes[0].ctrl.propose("A")
	nodes[1].ctrl.propose("B")
	sim.RunUntil(func() bool { return allDecided(nodes) }, 30_000_000_000)

	var out string
	for i, nd := range nodes {
		out += fmt.Sprintf("=== node %d ===\n", i)
		for _, d := range nd.decided.decisions() {
			out += fmt.Sprintf("value=%s ballot=%d\n", d.Value, d.Ballot)
		}
	}
	return out
}

func TestPaxos_DeterministicTrace(t *testing.T) {
	const seed = 0xDEC0DE

	first := runDuelAndTrace(t, seed)
	second := runDuelAndTrace(t, seed)
	if first != second {
		t.Fatalf("same-seed decision traces differ:\n--- first ---\n%s\n--- second ---\n%s", first, second)
	}
	if len(first) == 0 {
		t.Fatal("expected a non-empty trace")
	}

	// A different seed must itself be reproducible (whether or not it differs
	// from the first).
	other := runDuelAndTrace(t, seed+1)
	if other != runDuelAndTrace(t, seed+1) {
		t.Fatal("a fixed seed must reproduce regardless of its value")
	}
}
