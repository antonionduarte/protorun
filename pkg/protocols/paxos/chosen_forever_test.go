package paxos

import (
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/prototest"
)

// Invariant guarded here: a chosen value is chosen FOREVER. After the synod
// decides one value, later proposals carrying different values must still
// resolve to the ORIGINAL value — every proposer's adoption rule forces it,
// and no node ever publishes a second, conflicting Decided.

func TestPaxos_ChosenIsForever(t *testing.T) {
	const n = 5
	sim := prototest.NewSim(t, prototest.WithSeed(0xF04E4E))
	nodes := buildCluster(t, sim, n)

	sim.Run(1 * time.Second) // establish sessions

	// Decide an original value across the whole cluster.
	nodes[0].ctrl.propose("first")
	if !sim.RunUntil(func() bool { return allDecided(nodes) }, 15*time.Second) {
		t.Fatal("cluster did not decide the original value")
	}
	if agreed := assertAgreement(t, nodes); agreed != "first" {
		t.Fatalf("original decision was %q, want %q", agreed, "first")
	}

	// Now every OTHER node proposes a different value, well after the
	// decision. Each such proposal, if it runs at all, must adopt "first".
	for i := 1; i < n; i++ {
		nodes[i].ctrl.propose("late-" + string(rune('A'+i)))
	}
	sim.Run(5 * time.Second) // let the late proposals run their course

	// Still exactly one Decided per node, still "first" everywhere.
	agreed := assertAgreement(t, nodes)
	if agreed != "first" {
		t.Fatalf("late proposals changed the decree to %q, want %q", agreed, "first")
	}
	for i, nd := range nodes {
		if c := nd.decided.count(); c != 1 {
			t.Fatalf("node %d published Decided %d times, want exactly 1 (no re-decision)", i, c)
		}
		if v, _, _ := nd.decided.value(); v != "first" {
			t.Fatalf("node %d decided %q, want %q", i, v, "first")
		}
	}

	// The acceptors' durable state also still carries "first": no late
	// proposal overwrote a chosen value.
	for i, nd := range nodes {
		if st, ok := nd.ctrl.state(); ok && st.HasAccepted && string(st.AcceptedValue) != "first" {
			t.Fatalf("acceptor %d holds %q, want the chosen value %q", i, st.AcceptedValue, "first")
		}
	}
}

// TestPaxos_ChosenIsForever_TimeBudget confirms the whole scenario runs in
// virtual time, well under the real-time budget.
func TestPaxos_ChosenIsForever_TimeBudget(t *testing.T) {
	start := time.Now()
	sim := prototest.NewSim(t, prototest.WithSeed(0x5EED))
	nodes := buildCluster(t, sim, 5)
	sim.Run(1 * time.Second)
	nodes[0].ctrl.propose("x")
	sim.RunUntil(func() bool { return allDecided(nodes) }, 15*time.Second)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("virtual-time scenario took %v of real time, expected well under 5s", elapsed)
	}
}
