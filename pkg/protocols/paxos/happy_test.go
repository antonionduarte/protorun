package paxos

import (
	"errors"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/prototest"
)

// Invariant guarded here: Agreement + Integrity on the quiet path. Five
// nodes, a single proposer, no faults — every node must publish Decided
// with the SAME value, exactly once each, and that value must be the one
// proposed.

func TestPaxos_HappyPath_AllDecideOnce(t *testing.T) {
	sim := prototest.NewSim(t, prototest.WithSeed(0x9A705))
	nodes := buildCluster(t, sim, 5)

	// Let sessions establish before proposing.
	sim.Run(1 * time.Second)

	nodes[0].ctrl.propose("hello-paxos")

	ok := sim.RunUntil(func() bool { return allDecided(nodes) }, 15*time.Second)
	if !ok {
		for i, nd := range nodes {
			t.Logf("node %d decided=%d", i, nd.decided.count())
		}
		t.Fatal("not all nodes decided within the horizon")
	}

	// Agreement + Integrity: same value everywhere, exactly one Decided each.
	agreed := assertAgreement(t, nodes)
	if agreed != "hello-paxos" {
		t.Fatalf("decided %q, want the proposed value %q", agreed, "hello-paxos")
	}
	for i, nd := range nodes {
		if c := nd.decided.count(); c != 1 {
			t.Fatalf("node %d published Decided %d times, want exactly 1", i, c)
		}
	}
	assertDecidedWasProposed(t, nodes, map[string]bool{"hello-paxos": true})

	// A long quiet period must not produce any further Decided or change the
	// value (chosen-is-forever, checked lightly here; exhaustively in
	// TestPaxos_ChosenIsForever).
	sim.Run(5 * time.Second)
	for i, nd := range nodes {
		if c := nd.decided.count(); c != 1 {
			t.Fatalf("node %d published Decided %d times after settling, want 1", i, c)
		}
	}

	// Public-surface check: a Propose to an already-decided node fails with an
	// *AlreadyDecidedError carrying the chosen value, so a caller learns the
	// outcome immediately. The typed error is reached through errors.As
	// because the framework wraps a responder's Fail error in ErrResponderFailed.
	nodes[3].ctrl.propose("too-late")
	sim.Run(200 * time.Millisecond)
	err, have := nodes[3].ctrl.proposeErr()
	if !have || err == nil {
		t.Fatal("propose after decision should fail")
	}
	var ad *AlreadyDecidedError
	if !errors.As(err, &ad) {
		t.Fatalf("expected *AlreadyDecidedError, got %v", err)
	}
	if string(ad.Value) != "hello-paxos" {
		t.Fatalf("AlreadyDecidedError carried %q, want the chosen value %q", ad.Value, "hello-paxos")
	}
}
