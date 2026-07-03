package paxos

import (
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/prototest"
)

// Invariant guarded here: the Phase-2a value-adoption rule under real
// timing — once a value has been accepted by a majority, any later,
// higher-ballot proposer MUST adopt that value, even if it started out
// wanting a different one. We engineer, via precise delay timing, the exact
// dangerous state: proposer A's value is accepted by a majority but NOT yet
// learned by anyone (the Accepted announcements are dropped in flight);
// then A goes silent and proposer B drives a higher-ballot round to
// completion. B proposed "value-B" but the synod must decide "value-A".
//
// This is the property naive Paxos implementations violate — a proposer
// that ignores the accepted values in its promises and forces its own value
// can overwrite an already-chosen decree.

func TestPaxos_ValueAdoption_HigherBallotAdoptsMajorityValue(t *testing.T) {
	const (
		n     = 5
		delay = 10 * time.Millisecond // fixed, zero jitter: exact hop timing
	)
	sim := prototest.NewSim(t, prototest.WithSeed(0xADE7))
	nodes := buildCluster(t, sim, n)

	sim.Run(1 * time.Second) // establish sessions first

	// Fixed per-hop delay on every link, no jitter, so the schedule below is
	// exact: prepare (t+1D), promise (t+2D), accept + proposer's own accepted
	// (t+3D), the acceptors' Accepted announcements (t+4D).
	for i := range n {
		for j := i + 1; j < n; j++ {
			sim.Mesh().SetDelay(paxosHost(i), paxosHost(j), delay, 0)
		}
	}

	// A (node 0) proposes. Its first ballot is round1 = 1*5 + 0 = 5.
	const ballotA = 5
	nodes[0].ctrl.propose("value-A")

	// Run to 3.5D: the acceptors have accepted "value-A" (at 3D) but nobody
	// has learned it yet (the deciding Accepted announcements are due at 4D).
	sim.Run(35 * time.Millisecond)

	// Suppress learning: cut every link, which also purges the in-flight
	// Accepted announcements. This freezes the synod in the dangerous state —
	// a majority holds "value-A" as accepted, but no learner reached a
	// majority tally, so nothing is decided.
	for i := range n {
		for j := i + 1; j < n; j++ {
			sim.Mesh().Cut(paxosHost(i), paxosHost(j))
		}
	}
	sim.Run(30 * time.Millisecond) // let a debug poll refresh; no deliveries (all cut)

	// Guard 1: nobody decided. If this fails, the timing window was wrong and
	// the rest of the test would be meaningless.
	for i, nd := range nodes {
		if c := nd.decided.count(); c != 0 {
			t.Fatalf("node %d decided (%d) before B's round — learning was not suppressed", i, c)
		}
	}
	// Guard 2: a majority of acceptors hold "value-A" at ballot 5.
	held := 0
	for _, nd := range nodes {
		if st, ok := nd.ctrl.state(); ok && st.HasAccepted && st.AcceptedBallot == ballotA && string(st.AcceptedValue) == "value-A" {
			held++
		}
	}
	if held < majoritySize(n) {
		t.Fatalf("only %d acceptors hold value-A at ballot %d, want a majority (%d)", held, ballotA, majoritySize(n))
	}

	// A goes silent for good: keep node 0 isolated so ONLY B can drive the
	// deciding round. Heal the other four so B has a working majority.
	healed := []int{1, 2, 3, 4}
	for _, a := range healed {
		for _, b := range healed {
			if a < b {
				sim.Mesh().Heal(paxosHost(a), paxosHost(b))
			}
		}
	}
	sim.Run(300 * time.Millisecond) // sessions re-establish among the four

	// B (node 1) proposes its OWN, different value with a higher ballot.
	nodes[1].ctrl.propose("value-B")

	ok := sim.RunUntil(func() bool {
		for _, i := range healed {
			if nodes[i].decided.count() == 0 {
				return false
			}
		}
		return true
	}, 15*time.Second)
	if !ok {
		for _, i := range healed {
			t.Logf("node %d decided=%d", i, nodes[i].decided.count())
		}
		t.Fatal("B's group did not decide after adoption round")
	}

	// The decree is "value-A", even though B (which drove the deciding round)
	// proposed "value-B": adoption held.
	for _, i := range healed {
		v, decided, dup := nodes[i].decided.value()
		if dup {
			t.Fatalf("node %d decided more than once", i)
		}
		if !decided || v != "value-A" {
			t.Fatalf("node %d decided %q, want the adopted value %q", i, v, "value-A")
		}
	}
}
