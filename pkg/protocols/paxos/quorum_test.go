package paxos

import (
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/prototest"
)

// Invariant guarded here: quorum is necessary AND sufficient. A minority
// side of a partition can NEVER decide (it cannot form a majority); the
// majority side decides normally; and on heal the stranded minority learns
// the SAME value — never a different one.
//
// Catch-up design (documented): when a session (re)connects, an acceptor
// that holds an accepted value re-announces it to the new peer
// (OnSessionConnected in protocol.go). After a heal, each majority acceptor
// re-sends its Accepted for the chosen ballot to the reconnecting minority
// node, which collects a majority of them and decides. No polling, no
// special catch-up RPC — the ordinary Phase-2b announcement, replayed on
// reconnect.

func TestPaxos_Quorum_MinorityCannotDecide_HealConverges(t *testing.T) {
	const n = 5
	sim := prototest.NewSim(t, prototest.WithSeed(0x9207))
	nodes := buildCluster(t, sim, n)

	sim.Run(1 * time.Second) // establish sessions

	// Partition 2 | 3: minority {0,1}, majority {2,3,4}.
	minority := []int{0, 1}
	majority := []int{2, 3, 4}
	for _, a := range minority {
		for _, b := range majority {
			sim.Mesh().Cut(paxosHost(a), paxosHost(b))
		}
	}
	sim.Run(500 * time.Millisecond) // disconnects propagate

	// The majority side decides.
	nodes[2].ctrl.propose("majority-wins")
	ok := sim.RunUntil(func() bool {
		for _, i := range majority {
			if nodes[i].decided.count() == 0 {
				return false
			}
		}
		return true
	}, 10*time.Second)
	if !ok {
		t.Fatal("majority side did not decide")
	}
	for _, i := range majority {
		if v, _, _ := nodes[i].decided.value(); v != "majority-wins" {
			t.Fatalf("majority node %d decided %q, want %q", i, v, "majority-wins")
		}
	}

	// A minority proposer tries hard but can never reach a majority, so it
	// cannot even complete Phase 1 (only 2 of 5 promise), let alone decide.
	nodes[0].ctrl.propose("minority-loses")
	sim.Run(3 * time.Second) // give the doomed proposer ample time + retries
	for _, i := range minority {
		if c := nodes[i].decided.count(); c != 0 {
			t.Fatalf("minority node %d decided (%d) without a quorum", i, c)
		}
	}

	// Heal: the minority reconnects and learns the majority's value through
	// the on-reconnect Accepted re-announcement.
	for _, a := range minority {
		for _, b := range majority {
			sim.Mesh().Heal(paxosHost(a), paxosHost(b))
		}
	}
	ok = sim.RunUntil(func() bool { return allDecided(nodes) }, 15*time.Second)
	if !ok {
		for i, nd := range nodes {
			t.Logf("node %d decided=%d", i, nd.decided.count())
		}
		t.Fatal("minority did not catch up after heal")
	}

	// Global Agreement + Integrity: everyone decided "majority-wins", once.
	agreed := assertAgreement(t, nodes)
	if agreed != "majority-wins" {
		t.Fatalf("post-heal decided %q, want %q", agreed, "majority-wins")
	}
	assertDecidedWasProposed(t, nodes, map[string]bool{"majority-wins": true, "minority-loses": true})
	for i, nd := range nodes {
		if c := nd.decided.count(); c != 1 {
			t.Fatalf("node %d decided %d times, want exactly 1", i, c)
		}
	}
}
