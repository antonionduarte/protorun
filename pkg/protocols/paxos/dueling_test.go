package paxos

import (
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/prototest"
)

// Invariant guarded here (the classic Paxos safety gauntlet): with two
// proposers pushing DIFFERENT values concurrently under adversarial delays,
// exactly ONE value is chosen everywhere — never both, never a mix — and
// the dueling proposers do not livelock: randomized backoff drives the
// synod to a decision. Run across many seeds so Agreement is not an
// artifact of one lucky interleaving.

func TestPaxos_DuelingProposers_AgreementEverySeed(t *testing.T) {
	const n = 5
	seeds := []int64{
		0xD0E1, 0xD0E2, 0xD0E3, 0xD0E4, 0xD0E5,
		0xD0E6, 0xD0E7, 0xD0E8, 0xD0E9, 0xD0EA,
	}

	for _, seed := range seeds {
		t.Run(seedName(seed), func(t *testing.T) {
			sim := prototest.NewSim(t, prototest.WithSeed(seed))
			nodes := buildCluster(t, sim, n)

			// Delay every link so a full round-trip lags: this desynchronizes
			// the two proposers and forces genuine dueling (a promise for one
			// ballot racing an accept for another), which is exactly the case
			// naive Paxos implementations get wrong.
			for i := range n {
				for j := i + 1; j < n; j++ {
					sim.Mesh().SetDelay(paxosHost(i), paxosHost(j), 6*time.Millisecond, 5*time.Millisecond)
				}
			}

			sim.Run(1 * time.Second) // establish sessions

			// Two proposers, two different values, launched together.
			nodes[0].ctrl.propose("value-A")
			nodes[1].ctrl.propose("value-B")

			ok := sim.RunUntil(func() bool { return allDecided(nodes) }, 30*time.Second)
			if !ok {
				for i, nd := range nodes {
					st, _ := nd.ctrl.state()
					t.Logf("node %d decided=%d proposing=%v ballot=%d", i, nd.decided.count(), st.Proposing, st.MyBallot)
				}
				t.Fatalf("seed %#x: dueling proposers did not converge (livelock?)", seed)
			}

			// Exactly one value chosen, everywhere, exactly once — and it is
			// one of the two proposed values.
			agreed := assertAgreement(t, nodes)
			if agreed != "value-A" && agreed != "value-B" {
				t.Fatalf("seed %#x: decided %q, want one of the two proposed values", seed, agreed)
			}
			assertDecidedWasProposed(t, nodes, map[string]bool{"value-A": true, "value-B": true})
			for i, nd := range nodes {
				if c := nd.decided.count(); c != 1 {
					t.Fatalf("seed %#x: node %d decided %d times, want 1", seed, i, c)
				}
			}
		})
	}
}

func seedName(seed int64) string {
	const hex = "0123456789abcdef"
	var b [8]byte
	for i := 7; i >= 0; i-- {
		b[i] = hex[seed&0xf]
		seed >>= 4
	}
	return "seed_" + string(b[:])
}
