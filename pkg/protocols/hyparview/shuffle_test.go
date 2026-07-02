package hyparview

import (
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/prototest"
	"github.com/antonionduarte/protorun/pkg/transport"
)

// TestHyParView_ShuffleRotatesPassive asserts the periodic shuffle
// actually circulates passive-view membership: passive views become
// populated and their contents change over virtual time. Because the
// shuffle walk and reply travel only over active-view links (no transient
// sessions to passive peers), this also exercises that the path-retracing
// reply reaches origins and integrates.
func TestHyParView_ShuffleRotatesPassive(t *testing.T) {
	const n = 20
	sim := prototest.NewSim(t, prototest.WithSeed(0x5EED))
	probes := buildChain(t, sim, n)

	// Converge the overlay so shuffles have material to exchange.
	sim.Run(30 * time.Second)

	before := passiveSnapshot(probes)
	populated := 0
	for _, set := range before {
		if len(set) > 0 {
			populated++
		}
	}
	if populated == 0 {
		t.Fatalf("no node had a populated passive view after convergence; shuffle is not filling passive views")
	}

	// Let several more shuffle rounds run.
	sim.Run(20 * time.Second)
	after := passiveSnapshot(probes)

	changed := 0
	for i := range n {
		if !sameHostSet(before[i], after[i]) {
			changed++
		}
	}
	if changed == 0 {
		t.Fatalf("no passive view changed over 20s of shuffling; shuffle is not rotating passive views")
	}
}

func passiveSnapshot(probes []*stateProbe) []map[transport.Host]bool {
	out := make([]map[transport.Host]bool, len(probes))
	for i, p := range probes {
		set := make(map[transport.Host]bool)
		for _, h := range p.snapshotPassive() {
			set[h] = true
		}
		out[i] = set
	}
	return out
}

func sameHostSet(a, b map[transport.Host]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for h := range a {
		if !b[h] {
			return false
		}
	}
	return true
}
