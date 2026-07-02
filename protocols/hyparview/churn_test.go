package hyparview

import (
	"testing"
	"time"

	"github.com/antonionduarte/protorun/prototest"
	"github.com/antonionduarte/protorun/transport"
)

// TestHyParView_ChurnConverges brings up 20 nodes, kills a quarter of
// them (Isolate = the sim's "kill": every link cut, sessions torn down),
// and asserts the survivors reconverge to healthy views that exclude the
// dead nodes.
func TestHyParView_ChurnConverges(t *testing.T) {
	const n = 20
	const survivors = 15 // kill nodes 15..19
	sim := prototest.NewSim(t, prototest.WithSeed(0xBEEF))
	probes := buildChain(t, sim, n)

	// Converge first.
	sim.Run(60 * time.Second)
	for i := range survivors {
		if len(probes[i].snapshotActive()) == 0 {
			t.Fatalf("precondition failed: node %d empty before churn", i)
		}
	}

	// Kill the last quarter.
	dead := make(map[transport.Host]bool)
	for i := survivors; i < n; i++ {
		sim.Mesh().Isolate(hvHost(i))
		dead[hvHost(i)] = true
	}

	// Let the survivors detect the failures and reconverge.
	sim.Run(60 * time.Second)

	// Survivors must have non-empty, dead-free, symmetric active views.
	inView := make([]map[transport.Host]bool, survivors)
	for i := range survivors {
		inView[i] = make(map[transport.Host]bool)
		active := probes[i].snapshotActive()
		if len(active) == 0 {
			t.Errorf("survivor %d has an empty active view after churn", i)
		}
		for _, h := range active {
			if dead[h] {
				t.Errorf("survivor %d still lists dead node %s", i, h.String())
			}
			inView[i][h] = true
		}
	}
	for i := range survivors {
		for j := range survivors {
			if i == j {
				continue
			}
			if inView[i][hvHost(j)] && !inView[j][hvHost(i)] {
				t.Errorf("asymmetric survivor views: %d lists %d but not vice versa", i, j)
			}
		}
	}
}
