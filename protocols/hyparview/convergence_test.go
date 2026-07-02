package hyparview

import (
	"fmt"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/prototest"
	"github.com/antonionduarte/protorun/transport"
)

func hvHost(i int) transport.Host { return transport.NewHost(6000+i, fmt.Sprintf("10.1.0.%d", i)) }

// testConfig is a small-cluster HyParView tuning that converges quickly on
// the virtual clock.
func testConfig(contacts ...transport.Host) Config {
	return Config{
		Contacts:        contacts,
		ActiveSize:      4,
		PassiveSize:     12,
		ARWL:            6,
		PRWL:            3,
		ShuffleActive:   3,
		ShufflePassive:  4,
		ShuffleInterval: 1 * time.Second,
		JoinInterval:    1 * time.Second,
		NeighborTimeout: 2 * time.Second,
	}
}

// buildChain wires n HyParView nodes as a contact chain: node 0 is the
// rendezvous (no contacts), node i (>0) bootstraps off node i-1. Returns
// the per-node probes for view inspection.
func buildChain(t *testing.T, sim *prototest.Sim, n int) []*stateProbe {
	t.Helper()
	probes := make([]*stateProbe, n)
	for i := range n {
		var cfg Config
		if i == 0 {
			cfg = testConfig()
		} else {
			cfg = testConfig(hvHost(i - 1))
		}
		hv := New(hvHost(i), cfg)
		probe := &stateProbe{}
		probes[i] = probe
		sim.Node(hvHost(i), hv, probe)
	}
	return probes
}

// TestHyParView_ChainConvergence brings up 20 nodes from a single contact
// chain and asserts every node ends with a non-empty, self-free active
// view within the configured bound, and that active views are symmetric.
func TestHyParView_ChainConvergence(t *testing.T) {
	const n = 20
	sim := prototest.NewSim(t, prototest.WithSeed(0xC0FFEE))
	probes := buildChain(t, sim, n)

	sim.Run(60 * time.Second)

	assertHealthyViews(t, probes, n, 4)
}

// assertHealthyViews checks the core HyParView invariants across all
// nodes: non-empty active views, no self-references, sizes within bound,
// and symmetry (a in b's view iff b in a's view).
func assertHealthyViews(t *testing.T, probes []*stateProbe, n, activeSize int) {
	t.Helper()
	inView := make([]map[transport.Host]bool, n)
	for i := range probes {
		inView[i] = make(map[transport.Host]bool)
		active := probes[i].snapshotActive()
		if len(active) == 0 {
			t.Errorf("node %d has an empty active view", i)
		}
		if len(active) > activeSize {
			t.Errorf("node %d active view size %d exceeds bound %d", i, len(active), activeSize)
		}
		for _, h := range active {
			if h == hvHost(i) {
				t.Errorf("node %d has a self-reference in its active view", i)
			}
			inView[i][h] = true
		}
	}
	// Symmetry: if a lists b, b must list a.
	for i := range n {
		for j := range n {
			if i == j {
				continue
			}
			if inView[i][hvHost(j)] && !inView[j][hvHost(i)] {
				t.Errorf("asymmetric active view: %d lists %d but not vice versa", i, j)
			}
		}
	}
}
