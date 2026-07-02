package prototest

import (
	"fmt"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/transport"
)

// floodHost builds the i-th flood host with a readable address.
func floodHost(i int) transport.Host {
	return transport.NewHost(i, fmt.Sprintf("10.0.0.%d", i))
}

// buildRing wires n flood nodes into a bidirectional ring (each dials its
// two neighbours) on the given Sim and returns the protocols by index.
func buildRing(t *testing.T, sim *Sim, n int) []*floodProtocol {
	t.Helper()
	protos := make([]*floodProtocol, n)
	for i := range n {
		left := floodHost((i - 1 + n) % n)
		right := floodHost((i + 1) % n)
		p := newFloodProtocol(floodHost(i), left, right)
		protos[i] = p
		sim.Node(floodHost(i), p)
	}
	return protos
}

// traceString renders a node's trace as a stable, comparable string.
func traceString(p *floodProtocol) string {
	var b []byte
	for _, e := range p.snapshotTrace() {
		b = append(b, fmt.Sprintf("%s:%s:%d\n", e.Kind, e.Peer.String(), e.ID)...)
	}
	return string(b)
}

// runOnce builds a fresh ring, connects it, broadcasts from node 0, runs
// to convergence, and returns the concatenated per-node traces. Each run
// is fully in-process on a fresh Sim, so two calls with the same seed must
// produce byte-identical output.
func runOnce(t *testing.T, seed int64) string {
	t.Helper()
	sim := NewSim(t, WithSeed(seed))
	const n = 6
	protos := buildRing(t, sim, n)

	// Let the ring establish sessions.
	sim.Run(time.Second)
	// Originate a broadcast from node 0 and converge.
	protos[0].Broadcast(42, 16)
	sim.RunUntil(func() bool {
		for _, p := range protos {
			if !p.hasDelivered(42) {
				return false
			}
		}
		return true
	}, 30*time.Second)
	// A little more virtual time so any trailing refloods settle.
	sim.Run(2 * time.Second)

	var out string
	for i, p := range protos {
		out += fmt.Sprintf("=== node %d ===\n%s", i, traceString(p))
	}
	return out
}

// TestSim_DeterministicTrace is the headline proof: the same seed run
// twice in-process yields byte-identical event traces across the whole
// stack. Run under -race -count=20 it must never flake, which is only
// possible if quiescence detection and the seeded scheduler are sound.
func TestSim_DeterministicTrace(t *testing.T) {
	const seed = 1234567
	first := runOnce(t, seed)
	second := runOnce(t, seed)
	if first != second {
		t.Fatalf("same-seed traces differ:\n--- first ---\n%s\n--- second ---\n%s", first, second)
	}

	// A different seed is ALLOWED to differ; we don't assert it does (it
	// might coincide), only that the run still converges and is itself
	// reproducible.
	other := runOnce(t, seed+1)
	if other != runOnce(t, seed+1) {
		t.Fatalf("a fixed seed must be reproducible regardless of its value")
	}
	if len(first) == 0 {
		t.Fatalf("expected a non-empty trace")
	}
}

// TestSim_PartitionHeal partitions a 5-node flood mid-run, checks that the
// broadcast reaches only the originating side, then heals and checks full
// delivery — all in virtual time, in well under a second of real time.
func TestSim_PartitionHeal(t *testing.T) {
	start := time.Now()
	sim := NewSim(t, WithSeed(9001))

	// Fully-meshed 5-node cluster: every node dials every other.
	const n = 5
	protos := make([]*floodProtocol, n)
	for i := range n {
		var contacts []transport.Host
		for j := range n {
			if j != i {
				contacts = append(contacts, floodHost(j))
			}
		}
		protos[i] = newFloodProtocol(floodHost(i), contacts...)
		sim.Node(floodHost(i), protos[i])
	}
	sim.Run(2 * time.Second) // establish the full mesh

	// Partition {0,1} | {2,3,4}.
	left := []int{0, 1}
	right := []int{2, 3, 4}
	for _, a := range left {
		for _, b := range right {
			sim.Mesh().Cut(floodHost(a), floodHost(b))
		}
	}
	sim.Run(2 * time.Second) // let disconnects propagate

	// Broadcast from node 0 (left side). Converge within the partition.
	protos[0].Broadcast(7, 16)
	sim.Run(5 * time.Second)

	for _, i := range left {
		if !protos[i].hasDelivered(7) {
			t.Fatalf("left node %d should have delivered during the partition", i)
		}
	}
	for _, i := range right {
		if protos[i].hasDelivered(7) {
			t.Fatalf("right node %d must NOT receive the broadcast across a partition", i)
		}
	}

	// Heal every cut link; the flood protocol re-dials on its own timer.
	for _, a := range left {
		for _, b := range right {
			sim.Mesh().Heal(floodHost(a), floodHost(b))
		}
	}
	// Re-broadcast a fresh id after the heal and converge everywhere.
	sim.Run(2 * time.Second) // allow reconnect timers to re-establish
	protos[0].Broadcast(8, 16)
	ok := sim.RunUntil(func() bool {
		for _, p := range protos {
			if !p.hasDelivered(8) {
				return false
			}
		}
		return true
	}, 30*time.Second)
	if !ok {
		t.Fatalf("broadcast did not reach every node after heal")
	}

	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("virtual-time partition/heal took %v of real time, expected < 1s", elapsed)
	}
}

// TestSim_LossAndDelay covers two guarantees: total loss (p=1) drops every
// message, and a delayed message is delivered after a timer that was armed
// for an earlier virtual instant.
func TestSim_LossAndDelay(t *testing.T) {
	t.Run("total loss drops everything", func(t *testing.T) {
		sim := NewSim(t, WithSeed(2024))
		a := newFloodProtocol(floodHost(1), floodHost(2))
		b := newFloodProtocol(floodHost(2), floodHost(1))
		sim.Node(floodHost(1), a)
		sim.Node(floodHost(2), b)
		sim.Run(time.Second) // connect

		sim.Mesh().SetLoss(floodHost(1), floodHost(2), 1.0)
		a.Broadcast(100, 8)
		sim.Run(5 * time.Second)

		if !a.hasDelivered(100) {
			t.Fatalf("originator should deliver its own broadcast")
		}
		if b.hasDelivered(100) {
			t.Fatalf("peer must receive nothing across a fully-lossy link")
		}
	})

	t.Run("delay orders after an earlier timer", func(t *testing.T) {
		sim := NewSim(t, WithSeed(2025))

		// Order recorder on B: a timer armed at +2s must fire before a
		// message from A that the link delays by +5s.
		var order []string
		a := newFloodProtocol(floodHost(1), floodHost(2))
		b := &observerProtocol{
			self:        floodHost(2),
			handleFlood: true,
			onMsg:       func() { order = append(order, "message") },
			onInit: func(op *observerProtocol) {
				// Armed at virtual epoch (Init time); fires at +2s.
				op.ctx.After(2*time.Second, func() { order = append(order, "timer") })
			},
		}
		sim.Node(floodHost(1), a)
		sim.Node(floodHost(2), b)
		sim.Run(time.Second) // connect (virtual t = +1s)

		sim.Mesh().SetDelay(floodHost(1), floodHost(2), 5*time.Second, 0)
		a.Broadcast(200, 8) // sent at +1s, delivered at +6s

		sim.Run(10 * time.Second)

		if len(order) < 2 {
			t.Fatalf("expected both a timer and a message event, got %v", order)
		}
		if order[0] != "timer" {
			t.Fatalf("timer armed at +2s must fire before the +5s-delayed message; got order %v", order)
		}
	})
}

// TestSim_Step drives the simulation one deliverable event at a time and
// checks it makes progress and eventually runs dry.
func TestSim_Step(t *testing.T) {
	sim := NewSim(t, WithSeed(77))
	a := newFloodProtocol(floodHost(1), floodHost(2))
	b := newFloodProtocol(floodHost(2), floodHost(1))
	sim.Node(floodHost(1), a)
	sim.Node(floodHost(2), b)

	// Step until both sides have a session (each Step delivers one event
	// or advances the clock to the next deadline). A generous bound guards
	// against a stuck harness.
	steps := 0
	for steps < 1000 {
		if _, upA := probeConnected(a, floodHost(2)); upA {
			if _, upB := probeConnected(b, floodHost(1)); upB {
				break
			}
		}
		if !sim.Step() {
			break
		}
		steps++
	}
	if _, up := probeConnected(a, floodHost(2)); !up {
		t.Fatalf("stepping never established a session (after %d steps)", steps)
	}

	// Step until b delivers the broadcast. A periodic reconnect timer means
	// Step always has a next deadline (it never returns false), so bound
	// the loop with the predicate plus a generous step cap.
	a.Broadcast(1, 4)
	delivered := false
	for range 5000 {
		if !sim.Step() {
			break
		}
		if b.hasDelivered(1) {
			delivered = true
			break
		}
	}
	if !delivered {
		t.Fatalf("stepping should deliver the broadcast to b")
	}
}

// probeConnected reports whether p currently lists h as a live peer. Read
// at a quiescent point (between Steps), so no lock is needed beyond the
// protocol's own trace mutex.
func probeConnected(p *floodProtocol, h transport.Host) (transport.Host, bool) {
	for _, e := range p.snapshotTrace() {
		if e.Kind == "connect" && e.Peer == h {
			return h, true
		}
	}
	return transport.Host{}, false
}

// TestSim_TimerFiresInVirtualTime shows a plain ctx.After/ctx.Every timer
// firing inside the Sim on virtual time, with no real sleeps.
func TestSim_TimerFiresInVirtualTime(t *testing.T) {
	sim := NewSim(t, WithSeed(55))
	var afterFired, everyTicks int
	p := &observerProtocol{
		onInit: func(op *observerProtocol) {
			op.ctx.After(3*time.Second, func() { afterFired++ })
			op.ctx.Every(time.Second, func() { everyTicks++ })
		},
	}
	sim.Node(floodHost(1), p)

	sim.Run(3500 * time.Millisecond)
	if afterFired != 1 {
		t.Fatalf("one-shot after should fire exactly once by +3.5s, got %d", afterFired)
	}
	if everyTicks != 3 {
		t.Fatalf("periodic every(1s) should tick 3 times by +3.5s, got %d", everyTicks)
	}
	// It all happened in virtual time.
	if got := sim.Clock().Now(); !got.Equal(simEpoch.Add(3500 * time.Millisecond)) {
		t.Fatalf("virtual clock = %v, want epoch+3.5s", got)
	}
}
