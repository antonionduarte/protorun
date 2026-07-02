package prototest

import (
	"bytes"
	"runtime"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/protorun"
	"github.com/antonionduarte/protorun/pkg/transport"
)

// Sim runs a full protocol stack under seeded, virtual-time simulation.
// Every node is a real protorun.Runtime; the mesh under it delivers
// inbound traffic synchronously and holds a shared virtual clock, so the
// scheduler can drive the whole system one deliverable event at a time,
// settle it to quiescence, then step virtual time to the next deadline.
// A 30-second convergence test runs in milliseconds of real time and,
// for a given seed, produces the exact same schedule every run.
//
// Determinism contract. The schedule is reproducible for protocols that
// follow the authoring contract: all state mutation and all sends happen
// inside handlers (message / timer / session / IPC), via the
// ProtocolContext; no goroutines of the protocol's own; no wall-clock
// reads (use ctx timers, which run on the virtual clock). Under those
// rules the only sources of concurrency the Sim does not control are
// absent, so the seed fixes loss, jitter, and delivery interleaving, and
// two runs with the same seed yield byte-identical behavior. Out-of-
// contract protocols (background goroutines, time.Now, time.Sleep) get
// best-effort scheduling, not determinism.
//
// A Sim is not safe for concurrent use; drive it from the test goroutine.
type Sim struct {
	t     testing.TB
	mesh  *Mesh
	sched *scheduler
}

// NewSim builds a simulation with its own mesh on virtual time. WithSeed
// pins the schedule; without it the seed comes from the test name. The
// seed is logged so any run reproduces. WithRealClock is ignored — a Sim
// requires virtual time.
func NewSim(t testing.TB, opts ...Option) *Sim {
	t.Helper()
	m := newMesh(t, opts...)
	if m.clock == nil { // WithRealClock passed; a Sim needs virtual time.
		m.clock = NewFakeClock(simEpoch)
	}
	s := &scheduler{mesh: m, clock: m.clock}
	m.sched = s
	return &Sim{t: t, mesh: m, sched: s}
}

// Mesh returns the simulation's mesh, so a test can inject faults
// (Cut/Heal/Isolate/SetLoss/SetDelay) between steps.
func (s *Sim) Mesh() *Mesh { return s.mesh }

// Clock returns the shared virtual clock the simulation advances.
func (s *Sim) Clock() *FakeClock { return s.mesh.clock }

// Node adds a node to the simulation: a full runtime for self running the
// given protocols, wired onto the mesh with the shared virtual clock and
// a bounded shutdown registered on t.Cleanup. Returns the runtime so the
// test can register notification subscribers, read metrics, and so on.
func (s *Sim) Node(self transport.Host, protocols ...protorun.Protocol) *protorun.Runtime {
	s.t.Helper()
	return NewRuntime(s.t, s.mesh, self, protocols)
}

// Run advances the simulation over d of virtual time: it repeatedly
// delivers all deliverable events (in seeded order, settling to quiescence
// after each) and steps the clock to the next timer or delivery deadline,
// until virtual time reaches now+d. Real-time cost is proportional to the
// work done, not to d.
func (s *Sim) Run(d time.Duration) { s.sched.run(d, nil) }

// RunUntil advances like Run but stops as soon as pred returns true,
// giving up at the max virtual-time horizon. It reports whether pred was
// satisfied. pred is evaluated on the test goroutine at quiescent points,
// so it may safely read protocol state that the protocols only touch
// inside handlers.
func (s *Sim) RunUntil(pred func() bool, max time.Duration) bool {
	return s.sched.run(max, pred)
}

// Step performs the smallest unit of progress: it delivers one
// deliverable event (chosen in seeded order) and settles, or — if nothing
// is deliverable at the current virtual time — advances the clock to the
// next deadline and settles. Reports whether any progress was made.
// Intended for fine-grained tests that want to inspect state between
// individual deliveries.
func (s *Sim) Step() bool { return s.sched.step() }

// --- scheduler ---------------------------------------------------------

// delivery is one pending inbound event for a node: either an application
// message (msg set) or a session event (ev set), due at virtual time at.
// seq records enqueue order for stable identity; delivery ORDER among
// due events is chosen by the seeded RNG, not by seq.
type delivery struct {
	at   time.Time
	seq  uint64
	node *Node
	from transport.Host
	msg  *bytes.Buffer
	ev   transport.SessionEvent
}

// scheduler is the Sim's event pump. All of its mutable state (the
// pending set, the RNG) is guarded by the mesh mutex, so node event-loop
// goroutines can enqueue (via Node.Send) while the test goroutine drains.
type scheduler struct {
	mesh  *Mesh
	clock *FakeClock
	seq   uint64
}

// enqueue records a pending delivery. Guarded by mesh.mu.
func (s *scheduler) enqueue(d *delivery) {
	s.mesh.mu.Lock()
	s.seq++
	d.seq = s.seq
	s.mesh.pending = append(s.mesh.pending, d)
	s.mesh.mu.Unlock()
}

// send applies the link's fault policy and, unless the message is
// dropped, schedules its delivery. Called from a handler (Node.Send) on a
// node event-loop goroutine.
func (s *scheduler) send(from *Node, to transport.Host, msg bytes.Buffer) {
	m := s.mesh
	m.mu.Lock()
	_, connected := from.peers[to]
	toNode, exists := m.nodes[to]
	if !connected || !exists || m.dropLocked(from.self, to) {
		m.mu.Unlock()
		return
	}
	at := s.clock.Now()
	if spec, ok := m.delay[link{from.self, to}]; ok {
		at = at.Add(spec.sample(m.rng))
	}
	s.seq++
	buf := msg
	m.pending = append(m.pending, &delivery{at: at, seq: s.seq, node: toNode, from: from.self, msg: &buf})
	m.mu.Unlock()
}

// purgeLinkLocked drops every in-flight MESSAGE between a and b (either
// direction) from the pending set. Session events are left in place — a
// Cut lands its own SessionDisconnected. Caller holds mesh.mu.
func (s *scheduler) purgeLinkLocked(a, b transport.Host) {
	kept := s.mesh.pending[:0]
	for _, d := range s.mesh.pending {
		if d.msg != nil &&
			((d.from == a && d.node.self == b) || (d.from == b && d.node.self == a)) {
			continue
		}
		kept = append(kept, d)
	}
	s.mesh.pending = kept
}

// run is the core loop behind Run and RunUntil. It alternates draining
// deliverable events with stepping virtual time to the next deadline,
// stopping at the now+d horizon or as soon as pred (if any) holds.
func (s *scheduler) run(d time.Duration, pred func() bool) bool {
	horizon := s.clock.Now().Add(d)
	s.settle()
	for {
		if pred != nil && pred() {
			return true
		}
		s.drainDue()
		if pred != nil && pred() {
			return true
		}
		next, has := s.nextDeadline()
		if !has || next.After(horizon) {
			// Nothing more happens before the horizon: land exactly on it
			// (firing anything due at the horizon) and stop.
			s.advanceTo(horizon)
			if pred != nil {
				return pred()
			}
			return true
		}
		s.advanceTo(next)
	}
}

// step performs one unit of progress; see Sim.Step. Note that with a
// periodic timer live there is always a next deadline, so Step keeps
// returning true — it reports false only when the simulation has nothing
// left to do ever. Bound stepping with a predicate or a counter; use
// RunUntil when you want to stop on a condition.
func (s *scheduler) step() bool {
	s.settle()
	// A timer due at the current instant (After(0), or a re-arm landing on
	// Now) counts as one unit of progress.
	if s.clock.fireDue() {
		s.settle()
		return true
	}
	if d := s.takeOneDue(s.clock.Now()); d != nil {
		s.deliver(d)
		s.settle()
		return true
	}
	next, has := s.nextDeadline()
	if !has {
		return false
	}
	s.advanceTo(next)
	return true
}

// drainDue delivers everything due at the current virtual time: it fires
// any timers due now (at equal virtual time, clock fires precede network
// deliveries) and delivers due network events one at a time in seeded
// order, settling after each. It loops until a full pass makes no
// progress, so a handler's zero-delay send or a scheduled session event
// is picked up in the same instant. After it returns, nothing is due at
// the current time, so the next-deadline computed by the caller is
// strictly in the future and the clock always moves forward.
func (s *scheduler) drainDue() {
	for {
		progressed := false
		if s.clock.fireDue() {
			s.settle()
			progressed = true
		}
		if d := s.takeOneDue(s.clock.Now()); d != nil {
			s.deliver(d)
			s.settle()
			progressed = true
		}
		if !progressed {
			return
		}
	}
}

// takeOneDue removes and returns one delivery due at or before now,
// chosen uniformly at random from the RNG so the interleaving is seeded.
// Returns nil when nothing is due.
func (s *scheduler) takeOneDue(now time.Time) *delivery {
	s.mesh.mu.Lock()
	defer s.mesh.mu.Unlock()
	var due []int
	for i, d := range s.mesh.pending {
		if !d.at.After(now) {
			due = append(due, i)
		}
	}
	if len(due) == 0 {
		return nil
	}
	pick := due[s.mesh.rng.IntN(len(due))]
	d := s.mesh.pending[pick]
	s.mesh.pending = append(s.mesh.pending[:pick], s.mesh.pending[pick+1:]...)
	return d
}

// deliver hands one event to its target node's runtime, synchronously.
// After it returns the event has been routed onto a protocol mailbox
// (inFlight is already positive), so the following settle observes the
// resulting cascade.
func (s *scheduler) deliver(d *delivery) {
	if d.node.sink == nil {
		return
	}
	if d.msg != nil {
		d.node.sink.DeliverMessage(*d.msg, d.from)
		return
	}
	if d.ev != nil {
		d.node.sink.DeliverSessionEvent(d.ev)
	}
}

// nextDeadline is the earliest virtual time at which anything happens: a
// pending delivery or a scheduled clock timer, whichever is sooner.
func (s *scheduler) nextDeadline() (time.Time, bool) {
	s.mesh.mu.Lock()
	var next time.Time
	has := false
	for _, d := range s.mesh.pending {
		if !has || d.at.Before(next) {
			next = d.at
			has = true
		}
	}
	s.mesh.mu.Unlock()

	if td, ok := s.clock.nextDeadline(); ok {
		if !has || td.Before(next) {
			next = td
			has = true
		}
	}
	return next, has
}

// advanceTo steps the shared clock to t (firing any timers due up to it,
// which enqueue synchronously on this goroutine) and settles the cascade.
func (s *scheduler) advanceTo(t time.Time) {
	if d := t.Sub(s.clock.Now()); d > 0 {
		s.clock.Advance(d)
	}
	s.settle()
}

// settle spins until every node's runtime is quiescent — no mailbox holds
// an event and no handler is mid-dispatch. It is sound because the Sim is
// the only source of new work: inbound delivery and clock fires both
// enqueue synchronously (incrementing inFlight before this loop can
// observe zero), and a handler's own send lands in the scheduler's
// pending set rather than a peer mailbox, so it does not resurrect a
// settled node. runtime.Gosched yields to the event-loop goroutines
// between reads rather than burning the CPU. See Runtime.Quiescent for
// the memory-model argument.
func (s *scheduler) settle() {
	for !s.allQuiescent() {
		runtime.Gosched()
	}
}

func (s *scheduler) allQuiescent() bool {
	s.mesh.mu.Lock()
	rts := make([]*protorun.Runtime, 0, len(s.mesh.nodes))
	for _, n := range s.mesh.nodes {
		if n.rt != nil {
			rts = append(rts, n.rt)
		}
	}
	s.mesh.mu.Unlock()
	for _, rt := range rts {
		if !rt.Quiescent() {
			return false
		}
	}
	return true
}
