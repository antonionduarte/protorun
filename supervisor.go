package protorun

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/antonionduarte/protorun/transport"
)

// A supervisor drives the non-Resume directives for one protocol off
// its event loop. Shape: one goroutine per supervised protocol, not a
// single runtime-wide supervisor. The per-protocol shape keeps a
// protocol's restart budget, backoff attempt, and loop lifecycle
// colocated with the protocol they belong to, and lets an unrelated
// protocol panic and restart without serializing behind another
// protocol's (possibly long) backoff.
//
// The goroutine is started by the runtime after boot (startSupervisors)
// and is tracked by the runtime WaitGroup. It parks on the signals
// channel until a panic is reported, then applies the directive.
type supervisor struct {
	proto   *protoProtocol
	runtime *Runtime
	spec    Supervision

	// signals carries panic reports from safeCall (steady state) and
	// from the boot Start/Init recover paths. Buffered by one and
	// coalescing: once a restart is pending, additional reports are
	// dropped because the whole instance is being rebuilt anyway.
	signals chan panicSignal

	// times records the timestamps (runtime clock) of the panics seen
	// within the sliding Window. Only the supervisor goroutine touches
	// it, so it needs no lock.
	times []time.Time
}

// panicSignal is one reported panic: the handler tag and the recovered
// value, carried only for diagnostics.
type panicSignal struct {
	where string
	rec   any
}

func newSupervisor(p *protoProtocol, r *Runtime, spec Supervision) *supervisor {
	return &supervisor{
		proto:   p,
		runtime: r,
		spec:    spec,
		signals: make(chan panicSignal, 1),
	}
}

// signalPanic reports a panic to the supervisor without blocking. A
// full buffer means a restart is already pending; the report is
// coalesced (the pending restart supersedes it).
func (s *supervisor) signalPanic(where string, rec any) {
	select {
	case s.signals <- panicSignal{where: where, rec: rec}:
	default:
	}
}

// run is the supervisor goroutine. It parks until a panic is reported
// (or a boot panic was already buffered), applies the directive, and —
// for a successful Restart — loops to supervise the new instance.
// Stop, Escalate, and runtime shutdown end the goroutine.
func (s *supervisor) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case sig := <-s.signals:
			if !s.handle(ctx, sig) {
				return
			}
		}
	}
}

// handle applies the configured OnPanic directive. For Restart it runs
// the restart contract, re-entering it if the fresh instance's
// Start/Init panics, until either a restart succeeds (returns true) or
// the budget is exhausted and OnGiveUp fires (returns false). For Stop
// and Escalate it tears the protocol down once (returns false).
//
// The restart contract, executed by runRestart below, is:
//
//  1. Quarantine: enqueue drops every further event to dead-letter
//     (non-blocking, kind preserved) and the old mailbox is drained to
//     dead-letter. The old event loop is stopped and awaited.
//  2. Cancel every timer the old instance owns.
//  3. Auto-fail every pending SendRequest with ErrProtocolRestarting.
//  4. Deregister everything the old instance owns: message codecs/
//     routes, request-handler routes, notification subscriptions.
//  5. Wait out the backoff (runtime clock, cancellable by shutdown).
//  6. Build a fresh instance with a fresh mailbox, run Start, lift
//     quarantine, start the new loop, run Init.
//  7. Replay: deliver a synthetic SessionConnected through the new
//     mailbox for every currently-established peer.
//  8. Invoke the optional RestartHandler on the new instance's loop.
func (s *supervisor) handle(ctx context.Context, sig panicSignal) bool {
	switch s.spec.OnPanic {
	case Restart:
		return s.runRestart(ctx, sig)
	case Stop:
		s.stop()
		return false
	case Escalate:
		s.escalate(sig)
		return false
	default:
		// Resume never reaches a supervisor (no supervisor is created
		// for it); treat any unknown directive as Resume and keep the
		// instance running.
		return true
	}
}

// runRestart drives the restart contract, re-entering it when the
// fresh instance's Start/Init panics. Each panic (the original and any
// during rebuild) counts against the sliding-window budget; exceeding
// it triggers OnGiveUp.
func (s *supervisor) runRestart(ctx context.Context, sig panicSignal) bool {
	for {
		count := s.recordPanic()
		if count > s.spec.MaxRestarts {
			s.giveUp(sig)
			return false
		}
		attempt := count

		s.quarantineAndClean() // steps 1–4
		if !s.backoff(ctx, attempt) {
			return false // runtime shutting down
		}
		if rebuiltSig, ok := s.rebuild(); !ok { // steps 6 (Start/Init)
			sig = rebuiltSig // the fresh instance panicked; re-enter
			continue
		}
		s.finishRestart(attempt) // steps 7–8 + observability
		return true
	}
}

// quarantineAndClean performs steps 1–4 of the restart contract: it
// quarantines the protocol (further enqueues dead-letter), stops and
// awaits the old event loop, empties the old mailbox, cancels the old
// timers, fails pending requests, and deregisters everything the old
// instance owned. Safe to call repeatedly.
func (s *supervisor) quarantineAndClean() {
	p := s.proto
	p.quarantined.Store(true)
	p.stopLoop()
	p.drainMailboxToDeadLetter()
	p.cancelAllTimers()
	p.failPendingRestarting()
	s.runtime.codecs.RemoveOwner(p)
	s.runtime.ipc.RemoveOwner(p)
}

// backoff waits out the delay for this attempt on the runtime clock,
// returning false if the runtime shuts down first. A non-positive
// delay returns immediately.
func (s *supervisor) backoff(ctx context.Context, attempt int) bool {
	d := s.spec.Backoff(attempt)
	if d <= 0 {
		return true
	}
	done := make(chan struct{})
	timer := s.runtime.clock.AfterFunc(d, func() { close(done) })
	select {
	case <-done:
		return true
	case <-ctx.Done():
		timer.Stop()
		return false
	}
}

// rebuild performs step 6: build a fresh instance, run Start, lift
// quarantine, start the new loop, run Init. It returns ok=false and
// the panic signal if Start or Init panics, leaving the protocol ready
// for another restart iteration.
func (s *supervisor) rebuild() (panicSignal, bool) {
	p := s.proto
	p.resetForRestart(p.factory())

	if rec, panicked := p.callStart(); panicked {
		// Start never ran the loop, so nothing to unwind beyond the
		// quarantine (still set) — the next iteration cleans up.
		return panicSignal{where: "Start", rec: rec}, false
	}

	// Lift quarantine and start the fresh loop before Init so replies
	// to any request Init issues land on the new loop.
	p.quarantined.Store(false)
	p.startLoop(s.runtime.ctx, &s.runtime.wg)

	if rec, panicked := p.callInit(); panicked {
		return panicSignal{where: "Init", rec: rec}, false
	}
	return panicSignal{}, true
}

// finishRestart performs steps 7–8 and publishes observability. Called
// only after a rebuild whose Start and Init both returned cleanly.
func (s *supervisor) finishRestart(attempt int) {
	p := s.proto

	// Step 7: replay established peers through the new mailbox so the
	// fresh instance rebuilds peer state through the same code path it
	// used at boot. Sessions themselves are runtime-owned and were
	// never torn down.
	for _, host := range s.runtime.snapshotEstablished() {
		p.enqueue(s.runtime.ctx, protoEvent{
			kind: evSession,
			aux:  &eventAux{session: sessionEvent{kind: sessionConnectedEvent, host: host}},
		})
	}

	// Step 8: RestartHandler on the new instance, on its loop, after
	// replay (FIFO ordering guarantees "after").
	if rh, ok := p.protocol.(RestartHandler); ok {
		p.enqueue(s.runtime.ctx, protoEvent{
			kind: evCallback,
			aux:  &eventAux{run: func() { rh.OnRestart(attempt) }},
		})
	}

	s.runtime.metrics.Counter("protorun.protocol.restart", 1,
		Attr{Key: "protocol", Value: p.name},
		Attr{Key: "outcome", Value: "restarted"})
	s.runtime.publishProtocolFailed(p.name, "restarted", attempt)
	s.runtime.Logger().Warn("protorun: protocol restarted",
		"protocol", p.name, "attempt", attempt)
}

// giveUp is reached when the restart budget is exhausted. It applies
// OnGiveUp: Stop removes the protocol, Escalate additionally records a
// fatal error and cancels the runtime.
func (s *supervisor) giveUp(sig panicSignal) {
	if s.spec.OnGiveUp == Escalate {
		s.escalate(sig)
		return
	}
	s.stop()
}

// stop implements the Stop outcome: quarantine + deregister + remove
// the protocol permanently, then announce it.
func (s *supervisor) stop() {
	p := s.proto
	s.quarantineAndClean()
	s.runtime.removeProtocol(p)
	s.runtime.metrics.Counter("protorun.protocol.restart", 1,
		Attr{Key: "protocol", Value: p.name},
		Attr{Key: "outcome", Value: "stopped"})
	s.runtime.publishProtocolFailed(p.name, "stopped", len(s.times))
	s.runtime.Logger().Error("protorun: protocol stopped by supervisor", "protocol", p.name)
}

// escalate implements the Escalate outcome: quarantine + deregister +
// remove, record the fatal error, and cancel the runtime so Run
// returns ErrProtocolFailed. The notification is published before the
// runtime is cancelled so subscribers still on their loops have a
// chance to see it.
func (s *supervisor) escalate(sig panicSignal) {
	p := s.proto
	s.quarantineAndClean()
	s.runtime.removeProtocol(p)
	s.runtime.metrics.Counter("protorun.protocol.restart", 1,
		Attr{Key: "protocol", Value: p.name},
		Attr{Key: "outcome", Value: "escalated"})
	s.runtime.publishProtocolFailed(p.name, "escalated", len(s.times))
	s.runtime.Logger().Error("protorun: protocol escalated; cancelling runtime",
		"protocol", p.name, "where", sig.where, "recovered", fmt.Sprintf("%v", sig.rec))
	s.runtime.escalate(p.name, fmt.Sprintf("%s: %v", sig.where, sig.rec))
}

// recordPanic appends the current time, prunes entries older than the
// sliding Window, and returns the count within the window. Called once
// per restart iteration (each iteration corresponds to one panic).
func (s *supervisor) recordPanic() int {
	now := s.runtime.clock.Now()
	s.times = append(s.times, now)
	cutoff := now.Add(-s.spec.Window)
	kept := s.times[:0]
	for _, t := range s.times {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	s.times = kept
	return len(s.times)
}

// --- protoProtocol supervision helpers ---

// stopLoop cancels the current event-loop incarnation and waits for it
// to exit, so the supervisor can safely swap in a fresh mailbox and
// instance. A no-op if no loop is running (Start panicked before the
// loop started).
func (p *protoProtocol) stopLoop() {
	if p.loopCancel != nil {
		p.loopCancel()
	}
	if p.loopDone != nil {
		<-p.loopDone
	}
	p.loopCancel = nil
	p.loopDone = nil
}

// drainMailboxToDeadLetter empties the (quarantined) mailbox, routing
// every queued event to the dead-letter hook. The old loop has already
// exited, so nothing else is draining.
func (p *protoProtocol) drainMailboxToDeadLetter() {
	drained := p.currentMailbox().drain()
	for _, ev := range drained {
		p.deadLetterEvent(ev)
	}
	// These events were counted on enqueue and will never be dispatched;
	// release their inFlight count so Quiescent stays accurate.
	if n := len(drained); n > 0 {
		p.inFlight.Add(-int64(n))
	}
}

// deadLetterEvent counts a dropped event and hands it to the runtime's
// dead-letter hook. Used both by the quarantine branch of enqueue and
// by drainMailboxToDeadLetter, so a restarting protocol never blocks a
// producer and never silently swallows a queued event.
func (p *protoProtocol) deadLetterEvent(ev protoEvent) {
	p.runtime.metrics.Counter("protorun.mailbox.dropped", 1,
		Attr{Key: "protocol", Value: p.name},
		Attr{Key: "kind", Value: ev.kind.String()},
		Attr{Key: "policy", Value: "quarantine"})
	p.runtime.emitDeadLetter(DeadLetter{
		Protocol: p.name,
		Kind:     ev.kind.String(),
		Peer:     ev.peer(),
	})
}

// failPendingRestarting fails every outstanding SendRequest with
// ErrProtocolRestarting. Routing choice: the callbacks are invoked
// directly on the supervisor goroutine, NOT routed through the new
// instance's loop. They belong to the dead instance (they close over
// its state); the fresh instance has never heard of these requests, so
// delivering them to its loop would run stale closures against the
// wrong receiver. The old loop is already gone, so nothing else races
// on that state. Each callback is recovered so a panicking failure
// callback cannot take down the supervisor.
func (p *protoProtocol) failPendingRestarting() {
	p.pendingMu.Lock()
	pend := p.pending
	p.pending = make(map[uint64]pendingRequest)
	p.pendingMu.Unlock()
	for _, pr := range pend {
		cb := pr.cb
		func() {
			defer func() { _ = recover() }()
			cb(nil, ErrProtocolRestarting)
		}()
	}
}

// resetForRestart swaps in the fresh instance and a fresh mailbox and
// clears the per-instance tables, readying p for a new Start/Init. Safe
// because it runs while quarantined (enqueue short-circuits before
// touching p.mailbox) and after the old loop has exited.
func (p *protoProtocol) resetForRestart(instance Protocol) {
	p.protocol = instance
	p.setMailbox(newMailbox(p.mailboxCfg))
	p.handlers = make(map[uint64]func(Message, transport.Host))
	p.pendingMu.Lock()
	p.pending = make(map[uint64]pendingRequest)
	p.pendingMu.Unlock()
	p.timersMu.Lock()
	p.timers = make(map[uint64]*timerHandle)
	p.timersMu.Unlock()
	p.exitLoop.Store(false)
	p.setPhase(phaseUnstarted)
}

// callStart runs the fresh instance's Start under recover, reporting a
// panic (with a meaningful stack) and returning it rather than
// propagating. Used only on the restart path; boot uses the inline
// paths in Start.
func (p *protoProtocol) callStart() (rec any, panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			rec, panicked = r, true
			p.reportPanic("Start", r, debug.Stack())
		}
	}()
	p.setPhase(phaseRegistering)
	p.protocol.Start(p.ctx)
	p.setPhase(phaseRegistered)
	return nil, false
}

// callInit runs the fresh instance's Init under recover, with the same
// reporting contract as callStart.
func (p *protoProtocol) callInit() (rec any, panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			rec, panicked = r, true
			p.reportPanic("Init", r, debug.Stack())
		}
	}()
	p.setPhase(phaseInitializing)
	p.protocol.Init(p.ctx)
	p.setPhase(phaseRunning)
	return nil, false
}
