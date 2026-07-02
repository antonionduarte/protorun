package protorun

import (
	"fmt"
	"time"
)

// Strict mode is an opt-in set of runtime invariant checks: behaviors
// the type system can't enforce but the framework can detect at run
// time and surface loudly. Enable with WithStrict(true). The default
// is permissive (counters + warn-level logs only) so production
// deployments pay nothing.
//
// What strict mode catches:
//
//   - Double registration of a wireID for codec / handler /
//     request handler. Panics at the offending call site.
//   - Phase ordering: registration calls (RegisterCodec et al.) only
//     inside Start; active calls (Connect, Send, SendRequest,
//     PublishNotification) only inside or after Init. Violations
//     panic.
//   - Slow handlers: a per-event-loop watchdog logs (and counts) when
//     a single handler invocation exceeds the configured threshold
//     (default 5s, override via WithStrictHandlerTimeout).
//
// Calls landing in the wrong phase that the framework can recover
// from gracefully (e.g. Connect after Cancel) are not strict-mode
// failures; they return their normal sentinel errors.

// protoPhase is the per-protocol lifecycle phase. Tracked via
// atomic.Int32 because reads happen on many goroutines (the protocol's
// own event loop, the runtime's main goroutine, user goroutines that
// call ctx.Send / SendRequest / etc.).
type protoPhase int32

const (
	phaseUnstarted    protoPhase = 0 // bound to runtime, not yet started
	phaseRegistering  protoPhase = 1 // inside protocol.Start
	phaseRegistered   protoPhase = 2 // protocol.Start returned, before protocol.Init
	phaseInitializing protoPhase = 3 // inside protocol.Init
	phaseRunning      protoPhase = 4 // protocol.Init returned, event loop running
	phaseCancelled    protoPhase = 5 // runtime.Cancel observed
)

func (p protoPhase) String() string {
	switch p {
	case phaseUnstarted:
		return "unstarted"
	case phaseRegistering:
		return "registering"
	case phaseRegistered:
		return "registered"
	case phaseInitializing:
		return "initializing"
	case phaseRunning:
		return "running"
	case phaseCancelled:
		return "cancelled"
	}
	return fmt.Sprintf("unknown(%d)", int32(p))
}

// defaultStrictHandlerTimeout is the watchdog threshold for "this
// handler has been running too long". Picked as a generous upper
// bound; well-behaved handlers complete in microseconds.
const defaultStrictHandlerTimeout = 5 * time.Second

// WithStrict enables runtime invariant checks that the type system
// can't express. See the package doc on strict.go for the full list.
// Off by default; enabling adds atomic loads to dispatch hot paths
// (cheap) and arms a per-handler watchdog (one time.AfterFunc per
// dispatch, .Stop()'d on completion).
func WithStrict(strict bool) Option {
	return func(r *Runtime) {
		r.strict = strict
		if r.strictHandlerTimeout == 0 {
			r.strictHandlerTimeout = defaultStrictHandlerTimeout
		}
	}
}

// WithStrictHandlerTimeout overrides the slow-handler watchdog
// threshold. Only effective when WithStrict(true) is also set. A
// non-positive value disables the watchdog.
func WithStrictHandlerTimeout(d time.Duration) Option {
	return func(r *Runtime) {
		r.strictHandlerTimeout = d
	}
}

// strictPanic centralises strict-mode panic messages so they all carry
// the same prefix and the user can grep for "protorun strict:".
func strictPanic(format string, args ...any) {
	panic("protorun strict: " + fmt.Sprintf(format, args...))
}

// loadPhase reads the protocol's current phase atomically.
func (p *protoProtocol) loadPhase() protoPhase {
	return protoPhase(p.phase.Load())
}

// setPhase atomically stores the new phase. Lifecycle code (Start,
// Init, eventHandler exit, Cancel) calls this; everything else only
// reads.
func (p *protoProtocol) setPhase(ph protoPhase) {
	p.phase.Store(int32(ph))
}

// requireRegisterPhase panics in strict mode if registration is
// attempted outside the Start window. Cheap no-op when strict is off.
func (p *protoProtocol) requireRegisterPhase(action string) {
	if p.runtime == nil || !p.runtime.strict {
		return
	}
	if cur := p.loadPhase(); cur != phaseRegistering {
		strictPanic("%s must be called from Start(ctx); current phase=%s", action, cur)
	}
}

// requireActivePhase panics in strict mode if an active call (Connect,
// Send, SendRequest, PublishNotification) lands before Init has run
// (or before the protocol is bound to a runtime).
func (p *protoProtocol) requireActivePhase(action string) {
	if p.runtime == nil || !p.runtime.strict {
		return
	}
	cur := p.loadPhase()
	if cur < phaseInitializing {
		strictPanic("%s must be called from Init(ctx) or later; current phase=%s",
			action, cur)
	}
}

// strictWatchdog arms a watchdog timer for the supplied handler if
// strict mode is on with a positive timeout. The returned stop func
// cancels the watchdog (no-op if it already fired). It is safe to
// call Stop after the watchdog fired; the underlying time.AfterFunc
// is reentrant.
func (p *protoProtocol) strictWatchdog(where string) func() {
	if p.runtime == nil || !p.runtime.strict || p.runtime.strictHandlerTimeout <= 0 {
		return func() {}
	}
	threshold := p.runtime.strictHandlerTimeout
	timer := time.AfterFunc(threshold, func() {
		p.runtime.metrics.Counter("protorun.strict.slow_handler", 1,
			Attr{Key: "where", Value: where},
			Attr{Key: "protocol", Value: fmt.Sprintf("%T", p.protocol)})
		p.runtime.Logger().Error("protorun strict: handler exceeded threshold",
			"where", where,
			"threshold", threshold,
			"protocol", fmt.Sprintf("%T", p.protocol))
	})
	return func() { timer.Stop() }
}

// strictReplyWithoutHandler is invoked by deliverReply when an inbound
// reply lands without a matching pending entry. In strict mode it logs
// at warning level so the operator notices; non-strict it stays a
// counter increment only.
func (p *protoProtocol) strictReplyWithoutHandler() {
	if p.runtime == nil || !p.runtime.strict {
		return
	}
	p.runtime.Logger().Warn("protorun strict: reply landed with no pending request",
		"protocol", fmt.Sprintf("%T", p.protocol))
}
