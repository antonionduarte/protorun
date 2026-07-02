package protorun

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
)

// Supervision is the loudest gap protorun closes versus every actor
// framework: a panicking handler no longer just gets recovered and
// logged while the protocol limps on with half-mutated state. A
// protocol registered with a Supervision policy hands control to a
// runtime-side supervisor on panic, which applies the configured
// Directive off the event loop.
//
// Configure it at registration through WithSupervision, on either
// Register (singleton) or RegisterFactory (fresh instance per
// restart):
//
//	rt.RegisterFactory(newGossip, protorun.WithSupervision(protorun.Supervision{
//	    OnPanic:     protorun.Restart,
//	    MaxRestarts: 5,
//	    Window:      time.Minute,
//	    Backoff:     protorun.ExpBackoff(100*time.Millisecond, 5*time.Second),
//	    OnGiveUp:    protorun.Escalate,
//	}))
//
// The default (no WithSupervision, or OnPanic: Resume) is exactly
// today's behavior: recover, log, fire PanicHandler, keep the loop
// running.

// Directive selects what the runtime does when a supervised
// protocol's handler (or its Start/Init) panics.
type Directive int

const (
	// Resume recovers the panic, logs it, fires the optional
	// PanicHandler, and lets the same instance keep running — today's
	// behavior and the default. The protocol keeps whatever state it
	// had when the handler panicked.
	Resume Directive = iota

	// Restart discards the panicking instance and builds a fresh one
	// from the registered factory (see RegisterFactory). Fresh state
	// is the whole point, so Restart requires a factory. See the
	// numbered restart contract in supervisor.go.
	Restart

	// Stop discards the panicking instance and removes the protocol
	// from the runtime permanently. Its wire ids stop routing (they
	// fall through to the unknown-wireID path).
	Stop

	// Escalate records a fatal error and cancels the whole runtime;
	// Run returns an ErrProtocolFailed-wrapped error. Use it when a
	// protocol's failure means the process can no longer do its job.
	Escalate
)

func (d Directive) String() string {
	switch d {
	case Resume:
		return "resume"
	case Restart:
		return "restart"
	case Stop:
		return "stop"
	case Escalate:
		return "escalate"
	}
	return fmt.Sprintf("directive(%d)", int(d))
}

// Sentinel errors for the supervision surface. Test with errors.Is.
var (
	// ErrProtocolRestarting is delivered to every pending SendRequest
	// callback of a protocol that is being restarted. The old
	// instance's outstanding asks cannot be answered — its state is
	// being thrown away — so they fail terminally with this instead of
	// hanging until their timeouts.
	ErrProtocolRestarting = errors.New("protorun: protocol restarting; pending request auto-failed")

	// ErrProtocolFailed is returned (wrapped) by Run when the runtime
	// was shut down because a supervised protocol escalated. Unwrap /
	// errors.Is to detect it; the wrapped text names the protocol and
	// describes the panic.
	ErrProtocolFailed = errors.New("protorun: protocol failed")
)

// BackoffFunc computes the delay before the Nth restart attempt.
// attempt starts at 1 for the first restart. Supply your own to
// WithSupervision, or use ExpBackoff. Called on the supervisor
// goroutine; it must not block.
type BackoffFunc func(attempt int) time.Duration

// ExpBackoff returns a BackoffFunc that doubles from base, capping at
// max, with a little jitter (up to +25%) so a fleet restarting in
// lockstep spreads its retries out. attempt 1 yields ~base, attempt 2
// ~2*base, and so on, never exceeding max+jitter.
func ExpBackoff(base, max time.Duration) BackoffFunc {
	return func(attempt int) time.Duration {
		if attempt < 1 {
			attempt = 1
		}
		d := base
		// Double (attempt-1) times, saturating at max. Shift-based
		// doubling would overflow for large attempts; the explicit
		// loop with an early cap avoids it.
		for i := 1; i < attempt; i++ {
			d *= 2
			if d <= 0 || d >= max {
				d = max
				break
			}
		}
		if d > max {
			d = max
		}
		if d > 0 {
			d += rand.N(d/4 + 1) //nolint:gosec // jitter is timing variation, not crypto
		}
		return d
	}
}

// Supervision is a protocol's restart policy. The zero value means
// OnPanic: Resume (no supervision). Only OnPanic must be set; the
// rest default (MaxRestarts 5, Window 1m, Backoff
// ExpBackoff(100ms, 5s), OnGiveUp Stop).
type Supervision struct {
	// OnPanic is the directive applied when the protocol panics.
	OnPanic Directive

	// MaxRestarts is the number of restarts tolerated within Window
	// before OnGiveUp fires. Non-positive means the default (5).
	MaxRestarts int

	// Window is the sliding window over which MaxRestarts is counted.
	// Non-positive means the default (1 minute).
	Window time.Duration

	// Backoff computes the delay before each restart attempt. nil
	// means the default ExpBackoff(100ms, 5s).
	Backoff BackoffFunc

	// OnGiveUp is applied when the restart budget is exhausted. Only
	// Stop and Escalate are meaningful; anything else is treated as
	// Stop (the default).
	OnGiveUp Directive
}

// withDefaults fills the zero fields of a supervision policy. Called
// once at registration for any protocol whose OnPanic is not Resume.
func (s Supervision) withDefaults() Supervision {
	if s.MaxRestarts <= 0 {
		s.MaxRestarts = 5
	}
	if s.Window <= 0 {
		s.Window = time.Minute
	}
	if s.Backoff == nil {
		s.Backoff = ExpBackoff(100*time.Millisecond, 5*time.Second)
	}
	if s.OnGiveUp != Escalate {
		s.OnGiveUp = Stop
	}
	return s
}

// RestartHandler is an optional interface a protocol can implement to
// learn that it is a freshly-restarted instance. OnRestart is invoked
// on the new instance through its own event loop after session replay
// (see the restart contract), so it may touch protocol state without
// locking. attempt is the restart number (1 for the first).
type RestartHandler interface {
	OnRestart(attempt int)
}

// ProtocolFailed is published as a runtime notification whenever a
// supervised protocol's failure resolves to a terminal-ish outcome:
// it was restarted, stopped, or escalated. Sibling protocols can
// SubscribeNotification[ProtocolFailed] to react (e.g. drop cached
// peer state that the failed protocol fed them). Like every
// notification it is local-only and needs no codec.
type ProtocolFailed struct {
	BaseNotification

	// Protocol is the concrete protocol type formatted as %T.
	Protocol string

	// Outcome is one of "restarted", "stopped", "escalated".
	Outcome string

	// Attempt is the restart attempt count at the time of the outcome.
	Attempt int
}

// WithSupervision sets the registering protocol's supervision policy.
// It is a RegisterOption, valid on both Register and RegisterFactory.
// Restart requires a factory: on a singleton Register it is a
// registration-time strict-mode panic, or a warn-and-downgrade-to-
// Resume in non-strict mode.
func WithSupervision(s Supervision) RegisterOption {
	return func(c *registerConfig) {
		c.supervision = s
		c.hasSupervision = true
	}
}
