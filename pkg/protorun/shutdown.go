package protorun

import (
	"errors"
	"time"
)

// ErrShutdownTimeout is returned by Shutdown when graceful teardown
// did not complete within the supplied deadline. The runtime has
// still been cancelled (Cancel ran), but some goroutines may not
// have observed the cancellation in time.
var ErrShutdownTimeout = errors.New("protorun: shutdown timeout")

// Shutdown is the bounded-wait alternative to Cancel. It triggers
// the same teardown as Cancel (context cancellation, transport /
// session layer Cancel, timer cleanup, retry-table teardown), but
// returns ErrShutdownTimeout if the WaitGroup that tracks every
// runtime goroutine has not drained within the supplied timeout.
//
// Use Shutdown from main when you want bounded shutdown latency
// (e.g. a SIGINT handler that should never block longer than the
// platform's grace period). Use Cancel when you don't care how long
// teardown takes; it blocks unconditionally.
//
// A non-positive timeout falls back to defaultShutdownTimeout. Once
// Shutdown is invoked, calling it again or calling Cancel is a
// no-op; the underlying ctx is already cancelled.
func (r *Runtime) Shutdown(timeout time.Duration) error {
	if timeout <= 0 {
		timeout = defaultShutdownTimeout
	}

	done := make(chan struct{})
	go func() {
		r.Cancel()
		close(done)
	}()

	select {
	case <-done:
		// If a supervisor escalated, surface that as the shutdown cause
		// rather than a clean nil — teardown completed, but the runtime
		// died because a protocol failed terminally.
		if ferr := r.fatalError(); ferr != nil {
			return ferr
		}
		return nil
	case <-time.After(timeout):
		return ErrShutdownTimeout
	}
}

// defaultShutdownTimeout is the fallback for Shutdown(0). Picked as
// a generous upper bound for "the ctx-driven goroutines should have
// observed the cancel by now".
const defaultShutdownTimeout = 30 * time.Second
