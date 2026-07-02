package protorun

import (
	"math"
	"math/rand/v2"
	"time"

	"github.com/antonionduarte/go-simple-protocol-runtime/pkg/transport"
)

// RetryPolicy controls the backoff schedule used by ConnectWithRetry.
//
// All zero values are valid; New defaults are applied via the
// withDefaults helper at policy use time:
//   - Initial: 100ms
//   - Max:     30s
//   - Multiplier: 2.0
//   - MaxAttempts: 0 (unbounded)
//   - Jitter: 0.2
type RetryPolicy struct {
	Initial     time.Duration // first delay before the second attempt
	Max         time.Duration // upper bound on any single delay
	Multiplier  float64       // backoff factor applied after each failure
	MaxAttempts int           // 0 = unbounded; otherwise emit SessionGivenUp after N
	Jitter      float64       // [0..1] fraction of delay randomized symmetrically
}

func (p RetryPolicy) withDefaults() RetryPolicy {
	if p.Initial <= 0 {
		p.Initial = 100 * time.Millisecond
	}
	if p.Max <= 0 {
		p.Max = 30 * time.Second
	}
	if p.Multiplier <= 1 {
		p.Multiplier = 2.0
	}
	if p.Jitter < 0 {
		p.Jitter = 0
	}
	if p.Jitter > 1 {
		p.Jitter = 1
	}
	return p
}

// nextDelay returns the next backoff delay for the given attempt count
// (0-indexed: 0 is the delay before the second attempt).
func (p RetryPolicy) nextDelay(attempt int) time.Duration {
	d := float64(p.Initial) * math.Pow(p.Multiplier, float64(attempt))
	if d > float64(p.Max) {
		d = float64(p.Max)
	}
	if p.Jitter > 0 {
		// symmetric jitter: [d * (1 - j/2), d * (1 + j/2)].
		// math/rand/v2 is appropriate here: this is reconnect timing,
		// not security-sensitive randomness.
		j := p.Jitter
		factor := 1 - j/2 + rand.Float64()*j //nolint:gosec // jitter is timing variation, not crypto
		d *= factor
	}
	return time.Duration(d)
}

// WithRetryPolicy sets the runtime's default RetryPolicy used by
// ConnectWithRetry calls. Without this option, defaults are applied
// (see RetryPolicy.withDefaults).
func WithRetryPolicy(p RetryPolicy) Option {
	return func(r *Runtime) { r.retryPolicy = p }
}

// SessionGivenUpHandler is implemented by a protocol that wants to be
// notified when ConnectWithRetry exhausts its policy without succeeding.
type SessionGivenUpHandler interface {
	OnSessionGivenUp(host transport.Host, attempts int)
}

// retryState tracks an in-flight retry schedule for one peer.
type retryState struct {
	policy  RetryPolicy
	attempt int         // number of failed attempts so far
	timer   *time.Timer // the pending backoff timer, if any
}

// connectWithRetry registers retry intent for host and issues the
// initial connect. Subsequent SessionFailed / SessionDisconnected events
// on this host will reschedule a connect using the configured
// RetryPolicy. Returns the same kind of validation errors as Connect;
// transport-level failures surface asynchronously through events.
func (r *Runtime) connectWithRetry(host transport.Host) error {
	r.retryMu.Lock()
	if _, exists := r.connectionRetries[host]; exists {
		r.retryMu.Unlock()
		return nil // already tracked; do nothing
	}
	r.connectionRetries[host] = &retryState{policy: r.retryPolicy.withDefaults()}
	r.retryMu.Unlock()
	return r.connect(host)
}

// onSessionUpForRetry clears retry state when a session has been
// successfully established. Returns true if state was cleared.
func (r *Runtime) onSessionUpForRetry(host transport.Host) bool {
	r.retryMu.Lock()
	defer r.retryMu.Unlock()
	st, ok := r.connectionRetries[host]
	if !ok {
		return false
	}
	if st.timer != nil {
		st.timer.Stop()
	}
	delete(r.connectionRetries, host)
	return true
}

// onSessionDownForRetry handles a SessionFailed / SessionDisconnected for
// a tracked host: increments the attempt count and either schedules the
// next backoff or emits SessionGivenUp if the policy is exhausted.
// Returns true if the runtime should suppress the standard fanout for
// this event (when a retry is scheduled, protocols don't need to see
// every transient SessionFailed).
//
// The current implementation does NOT suppress fanout: it simply
// schedules a retry alongside whatever protocols receive. Returning
// false keeps fanout enabled.
func (r *Runtime) onSessionDownForRetry(host transport.Host) (giveUp bool, attempts int) {
	r.retryMu.Lock()
	defer r.retryMu.Unlock()
	st, ok := r.connectionRetries[host]
	if !ok {
		return false, 0
	}
	if st.timer != nil {
		st.timer.Stop()
		st.timer = nil
	}
	st.attempt++
	if st.policy.MaxAttempts > 0 && st.attempt >= st.policy.MaxAttempts {
		// Exhausted. Clear state and signal give-up.
		attempts = st.attempt
		delete(r.connectionRetries, host)
		return true, attempts
	}
	delay := st.policy.nextDelay(st.attempt - 1)
	r.Logger().Debug("scheduling reconnect",
		"host", host.String(),
		"attempt", st.attempt,
		"delay", delay,
	)
	st.timer = time.AfterFunc(delay, func() {
		// Re-check state on fire: another caller may have cleared us.
		r.retryMu.Lock()
		_, stillTracked := r.connectionRetries[host]
		r.retryMu.Unlock()
		if !stillTracked {
			return
		}
		if r.ctx.Err() != nil {
			return
		}
		if err := r.connect(host); err != nil {
			r.Logger().Debug("retry connect failed", "host", host.String(), "err", err)
		}
	})
	return false, 0
}

// giveUpRetryNow terminates any retry schedule for host immediately,
// returning the number of attempts made so far. Used for terminal
// failures (e.g. a handshake Reject) where further dialing cannot
// succeed regardless of the remaining budget.
func (r *Runtime) giveUpRetryNow(host transport.Host) (attempts int) {
	r.retryMu.Lock()
	defer r.retryMu.Unlock()
	st, ok := r.connectionRetries[host]
	if !ok {
		return 0
	}
	if st.timer != nil {
		st.timer.Stop()
	}
	delete(r.connectionRetries, host)
	return st.attempt
}

// stopRetryFor cancels any scheduled retry for host. Called when the user
// explicitly disconnects.
func (r *Runtime) stopRetryFor(host transport.Host) {
	r.retryMu.Lock()
	defer r.retryMu.Unlock()
	if st, ok := r.connectionRetries[host]; ok {
		if st.timer != nil {
			st.timer.Stop()
		}
		delete(r.connectionRetries, host)
	}
}

// retryTeardown is called by Runtime.Cancel to stop any in-flight retry
// timers. The runtime context cancellation alone wouldn't stop them
// (time.AfterFunc is detached from ctx).
func (r *Runtime) retryTeardown() {
	r.retryMu.Lock()
	defer r.retryMu.Unlock()
	for host, st := range r.connectionRetries {
		if st.timer != nil {
			st.timer.Stop()
		}
		delete(r.connectionRetries, host)
	}
}
