package protorun

import "time"

// Clock is the seam through which the runtime tells time. Everything
// the runtime schedules — the timer table (After/Every), retry
// backoff, request-timeout arming, the strict-mode slow-handler
// watchdog — and every wall-clock reading (IPC latency) flows through
// a Clock. The default is realClock, a zero-allocation wrapper over
// the time package; tests inject a virtual clock (see
// prototest.FakeClock) to make timer order fully deterministic.
//
// Implementations MUST be safe for concurrent use: the runtime calls
// Clock methods from many goroutines.
type Clock interface {
	// Now returns the current time.
	Now() time.Time

	// AfterFunc schedules fn to run in its own goroutine after d
	// elapses. The returned ClockTimer can Stop the pending fire.
	AfterFunc(d time.Duration, fn func()) ClockTimer

	// NewTicker returns a ticker that delivers the time on its channel
	// every d until stopped.
	NewTicker(d time.Duration) ClockTicker
}

// ClockTimer is a pending one-shot scheduled via Clock.AfterFunc. Stop
// prevents the fire if it has not happened yet, reporting whether it
// did so (mirrors time.Timer.Stop).
type ClockTimer interface {
	Stop() bool
}

// ClockTicker is a recurring tick source from Clock.NewTicker. C is the
// delivery channel; Stop halts further ticks (but does not close C).
type ClockTicker interface {
	C() <-chan time.Time
	Stop()
}

// realClock is the production Clock: a thin, allocation-free adapter
// over the time package. It is the default when no WithClock option is
// supplied.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) AfterFunc(d time.Duration, fn func()) ClockTimer {
	return realTimer{t: time.AfterFunc(d, fn)}
}

func (realClock) NewTicker(d time.Duration) ClockTicker {
	return realTicker{t: time.NewTicker(d)}
}

type realTimer struct{ t *time.Timer }

func (r realTimer) Stop() bool { return r.t.Stop() }

type realTicker struct{ t *time.Ticker }

func (r realTicker) C() <-chan time.Time { return r.t.C }
func (r realTicker) Stop()               { r.t.Stop() }

// WithClock overrides the runtime's Clock. Pass a nil clock (or omit
// the option) to keep the real-time default. See Clock.
func WithClock(c Clock) Option {
	return func(r *Runtime) {
		if c != nil {
			r.clock = c
		}
	}
}
