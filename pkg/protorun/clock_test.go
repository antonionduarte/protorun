package protorun

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/transport"
)

// manualClock is an in-package test Clock (prototest.FakeClock can't be
// imported here without a cycle). It advances only via Advance, which
// fires every due AfterFunc callback.
type manualClock struct {
	mu      sync.Mutex
	now     time.Time
	entries []*manualTimer
}

type manualTimer struct {
	clock    *manualClock
	deadline time.Time
	fn       func()
	stopped  bool
}

func newManualClock() *manualClock {
	return &manualClock{now: time.Unix(0, 0)}
}

func (m *manualClock) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

func (m *manualClock) AfterFunc(d time.Duration, fn func()) ClockTimer {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := &manualTimer{clock: m, deadline: m.now.Add(d), fn: fn}
	m.entries = append(m.entries, t)
	return t
}

func (m *manualClock) Advance(d time.Duration) {
	m.mu.Lock()
	m.now = m.now.Add(d)
	var due []*manualTimer
	kept := m.entries[:0]
	for _, t := range m.entries {
		if !t.stopped && !t.deadline.After(m.now) {
			t.stopped = true
			due = append(due, t)
		} else if !t.stopped {
			kept = append(kept, t)
		}
	}
	m.entries = kept
	m.mu.Unlock()
	for _, t := range due {
		t.fn()
	}
}

func (t *manualTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	if t.stopped {
		return false
	}
	t.stopped = true
	return true
}

// TestWithClock_DrivesRequestTimeout verifies WithClock swaps the
// runtime's time source and that the request-timeout path reads it: no
// real time passes, yet advancing the fake clock past the timeout
// surfaces ErrRequestTimeout.
func TestWithClock_DrivesRequestTimeout(t *testing.T) {
	self := transport.NewHost(0, "127.0.0.1")
	clock := newManualClock()
	p := &timeoutProtocol{result: make(chan error, 1)}

	rt := New(self, WithClock(clock), WithDefaultRequestTimeout(time.Second))
	_ = registerMockStack(rt, self)
	rt.Register(p)
	if err := rt.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer rt.Cancel()

	// The request was armed in Init; nothing fires until we advance.
	select {
	case err := <-p.result:
		t.Fatalf("request completed before the clock advanced: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	clock.Advance(2 * time.Second)

	select {
	case err := <-p.result:
		if !errors.Is(err, ErrRequestTimeout) {
			t.Fatalf("expected ErrRequestTimeout, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("advancing the fake clock did not fire the timeout")
	}
}
