package protorun

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/transport"
)

// startTimerRuntime stands up a started runtime with one MockProtocol
// whose event loop is running, so timers armed via rt.after / rt.every
// actually fire their callbacks through the mailbox. Cancel runs on
// t.Cleanup.
func startTimerRuntime(t *testing.T) (*Runtime, *protoProtocol) {
	t.Helper()
	self := transport.NewHost(0, "127.0.0.1")
	rt := New(self)
	_ = registerMockStack(rt, self)
	proto := newProtoProtocol(&MockProtocol{}, 0)
	rt.registerProtocol(proto)
	if err := rt.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(rt.Cancel)
	return rt, proto
}

// TestTimer_AfterFires verifies a one-shot callback runs once on the
// owning protocol's event loop.
func TestTimer_AfterFires(t *testing.T) {
	rt, proto := startTimerRuntime(t)

	fired := make(chan struct{}, 1)
	rt.after(proto, 5*time.Millisecond, func() { fired <- struct{}{} })

	select {
	case <-fired:
	case <-time.After(time.Second):
		t.Fatalf("expected After callback to fire")
	}
}

// TestTimer_CancelBeforeFire verifies cancelling a one-shot before its
// deadline suppresses the callback entirely.
func TestTimer_CancelBeforeFire(t *testing.T) {
	rt, proto := startTimerRuntime(t)

	var fired atomic.Bool
	h := rt.after(proto, 50*time.Millisecond, func() { fired.Store(true) })
	h.Cancel()

	time.Sleep(120 * time.Millisecond)
	if fired.Load() {
		t.Fatalf("expected cancelled timer not to fire")
	}
}

// TestTimer_CancelAfterFire_NoOp verifies that Cancel after the callback
// has already run is a harmless no-op.
func TestTimer_CancelAfterFire_NoOp(t *testing.T) {
	rt, proto := startTimerRuntime(t)

	fired := make(chan struct{}, 1)
	h := rt.after(proto, 5*time.Millisecond, func() { fired <- struct{}{} })

	select {
	case <-fired:
	case <-time.After(time.Second):
		t.Fatalf("expected callback to fire")
	}
	h.Cancel() // must not panic
	h.Cancel() // double cancel must not panic
}

// TestTimer_DoubleCancel verifies Cancel is idempotent even before a
// fire.
func TestTimer_DoubleCancel(t *testing.T) {
	rt, proto := startTimerRuntime(t)
	h := rt.after(proto, time.Hour, func() {})
	h.Cancel()
	h.Cancel()
}

// TestTimer_CancelFromInsideCallback verifies a periodic timer can
// cancel itself from within its own callback and never fires again. The
// handle is published through a channel so the callback's read of it is
// synchronized (no data race).
func TestTimer_CancelFromInsideCallback(t *testing.T) {
	rt, proto := startTimerRuntime(t)

	handleCh := make(chan TimerHandle, 1)
	var count atomic.Int64
	done := make(chan struct{})
	h := rt.every(proto, 20*time.Millisecond, func() {
		if count.Add(1) == 1 {
			(<-handleCh).Cancel()
			close(done)
		}
	})
	handleCh <- h

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("expected periodic callback to fire at least once")
	}

	time.Sleep(120 * time.Millisecond)
	if got := count.Load(); got != 1 {
		t.Fatalf("expected exactly one callback after self-cancel, got %d", got)
	}
}

// TestTimer_PeriodicFiresThenCancel verifies a periodic timer fires
// repeatedly and stops after Cancel.
func TestTimer_PeriodicFiresThenCancel(t *testing.T) {
	rt, proto := startTimerRuntime(t)

	var count atomic.Int64
	h := rt.every(proto, 10*time.Millisecond, func() { count.Add(1) })

	deadline := time.Now().Add(time.Second)
	for count.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if count.Load() < 3 {
		t.Fatalf("expected at least 3 periodic fires, got %d", count.Load())
	}
	h.Cancel()

	time.Sleep(30 * time.Millisecond)
	after := count.Load()
	time.Sleep(60 * time.Millisecond)
	if count.Load() != after {
		t.Fatalf("expected no fires after Cancel, count grew from %d to %d", after, count.Load())
	}
}

// TestTimer_ShutdownCancelsAll verifies that runtime shutdown cancels
// every timer a protocol owns: no callback runs after Cancel returns.
func TestTimer_ShutdownCancelsAll(t *testing.T) {
	self := transport.NewHost(0, "127.0.0.1")
	rt := New(self)
	_ = registerMockStack(rt, self)
	proto := newProtoProtocol(&MockProtocol{}, 0)
	rt.registerProtocol(proto)
	if err := rt.start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	var fired atomic.Bool
	rt.after(proto, 40*time.Millisecond, func() { fired.Store(true) })
	rt.every(proto, 20*time.Millisecond, func() { fired.Store(true) })

	rt.Cancel() // cancels all timers and waits for goroutines

	fired.Store(false)
	time.Sleep(120 * time.Millisecond)
	if fired.Load() {
		t.Fatalf("expected no timer callback to run after shutdown")
	}
}
