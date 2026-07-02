package protorun

import (
	"errors"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/transport"
)

// TestShutdown_CompletesUnderTimeout verifies the happy path: a
// well-behaved runtime tears down well within the supplied
// timeout, and Shutdown returns nil.
func TestShutdown_CompletesUnderTimeout(t *testing.T) {
	self := transport.NewHost(0, "127.0.0.1")
	rt := New(self)
	mock := registerMockStack(rt, self)

	if err := rt.start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	if err := rt.Shutdown(5 * time.Second); err != nil {
		t.Errorf("Shutdown returned %v, want nil", err)
	}
	if !mock.CancelCalled {
		t.Errorf("expected layer.Cancel to fire during Shutdown")
	}
}

// TestShutdown_ZeroTimeoutUsesDefault verifies a zero timeout falls
// back to the package default. Without the fallback, Shutdown(0)
// would race-fail on every machine.
func TestShutdown_ZeroTimeoutUsesDefault(t *testing.T) {
	self := transport.NewHost(0, "127.0.0.1")
	rt := New(self)
	_ = registerMockStack(rt, self)

	if err := rt.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	// A zero timeout should not return ErrShutdownTimeout immediately.
	if err := rt.Shutdown(0); err != nil {
		t.Errorf("Shutdown(0): expected default timeout to apply, got err=%v", err)
	}
}

// TestShutdown_TimeoutSentinel verifies the sentinel is returned
// when shutdown takes longer than the supplied timeout. The
// scenario is contrived (a stuck handler); under normal operation
// shutdown completes in milliseconds.
func TestShutdown_TimeoutSentinel(t *testing.T) {
	self := transport.NewHost(0, "127.0.0.1")
	rt := New(self)
	_ = registerMockStack(rt, self)

	stuck := newProtoProtocol(&MockProtocol{}, 0)
	rt.registerProtocol(stuck)
	stuck.ensureContext()
	// Block the event loop with a handler that never returns.
	block := make(chan struct{})
	defer close(block)
	RegisterRequestHandler(stuck.ctx, func(_ *benchReq, _ Responder[*benchRep]) {
		<-block // Block forever (until test cleanup).
	})

	if err := rt.start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Issue a request that the stuck handler will accept and never reply to.
	SendRequest(stuck.ctx, &benchReq{}, func(_ *benchRep, _ error) {})

	// Allow it to start handling before we shut down.
	time.Sleep(50 * time.Millisecond)

	err := rt.Shutdown(100 * time.Millisecond)
	if !errors.Is(err, ErrShutdownTimeout) {
		t.Errorf("expected ErrShutdownTimeout, got %v", err)
	}
}
