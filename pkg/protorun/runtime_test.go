package protorun

import (
	"context"
	"testing"

	"github.com/antonionduarte/protorun/pkg/transport"
)

// TestRuntime_WithTransport_AcceptsCustomLayer verifies the public
// IoC seam: callers can inject any transport.Layer + *SessionLayer
// without going through the unexported registerNetworkLayer /
// registerSessionLayer helpers. Mock transport here stands in for any
// non-default backend (UDP, in-memory, fault injection, ...).
func TestRuntime_WithTransport_AcceptsCustomLayer(t *testing.T) {
	self := transport.NewHost(0, "127.0.0.1")
	mock := NewMockNetworkLayer()
	sess := transport.NewSessionLayer(mock, self, context.Background(), 0, 0)

	rt := New(self, WithTransport(mock, sess))
	if err := rt.start(); err != nil {
		t.Fatalf("start with injected transport: %v", err)
	}
	rt.Cancel()

	if !mock.CancelCalled {
		t.Errorf("expected injected layer.Cancel() to fire on runtime Cancel")
	}
}

// TestRuntime_WithTransport_NilArgsAreNoOps verifies the option's
// nil-tolerance: passing nil for either argument leaves that slot
// unchanged, so callers can layer a network injection on top of a
// previously-configured session layer (and vice versa).
func TestRuntime_WithTransport_NilArgsAreNoOps(t *testing.T) {
	self := transport.NewHost(0, "127.0.0.1")
	rt := New(self, WithTransport(nil, nil))
	if rt.networkLayer != nil || rt.sessionLayer != nil {
		t.Errorf("nil arguments should not assign layers")
	}
}
