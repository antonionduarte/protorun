package transport

import (
	"bytes"
	"context"
	"testing"
	"time"
)

// waitSessionEvent waits for a SessionEvent on the given channel or fails the test
// after the provided timeout.
func waitSessionEvent(t *testing.T, ch chan SessionEvent, timeout time.Duration) SessionEvent {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for SessionEvent")
		return nil
	}
}

// TestSessionLayerSuccessfulHandshake verifies that two SessionLayers built on top
// of TCPLayer perform the handshake and emit SessionConnected events with the
// correct logical hosts.
func TestSessionLayerSuccessfulHandshake(t *testing.T) {
	// two logical hosts
	h1 := NewHost(7001, "127.0.0.1")
	h2 := NewHost(7002, "127.0.0.1")

	ctx1 := t.Context()
	ctx2 := t.Context()

	tcp1 := NewTCPLayer(h1, ctx1, 0)
	tcp2 := NewTCPLayer(h2, ctx2, 0)
	defer tcp1.Cancel()
	defer tcp2.Cancel()

	s1 := NewSessionLayer(tcp1, h1, ctx1, 0, 0)
	s2 := NewSessionLayer(tcp2, h2, ctx2, 0, 0)
	defer s1.Cancel()
	defer s2.Cancel()

	// initiate session from h1 to h2
	s1.Connect(h2)

	ev1 := waitSessionEvent(t, s1.OutChannelEvents(), 5*time.Second)
	ev2 := waitSessionEvent(t, s2.OutChannelEvents(), 5*time.Second)

	c1, ok1 := ev1.(*SessionConnected)
	c2, ok2 := ev2.(*SessionConnected)
	if !ok1 || !ok2 {
		t.Fatalf("expected SessionConnected on both sides, got %T and %T", ev1, ev2)
	}

	if c1.Host() != h2 {
		t.Fatalf("expected s1 to see peer %v, got %v", h2, c1.Host())
	}
	if c2.Host() != h1 {
		t.Fatalf("expected s2 to see peer %v, got %v", h1, c2.Host())
	}
}

// TestSessionLayerFailedConnection verifies that a failed connect attempt
// results in a SessionFailed event with the correct logical host.
func TestSessionLayerFailedConnection(t *testing.T) {
	hClient := NewHost(7101, "127.0.0.1")
	hNoServer := NewHost(7102, "127.0.0.1")

	ctx := t.Context()

	tcp := NewTCPLayer(hClient, ctx, 0)
	defer tcp.Cancel()

	s := NewSessionLayer(tcp, hClient, ctx, 0, 0)
	defer s.Cancel()

	// Connect to a host that has no listener
	s.Connect(hNoServer)

	ev := waitSessionEvent(t, s.OutChannelEvents(), 5*time.Second)
	failed, ok := ev.(*SessionFailed)
	if !ok {
		t.Fatalf("expected SessionFailed, got %T", ev)
	}
	if failed.Host() != hNoServer {
		t.Fatalf("expected failed host %v, got %v", hNoServer, failed.Host())
	}
}

// TestSessionLayer_SendAfterCancel ensures that Connect/Disconnect/Send
// calls arriving after Cancel do not block indefinitely. Pre-fix, these
// methods did raw blocking sends on unbuffered channels whose only consumer
// (the handler goroutine) had already exited, deadlocking any caller that
// happened to be in flight when Cancel ran, including a protocol's
// ctx.Send invoked during shutdown.
func TestSessionLayer_SendAfterCancel(t *testing.T) {
	self := NewHost(7250, "127.0.0.1")
	tcp := NewTCPLayer(self, context.Background(), 0)
	defer tcp.Cancel()
	s := NewSessionLayer(tcp, self, context.Background(), 0, 0)
	s.Cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Connect(NewHost(1, "127.0.0.1"))
		s.Disconnect(NewHost(2, "127.0.0.1"))
		var buf bytes.Buffer
		s.Send(buf, NewHost(3, "127.0.0.1"))
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("Connect/Disconnect/Send after Cancel blocked")
	}
}

// TestSessionLayerCancelStopsEvents ensures that calling Cancel on the
// SessionLayer stops the handler loop and no further events are emitted.
func TestSessionLayerCancelStopsEvents(t *testing.T) {
	h := NewHost(7201, "127.0.0.1")

	ctx := t.Context()

	tcp := NewTCPLayer(h, ctx, 0)
	defer tcp.Cancel()

	s := NewSessionLayer(tcp, h, ctx, 0, 0)

	// Immediately cancel the session layer; there should be no events after this.
	s.Cancel()

	select {
	case ev := <-s.OutChannelEvents():
		t.Fatalf("did not expect event after SessionLayer.Cancel(), got %T", ev)
	case <-time.After(20 * time.Millisecond):
		// ok
	}
}
