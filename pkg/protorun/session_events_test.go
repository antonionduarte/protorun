package protorun

import (
	"context"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/transport"
)

// TestDispatchSessionEvent_CoversAllEventKinds feeds one of every
// concrete transport.SessionEvent kind through the runtime's event
// mapper and asserts none of them fall through to the unhandled-kind
// default. When the transport package grows a new SessionEvent type,
// add it here and give it an explicit route in dispatchSessionEvent.
func TestDispatchSessionEvent_CoversAllEventKinds(t *testing.T) {
	host := transport.NewHost(9001, "127.0.0.1")
	events := []transport.SessionEvent{
		transport.NewSessionConnected(host),
		transport.NewSessionDisconnected(host),
		transport.NewSessionFailed(host),
		transport.NewSessionVersionMismatch(host, 42, true),
		transport.NewSessionVersionMismatch(host, 42, false),
	}

	metrics := newRecordingMetrics()
	rt := New(transport.NewHost(9000, "127.0.0.1"), WithMetrics(metrics))

	for _, ev := range events {
		if !rt.dispatchSessionEvent(context.Background(), ev) {
			t.Fatalf("dispatchSessionEvent(%T) reported ctx cancellation", ev)
		}
	}

	if n := metrics.totalCounter("protorun.session.unhandled_event"); n != 0 {
		t.Fatalf("expected every event kind to have an explicit route, got %d unhandled", n)
	}
}

// TestDispatchSessionEvent_DialRejected_TerminalGivenUp verifies that a
// Rejected dial (outbound version mismatch) terminates the retry
// schedule immediately and reaches protocols as given-up, carrying the
// attempts made so far.
func TestDispatchSessionEvent_DialRejected_TerminalGivenUp(t *testing.T) {
	peer := transport.NewHost(9002, "127.0.0.1")
	metrics := newRecordingMetrics()
	rt := New(transport.NewHost(9000, "127.0.0.1"), WithMetrics(metrics))
	rt.Register(&MockProtocol{})

	// Simulate an in-flight retry schedule for the peer.
	rt.retryMu.Lock()
	rt.connectionRetries[peer] = &retryState{policy: rt.retryPolicy.withDefaults(), attempt: 3}
	rt.retryMu.Unlock()

	ev := transport.NewSessionVersionMismatch(peer, 42, false)
	if !rt.dispatchSessionEvent(context.Background(), ev) {
		t.Fatalf("dispatchSessionEvent reported ctx cancellation")
	}

	rt.retryMu.Lock()
	_, stillTracked := rt.connectionRetries[peer]
	rt.retryMu.Unlock()
	if stillTracked {
		t.Errorf("expected retry state to be terminated by the Reject")
	}

	queued, ok := recvEvent(rt.protocols[0], time.Second)
	if !ok {
		t.Fatalf("expected a given-up fanout to the protocol")
	}
	if queued.kind != evSession {
		t.Fatalf("expected a session event, got kind=%v", queued.kind)
	}
	got := queued.aux.session
	if got.kind != sessionGivenUpEvent {
		t.Errorf("expected sessionGivenUpEvent fanout, got kind=%v", got.kind)
	}
	if got.host != peer {
		t.Errorf("expected given-up host %v, got %v", peer, got.host)
	}
	if got.attempts != 3 {
		t.Errorf("expected attempts=3, got %d", got.attempts)
	}

	if n := metrics.totalCounter("protorun.session.version_mismatch"); n != 1 {
		t.Errorf("expected 1 version_mismatch counter, got %d", n)
	}
	if n := metrics.totalCounter("protorun.session.given_up"); n != 1 {
		t.Errorf("expected 1 given_up counter, got %d", n)
	}
}

// TestDispatchSessionEvent_InboundMismatch_NoFanout verifies that an
// inbound version mismatch (we Rejected an unknown dialer) is recorded
// for observability but never reaches protocols: there is nothing to
// give up on and no Host they know.
func TestDispatchSessionEvent_InboundMismatch_NoFanout(t *testing.T) {
	metrics := newRecordingMetrics()
	rt := New(transport.NewHost(9000, "127.0.0.1"), WithMetrics(metrics))
	rt.Register(&MockProtocol{})

	ev := transport.NewSessionVersionMismatch(transport.NewHost(53211, "127.0.0.1"), 42, true)
	if !rt.dispatchSessionEvent(context.Background(), ev) {
		t.Fatalf("dispatchSessionEvent reported ctx cancellation")
	}

	if ev, ok := recvEvent(rt.protocols[0], 50*time.Millisecond); ok {
		t.Fatalf("expected no fanout for inbound mismatch, got kind=%v", ev.kind)
	}

	if n := metrics.totalCounter("protorun.session.version_mismatch"); n != 1 {
		t.Errorf("expected 1 version_mismatch counter, got %d", n)
	}
}
