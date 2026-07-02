package quic

import (
	"bytes"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/transport"
)

func waitSessionEvent(t *testing.T, ch chan transport.SessionEvent, timeout time.Duration) transport.SessionEvent {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for session event")
		return nil
	}
}

// TestSessionLayer_HandshakeOverQUIC runs protorun's unchanged Hello/Ack
// SessionLayer on top of two QUIC layers: the whole point of the backend
// is that nothing above the wire has to know it's QUIC. Both sides must
// reach SessionConnected with the correct logical hosts, then exchange an
// application frame resolved to the logical identity.
func TestSessionLayer_HandshakeOverQUIC(t *testing.T) {
	base := reservePorts(2)
	hA := transport.NewHost(base, "127.0.0.1")
	hB := transport.NewHost(base+1, "127.0.0.1")

	cfg := sharedDevTLS(t)
	qa := newDevLayer(t, hA, cfg)
	qb := newDevLayer(t, hB, cfg)

	sa := transport.NewSessionLayer(qa, hA, t.Context(), 0, 0)
	t.Cleanup(sa.Cancel)
	sb := transport.NewSessionLayer(qb, hB, t.Context(), 0, 0)
	t.Cleanup(sb.Cancel)

	sa.Connect(hB)

	evA := waitSessionEvent(t, sa.OutChannelEvents(), 5*time.Second)
	evB := waitSessionEvent(t, sb.OutChannelEvents(), 5*time.Second)

	ca, ok := evA.(*transport.SessionConnected)
	if !ok {
		t.Fatalf("dialer: expected SessionConnected, got %T", evA)
	}
	cb, ok := evB.(*transport.SessionConnected)
	if !ok {
		t.Fatalf("acceptor: expected SessionConnected, got %T", evB)
	}
	if ca.Host() != hB {
		t.Fatalf("dialer saw peer %v, want %v", ca.Host(), hB)
	}
	if cb.Host() != hA {
		t.Fatalf("acceptor saw peer %v, want %v", cb.Host(), hA)
	}

	sa.Send(*bytes.NewBuffer([]byte("hello quic")), hB)
	select {
	case in := <-sb.OutMessages():
		if in.Msg.String() != "hello quic" {
			t.Fatalf("acceptor got %q, want %q", in.Msg.String(), "hello quic")
		}
		if in.Host() != hA {
			t.Fatalf("acceptor saw logical host %v, want %v", in.Host(), hA)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("acceptor never received the application frame over QUIC")
	}
}
