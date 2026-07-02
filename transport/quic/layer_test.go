package quic

import (
	"bytes"
	"crypto/tls"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/transport"
	quicgo "github.com/quic-go/quic-go"
)

// basePort hands out fresh UDP ports so the suite survives `go test
// -count=N`: a fixed port from a prior iteration can still be bound while
// the next one starts (quic-go closes its socket asynchronously). The
// range grows upward from 7320, staying inside the 7300–7399 band this
// module reserves.
var basePort atomic.Int32

func init() { basePort.Store(7320) }

// reservePorts returns the first of n consecutive fresh ports.
func reservePorts(n int) int { return int(basePort.Add(int32(n))) - n }

// sharedDevTLS builds one throwaway TLS config for a whole test. Peers
// must present a mutually trusted identity, so every layer in a test
// shares the same self-signed cert rather than minting its own.
func sharedDevTLS(t *testing.T) *tls.Config {
	t.Helper()
	cfg, err := DevTLS()
	if err != nil {
		t.Fatalf("DevTLS: %v", err)
	}
	return cfg
}

func newDevLayer(t *testing.T, self transport.Host, cfg *tls.Config) *Layer {
	t.Helper()
	l, err := NewLayer(self, t.Context(), cfg)
	if err != nil {
		t.Fatalf("NewLayer(%s): %v", self.String(), err)
	}
	t.Cleanup(l.Cancel)
	return l
}

func waitEvent(t *testing.T, ch chan transport.Event, timeout time.Duration) transport.Event {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for transport event")
		return nil
	}
}

// TestLayer_FrameExchange drives two raw QUIC layers through a connect and
// a frame in each direction, asserting the 4-byte length framing round-
// trips over the single bidirectional stream.
//
// Unlike TCP, the QUIC acceptor only surfaces Connected once the dialer's
// stream carries its first frame (streams are announced by their first
// write), so the test waits on the dialer's Connected, sends, then reads
// the acceptor's Connected + message.
func TestLayer_FrameExchange(t *testing.T) {
	base := reservePorts(2)
	hA := transport.NewHost(base, "127.0.0.1")
	hB := transport.NewHost(base+1, "127.0.0.1")

	cfg := sharedDevTLS(t)
	a := newDevLayer(t, hA, cfg)
	b := newDevLayer(t, hB, cfg)

	a.Connect(hB)
	if _, ok := waitEvent(t, a.OutEvents(), 5*time.Second).(*transport.Connected); !ok {
		t.Fatal("dialer: expected Connected")
	}

	a.Send(transport.NewMessage(*bytes.NewBuffer([]byte("a->b")), hB), hB)

	if _, ok := waitEvent(t, b.OutEvents(), 5*time.Second).(*transport.Connected); !ok {
		t.Fatal("acceptor: expected Connected")
	}
	select {
	case m := <-b.OutChannel():
		if m.Msg.String() != "a->b" {
			t.Fatalf("acceptor got %q, want %q", m.Msg.String(), "a->b")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("acceptor never received the frame")
	}

	// Reverse direction over the acceptor's side of the same connection.
	var peer transport.Host
	b.mu.Lock()
	for h := range b.conns {
		peer = h
	}
	b.mu.Unlock()
	b.Send(transport.NewMessage(*bytes.NewBuffer([]byte("b->a")), peer), peer)
	select {
	case m := <-a.OutChannel():
		if m.Msg.String() != "b->a" {
			t.Fatalf("dialer got %q, want %q", m.Msg.String(), "b->a")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("dialer never received the reverse frame")
	}
}

// TestLayer_NilTLS asserts NewLayer refuses to build without a TLS config,
// since QUIC has no cleartext mode.
func TestLayer_NilTLS(t *testing.T) {
	if _, err := NewLayer(transport.NewHost(reservePorts(1), "127.0.0.1"), t.Context(), nil); err == nil {
		t.Fatal("expected NewLayer to reject a nil tls.Config")
	}
}

// TestLayer_Options exercises the WithQUICConfig and WithOutBuffer
// construction options and asserts the layer still handshakes and carries a
// frame with them applied.
func TestLayer_Options(t *testing.T) {
	base := reservePorts(2)
	hA := transport.NewHost(base, "127.0.0.1")
	hB := transport.NewHost(base+1, "127.0.0.1")

	cfg := sharedDevTLS(t)
	qconf := &quicgo.Config{MaxIdleTimeout: 30 * time.Second}

	mk := func(self transport.Host) *Layer {
		l, err := NewLayer(self, t.Context(), cfg, WithQUICConfig(qconf), WithOutBuffer(8))
		if err != nil {
			t.Fatalf("NewLayer(%s): %v", self.String(), err)
		}
		t.Cleanup(l.Cancel)
		return l
	}
	a, b := mk(hA), mk(hB)

	a.Connect(hB)
	if _, ok := waitEvent(t, a.OutEvents(), 5*time.Second).(*transport.Connected); !ok {
		t.Fatal("dialer: expected Connected")
	}
	a.Send(transport.NewMessage(*bytes.NewBuffer([]byte("opt")), hB), hB)
	if _, ok := waitEvent(t, b.OutEvents(), 5*time.Second).(*transport.Connected); !ok {
		t.Fatal("acceptor: expected Connected")
	}
	select {
	case m := <-b.OutChannel():
		if m.Msg.String() != "opt" {
			t.Fatalf("got %q, want %q", m.Msg.String(), "opt")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("acceptor never received the frame")
	}
}
