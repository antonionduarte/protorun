package quic

import (
	"crypto/tls"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antonionduarte/protorun"
	"github.com/antonionduarte/protorun/transport"
)

// ping is the one-field message the integration protocol exchanges. It
// embeds BaseMessage for the Sender plumbing and is encoded by WireCodec
// via Handle.
type ping struct {
	protorun.BaseMessage
	N uint32
}

func (ping) WireName() string { return "quic_test.ping" }

// twoSided connects to its peer on Init, sends one ping when the session
// comes up, and records receipt so the test can observe a full round trip
// over the QUIC stack.
type twoSided struct {
	peer     transport.Host
	ctx      protorun.ProtocolContext
	received atomic.Bool
}

func (p *twoSided) Start(ctx protorun.ProtocolContext) {
	p.ctx = ctx
	protorun.Handle(ctx, p.onPing)
}

func (p *twoSided) Init(ctx protorun.ProtocolContext) {
	p.ctx = ctx
	_ = ctx.Connect(p.peer)
}

func (p *twoSided) OnSessionConnected(h transport.Host) {
	if h == p.peer && p.ctx != nil {
		_ = p.ctx.Send(&ping{N: 1}, p.peer)
	}
}

func (p *twoSided) OnSessionDisconnected(transport.Host) {}

func (p *twoSided) onPing(_ *ping, _ transport.Host) { p.received.Store(true) }

// buildRuntime wires a protorun.Runtime onto a QUIC transport + the stock
// SessionLayer, exactly as a user would for a non-TCP backend via
// WithTransport.
func buildRuntime(t *testing.T, self transport.Host, p protorun.Protocol, cfg *tls.Config) *protorun.Runtime {
	t.Helper()
	q, err := NewLayer(self, t.Context(), cfg)
	if err != nil {
		t.Fatalf("NewLayer: %v", err)
	}
	sess := transport.NewSessionLayer(q, self, t.Context(), 0, 0)
	rt := protorun.New(self, protorun.WithTransport(q, sess))
	rt.Register(p)
	return rt
}

// TestRuntime_TwoRuntimes_OverQUIC stands up two independent protorun
// runtimes whose only transport is QUIC and asserts they complete the
// handshake and exchange an application message in each direction — the
// end-to-end proof that transport.Layer + Address is a real seam.
func TestRuntime_TwoRuntimes_OverQUIC(t *testing.T) {
	base := reservePorts(2)
	hA := transport.NewHost(base, "127.0.0.1")
	hB := transport.NewHost(base+1, "127.0.0.1")

	pa := &twoSided{peer: hB}
	pb := &twoSided{peer: hA}

	cfg := sharedDevTLS(t)
	rtA := buildRuntime(t, hA, pa, cfg)
	rtB := buildRuntime(t, hB, pb, cfg)

	if err := rtA.Start(); err != nil {
		t.Fatalf("rtA.Start: %v", err)
	}
	t.Cleanup(rtA.Cancel)
	if err := rtB.Start(); err != nil {
		t.Fatalf("rtB.Start: %v", err)
	}
	t.Cleanup(rtB.Cancel)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if pa.received.Load() && pb.received.Load() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timed out waiting for ping exchange over QUIC")
}
