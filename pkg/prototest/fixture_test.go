package prototest

import (
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/protorun"
	"github.com/antonionduarte/protorun/pkg/transport"
)

type pingMsg struct{ protorun.BaseMessage }

type pingCodec struct{}

func (pingCodec) Marshal(_ *pingMsg) ([]byte, error)   { return nil, nil }
func (pingCodec) Unmarshal(_ []byte) (*pingMsg, error) { return &pingMsg{}, nil }

// pingProtocol dials peer on Init (if set), sends one ping on every
// session establishment, and records who pinged it.
type pingProtocol struct {
	peer transport.Host

	ctx protorun.ProtocolContext
	got chan transport.Host
}

func newPingProtocol(peer transport.Host) *pingProtocol {
	return &pingProtocol{peer: peer, got: make(chan transport.Host, 1)}
}

func (p *pingProtocol) Start(ctx protorun.ProtocolContext) {
	p.ctx = ctx
	protorun.RegisterCodec(ctx, pingCodec{})
	protorun.RegisterHandler(ctx, func(_ *pingMsg, from transport.Host) {
		p.got <- from
	})
}

func (p *pingProtocol) Init(ctx protorun.ProtocolContext) {
	if p.peer.Port != 0 {
		ctx.Connect(p.peer)
	}
}

func (p *pingProtocol) OnSessionConnected(h transport.Host) {
	_ = p.ctx.Send(&pingMsg{}, h)
}

func (p *pingProtocol) OnSessionDisconnected(_ transport.Host) {}

// TestNewRuntime_TwoNodesExchangeMessages is the fixture's raison
// d'être: two full runtimes talking through the mesh — codec routing,
// session events, protocol event loops — with no TCP, no handshake,
// and no ports to reserve.
func TestNewRuntime_TwoNodesExchangeMessages(t *testing.T) {
	mesh := NewMesh(t)
	hostA := transport.NewHost(1, "10.0.0.1")
	hostB := transport.NewHost(2, "10.0.0.2")

	protoB := newPingProtocol(transport.Host{}) // passive side first
	NewRuntime(t, mesh, hostB, []protorun.Protocol{protoB})

	protoA := newPingProtocol(hostB) // dials B on Init
	NewRuntime(t, mesh, hostA, []protorun.Protocol{protoA})

	waitPing := func(name string, ch chan transport.Host, want transport.Host) {
		t.Helper()
		select {
		case from := <-ch:
			if from != want {
				t.Errorf("%s: expected ping from %v, got %v", name, want, from)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: timed out waiting for ping", name)
		}
	}
	waitPing("B", protoB.got, hostA)
	waitPing("A", protoA.got, hostB)
}

// TestNewRuntime_WithRealClock opts a mesh out of virtual time: Clock is
// nil and the runtimes run on wall time, yet the exchange still works.
func TestNewRuntime_WithRealClock(t *testing.T) {
	mesh := NewMesh(t, WithRealClock())
	if mesh.Clock() != nil {
		t.Fatalf("WithRealClock mesh should expose a nil Clock, got %v", mesh.Clock())
	}
	hostA := transport.NewHost(1, "10.0.0.1")
	hostB := transport.NewHost(2, "10.0.0.2")

	protoB := newPingProtocol(transport.Host{})
	NewRuntime(t, mesh, hostB, []protorun.Protocol{protoB})

	protoA := newPingProtocol(hostB)
	NewRuntime(t, mesh, hostA, []protorun.Protocol{protoA})

	select {
	case <-protoB.got:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for ping on a real-clock mesh")
	}
}

// TestNewRuntime_StrictModeWorksOnMesh runs the same exchange under
// WithStrict to prove the fixture forwards runtime options and the
// mesh respects the runtime's phase discipline.
func TestNewRuntime_StrictModeWorksOnMesh(t *testing.T) {
	mesh := NewMesh(t)
	hostA := transport.NewHost(1, "10.0.0.1")
	hostB := transport.NewHost(2, "10.0.0.2")

	protoB := newPingProtocol(transport.Host{})
	NewRuntime(t, mesh, hostB, []protorun.Protocol{protoB}, protorun.WithStrict(true))

	protoA := newPingProtocol(hostB)
	NewRuntime(t, mesh, hostA, []protorun.Protocol{protoA}, protorun.WithStrict(true))

	select {
	case <-protoB.got:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for ping under strict mode")
	}
}
