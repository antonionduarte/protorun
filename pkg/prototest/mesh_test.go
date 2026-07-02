package prototest

import (
	"bytes"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/transport"
)

func waitEvent(t *testing.T, ch chan transport.SessionEvent) transport.SessionEvent {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for session event")
		return nil
	}
}

// TestMesh_Connect_BothSidesEstablished mirrors the real handshake
// contract: one Connect, SessionConnected on both endpoints, each
// carrying the other's Host.
func TestMesh_Connect_BothSidesEstablished(t *testing.T) {
	mesh := NewMesh(t)
	a := mesh.Node(transport.NewHost(1, "10.0.0.1"))
	b := mesh.Node(transport.NewHost(2, "10.0.0.2"))
	defer a.Cancel()
	defer b.Cancel()

	a.Connect(b.self)

	evA := waitEvent(t, a.OutChannelEvents())
	evB := waitEvent(t, b.OutChannelEvents())
	if _, ok := evA.(*transport.SessionConnected); !ok || evA.Host() != b.self {
		t.Fatalf("dialer: expected SessionConnected for %v, got %T %v", b.self, evA, evA.Host())
	}
	if _, ok := evB.(*transport.SessionConnected); !ok || evB.Host() != a.self {
		t.Fatalf("listener: expected SessionConnected for %v, got %T %v", a.self, evB, evB.Host())
	}
}

// TestMesh_ConnectAbsentHost_Fails mirrors dialing a Host with no
// listener: the dialer sees SessionFailed.
func TestMesh_ConnectAbsentHost_Fails(t *testing.T) {
	mesh := NewMesh(t)
	a := mesh.Node(transport.NewHost(1, "10.0.0.1"))
	defer a.Cancel()

	nobody := transport.NewHost(9, "10.0.0.9")
	a.Connect(nobody)

	ev := waitEvent(t, a.OutChannelEvents())
	if _, ok := ev.(*transport.SessionFailed); !ok || ev.Host() != nobody {
		t.Fatalf("expected SessionFailed for %v, got %T %v", nobody, ev, ev.Host())
	}
}

// TestMesh_SendDeliversToPeer verifies payload delivery over an
// established session, with the sender's Host on the envelope.
func TestMesh_SendDeliversToPeer(t *testing.T) {
	mesh := NewMesh(t)
	a := mesh.Node(transport.NewHost(1, "10.0.0.1"))
	b := mesh.Node(transport.NewHost(2, "10.0.0.2"))
	defer a.Cancel()
	defer b.Cancel()

	a.Connect(b.self)
	waitEvent(t, a.OutChannelEvents())
	waitEvent(t, b.OutChannelEvents())

	var payload bytes.Buffer
	payload.WriteString("ping")
	a.Send(payload, b.self)

	select {
	case msg := <-b.OutMessages():
		if msg.Host() != a.self {
			t.Errorf("expected sender %v, got %v", a.self, msg.Host())
		}
		if msg.Msg.String() != "ping" {
			t.Errorf("expected payload %q, got %q", "ping", msg.Msg.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for delivery")
	}
}

// TestMesh_SendWithoutSession_Drops mirrors the real transport: no
// live session means a silent drop, not an error or a delivery.
func TestMesh_SendWithoutSession_Drops(t *testing.T) {
	mesh := NewMesh(t)
	a := mesh.Node(transport.NewHost(1, "10.0.0.1"))
	b := mesh.Node(transport.NewHost(2, "10.0.0.2"))
	defer a.Cancel()
	defer b.Cancel()

	var payload bytes.Buffer
	payload.WriteString("lost")
	a.Send(payload, b.self)

	select {
	case msg := <-b.OutMessages():
		t.Fatalf("expected no delivery without a session, got %q", msg.Msg.String())
	case <-time.After(50 * time.Millisecond):
	}
}

// TestMesh_Disconnect_BothSidesSee verifies teardown symmetry.
func TestMesh_Disconnect_BothSidesSee(t *testing.T) {
	mesh := NewMesh(t)
	a := mesh.Node(transport.NewHost(1, "10.0.0.1"))
	b := mesh.Node(transport.NewHost(2, "10.0.0.2"))
	defer a.Cancel()
	defer b.Cancel()

	a.Connect(b.self)
	waitEvent(t, a.OutChannelEvents())
	waitEvent(t, b.OutChannelEvents())

	a.Disconnect(b.self)

	evA := waitEvent(t, a.OutChannelEvents())
	evB := waitEvent(t, b.OutChannelEvents())
	if _, ok := evA.(*transport.SessionDisconnected); !ok {
		t.Errorf("dialer: expected SessionDisconnected, got %T", evA)
	}
	if _, ok := evB.(*transport.SessionDisconnected); !ok {
		t.Errorf("peer: expected SessionDisconnected, got %T", evB)
	}
}

// TestMesh_Cancel_PeersSeeDisconnect mirrors a node going away: its
// peers observe SessionDisconnected, and dialing it afterwards fails.
func TestMesh_Cancel_PeersSeeDisconnect(t *testing.T) {
	mesh := NewMesh(t)
	a := mesh.Node(transport.NewHost(1, "10.0.0.1"))
	b := mesh.Node(transport.NewHost(2, "10.0.0.2"))
	defer b.Cancel()

	a.Connect(b.self)
	waitEvent(t, a.OutChannelEvents())
	waitEvent(t, b.OutChannelEvents())

	a.Cancel()

	ev := waitEvent(t, b.OutChannelEvents())
	if _, ok := ev.(*transport.SessionDisconnected); !ok || ev.Host() != a.self {
		t.Fatalf("expected SessionDisconnected for %v, got %T %v", a.self, ev, ev.Host())
	}

	b.Connect(a.self)
	ev = waitEvent(t, b.OutChannelEvents())
	if _, ok := ev.(*transport.SessionFailed); !ok {
		t.Fatalf("expected SessionFailed dialing a cancelled node, got %T", ev)
	}
}
