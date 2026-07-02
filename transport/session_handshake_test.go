package transport

import (
	"bytes"
	"testing"
	"time"
)

// waitTransportEvent waits for a transport-level Event or fails the test.
func waitTransportEvent(t *testing.T, ch chan Event, timeout time.Duration) Event {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for transport Event")
		return nil
	}
}

// TestSessionLayer_RejectsMismatchedHello drives a SessionLayer server
// with a raw TCPLayer client that speaks a future wire-format version.
// The server must emit an inbound SessionVersionMismatch and answer the
// dialer with a Reject carrying its own version before closing.
func TestSessionLayer_RejectsMismatchedHello(t *testing.T) {
	hServer := NewHost(7261, "127.0.0.1")
	hClient := NewHost(7262, "127.0.0.1")

	ctx := t.Context()
	tcpServer := NewTCPLayer(hServer, ctx, 0)
	defer tcpServer.Cancel()
	sServer := NewSessionLayer(tcpServer, hServer, ctx, 0, 0)
	defer sServer.Cancel()

	tcpClient := NewTCPLayer(hClient, ctx, 0)
	defer tcpClient.Cancel()

	tcpClient.Connect(hServer)
	if ev := waitTransportEvent(t, tcpClient.OutEvents(), 5*time.Second); ev == nil {
		return
	}

	// Fabricate a Hello from a build that speaks a future version.
	hello := bytes.NewBuffer(nil)
	hello.WriteByte(byte(HandshakeHello))
	hello.WriteByte(ProtocolVersion + 1)
	_ = WriteHost(hello, hClient)
	msg := serializeMessage(SessionMessage{host: hServer, layer: Session, Msg: *hello})
	tcpClient.Send(msg, hServer)

	ev := waitSessionEvent(t, sServer.OutChannelEvents(), 5*time.Second)
	vm, ok := ev.(*SessionVersionMismatch)
	if !ok {
		t.Fatalf("expected SessionVersionMismatch, got %T", ev)
	}
	if !vm.Inbound() {
		t.Errorf("expected inbound mismatch (server side)")
	}
	if vm.PeerVersion() != ProtocolVersion+1 {
		t.Errorf("expected peer version %d, got %d", ProtocolVersion+1, vm.PeerVersion())
	}

	// The dialer must learn why: a Reject with the server's version.
	select {
	case raw := <-tcpClient.OutChannel():
		sm := deserializeMessage(raw)
		if sm.layer != Session {
			t.Fatalf("expected a session-layer frame, got layer=%d", sm.layer)
		}
		p, err := parseHandshakePayload(&sm.Msg)
		if err != nil {
			t.Fatalf("parseHandshakePayload: %v", err)
		}
		if p.typ != HandshakeReject {
			t.Errorf("expected HandshakeReject, got %v", p.typ)
		}
		if p.version != ProtocolVersion {
			t.Errorf("expected Reject to carry version %d, got %d", ProtocolVersion, p.version)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("dialer never received the Reject")
	}
}

// TestSessionLayer_DialRejected_EmitsVersionMismatch drives a
// SessionLayer client against a raw TCPLayer "server" that answers its
// Hello with a Reject. The client must emit an outbound
// SessionVersionMismatch carrying the logical host it dialed — and no
// SessionConnected beforehand, since Established requires the Ack.
func TestSessionLayer_DialRejected_EmitsVersionMismatch(t *testing.T) {
	hClient := NewHost(7263, "127.0.0.1")
	hServer := NewHost(7264, "127.0.0.1")

	ctx := t.Context()
	tcpClient := NewTCPLayer(hClient, ctx, 0)
	defer tcpClient.Cancel()
	sClient := NewSessionLayer(tcpClient, hClient, ctx, 0, 0)
	defer sClient.Cancel()

	tcpServer := NewTCPLayer(hServer, ctx, 0) // raw: no session layer on top
	defer tcpServer.Cancel()

	sClient.Connect(hServer)

	// The raw server waits for the client's Hello and answers Reject.
	select {
	case raw := <-tcpServer.OutChannel():
		sm := deserializeMessage(raw)
		p, err := parseHandshakePayload(&sm.Msg)
		if err != nil || p.typ != HandshakeHello {
			t.Fatalf("expected a Hello from the dialing client, got typ=%v err=%v", p.typ, err)
		}
		reject := serializeMessage(SessionMessage{host: raw.Host, layer: Session, Msg: encodeReject()})
		tcpServer.Send(reject, raw.Host)
	case <-time.After(5 * time.Second):
		t.Fatalf("raw server never received the Hello")
	}

	ev := waitSessionEvent(t, sClient.OutChannelEvents(), 5*time.Second)
	vm, ok := ev.(*SessionVersionMismatch)
	if !ok {
		t.Fatalf("expected SessionVersionMismatch as the first client event (no premature SessionConnected), got %T", ev)
	}
	if vm.Inbound() {
		t.Errorf("expected outbound mismatch (our dial was rejected)")
	}
	if vm.Host() != hServer {
		t.Errorf("expected mismatch host %v (the logical host dialed), got %v", hServer, vm.Host())
	}
	if vm.PeerVersion() != ProtocolVersion {
		t.Errorf("expected peer version %d, got %d", ProtocolVersion, vm.PeerVersion())
	}
}

// TestSessionLayer_HandshakeTimeout verifies that a peer which accepts
// the connection but never answers the Hello fails the handshake after
// the configured timeout instead of parking the session forever.
func TestSessionLayer_HandshakeTimeout(t *testing.T) {
	hClient := NewHost(7265, "127.0.0.1")
	hServer := NewHost(7266, "127.0.0.1")

	ctx := t.Context()
	tcpClient := NewTCPLayer(hClient, ctx, 0)
	defer tcpClient.Cancel()
	sClient := NewSessionLayer(tcpClient, hClient, ctx, 0, 0,
		WithHandshakeTimeout(100*time.Millisecond))
	defer sClient.Cancel()

	tcpServer := NewTCPLayer(hServer, ctx, 0) // accepts, never speaks
	defer tcpServer.Cancel()

	sClient.Connect(hServer)

	ev := waitSessionEvent(t, sClient.OutChannelEvents(), 5*time.Second)
	failed, ok := ev.(*SessionFailed)
	if !ok {
		t.Fatalf("expected SessionFailed on handshake timeout, got %T", ev)
	}
	if failed.Host() != hServer {
		t.Errorf("expected failed host %v, got %v", hServer, failed.Host())
	}
}
