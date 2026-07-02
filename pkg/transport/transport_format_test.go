package transport

import (
	"bytes"
	"testing"
)

// TestFrameSplit_RoundTrip verifies that frameFor and splitFrame form a
// round-trip for both application and session payloads: the layer byte is
// prefixed on the way out and stripped on the way back in, leaving the
// body intact.
func TestFrameSplit_RoundTrip(t *testing.T) {
	host := NewHost(6609, "127.0.0.1")

	// Application message body: arbitrary payload.
	appPayload := *bytes.NewBuffer([]byte{0xAA, 0xBB, 0xCC})
	appTransport := frameFor(Application, appPayload, host)
	if !appTransport.Peer.Equal(host) {
		t.Fatalf("expected peer %v, got %v", host, appTransport.Peer)
	}
	layer, body := splitFrame(appTransport)
	if layer != Application {
		t.Fatalf("expected Application layer, got %v", layer)
	}
	if !bytes.Equal(body.Bytes(), appPayload.Bytes()) {
		t.Fatalf("application payload mismatch: got %v, want %v", body.Bytes(), appPayload.Bytes())
	}

	// Session message body: handshake payload (using encodeHello).
	helloPayload, err := encodeHello(host)
	if err != nil {
		t.Fatalf("encodeHello: %v", err)
	}
	sessTransport := frameFor(Session, helloPayload, host)
	sessLayer, sessBody := splitFrame(sessTransport)
	if sessLayer != Session {
		t.Fatalf("expected Session layer, got %v", sessLayer)
	}

	// The first byte of the session payload should be HandshakeHello.
	if sessBody.Len() == 0 {
		t.Fatalf("expected non-empty session payload")
	}
	if HandshakeType(sessBody.Bytes()[0]) != HandshakeHello {
		t.Fatalf("expected HandshakeHello type, got %d", sessBody.Bytes()[0])
	}
}
