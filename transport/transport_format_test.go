package transport

import (
	"bytes"
	"testing"
)

// TestSerializeDeserializeMessage_RoundTrip verifies that
// serializeMessage and deserializeMessage form a
// round-trip for both application and session messages.
func TestSerializeDeserializeMessage_RoundTrip(t *testing.T) {
	host := NewHost(6609, "127.0.0.1")

	// Application message body: arbitrary payload
	appPayload := *bytes.NewBuffer([]byte{0xAA, 0xBB, 0xCC})
	appMsg := SessionMessage{
		host:  host,
		layer: Application,
		Msg:   appPayload,
	}
	appTransport := serializeMessage(appMsg)
	decodedApp := deserializeMessage(appTransport)

	if decodedApp.layer != Application {
		t.Fatalf("expected Application layer, got %v", decodedApp.layer)
	}
	if decodedApp.host != host {
		t.Fatalf("expected host %v, got %v", host, decodedApp.host)
	}
	if !bytes.Equal(decodedApp.Msg.Bytes(), appPayload.Bytes()) {
		t.Fatalf("application payload mismatch: got %v, want %v", decodedApp.Msg.Bytes(), appPayload.Bytes())
	}

	// Session message body: handshake payload (using encodeHello)
	helloPayload, err := encodeHello(host)
	if err != nil {
		t.Fatalf("encodeHello: %v", err)
	}
	sessMsg := SessionMessage{
		host:  host,
		layer: Session,
		Msg:   helloPayload,
	}
	sessTransport := serializeMessage(sessMsg)
	decodedSess := deserializeMessage(sessTransport)

	if decodedSess.layer != Session {
		t.Fatalf("expected Session layer, got %v", decodedSess.layer)
	}
	if decodedSess.host != host {
		t.Fatalf("expected host %v, got %v", host, decodedSess.host)
	}

	// The first byte of the session payload should be HandshakeHello.
	buf := decodedSess.Msg
	if buf.Len() == 0 {
		t.Fatalf("expected non-empty session payload")
	}
	hdr := buf.Bytes()[0]
	if HandshakeType(hdr) != HandshakeHello {
		t.Fatalf("expected HandshakeHello type, got %d", hdr)
	}
}
