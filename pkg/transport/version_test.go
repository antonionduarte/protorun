package transport

import (
	"bytes"
	"errors"
	"testing"
)

// TestHandshakeVersion_RoundTripCurrent verifies that an encoded
// Hello at the current version round-trips through the parser.
func TestHandshakeVersion_RoundTripCurrent(t *testing.T) {
	original := Host{IP: "127.0.0.1", Port: 9000}
	buf, err := encodeHello(original)
	if err != nil {
		t.Fatalf("encodeHello: %v", err)
	}
	p, err := parseHandshakePayload(&buf)
	if err != nil {
		t.Fatalf("parseHandshakePayload: %v", err)
	}
	if p.typ != HandshakeHello {
		t.Errorf("got HandshakeType=%v, want HandshakeHello", p.typ)
	}
	if p.host != original {
		t.Errorf("got host=%+v, want %+v", p.host, original)
	}
	if p.version != ProtocolVersion {
		t.Errorf("got version=%d, want %d", p.version, ProtocolVersion)
	}
}

// TestHandshakeVersion_Mismatch fabricates a Hello with a wrong
// version byte and verifies parseHandshakePayload returns an error
// wrapping ErrVersionMismatch.
func TestHandshakeVersion_Mismatch(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	buf.WriteByte(byte(HandshakeHello))
	buf.WriteByte(ProtocolVersion + 1) // pretend we're a future build
	_ = WriteHost(buf, Host{IP: "127.0.0.1", Port: 7000})

	p, err := parseHandshakePayload(buf)
	if err == nil {
		t.Fatalf("expected error for version mismatch, got nil")
	}
	if !errors.Is(err, ErrVersionMismatch) {
		t.Errorf("expected error to wrap ErrVersionMismatch, got %v", err)
	}
	if p.version != ProtocolVersion+1 {
		t.Errorf("expected offending version %d to survive the parse error, got %d", ProtocolVersion+1, p.version)
	}
}

// TestHandshakeReject_RoundTrip verifies that an encoded Reject
// round-trips through the parser with the sender's version intact.
func TestHandshakeReject_RoundTrip(t *testing.T) {
	buf := encodeReject()
	p, err := parseHandshakePayload(&buf)
	if err != nil {
		t.Fatalf("parseHandshakePayload: %v", err)
	}
	if p.typ != HandshakeReject {
		t.Errorf("got HandshakeType=%v, want HandshakeReject", p.typ)
	}
	if p.version != ProtocolVersion {
		t.Errorf("got version=%d, want %d", p.version, ProtocolVersion)
	}
}

// TestHandshakeVersion_TruncatedAtVersion verifies that a Hello
// truncated immediately after the type byte (no version) is reported
// as a parse error rather than silently accepted.
func TestHandshakeVersion_TruncatedAtVersion(t *testing.T) {
	buf := bytes.NewBuffer([]byte{byte(HandshakeHello)})
	_, err := parseHandshakePayload(buf)
	if err == nil {
		t.Fatalf("expected truncated handshake to error")
	}
}
