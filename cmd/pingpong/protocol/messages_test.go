package protocol

import (
	"testing"

	"github.com/antonionduarte/protorun"
)

// Pingpong registers its messages with protorun.Handle, which selects
// the reflective WireCodec[*M]. These tests exercise that same codec
// directly to prove the fixed-size round-trip.

func TestPingWireCodec_RoundTrip(t *testing.T) {
	codec := protorun.WireCodec[*PingMessage]{}
	original := NewPingMessage(42)
	payload, err := codec.Marshal(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := codec.Unmarshal(payload)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Seq != 42 {
		t.Fatalf("Seq round-trip: got %d, want 42", got.Seq)
	}
}

func TestPongWireCodec_RoundTrip(t *testing.T) {
	codec := protorun.WireCodec[*PongMessage]{}
	original := NewPongMessage(99)
	payload, err := codec.Marshal(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := codec.Unmarshal(payload)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Seq != 99 {
		t.Fatalf("Seq round-trip: got %d, want 99", got.Seq)
	}
}
