package protocol

import (
	"testing"

	"github.com/antonionduarte/protorun"
)

// Pingpong uses protorun.BinaryCodec for its messages. BaseMessage is
// a zero-byte marker, so encoding/binary can size the structs and the
// codec works with no manual encode/decode logic.

func TestPingBinaryCodec_RoundTrip(t *testing.T) {
	codec := protorun.BinaryCodec[*PingMessage]{}
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

func TestPongBinaryCodec_RoundTrip(t *testing.T) {
	codec := protorun.BinaryCodec[*PongMessage]{}
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
