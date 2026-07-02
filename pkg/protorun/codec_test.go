package protorun

import "testing"

// fixedSizePing is a test message used to exercise BinaryCodec.
// Every field is fixed-size so encoding/binary can size the struct.
// It embeds BaseMessage (zero-byte) to satisfy the Message interface.
type fixedSizePing struct {
	BaseMessage
	Seq   uint64
	Round uint32
}

func TestBinaryCodec_FixedSize_RoundTrip(t *testing.T) {
	codec := BinaryCodec[*fixedSizePing]{}
	original := &fixedSizePing{Seq: 0xdeadbeef, Round: 7}

	payload, err := codec.Marshal(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(payload) != 12 {
		t.Fatalf("expected 12 bytes (uint64+uint32), got %d", len(payload))
	}

	got, err := codec.Unmarshal(payload)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if *got != *original {
		t.Fatalf("round-trip: got %+v, want %+v", got, original)
	}
}

func TestBinaryCodec_DecodeShortPayload(t *testing.T) {
	codec := BinaryCodec[*fixedSizePing]{}
	if _, err := codec.Unmarshal([]byte{0x01, 0x02}); err == nil {
		t.Fatalf("expected error for short payload")
	}
}
