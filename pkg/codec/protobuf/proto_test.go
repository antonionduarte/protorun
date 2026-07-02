package protobuf

import (
	"testing"

	"github.com/antonionduarte/protorun/pkg/protorun"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// The tests use the pre-generated well-known types (wrapperspb) so no
// protoc / codegen is needed to exercise the codec.

// bothStub is a type that is simultaneously a proto.Message (promoted
// from the embedded generated message) and a protorun.Message (from the
// embedded BaseMessage) — exactly the shape a user registers with the
// runtime through ProtoCodec.
type bothStub struct {
	protorun.BaseMessage
	*wrapperspb.StringValue
}

// Compile-time proof that ProtoCodec is a valid protorun.Codec for a
// message that satisfies both interfaces — i.e. it drops straight into
// protorun.RegisterCodec.
var _ protorun.Codec[*bothStub] = ProtoCodec[*bothStub]{}

func TestProtoCodec_RoundTrip(t *testing.T) {
	c := ProtoCodec[*wrapperspb.StringValue]{}
	original := wrapperspb.String("hello protobuf")

	payload, err := c.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := c.Unmarshal(payload)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !proto.Equal(got, original) {
		t.Fatalf("round-trip: got %v, want %v", got, original)
	}
	if got.GetValue() != "hello protobuf" {
		t.Fatalf("value round-trip: got %q", got.GetValue())
	}
}

func TestProtoCodec_Int64RoundTrip(t *testing.T) {
	c := ProtoCodec[*wrapperspb.Int64Value]{}
	original := wrapperspb.Int64(-1234567890123)

	payload, err := c.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := c.Unmarshal(payload)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.GetValue() != -1234567890123 {
		t.Fatalf("value round-trip: got %d", got.GetValue())
	}
}

func TestProtoCodec_UnmarshalGarbage(t *testing.T) {
	c := ProtoCodec[*wrapperspb.Int64Value]{}
	// A truncated varint is invalid protobuf and must error, not panic.
	if _, err := c.Unmarshal([]byte{0x08, 0xFF}); err == nil {
		t.Fatalf("expected error decoding malformed protobuf")
	}
}
