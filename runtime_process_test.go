package protorun

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/transport"
)

// processFrame builds the on-wire bytes for a single application-level
// message: [WireID(uint64 LE)][payload]. Used by the processMessage tests.
func processFrame(t *testing.T, wireID uint64, payload []byte) bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, wireID); err != nil {
		t.Fatalf("write wireID: %v", err)
	}
	buf.Write(payload)
	return buf
}

// TestProcessMessage_DispatchesToHandler verifies that a well-formed frame
// is decoded and pushed into the owning protocol's messageChannel.
func TestProcessMessage_DispatchesToHandler(t *testing.T) {
	rt := New(transport.NewHost(9001, "127.0.0.1"))

	proto := newProtoProtocol(&MockProtocol{}, 0)
	rt.registerProtocol(proto)
	proto.ensureContext()
	RegisterCodec(proto.ctx, localCodec{})

	frame := processFrame(t, WireID[*localMessage](), nil)
	rt.processMessage(frame, transport.NewHost(9999, "127.0.0.1"))

	select {
	case env := <-proto.messageChannel:
		if _, ok := env.msg.(*localMessage); !ok {
			t.Fatalf("expected *localMessage, got %T", env.msg)
		}
	case <-time.After(time.Second):
		t.Fatalf("expected a message to be dispatched to proto.messageChannel")
	}
}

// TestProcessMessage_UnknownWireID ensures that an unknown wire id is
// dropped without panic and without dispatch.
func TestProcessMessage_UnknownWireID(t *testing.T) {
	rt := New(transport.NewHost(0, "127.0.0.1"))
	frame := processFrame(t, 0xdeadbeef, nil)
	rt.processMessage(frame, transport.NewHost(8888, "127.0.0.1"))
}

// TestProcessMessage_DecodeError ensures that if the codec returns an error
// the message is not dispatched.
func TestProcessMessage_DecodeError(t *testing.T) {
	rt := New(transport.NewHost(9201, "127.0.0.1"))

	proto := newProtoProtocol(&MockProtocol{}, 0)
	rt.registerProtocol(proto)
	proto.ensureContext()
	RegisterCodec(proto.ctx, failingCodec{})

	frame := processFrame(t, WireID[*failingMessageBM](), nil)
	rt.processMessage(frame, transport.NewHost(9999, "127.0.0.1"))

	select {
	case env := <-proto.messageChannel:
		t.Fatalf("did not expect a message to be dispatched when Decode fails, got %+v", env)
	default:
		// ok
	}
}

// TestProcessMessage_TruncatedHeader ensures that a buffer that doesn't
// contain enough bytes for the uint64 wire id is handled gracefully.
func TestProcessMessage_TruncatedHeader(t *testing.T) {
	rt := New(transport.NewHost(0, "127.0.0.1"))
	var buf bytes.Buffer
	buf.WriteByte(0x01)
	rt.processMessage(buf, transport.NewHost(9999, "127.0.0.1"))
}
