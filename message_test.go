package protorun

import (
	"testing"

	"github.com/antonionduarte/protorun/transport"
)

// TestSendMessage_CodecError verifies that sendMessage propagates the
// error from a registered codec's Encode call.
func TestSendMessage_CodecError(t *testing.T) {
	self := transport.NewHost(0, "127.0.0.1")
	rt := New(self)

	mock := &MockProtocol{}
	proto := newProtoProtocol(mock, 0)
	rt.registerProtocol(proto)

	// Construct the protocolContext so we can route through the registration
	// path that updates the codec registry. Calling Start() would do this,
	// but avoiding it keeps the test isolated to sendMessage.
	proto.ensureContext()
	RegisterCodec(proto.ctx, failingCodec{})

	msg := &failingMessageBM{}
	if err := rt.sendMessage(msg, transport.NewHost(8000, "127.0.0.1")); err == nil {
		t.Fatalf("expected sendMessage to return error when Encode fails")
	}
}

// TestSendMessage_NoCodecRegistered verifies that sendMessage returns a
// clear error if no codec is registered for the message type, instead of
// nil-dereferencing.
func TestSendMessage_NoCodecRegistered(t *testing.T) {
	self := transport.NewHost(0, "127.0.0.1")
	rt := New(self)
	rt.registerProtocol(newProtoProtocol(&MockProtocol{}, 0))

	msg := &localMessage{}
	if err := rt.sendMessage(msg, transport.NewHost(8000, "127.0.0.1")); err == nil {
		t.Fatalf("expected sendMessage to error when no codec is registered")
	}
}
