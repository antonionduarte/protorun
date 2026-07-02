package protorun

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/antonionduarte/protorun/transport"
)

// Message is a marker interface satisfied by any type that embeds
// BaseMessage (or implements isMessage() directly). The wire identifier
// is derived from the concrete type by the framework; no manual ID is
// required, and message values carry no framework-required fields.
//
// Sender information is delivered to handlers as a separate parameter
// (handlers have signature func(M, transport.Host)). Messages don't
// have to encode it on the wire, which keeps simple fixed-size
// message types compatible with BinaryCodec[M].
type Message interface {
	isMessage()
}

// BaseMessage is a zero-byte embeddable type. Embedding it makes any
// struct satisfy the Message interface without imposing layout
// constraints; encoding/binary can size structs that embed
// BaseMessage, so BinaryCodec[M] works on them.
//
//	type Ping struct {
//	    runtime.BaseMessage
//	    Seq uint64
//	}
type BaseMessage struct{}

func (BaseMessage) isMessage() {}

// sendMessage encodes the message using its registered codec, prepends the
// 8-byte little-endian wire identifier, and hands the buffer to the session
// layer. Returns an error if no codec is registered for the message type.
func (r *Runtime) sendMessage(msg Message, sendTo transport.Host) error {
	logger := r.Logger()

	wireID := wireIDOf(msg)
	entry, ok := r.codecs.Get(wireID)
	if !ok {
		return fmt.Errorf("%w: %T (wireID=%#x)", ErrNoCodec, msg, wireID)
	}

	payload, err := entry.codec.marshal(msg)
	if err != nil {
		logger.Error("failed to encode message",
			"type", fmt.Sprintf("%T", msg),
			"to", sendTo.String(),
			"err", err,
		)
		return err
	}

	var buffer bytes.Buffer
	if err := binary.Write(&buffer, binary.LittleEndian, wireID); err != nil {
		logger.Error("failed to write wireID header",
			"type", fmt.Sprintf("%T", msg),
			"to", sendTo.String(),
			"err", err,
		)
		return err
	}
	buffer.Write(payload)

	r.sessionLayer.Send(buffer, sendTo)
	return nil
}
