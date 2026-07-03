package protorun

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/antonionduarte/protorun/pkg/transport"
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
// layer — or, when sendTo is the local Host, loops it back through the
// normal inbound path. Returns an error if no codec is registered for the
// message type.
//
// Self-delivery semantics: the frame is decoded exactly as if it had
// arrived from a peer, so the handler receives a FRESH instance (no
// aliasing of the sender's struct — mutating the sent value after Send
// cannot affect the handler), the message routes to whichever protocol
// registered the codec, and it is enqueued behind whatever is already in
// that protocol's mailbox — a handler that Sends to self returns before
// the self-message is handled. This exists for protocols where the local
// node is a full member of its own quorum (a Paxos node is proposer AND
// acceptor); without it, Send-to-self silently reached no one.
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

	if r.tracerEnabled {
		r.trace(&TraceEvent{Kind: "send", Peer: sendTo, Wire: wireID, Bytes: len(payload)})
	}

	if sendTo == r.self {
		r.processMessage(buffer, r.self)
		return nil
	}
	r.sessionLayer.Send(buffer, sendTo)
	return nil
}
