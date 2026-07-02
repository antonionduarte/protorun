package protocol

import "github.com/antonionduarte/protorun"

// PingMessage and PongMessage are pure fixed-size payloads. Because
// protorun.BaseMessage is a zero-byte marker, encoding/binary can size
// these structs and protorun.BinaryCodec[*M] handles encode/decode for
// us; no manual codec needed.
type (
	PingMessage struct {
		protorun.BaseMessage
		Seq uint64
	}

	PongMessage struct {
		protorun.BaseMessage
		Seq uint64
	}
)

func NewPingMessage(seq uint64) *PingMessage { return &PingMessage{Seq: seq} }
func NewPongMessage(seq uint64) *PongMessage { return &PongMessage{Seq: seq} }
