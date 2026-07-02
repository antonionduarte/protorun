package protocol

import "github.com/antonionduarte/protorun/pkg/protorun"

// PingMessage and PongMessage are pure fixed-size payloads. The
// protocol registers them with protorun.Handle, which picks the
// reflective WireCodec[*M] automatically; no manual codec needed.
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
