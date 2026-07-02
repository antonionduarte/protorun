// Package gossip implements an eager-push gossip protocol on top of
// the membership protocol.
package gossip

import "github.com/antonionduarte/protorun/pkg/protorun"

// Message is a single gossip envelope: an ID for deduplication, plus
// an opaque payload. Forwarded verbatim through the network until
// every node has seen it once.
//
// The variable-length Payload used to need a hand-written codec over
// the wire helpers; the reflective WireCodec (registered via
// protorun.Handle in Start) now expresses it directly.
type Message struct {
	protorun.BaseMessage
	ID      uint64
	Payload []byte
}
