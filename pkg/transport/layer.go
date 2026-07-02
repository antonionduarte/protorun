package transport

import "bytes"

// Layer is a transport backend: it opens connections to peers, carries
// opaque byte frames to them, and reports connection lifecycle events.
//
// Every method addresses a peer by Address, not Host. Host is the
// endpoint type the TCP (and QUIC) backends happen to use, but the
// interface stays abstract so a backend can address peers by something
// that isn't ip:port. The SessionLayer above translates between these
// transport-level Addresses and the stable logical Hosts protocols see
// (see session.go); the runtime never touches an Address directly.
type Layer interface {
	Connect(peer Address)
	Disconnect(peer Address)
	Send(msg Message, sendTo Address)

	OutChannel() chan Message
	OutEvents() chan Event

	Cancel()
}

type (
	// Message is one inbound/outbound frame. Peer is the transport-level
	// Address it came from (inbound) or the routing hint for a send
	// (outbound — the explicit sendTo argument is authoritative, Peer is
	// only carried for symmetry). Msg is the opaque body; the layers
	// above own its structure (see docs/wire-format.md).
	Message struct {
		Peer Address
		Msg  bytes.Buffer
	}

	// Event is a transport-level connection lifecycle signal. Peer is the
	// transport Address it concerns.
	Event interface {
		Peer() Address
	}

	Connected struct {
		peer Address
	}

	Disconnected struct {
		peer Address
	}

	Failed struct {
		peer Address
	}
)

// NewMessage builds a Message addressed to peer.
func NewMessage(msg bytes.Buffer, peer Address) Message {
	return Message{Msg: msg, Peer: peer}
}

// Event constructors. The event structs keep their peer field unexported,
// so out-of-tree Layer backends (the QUIC module, custom transports)
// construct events through these instead of a struct literal.
func NewConnected(peer Address) *Connected       { return &Connected{peer: peer} }
func NewDisconnected(peer Address) *Disconnected { return &Disconnected{peer: peer} }
func NewFailed(peer Address) *Failed             { return &Failed{peer: peer} }

func (e *Connected) Peer() Address    { return e.peer }
func (e *Disconnected) Peer() Address { return e.peer }
func (e *Failed) Peer() Address       { return e.peer }
