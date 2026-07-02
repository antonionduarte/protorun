package protorun

import (
	"bytes"

	"github.com/antonionduarte/go-simple-protocol-runtime/pkg/transport"
)

// Sessions is the runtime's view of session management — the seam
// between the runtime and whatever binds peers to logical Hosts and
// carries application payloads to them. It is defined here, next to
// its consumer, and its shape is derived from exactly what the
// Runtime uses: nothing more.
//
// Two adapters exist: *transport.SessionLayer (the production
// handshake layer over a transport.Layer) and prototest's in-memory
// mesh (no wire, no handshake, deterministic delivery for protocol
// tests).
type Sessions interface {
	// Connect asks for a session with host. Outcomes surface
	// asynchronously on OutChannelEvents: SessionConnected once both
	// sides accept, SessionFailed or SessionVersionMismatch otherwise.
	Connect(host transport.Host)

	// Disconnect tears down the session with host; the resulting
	// SessionDisconnected surfaces on OutChannelEvents.
	Disconnect(host transport.Host)

	// Send carries an application payload to host. Delivery is
	// asynchronous and best-effort; failures surface as session
	// events, not errors.
	Send(msg bytes.Buffer, sendTo transport.Host)

	// OutMessages streams inbound application payloads.
	OutMessages() chan transport.SessionMessage

	// OutChannelEvents streams session lifecycle events.
	OutChannelEvents() chan transport.SessionEvent

	// Cancel stops the adapter's internal goroutines.
	Cancel()
}

// The production adapter satisfies the seam.
var _ Sessions = (*transport.SessionLayer)(nil)
