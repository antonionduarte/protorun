package transport

// Address is the abstract identity of a peer reachable through a
// transport.Layer. Today the only implementation is Host (an IP/port
// pair tied to TCP), but the interface exists so future Layer
// backends (UDP, in-memory mesh for tests, QUIC, transport-over-
// websockets) can supply their own address type without forcing the
// runtime to know about it.
//
// String must return a stable human-readable representation; the
// session layer uses it as a map key (because Go interfaces aren't
// comparable in general).
//
// Equal compares this address with another for identity. It must be
// reflexive, symmetric, and transitive. Implementations should
// fast-path the same-concrete-type case and return false for
// addresses of unrelated kinds.
//
// Host satisfies Address via its String() method (a value-receiver
// "IP:port" formatter) and Equal(other Address) which type-asserts
// the other to Host and compares structurally.
//
// Layer's methods (Connect/Disconnect/Send, Message.Peer, Event.Peer())
// are parameterised on Address, not Host (this migration landed in
// Phase 3 / v0.5.0 once the QUIC backend gave it a real second
// implementation to validate against). SessionLayer is the sole
// translation point between transport-level Addresses and the stable
// logical Hosts protocols see; NewSessionLayer itself still takes a
// Host for `self`, since a session's own logical identity is Host-typed
// regardless of which Address type the Layer underneath uses.
type Address interface {
	String() string
	Equal(other Address) bool
}

// Equal compares this Host to another Address. Returns false if the
// other side isn't a Host (different transport backend or test
// double). Reflexive, symmetric, transitive: Host is a comparable
// struct, so == on the underlying values gives us all three for free.
func (h Host) Equal(other Address) bool {
	if other == nil {
		return false
	}
	o, ok := other.(Host)
	if !ok {
		// Pointer-receiver call site; accept *Host too for user code
		// that holds onto a *Host (rare but legal).
		hp, isPtr := other.(*Host)
		if !isPtr || hp == nil {
			return false
		}
		o = *hp
	}
	return h == o
}
