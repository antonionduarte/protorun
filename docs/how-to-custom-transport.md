# How to: a custom transport backend

`protorun`'s TCP backend is the reference implementation, not the only
possible one. Everything above `transport.Layer` — the Hello/Ack
`SessionLayer` handshake, the runtime's dispatch, `Handle`, IPC — is
written against the `Layer` interface, not against sockets. This page
shows the interface, then walks the QUIC backend
([`pkg/transport/quic`](../pkg/transport/quic/)) as a worked example of a real
second implementation, so you can see exactly what "written against
the interface" bought in practice.

## The interface

```go
// transport.Layer
type Layer interface {
	Connect(peer Address)
	Disconnect(peer Address)
	Send(msg Message, sendTo Address)

	OutChannel() chan Message
	OutEvents() chan Event

	Cancel()
}
```

A `Layer`'s whole job is: open connections to peers, carry opaque byte
frames to and from them, and report connection lifecycle
(`Connected`/`Disconnected`/`Failed`) on `OutEvents()`. It knows
nothing about Hello/Ack, wire IDs, codecs, or protocols — all of that
lives above it, unchanged, no matter which `Layer` is underneath.

Peers are addressed by `transport.Address` (`String() string` +
`Equal(other Address) bool`), not by a concrete type — `Host` (ip:port)
is the `Address` both the TCP and QUIC backends happen to use, but
nothing stops a backend from inventing its own address type (see the
package doc on `transport.Address`). `SessionLayer` is the sole
translation point between transport-level `Address`es and the stable
logical `Host`s protocols see; a custom `Layer` never needs to know
that.

**Framing is entirely yours below one byte.** `SessionLayer` frames
every outbound payload with a single `LayerIdentifier` byte
(Application vs. Session/handshake — see
[`docs/wire-format.md`](wire-format.md)) and reassembles inbound bytes
back into whole frames per peer, in order. How you get bytes from one
process to another — TCP's 4-byte big-endian length prefix, QUIC's
per-connection stream framing, or something else entirely (a message
queue, `net.PacketConn`, an in-memory channel) — is exactly the part
`Layer` exists to abstract over.

## Worked example: `pkg/transport/quic`

The QUIC backend is a real second `Layer` implementation, built to
validate the abstraction (see the Phase 3 write-up in
[`docs/roadmap.md`](roadmap.md)) — it "slotted in behind the same
interface, and the SessionLayer ran byte-for-byte unchanged" once it
was done. A few things worth borrowing the pattern from:

- **Same framing convention as TCP**, one layer down: `quic.Layer`
  still writes the 4-byte big-endian length prefix `SessionLayer`
  expects *inside* its own QUIC stream — the wire format above the
  `Layer` boundary is identical; only how bytes cross the network
  differs. You don't have to reuse this convention (it's an
  implementation choice, not part of the `Layer` contract), but reusing
  it means every codec and the whole `SessionLayer` need zero changes.
- **One connection, one stream, per peer.** `quic.NewLayer` opens one
  QUIC connection per peer pair and a single bidirectional stream on
  it — deliberately the simplest topology that satisfies `Layer`,
  mirroring TCP's one-socket-per-peer model.
- **A construction-time requirement the interface itself doesn't
  impose**: QUIC mandates TLS, so `quic.NewLayer(self, ctx, tlsConf,
  opts...)` takes a `*tls.Config` as a required parameter rather than
  an optional one (contrast `transport.WithTLS`, which is opt-in
  sugar over the TCP backend). Your own backend's constructor can
  impose whatever construction-time requirements make sense for it —
  `Layer` doesn't prescribe a constructor shape, only the resulting
  interface.
- **One documented, narrow behavioral difference.** QUIC streams are
  announced by their first write, so `quic.Layer`'s acceptor emits
  `Connected` only once the dialer sends its first frame (the Hello) —
  slightly later than TCP's accept-time `Connected`. This is invisible
  to `SessionLayer` (the server does nothing between `Connected` and
  the Hello) but it is a real, documented difference in the raw
  `Layer` contract's timing. If your backend has a similar quirk,
  document it the same way: precisely, and scoped to what actually
  changes for callers.

## Wiring it in

`protorun.WithTCPTransport` is sugar over `NewTCPLayer` +
`NewSessionLayer`. A custom backend skips that sugar and builds both
halves explicitly, then hands them to `WithTransport`:

```go
layer, err := quic.NewLayer(self, ctx, tlsConf) // or your own Layer
if err != nil {
	return err
}
session := transport.NewSessionLayer(layer, self, ctx, 0, 0)

rt := protorun.New(self, protorun.WithTransport(layer, session))
```

`NewSessionLayer` still takes a `Host` for `self` — a session's own
logical identity is `Host`-typed regardless of which `Address` type
the `Layer` underneath uses (see the package doc on
`transport.Address`). Everything above this line — every protocol,
every `Handle` registration, every IPC call — is completely unaware
which `Layer` it's running on.

## Testing

Don't reach for a custom `Layer` to unit-test protocols — that's what
[`prototest`](simulation.md) is for (an in-memory mesh at the
`Sessions` seam, not the `Layer` seam, so it doesn't require a `Layer`
implementation at all). Build a custom `Layer` when you have a real
transport to carry bytes over; test *it* the way `pkg/transport/quic` tests
itself: a layer-only suite (dial/listen, framing, events) plus a
`SessionLayer`-over-your-layer suite (handshake, established peers)
plus a two-`Runtime` integration suite — see
[`pkg/transport/quic`](../pkg/transport/quic/)'s test files for the pattern.
