// Package membership is the interchangeability seam for the protocol
// library: a tiny contract of IPC types — no implementation — that any
// membership protocol can publish and any dissemination protocol can
// consume.
//
// The contract is deliberately minimal. A membership protocol answers
// GetView with the set of peers it is currently session-connected to
// (its "active view"), and publishes NeighborUp / NeighborDown as that
// set changes. A dissemination protocol (gossip, Plumtree, ...) written
// against these three types works over *any* membership protocol that
// honours them: static membership, HyParView, a future SWIM — swap the
// membership layer without touching the layer above.
//
// This is interchangeability via typed IPC contracts, not Go interfaces.
// In protorun, cross-protocol coordination is IPC — never direct method
// calls — so the "interface" two protocols share is a set of request /
// reply / notification types routed through the runtime, not a Go
// interface{} either side imports. That is the composition property
// actor frameworks structurally cannot express.
//
// # These are IPC types, not wire messages
//
// GetView / View are a Request / Reply pair; NeighborUp / NeighborDown
// are Notifications. IPC in protorun is strictly local — same-runtime,
// same-process — so these types travel between protocols on one node and
// never touch the network. Consequently they need NO codec and NO
// WireName: a codec is only for bytes on a transport, and WireName only
// freezes a *network* wire id across renames. The runtime routes IPC by
// an in-process type id (a hash of the Go type), which is stable within
// a single binary by construction. Contrast the membership protocol's
// own peer-to-peer messages (e.g. HyParView's ForwardJoin), which DO
// cross nodes and therefore DO carry codecs and WireName.
package membership

import (
	"github.com/antonionduarte/protorun/pkg/protorun"
	"github.com/antonionduarte/protorun/pkg/transport"
)

// GetView is the request a dissemination protocol issues to fetch a
// snapshot of the current active view. The membership protocol answers
// with View. Local IPC only — no codec, no WireName (see package doc).
type GetView struct{ protorun.BaseRequest }

// View is the reply to GetView. Active is a fresh slice owned by the
// caller and safe to retain and mutate.
//
// The contract carries only the active view — the peers the membership
// protocol holds a live session with, i.e. the peers a dissemination
// protocol can actually push to. It deliberately omits any "passive
// view": a passive view (a larger, unconnected sample of the system) is
// a HyParView implementation detail with no meaning for a static
// membership list or a SWIM-style protocol, and a dissemination protocol
// has nothing to send over a peer it has no session with. Keeping the
// contract to the session-backed set is what lets protocols as different
// as a static list and HyParView satisfy it interchangeably.
type View struct {
	protorun.BaseReply
	Active []transport.Host
}

// NeighborUp is published when a peer enters the active view — a new
// session-backed neighbour a dissemination protocol may now push to.
// Local IPC only — no codec, no WireName (see package doc).
type NeighborUp struct {
	protorun.BaseNotification
	Peer transport.Host
}

// NeighborDown is published when a peer leaves the active view, whether
// gracefully or through a detected session failure. A dissemination
// protocol should drop the peer from its own link sets on receipt.
type NeighborDown struct {
	protorun.BaseNotification
	Peer transport.Host
}
