package hyparview

import (
	"bytes"

	"github.com/antonionduarte/protorun/pkg/protorun"
	"github.com/antonionduarte/protorun/pkg/transport"
	"github.com/antonionduarte/protorun/pkg/wire"
)

// The HyParView control messages. Every one crosses the network, so
// every one carries a codec and a WireName: WireName freezes the wire id
// across a future rename of the Go type, which is the production
// requirement the framework's strict mode nudges about.
//
// Encoding uses the SelfMarshaler path (MarshalWire/UnmarshalWire) rather
// than the reflective WireCodec. The reason is transport.Host: it holds
// a plain int port, and WireCodec deliberately rejects platform-sized
// int (its width is not fixed across architectures, so it is unsafe on
// the wire). transport ships WriteHost / ReadHost for exactly this, so a
// hand-written SelfMarshaler over the wire helpers is both the simplest
// and the only correct option for the Host-carrying types here — and we
// keep the whole message set on one encoding path for consistency.

// Join is sent by a node bootstrapping into the overlay, to a contact,
// immediately after the session to that contact comes up. The contact
// accepts unconditionally (dropping a random active peer if it must make
// room) and disseminates a ForwardJoin random walk.
type Join struct{ protorun.BaseMessage }

func (Join) WireName() string             { return "hyparview.Join" }
func (Join) MarshalWire() ([]byte, error) { return nil, nil }
func (*Join) UnmarshalWire([]byte) error  { return nil }

// ForwardJoin propagates a join outward as a random walk over active
// views. NewNode is the node that joined; TTL is the remaining walk
// length. At TTL 0 (or a node with an otherwise-empty active view) the
// walk terminates and the receiver adds NewNode to its active view.
type ForwardJoin struct {
	protorun.BaseMessage
	NewNode transport.Host
	TTL     uint32
}

func (ForwardJoin) WireName() string { return "hyparview.ForwardJoin" }

func (m *ForwardJoin) MarshalWire() ([]byte, error) {
	var b bytes.Buffer
	if err := transport.WriteHost(&b, m.NewNode); err != nil {
		return nil, err
	}
	if err := wire.WriteUint32(&b, m.TTL); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (m *ForwardJoin) UnmarshalWire(data []byte) error {
	r := bytes.NewReader(data)
	h, err := transport.ReadHost(r)
	if err != nil {
		return err
	}
	ttl, err := wire.ReadUint32(r)
	if err != nil {
		return err
	}
	m.NewNode, m.TTL = h, ttl
	return nil
}

// Neighbor asks the receiver to add the sender to its active view.
// Priority is set when the sender's own active view is empty (a node
// with no neighbours must be admitted), in which case the receiver
// accepts even if it has to evict an existing active peer. A
// low-priority request is rejected when the receiver's active view is
// full.
type Neighbor struct {
	protorun.BaseMessage
	Priority bool
}

func (Neighbor) WireName() string { return "hyparview.Neighbor" }

func (m *Neighbor) MarshalWire() ([]byte, error) { return marshalBool(m.Priority), nil }
func (m *Neighbor) UnmarshalWire(data []byte) error {
	m.Priority = unmarshalBool(data)
	return nil
}

// NeighborReply answers a Neighbor request. Accepted false means the
// receiver's active view was full and the request was low priority; the
// requester drops the session and files the peer back into its passive
// view.
type NeighborReply struct {
	protorun.BaseMessage
	Accepted bool
}

func (NeighborReply) WireName() string { return "hyparview.NeighborReply" }

func (m *NeighborReply) MarshalWire() ([]byte, error) { return marshalBool(m.Accepted), nil }
func (m *NeighborReply) UnmarshalWire(data []byte) error {
	m.Accepted = unmarshalBool(data)
	return nil
}

// Disconnect is a graceful "I am removing you from my active view"
// notice. The receiver moves the sender from its active view to its
// passive view (as opposed to a detected session failure, after which
// the peer is presumed dead and NOT filed into passive).
type Disconnect struct{ protorun.BaseMessage }

func (Disconnect) WireName() string             { return "hyparview.Disconnect" }
func (Disconnect) MarshalWire() ([]byte, error) { return nil, nil }
func (*Disconnect) UnmarshalWire([]byte) error  { return nil }

// Shuffle is the periodic passive-view exchange, propagated as a TTL
// random walk over active views (never over passive peers — see the
// shuffle note in protocol.go). Origin is the node that initiated the
// shuffle; Active/Passive are the sample it offers; Path records the
// forwarders the walk has visited (Origin first) so the ShuffleReply can
// retrace it home over the same active links.
type Shuffle struct {
	protorun.BaseMessage
	Origin  transport.Host
	TTL     uint32
	Active  []transport.Host
	Passive []transport.Host
	Path    []transport.Host
}

func (Shuffle) WireName() string { return "hyparview.Shuffle" }

func (m *Shuffle) MarshalWire() ([]byte, error) {
	var b bytes.Buffer
	if err := transport.WriteHost(&b, m.Origin); err != nil {
		return nil, err
	}
	if err := wire.WriteUint32(&b, m.TTL); err != nil {
		return nil, err
	}
	if err := writeHostList(&b, m.Active); err != nil {
		return nil, err
	}
	if err := writeHostList(&b, m.Passive); err != nil {
		return nil, err
	}
	if err := writeHostList(&b, m.Path); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (m *Shuffle) UnmarshalWire(data []byte) error {
	r := bytes.NewReader(data)
	origin, err := transport.ReadHost(r)
	if err != nil {
		return err
	}
	ttl, err := wire.ReadUint32(r)
	if err != nil {
		return err
	}
	if m.Active, err = readHostList(r); err != nil {
		return err
	}
	if m.Passive, err = readHostList(r); err != nil {
		return err
	}
	if m.Path, err = readHostList(r); err != nil {
		return err
	}
	m.Origin, m.TTL = origin, ttl
	return nil
}

// ShuffleReply carries a sample of the accepting node's passive view
// back to the shuffle's origin. Route is the remaining active-link path
// to the origin (origin first); each hop forwards the reply to the last
// element and trims it, until the origin recognises itself at the head.
type ShuffleReply struct {
	protorun.BaseMessage
	Nodes []transport.Host
	Route []transport.Host
}

func (ShuffleReply) WireName() string { return "hyparview.ShuffleReply" }

func (m *ShuffleReply) MarshalWire() ([]byte, error) {
	var b bytes.Buffer
	if err := writeHostList(&b, m.Nodes); err != nil {
		return nil, err
	}
	if err := writeHostList(&b, m.Route); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (m *ShuffleReply) UnmarshalWire(data []byte) error {
	r := bytes.NewReader(data)
	nodes, err := readHostList(r)
	if err != nil {
		return err
	}
	route, err := readHostList(r)
	if err != nil {
		return err
	}
	m.Nodes, m.Route = nodes, route
	return nil
}

// --- wire helpers ------------------------------------------------------

func marshalBool(v bool) []byte {
	if v {
		return []byte{1}
	}
	return []byte{0}
}

func unmarshalBool(b []byte) bool { return len(b) > 0 && b[0] != 0 }

// writeHostList encodes a slice of Hosts as a uint16 count followed by
// each Host via transport.WriteHost.
func writeHostList(b *bytes.Buffer, hosts []transport.Host) error {
	if err := wire.WriteUint16(b, uint16(len(hosts))); err != nil {
		return err
	}
	for _, h := range hosts {
		if err := transport.WriteHost(b, h); err != nil {
			return err
		}
	}
	return nil
}

// readHostList reads a list written by writeHostList.
func readHostList(r *bytes.Reader) ([]transport.Host, error) {
	n, err := wire.ReadUint16(r)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	hosts := make([]transport.Host, 0, n)
	for range int(n) {
		h, err := transport.ReadHost(r)
		if err != nil {
			return nil, err
		}
		hosts = append(hosts, h)
	}
	return hosts, nil
}
