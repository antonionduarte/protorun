package plumtree

import (
	"bytes"

	"github.com/antonionduarte/protorun/pkg/protorun"
	"github.com/antonionduarte/protorun/pkg/transport"
	"github.com/antonionduarte/protorun/pkg/wire"
)

// MessageID identifies a broadcast uniquely and cheaply as (origin, seq):
// the host that first broadcast it plus a per-origin monotonic counter.
//
// This is the sender+seq scheme, chosen over content-addressing (hashing
// the payload). Justification: it is O(1) to mint (no payload hash),
// collision-free by construction (each origin owns its own sequence
// space and hosts are unique), compact and fixed-cost as a map key and
// cache key, and it names the origin — useful for debugging and for the
// Delivered.From surface. Content-addressing would let identical payloads
// dedup, but Plumtree's job is to deliver each *broadcast* once, not to
// coalesce equal bytes, so origin+seq is the better fit. MessageID is a
// comparable struct, so it is used directly as a Go map key.
type MessageID struct {
	Origin transport.Host
	Seq    uint64
}

func writeMessageID(b *bytes.Buffer, id MessageID) error {
	if err := transport.WriteHost(b, id.Origin); err != nil {
		return err
	}
	return wire.WriteUint64(b, id.Seq)
}

func readMessageID(r *bytes.Reader) (MessageID, error) {
	var id MessageID
	origin, err := transport.ReadHost(r)
	if err != nil {
		return id, err
	}
	seq, err := wire.ReadUint64(r)
	if err != nil {
		return id, err
	}
	id.Origin, id.Seq = origin, seq
	return id, nil
}

// The Plumtree wire messages. Each crosses the network, so each carries a
// WireName (freezes the wire id across a rename) and a codec. They use
// the SelfMarshaler path for the same reason as HyParView's: MessageID
// embeds a transport.Host, whose int port WireCodec rejects, and
// transport.WriteHost/ReadHost make the hand-written encoding trivial.

// Gossip is an eager push of a full broadcast down a tree link. Round is
// the hop distance from the origin, used to move senders between the
// eager and lazy sets during tree repair.
type Gossip struct {
	protorun.BaseMessage
	ID      MessageID
	Round   uint32
	Payload []byte
}

func (Gossip) WireName() string { return "plumtree.Gossip" }

func (m *Gossip) MarshalWire() ([]byte, error) {
	var b bytes.Buffer
	if err := writeMessageID(&b, m.ID); err != nil {
		return nil, err
	}
	if err := wire.WriteUint32(&b, m.Round); err != nil {
		return nil, err
	}
	if err := wire.WriteBytes(&b, m.Payload); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (m *Gossip) UnmarshalWire(data []byte) error {
	r := bytes.NewReader(data)
	id, err := readMessageID(r)
	if err != nil {
		return err
	}
	round, err := wire.ReadUint32(r)
	if err != nil {
		return err
	}
	payload, err := wire.ReadBytes(r)
	if err != nil {
		return err
	}
	m.ID, m.Round, m.Payload = id, round, payload
	return nil
}

// announce is one lazy-push announcement: "I have message ID (seen at
// round Round)". Batched into IHave.
type announce struct {
	ID    MessageID
	Round uint32
}

// IHave is a lazy push: a batched announcement of message ids to a
// non-tree neighbour. The receiver arms a timer and, if the real Gossip
// does not arrive first, GRAFTs the announcer to pull the message and
// repair the tree.
type IHave struct {
	protorun.BaseMessage
	Announcements []announce
}

func (IHave) WireName() string { return "plumtree.IHave" }

func (m *IHave) MarshalWire() ([]byte, error) {
	var b bytes.Buffer
	if err := wire.WriteUint16(&b, uint16(len(m.Announcements))); err != nil {
		return nil, err
	}
	for _, a := range m.Announcements {
		if err := writeMessageID(&b, a.ID); err != nil {
			return nil, err
		}
		if err := wire.WriteUint32(&b, a.Round); err != nil {
			return nil, err
		}
	}
	return b.Bytes(), nil
}

func (m *IHave) UnmarshalWire(data []byte) error {
	r := bytes.NewReader(data)
	n, err := wire.ReadUint16(r)
	if err != nil {
		return err
	}
	anns := make([]announce, 0, n)
	for range int(n) {
		id, err := readMessageID(r)
		if err != nil {
			return err
		}
		round, err := wire.ReadUint32(r)
		if err != nil {
			return err
		}
		anns = append(anns, announce{ID: id, Round: round})
	}
	m.Announcements = anns
	return nil
}

// Graft asks the receiver to (re)start eager-pushing: add the sender to
// its eager set and, if it still holds ID in its cache, replay the
// Gossip. It is triggered by a missing-message timer firing on an IHave.
type Graft struct {
	protorun.BaseMessage
	ID    MessageID
	Round uint32
}

func (Graft) WireName() string { return "plumtree.Graft" }

func (m *Graft) MarshalWire() ([]byte, error) {
	var b bytes.Buffer
	if err := writeMessageID(&b, m.ID); err != nil {
		return nil, err
	}
	if err := wire.WriteUint32(&b, m.Round); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (m *Graft) UnmarshalWire(data []byte) error {
	r := bytes.NewReader(data)
	id, err := readMessageID(r)
	if err != nil {
		return err
	}
	round, err := wire.ReadUint32(r)
	if err != nil {
		return err
	}
	m.ID, m.Round = id, round
	return nil
}

// Prune moves the sender to the receiver's lazy set: it is sent on a
// duplicate Gossip receipt, which means the link is a redundant tree
// edge. Pruning duplicate edges is what collapses the eager-push graph
// toward a spanning tree.
type Prune struct{ protorun.BaseMessage }

func (Prune) WireName() string             { return "plumtree.Prune" }
func (Prune) MarshalWire() ([]byte, error) { return nil, nil }
func (*Prune) UnmarshalWire([]byte) error  { return nil }
