package prototest

import (
	"encoding/binary"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/antonionduarte/protorun"
	"github.com/antonionduarte/protorun/transport"
)

// reconnectInterval is how often a flood node re-dials contacts it is not
// connected to, so a healed partition reconverges on the virtual clock.
const reconnectInterval = 500 * time.Millisecond

// errShortFlood is returned when a floodMsg payload is truncated.
var errShortFlood = errors.New("prototest: short floodMsg payload")

// This file holds a small eager-push flood-broadcast protocol used by the
// Sim proof tests. It is deliberately minimal but exercises the surface a
// real gossip protocol uses: a message codec, session-event handlers to
// track live peers, a reconnect timer, and an event trace so tests can
// assert on exactly what each node handled and in what order.

// floodMsg is a broadcast: a unique id plus a decrementing hop budget so
// a flood terminates. Encoded with the SelfMarshaler path so the codec is
// explicit and stable.
type floodMsg struct {
	protorun.BaseMessage
	ID   uint64
	Hops uint32
}

func (floodMsg) WireName() string { return "prototest.floodMsg" }

func (m *floodMsg) MarshalWire() ([]byte, error) {
	b := make([]byte, 12)
	binary.LittleEndian.PutUint64(b[0:8], m.ID)
	binary.LittleEndian.PutUint32(b[8:12], m.Hops)
	return b, nil
}

func (m *floodMsg) UnmarshalWire(b []byte) error {
	if len(b) < 12 {
		return errShortFlood
	}
	m.ID = binary.LittleEndian.Uint64(b[0:8])
	m.Hops = binary.LittleEndian.Uint32(b[8:12])
	return nil
}

// traceEntry is one recorded event in a node's handling order.
type traceEntry struct {
	Kind string // "connect", "disconnect", "deliver", "reflood"
	Peer transport.Host
	ID   uint64
}

// floodProtocol is an eager-push flood: on receiving a new broadcast it
// delivers it once and forwards it to every live peer except the sender.
// It dials a fixed contact set on Init and re-dials disconnected peers on
// a periodic timer, so a healed partition reconverges without any test
// intervention.
type floodProtocol struct {
	self     transport.Host
	contacts []transport.Host

	ctx   protorun.ProtocolContext
	peers map[transport.Host]struct{}
	seen  map[uint64]struct{}

	// delivered records, under mu, the set of broadcast ids this node
	// delivered to the application. Read by tests from the test goroutine
	// at quiescent points.
	mu        sync.Mutex
	delivered map[uint64]struct{}
	trace     []traceEntry
}

func newFloodProtocol(self transport.Host, contacts ...transport.Host) *floodProtocol {
	return &floodProtocol{
		self:      self,
		contacts:  contacts,
		peers:     make(map[transport.Host]struct{}),
		seen:      make(map[uint64]struct{}),
		delivered: make(map[uint64]struct{}),
	}
}

func (p *floodProtocol) Start(ctx protorun.ProtocolContext) {
	p.ctx = ctx
	protorun.Handle(ctx, p.onFlood)
}

func (p *floodProtocol) Init(ctx protorun.ProtocolContext) {
	// Re-dial any contact we are not currently connected to, now and every
	// interval. Plain Connect (not the retry table) keeps reconnection
	// purely timer-driven: a partition heals deterministically on the
	// shared virtual clock, with no backoff/give-up state to reason about.
	redial := func() {
		for _, c := range p.contacts {
			if _, up := p.peers[c]; !up {
				_ = p.ctx.Connect(c)
			}
		}
	}
	redial()
	ctx.Every(reconnectInterval, redial)
}

func (p *floodProtocol) OnSessionConnected(h transport.Host) {
	p.peers[h] = struct{}{}
	p.record(traceEntry{Kind: "connect", Peer: h})
}

func (p *floodProtocol) OnSessionDisconnected(h transport.Host) {
	delete(p.peers, h)
	p.record(traceEntry{Kind: "disconnect", Peer: h})
}

// Broadcast originates a new flood from this node. Safe to call from a
// handler (e.g. a timer) — it runs on the event loop.
func (p *floodProtocol) Broadcast(id uint64, hops uint32) {
	p.seen[id] = struct{}{}
	p.deliver(id)
	p.forward(&floodMsg{ID: id, Hops: hops}, transport.Host{})
}

func (p *floodProtocol) onFlood(m *floodMsg, from transport.Host) {
	if _, dup := p.seen[m.ID]; dup {
		return
	}
	p.seen[m.ID] = struct{}{}
	p.deliver(m.ID)
	p.record(traceEntry{Kind: "reflood", Peer: from, ID: m.ID})
	if m.Hops > 0 {
		p.forward(&floodMsg{ID: m.ID, Hops: m.Hops - 1}, from)
	}
}

func (p *floodProtocol) forward(m *floodMsg, except transport.Host) {
	// Iterate peers in a stable order: Go randomizes map iteration, and
	// send order determines the order deliveries land in the scheduler,
	// which the seeded RNG then interleaves. Sorting here is what keeps a
	// protocol that follows the authoring contract fully deterministic.
	peers := make([]transport.Host, 0, len(p.peers))
	for peer := range p.peers {
		peers = append(peers, peer)
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i].String() < peers[j].String() })
	for _, peer := range peers {
		if peer == except {
			continue
		}
		_ = p.ctx.Send(m, peer)
	}
}

func (p *floodProtocol) deliver(id uint64) {
	p.mu.Lock()
	p.delivered[id] = struct{}{}
	p.trace = append(p.trace, traceEntry{Kind: "deliver", ID: id})
	p.mu.Unlock()
}

func (p *floodProtocol) record(e traceEntry) {
	p.mu.Lock()
	p.trace = append(p.trace, e)
	p.mu.Unlock()
}

// delivered reports whether this node has delivered broadcast id.
func (p *floodProtocol) hasDelivered(id uint64) bool {
	p.mu.Lock()
	_, ok := p.delivered[id]
	p.mu.Unlock()
	return ok
}

// snapshotTrace copies this node's handling trace.
func (p *floodProtocol) snapshotTrace() []traceEntry {
	p.mu.Lock()
	out := make([]traceEntry, len(p.trace))
	copy(out, p.trace)
	p.mu.Unlock()
	return out
}

// observerProtocol is a tiny probe protocol for the timer/delay proof
// tests: it can dial contacts, arm timers in onInit, and observe inbound
// floodMsgs. All callbacks run on its event loop, so they read/write test
// slices without their own locking (the Sim settles between steps, and the
// test only reads at quiescent points).
type observerProtocol struct {
	self        transport.Host
	contacts    []transport.Host
	handleFlood bool
	onInit      func(*observerProtocol)
	onMsg       func()

	ctx protorun.ProtocolContext
}

func (o *observerProtocol) Start(ctx protorun.ProtocolContext) {
	o.ctx = ctx
	if o.handleFlood {
		protorun.Handle(ctx, o.onFlood)
	}
}

func (o *observerProtocol) Init(ctx protorun.ProtocolContext) {
	for _, c := range o.contacts {
		_ = ctx.ConnectWithRetry(c)
	}
	if o.onInit != nil {
		o.onInit(o)
	}
}

func (o *observerProtocol) onFlood(_ *floodMsg, _ transport.Host) {
	if o.onMsg != nil {
		o.onMsg()
	}
}
