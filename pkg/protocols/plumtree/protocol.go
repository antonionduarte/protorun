// Package plumtree implements Plumtree — the Epidemic Broadcast Trees
// protocol of Leitão, Pereira and Rodrigues (2007) — over the
// protocols/membership contract. It builds a spanning tree of "eager"
// links along which full messages are pushed, while the remaining
// neighbours receive cheap IHAVE announcements ("lazy" links). Duplicate
// receipts PRUNE redundant tree edges and missing-message timers GRAFT
// lazy edges back into the tree, so the eager-link graph self-optimises
// toward a spanning tree and self-heals after churn.
//
// It consumes membership.NeighborUp/NeighborDown to learn its peer set
// (every new neighbour starts eager, per the paper) and answers the app
// layer with a Delivered notification per unique broadcast. Originate a
// broadcast with the Broadcast request. This mirrors the trigger pattern
// the gossip example established: cross-protocol and app coordination is
// IPC, so the public surface is request/reply + notification types, not
// exported methods.
//
// # Anti-entropy caveat
//
// Plumtree recovers a missed message only by GRAFTing a peer that still
// holds it in its bounded cache (Config.CacheSize). It is NOT a full
// anti-entropy protocol: if a node is partitioned for longer than the
// cache retains a message — or falls further behind than CacheSize
// messages — that message is gone for it, and no GRAFT can recover it.
// Subsequent broadcasts are unaffected (the tree repairs and delivers
// them everywhere); only the messages that aged out of every reachable
// cache during the outage are lost. Bridging longer partitions is the
// application's responsibility (e.g. a periodic state snapshot), by
// design — this keeps Plumtree honest and cheap.
package plumtree

import (
	"sort"

	"github.com/antonionduarte/protorun/pkg/protocols/membership"
	"github.com/antonionduarte/protorun/pkg/protorun"
	"github.com/antonionduarte/protorun/pkg/transport"
)

// Broadcast is the request the app (or a peer protocol) issues to
// originate a broadcast. Fire-and-forget: the reply BroadcastAck only
// confirms the payload was accepted and queued for dissemination, exactly
// like the gossip example's TriggerBroadcast. Local IPC — no codec.
type Broadcast struct {
	protorun.BaseRequest
	Payload []byte
}

// BroadcastAck is the empty reply to Broadcast.
type BroadcastAck struct{ protorun.BaseReply }

// Delivered is published once per unique broadcast this node delivers.
// ID identifies the broadcast, Payload is its bytes, and From is the peer
// it was received from (or this node's own host for a locally-originated
// broadcast). Local IPC notification — no codec.
type Delivered struct {
	protorun.BaseNotification
	ID      MessageID
	Payload []byte
	From    transport.Host
}

// pendingGraft is one recorded IHAVE awaiting its message: who announced
// it and at what round (the round is echoed in the GRAFT).
type pendingGraft struct {
	peer  transport.Host
	round uint32
}

// Protocol is a Plumtree instance. Construct with New and Register it
// with a runtime that also hosts a membership protocol publishing the
// contract. All state is owned by the event loop.
type Protocol struct {
	cfg  Config
	ctx  protorun.ProtocolContext
	self transport.Host

	eager *hostSet // tree links: full messages pushed here
	lazy  *hostSet // non-tree neighbours: only IHAVE announcements

	received map[MessageID]struct{} // ids delivered, for dedup
	cache    *payloadCache          // retained payloads, for serving GRAFTs

	// missing tracks IHAVE announcements for messages not yet received,
	// and the timer that will GRAFT for each.
	missing map[MessageID][]pendingGraft
	timers  map[MessageID]protorun.TimerHandle

	// lazyQueue batches outgoing IHAVE announcements per peer between
	// flushes.
	lazyQueue map[transport.Host][]announce

	seq uint64 // per-origin sequence counter for our own broadcasts

	// duplicates counts Gossip messages received for an already-seen id
	// (each triggers a PRUNE). Surfaced via DebugStats so tests can assert
	// the eager graph converges toward a spanning tree (few duplicates).
	duplicates int
}

// New returns a Plumtree protocol configured by cfg. The zero Config is
// valid — see Config for the defaults.
func New(self transport.Host, cfg Config) *Protocol {
	cfg = cfg.withDefaults()
	return &Protocol{
		cfg:       cfg,
		self:      self,
		eager:     newHostSet(),
		lazy:      newHostSet(),
		received:  make(map[MessageID]struct{}),
		cache:     newPayloadCache(cfg.CacheSize),
		missing:   make(map[MessageID][]pendingGraft),
		timers:    make(map[MessageID]protorun.TimerHandle),
		lazyQueue: make(map[transport.Host][]announce),
	}
}

func (p *Protocol) Start(ctx protorun.ProtocolContext) {
	p.ctx = ctx
	protorun.Handle(ctx, p.onGossip)
	protorun.Handle(ctx, p.onIHave)
	protorun.Handle(ctx, p.onGraft)
	protorun.Handle(ctx, p.onPrune)
	protorun.SubscribeNotification(ctx, p.onNeighborUp)
	protorun.SubscribeNotification(ctx, p.onNeighborDown)
	protorun.RegisterRequestHandler(ctx, p.handleBroadcast)
	protorun.RegisterRequestHandler(ctx, p.handleDebugStats)
}

// DebugStats is a Plumtree-specific introspection request exposing tree
// shape and duplicate counters for tests and tooling. IPC keeps the read
// on the framework's supported path rather than racing event-loop state.
type DebugStats struct{ protorun.BaseRequest }

// DebugStatsReply carries a snapshot of this node's Plumtree counters
// and tree shape. EagerPeers/LazyPeers are the actual sets (sorted), so
// tooling (protoviz's broadcast-tree lens) can draw the tree from real
// state instead of reconstructing it from the message stream; the
// counters remain for tests that only assert cardinality.
type DebugStatsReply struct {
	protorun.BaseReply
	Delivered  int // unique broadcasts delivered
	Duplicates int // Gossip received for an already-seen id (each pruned)
	Eager      int // eager (tree) peer count
	Lazy       int // lazy peer count

	EagerPeers []transport.Host // eager (tree) links, sorted
	LazyPeers  []transport.Host // lazy (IHave) links, sorted
}

func (p *Protocol) handleDebugStats(_ *DebugStats, r protorun.Responder[*DebugStatsReply]) {
	r.Reply(&DebugStatsReply{
		Delivered:  len(p.received),
		Duplicates: p.duplicates,
		Eager:      p.eager.len(),
		Lazy:       p.lazy.len(),
		EagerPeers: p.eager.sorted(),
		LazyPeers:  p.lazy.sorted(),
	})
}

func (p *Protocol) Init(ctx protorun.ProtocolContext) {
	// Seed the eager set from whatever active view the membership protocol
	// already has (new peers start eager, per the paper).
	protorun.SendRequest(ctx, &membership.GetView{}, func(rep *membership.View, err error) {
		if err != nil {
			ctx.Logger().Debug("plumtree: initial GetView failed", "err", err)
			return
		}
		for _, peer := range rep.Active {
			p.eager.add(peer)
			p.lazy.remove(peer)
		}
	})
	ctx.Every(p.cfg.LazyInterval, p.flushLazy)
}

// --- membership contract -----------------------------------------------

func (p *Protocol) onNeighborUp(ev membership.NeighborUp) {
	if ev.Peer == p.self {
		return
	}
	p.eager.add(ev.Peer) // new peers start eager
	p.lazy.remove(ev.Peer)
}

func (p *Protocol) onNeighborDown(ev membership.NeighborDown) {
	p.eager.remove(ev.Peer)
	p.lazy.remove(ev.Peer)
	delete(p.lazyQueue, ev.Peer)
	// Drop the dead peer from any outstanding IHAVE record so a GRAFT is
	// never aimed at it.
	for id, entries := range p.missing {
		kept := entries[:0]
		for _, e := range entries {
			if e.peer != ev.Peer {
				kept = append(kept, e)
			}
		}
		if len(kept) == 0 {
			p.cancelMissing(id)
		} else {
			p.missing[id] = kept
		}
	}
}

// --- broadcast origination ---------------------------------------------

func (p *Protocol) handleBroadcast(req *Broadcast, r protorun.Responder[*BroadcastAck]) {
	p.seq++
	id := MessageID{Origin: p.self, Seq: p.seq}
	p.received[id] = struct{}{}
	p.cache.put(id, req.Payload)
	p.deliver(id, req.Payload, p.self)
	p.eagerPush(id, req.Payload, 0, p.self)
	p.lazyPush(id, 0, p.self)
	r.Reply(&BroadcastAck{})
}

// --- message handlers --------------------------------------------------

func (p *Protocol) onGossip(msg *Gossip, from transport.Host) {
	if _, dup := p.received[msg.ID]; dup {
		// Redundant tree edge: prune it back to lazy.
		p.duplicates++
		p.eager.remove(from)
		p.lazy.add(from)
		_ = p.ctx.Send(&Prune{}, from)
		return
	}
	p.received[msg.ID] = struct{}{}
	p.cache.put(msg.ID, msg.Payload)
	p.cancelMissing(msg.ID)
	p.deliver(msg.ID, msg.Payload, from)
	// The sender is a confirmed tree link.
	p.eager.add(from)
	p.lazy.remove(from)
	p.eagerPush(msg.ID, msg.Payload, msg.Round+1, from)
	p.lazyPush(msg.ID, msg.Round+1, from)
}

func (p *Protocol) onIHave(msg *IHave, from transport.Host) {
	for _, a := range msg.Announcements {
		if _, have := p.received[a.ID]; have {
			continue
		}
		p.missing[a.ID] = append(p.missing[a.ID], pendingGraft{peer: from, round: a.Round})
		if _, armed := p.timers[a.ID]; !armed {
			id := a.ID
			p.timers[id] = p.ctx.After(p.cfg.MissingTimeout, func() { p.onMissingTimeout(id) })
		}
	}
}

func (p *Protocol) onGraft(msg *Graft, from transport.Host) {
	// A GRAFT always makes the sender an eager (tree) peer again.
	p.eager.add(from)
	p.lazy.remove(from)
	if payload, ok := p.cache.get(msg.ID); ok {
		_ = p.ctx.Send(&Gossip{ID: msg.ID, Round: msg.Round, Payload: payload}, from)
	}
	// Cache miss: the message aged out (anti-entropy caveat). Nothing to
	// replay; the requester's retry timer will eventually give up.
}

func (p *Protocol) onPrune(_ *Prune, from transport.Host) {
	p.eager.remove(from)
	p.lazy.add(from)
}

// --- missing-message timer (GRAFT) -------------------------------------

func (p *Protocol) onMissingTimeout(id MessageID) {
	delete(p.timers, id)
	if _, have := p.received[id]; have {
		delete(p.missing, id)
		return
	}
	entries := p.missing[id]
	if len(entries) == 0 {
		return
	}
	// Take the oldest announcer and GRAFT it. Re-arm a shorter retry timer
	// so a failed GRAFT falls through to the next announcer.
	first := entries[0]
	p.missing[id] = entries[1:]
	p.timers[id] = p.ctx.After(p.cfg.GraftRetryTimeout, func() { p.onMissingTimeout(id) })
	p.eager.add(first.peer)
	p.lazy.remove(first.peer)
	_ = p.ctx.Send(&Graft{ID: id, Round: first.round}, first.peer)
}

func (p *Protocol) cancelMissing(id MessageID) {
	if t, ok := p.timers[id]; ok {
		t.Cancel()
		delete(p.timers, id)
	}
	delete(p.missing, id)
}

// --- push helpers ------------------------------------------------------

// eagerPush sends the full Gossip to every eager peer except the one it
// came from.
func (p *Protocol) eagerPush(id MessageID, payload []byte, round uint32, except transport.Host) {
	for _, peer := range p.eager.sorted() {
		if peer == except {
			continue
		}
		_ = p.ctx.Send(&Gossip{ID: id, Round: round, Payload: payload}, peer)
	}
}

// lazyPush queues an IHAVE announcement for every lazy peer except the
// one the message came from. Flushed in batches by flushLazy.
func (p *Protocol) lazyPush(id MessageID, round uint32, except transport.Host) {
	for _, peer := range p.lazy.sorted() {
		if peer == except {
			continue
		}
		p.lazyQueue[peer] = append(p.lazyQueue[peer], announce{ID: id, Round: round})
	}
}

// flushLazy sends each lazy peer its queued announcements as one batched
// IHAVE, then clears the queue. Runs on the LazyInterval timer.
func (p *Protocol) flushLazy() {
	if len(p.lazyQueue) == 0 {
		return
	}
	peers := make([]transport.Host, 0, len(p.lazyQueue))
	for peer := range p.lazyQueue {
		peers = append(peers, peer)
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i].String() < peers[j].String() })
	for _, peer := range peers {
		anns := p.lazyQueue[peer]
		if len(anns) > 0 {
			_ = p.ctx.Send(&IHave{Announcements: anns}, peer)
		}
	}
	p.lazyQueue = make(map[transport.Host][]announce)
}

// --- delivery ----------------------------------------------------------

func (p *Protocol) deliver(id MessageID, payload []byte, from transport.Host) {
	protorun.PublishNotification(p.ctx, Delivered{ID: id, Payload: payload, From: from})
}
