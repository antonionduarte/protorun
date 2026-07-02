package gossip

import (
	"math/rand/v2"
	"slices"
	"time"

	"github.com/antonionduarte/protorun"
	"github.com/antonionduarte/protorun/cmd/gossip/membership"
	"github.com/antonionduarte/protorun/transport"
)

// Protocol is the eager-push gossip protocol. Each node broadcasts a
// payload by sending it to every peer in its (cached) membership
// view; on receipt, the node delivers locally if it hasn't seen the
// ID before and forwards to every peer except the sender.
//
// All state is owned by this protocol's event loop. To originate a
// broadcast, peer code on the same runtime issues a TriggerBroadcast
// IPC request via protorun.SendRequest; the handler runs on this
// protocol's event loop and replies with BroadcastAck once the
// payload has been queued for fanout.
//
// Optionally, EnablePeriodic configures the protocol to broadcast a
// caller-supplied payload at a fixed interval, handy for heartbeats
// or demo programs.
type Protocol struct {
	ctx       protorun.ProtocolContext
	onDeliver func([]byte)

	seen map[uint64]struct{}
	view []transport.Host

	periodicInterval time.Duration
	periodicPayload  func() []byte
}

// New returns a Protocol that calls onDeliver(payload) exactly once
// per unique payload it observes, including on the originator when
// Broadcast is called locally.
func New(onDeliver func([]byte)) *Protocol {
	return &Protocol{
		onDeliver: onDeliver,
		seen:      make(map[uint64]struct{}),
	}
}

// EnablePeriodic configures the protocol to call payload() and
// broadcast the returned bytes once every interval, starting after
// Init. Both a positive interval and a non-nil payload are required;
// otherwise this call is a no-op. Returns p so callers can chain
// onto New. Must be invoked before Register.
//
// payload runs on the protocol's event loop, so it can read protocol-
// scoped state without locking, but it must not block.
func (p *Protocol) EnablePeriodic(interval time.Duration, payload func() []byte) *Protocol {
	if interval <= 0 || payload == nil {
		return p
	}
	p.periodicInterval = interval
	p.periodicPayload = payload
	return p
}

// TriggerBroadcast is the IPC request peer code uses to originate a
// gossip broadcast. Routing through SendRequest /
// RegisterRequestHandler ensures the payload always lands on this
// protocol's event loop, regardless of which goroutine the caller
// runs on.
type TriggerBroadcast struct {
	protorun.BaseRequest
	Payload []byte
}

// BroadcastAck is the empty reply to TriggerBroadcast; receiving it
// means the payload has been seen-set'd and queued for fanout.
type BroadcastAck struct {
	protorun.BaseReply
}

// periodicTick is the Timer marker for the optional periodic
// broadcast. The runtime keys handlers by TimerID, scoped to the
// owning protocol, so any constant int will do.
type periodicTick struct{}

func (periodicTick) TimerID() int { return 1 }

func (p *Protocol) Start(ctx protorun.ProtocolContext) {
	p.ctx = ctx
	protorun.RegisterCodec(ctx, Codec{})
	protorun.RegisterHandler(ctx, p.handleInbound)
	protorun.SubscribeNotification(ctx, p.handleViewChanged)
	protorun.RegisterRequestHandler(ctx, p.handleBroadcast)
	if p.periodicEnabled() {
		ctx.RegisterTimerHandler(periodicTick{}, p.handlePeriodicTick)
	}
}

func (p *Protocol) Init(ctx protorun.ProtocolContext) {
	protorun.SendRequest(ctx, &membership.GetView{}, p.handleInitialView)
	if p.periodicEnabled() {
		ctx.SetupPeriodicTimer(periodicTick{}, p.periodicInterval)
	}
}

func (p *Protocol) periodicEnabled() bool {
	return p.periodicInterval > 0 && p.periodicPayload != nil
}

func (p *Protocol) handleBroadcast(req *TriggerBroadcast, r protorun.Responder[*BroadcastAck]) {
	p.disseminate(req.Payload)
	r.Reply(&BroadcastAck{})
}

func (p *Protocol) handlePeriodicTick(_ protorun.Timer) {
	p.disseminate(p.periodicPayload())
}

// disseminate is the event-loop-only path that originates a fresh
// broadcast: mints an ID, marks it seen, self-delivers, and fans out
// to every known peer. Shared by handleBroadcast (the IPC entry) and
// handlePeriodicTick (the optional timer entry).
func (p *Protocol) disseminate(payload []byte) {
	// Math/rand/v2 is fine here: a 64-bit value is collision-free in
	// practice and we don't care about adversarial collisions; gossip
	// IDs are not authenticated.
	id := rand.Uint64() //nolint:gosec // see comment
	p.seen[id] = struct{}{}
	p.onDeliver(payload)
	p.fanout(&Message{ID: id, Payload: payload}, p.view)
}

func (p *Protocol) handleInbound(msg *Message, sender transport.Host) {
	if _, dup := p.seen[msg.ID]; dup {
		return
	}
	p.seen[msg.ID] = struct{}{}
	p.onDeliver(msg.Payload)
	p.fanout(msg, excluding(p.view, sender))
}

func (p *Protocol) handleInitialView(rep *membership.View, err error) {
	if err != nil {
		p.ctx.Logger().Error("initial GetView failed", "err", err)
		return
	}
	p.view = rep.Peers
}

func (p *Protocol) handleViewChanged(ev membership.ViewChanged) {
	switch {
	case ev.HasAdded:
		if !slices.ContainsFunc(p.view, sameHost(ev.Added)) {
			p.view = append(p.view, ev.Added)
		}
	case ev.HasRemoved:
		p.view = slices.DeleteFunc(p.view, sameHost(ev.Removed))
	}
}

// fanout sends msg to each peer. Errors are logged and otherwise
// ignored. Gossip is best-effort, and the runtime already emits
// SessionFailed events that membership translates into a view shrink,
// which heals subsequent broadcasts.
func (p *Protocol) fanout(msg *Message, peers []transport.Host) {
	for _, peer := range peers {
		if err := p.ctx.Send(msg, peer); err != nil {
			p.ctx.Logger().Warn("gossip send failed", "peer", peer.String(), "err", err)
		}
	}
}

func excluding(peers []transport.Host, exclude transport.Host) []transport.Host {
	out := make([]transport.Host, 0, len(peers))
	for _, h := range peers {
		if h != exclude {
			out = append(out, h)
		}
	}
	return out
}

func sameHost(target transport.Host) func(transport.Host) bool {
	return func(h transport.Host) bool { return h == target }
}
