// Package membership is the gossip example's membership layer: the
// simplest possible implementation of the protocols/membership contract.
// It connects to a fixed, static list of contacts and reports the peers
// it is session-connected to. It answers membership.GetView and publishes
// membership.NeighborUp / NeighborDown — nothing more.
//
// It exists to make one point concrete: interchangeability. The gossip
// protocol layered above talks only to the contract, so this static list
// can be swapped for HyParView (protocols/hyparview) without touching a
// line of the gossip protocol. This package is the pedagogical baseline;
// HyParView is the real thing.
//
// All state is owned by this protocol's event loop. Observers reach it
// through IPC (GetView) or the fan-out notifications, which the runtime
// routes onto the loop for them.
package membership

import (
	"github.com/antonionduarte/protorun"
	"github.com/antonionduarte/protorun/protocols/membership"
	"github.com/antonionduarte/protorun/transport"
)

// Protocol is the static membership protocol. Construct with New, then
// Register with a Runtime.
type Protocol struct {
	contacts []transport.Host
	ctx      protorun.ProtocolContext
	view     map[transport.Host]struct{}
}

// New returns a Protocol bootstrapped with the given contact peers. On
// Init it ConnectWithRetry's to each contact; every established session
// becomes a NeighborUp, every lost one a NeighborDown.
func New(contacts []transport.Host) *Protocol {
	return &Protocol{
		contacts: contacts,
		view:     make(map[transport.Host]struct{}),
	}
}

func (p *Protocol) Start(ctx protorun.ProtocolContext) {
	p.ctx = ctx
	protorun.RegisterRequestHandler(ctx, p.handleGetView)
}

func (p *Protocol) Init(ctx protorun.ProtocolContext) {
	for _, c := range p.contacts {
		if err := ctx.ConnectWithRetry(c); err != nil {
			ctx.Logger().Error("ConnectWithRetry failed", "contact", c.String(), "err", err)
		}
	}
}

func (p *Protocol) OnSessionConnected(h transport.Host) {
	if _, exists := p.view[h]; exists {
		return
	}
	p.view[h] = struct{}{}
	protorun.PublishNotification(p.ctx, membership.NeighborUp{Peer: h})
}

func (p *Protocol) OnSessionDisconnected(h transport.Host) {
	if _, exists := p.view[h]; !exists {
		return
	}
	delete(p.view, h)
	protorun.PublishNotification(p.ctx, membership.NeighborDown{Peer: h})
}

func (p *Protocol) handleGetView(_ *membership.GetView, r protorun.Responder[*membership.View]) {
	peers := make([]transport.Host, 0, len(p.view))
	for h := range p.view {
		peers = append(peers, h)
	}
	r.Reply(&membership.View{Active: peers})
}
