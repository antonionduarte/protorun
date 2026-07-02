// Package membership implements a minimal membership layer for the
// gossip example: it tracks the set of peers we are currently
// session-connected to, and exposes that view to other protocols on
// the same runtime via protorun's IPC primitives.
//
// Two access patterns are supported:
//
//   - Synchronous: a peer protocol issues a GetView request and
//     receives a snapshot of the current view in the reply.
//   - Asynchronous: a peer protocol subscribes to ViewChanged
//     notifications and receives one event per add or remove.
//
// All state is owned by this protocol's event loop. There are no
// public methods that read state from outside the loop. Observers
// use IPC, which the runtime routes onto the loop for them.
package membership

import (
	"github.com/antonionduarte/protorun"
	"github.com/antonionduarte/protorun/transport"
)

// GetView is sent by a peer protocol to fetch a snapshot of the
// current active view.
type GetView struct{ protorun.BaseRequest }

// View is the reply to GetView. Peers is a fresh slice owned by the
// caller and safe to mutate.
type View struct {
	protorun.BaseReply
	Peers []transport.Host
}

// ViewChanged is published every time the active view gains or loses
// a peer. Exactly one of HasAdded / HasRemoved is true per event.
type ViewChanged struct {
	protorun.BaseNotification
	Added, Removed       transport.Host
	HasAdded, HasRemoved bool
}

// Protocol is the membership protocol. Construct with New, then
// Register with a Runtime.
type Protocol struct {
	contacts []transport.Host
	ctx      protorun.ProtocolContext
	view     map[transport.Host]struct{}
}

// New returns a Protocol bootstrapped with the given contact peers.
// On Init the protocol will ConnectWithRetry to each contact.
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
	protorun.PublishNotification(p.ctx, ViewChanged{Added: h, HasAdded: true})
}

func (p *Protocol) OnSessionDisconnected(h transport.Host) {
	if _, exists := p.view[h]; !exists {
		return
	}
	delete(p.view, h)
	protorun.PublishNotification(p.ctx, ViewChanged{Removed: h, HasRemoved: true})
}

func (p *Protocol) handleGetView(_ *GetView, r protorun.Responder[*View]) {
	peers := make([]transport.Host, 0, len(p.view))
	for h := range p.view {
		peers = append(peers, h)
	}
	r.Reply(&View{Peers: peers})
}
