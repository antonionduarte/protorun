// Package prototest lets protocol authors test their protorun
// protocols without touching a real network: an in-memory mesh of
// nodes stands in for the TCP + handshake stack at the runtime's
// Sessions seam (no wire, no handshake, deterministic in-process
// delivery), and NewRuntime stands up a runnable runtime around a
// mesh node in one call.
//
//	mesh := prototest.NewMesh()
//	a := transport.NewHost(1, "10.0.0.1")
//	b := transport.NewHost(2, "10.0.0.2")
//	rtA := prototest.NewRuntime(t, mesh, a, myProtocolA)
//	rtB := prototest.NewRuntime(t, mesh, b, myProtocolB)
//	// protocols on rtA can now Connect/Send to b and vice versa.
package prototest

import (
	"bytes"
	"context"
	"sync"

	"github.com/antonionduarte/go-simple-protocol-runtime/pkg/protorun"
	"github.com/antonionduarte/go-simple-protocol-runtime/pkg/transport"
)

// meshChannelBuffer sizes each node's outbound message and event
// channels. Matches the session layer's defaults.
const meshChannelBuffer = 16

// Mesh is a set of in-process nodes addressable by Host. Nodes join
// via Node (usually indirectly, through NewRuntime) and reach each
// other with the semantics of the real stack: Connect yields
// SessionConnected on both sides, Disconnect yields
// SessionDisconnected on both sides, dialing an absent Host yields
// SessionFailed, and Send to a peer without a session is dropped.
type Mesh struct {
	mu    sync.Mutex
	nodes map[transport.Host]*Node
}

func NewMesh() *Mesh {
	return &Mesh{nodes: make(map[transport.Host]*Node)}
}

// Node returns the mesh node for self, creating it on first use. The
// returned Node implements protorun.Sessions and is what NewRuntime
// wires into the runtime via WithTransport.
func (m *Mesh) Node(self transport.Host) *Node {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n, ok := m.nodes[self]; ok {
		return n
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		mesh:        m,
		self:        self,
		ctx:         ctx,
		cancelFunc:  cancel,
		outMessages: make(chan transport.SessionMessage, meshChannelBuffer),
		outEvents:   make(chan transport.SessionEvent, meshChannelBuffer),
		peers:       make(map[transport.Host]struct{}),
	}
	m.nodes[self] = n
	return n
}

// Node is one Host's endpoint on a Mesh. It satisfies
// protorun.Sessions: the runtime drives it exactly like the real
// session layer, and it answers with the same event vocabulary.
type Node struct {
	mesh *Mesh
	self transport.Host

	ctx        context.Context
	cancelFunc context.CancelFunc

	outMessages chan transport.SessionMessage
	outEvents   chan transport.SessionEvent

	// peers holds the Hosts this node has a live session with.
	// Guarded by mesh.mu so both endpoints mutate under one lock.
	peers map[transport.Host]struct{}
}

var _ protorun.Sessions = (*Node)(nil)

// Connect establishes a session with host: both endpoints see
// SessionConnected, mirroring the real handshake where the dialer is
// Established on Ack and the listener on Hello. Dialing a Host that
// isn't on the mesh (or has been cancelled) yields SessionFailed.
// Connecting to an already-connected peer is a no-op, like the real
// FSM's non-idle connect.
func (n *Node) Connect(host transport.Host) {
	n.mesh.mu.Lock()
	peer, ok := n.mesh.nodes[host]
	if ok && peer.ctx.Err() != nil {
		ok = false
	}
	if !ok {
		n.mesh.mu.Unlock()
		n.emit(transport.NewSessionFailed(host))
		return
	}
	if _, already := n.peers[host]; already {
		n.mesh.mu.Unlock()
		return
	}
	n.peers[host] = struct{}{}
	peer.peers[n.self] = struct{}{}
	n.mesh.mu.Unlock()

	n.emit(transport.NewSessionConnected(host))
	peer.emit(transport.NewSessionConnected(n.self))
}

// Disconnect tears down the session with host; both endpoints see
// SessionDisconnected. A no-op if no session exists.
func (n *Node) Disconnect(host transport.Host) {
	n.mesh.mu.Lock()
	if _, connected := n.peers[host]; !connected {
		n.mesh.mu.Unlock()
		return
	}
	delete(n.peers, host)
	peer, peerExists := n.mesh.nodes[host]
	if peerExists {
		delete(peer.peers, n.self)
	}
	n.mesh.mu.Unlock()

	n.emit(transport.NewSessionDisconnected(host))
	if peerExists {
		peer.emit(transport.NewSessionDisconnected(n.self))
	}
}

// Send delivers an application payload to sendTo's inbound message
// stream. Like the real transport, sending without a live session is
// a silent drop — failures surface as session events, not here.
func (n *Node) Send(msg bytes.Buffer, sendTo transport.Host) {
	n.mesh.mu.Lock()
	_, connected := n.peers[sendTo]
	peer, peerExists := n.mesh.nodes[sendTo]
	n.mesh.mu.Unlock()
	if !connected || !peerExists {
		return
	}
	sessionMsg := transport.NewSessionMessage(msg, n.self)
	select {
	case peer.outMessages <- sessionMsg:
	case <-peer.ctx.Done():
	case <-n.ctx.Done():
	}
}

func (n *Node) OutMessages() chan transport.SessionMessage    { return n.outMessages }
func (n *Node) OutChannelEvents() chan transport.SessionEvent { return n.outEvents }

// Cancel takes the node off the mesh: every live session is torn
// down (peers see SessionDisconnected, as they would on a closed
// connection) and further Connect attempts against this Host fail.
func (n *Node) Cancel() {
	n.cancelFunc()

	n.mesh.mu.Lock()
	peers := make([]*Node, 0, len(n.peers))
	for host := range n.peers {
		delete(n.peers, host)
		if peer, ok := n.mesh.nodes[host]; ok {
			delete(peer.peers, n.self)
			peers = append(peers, peer)
		}
	}
	n.mesh.mu.Unlock()

	for _, peer := range peers {
		peer.emit(transport.NewSessionDisconnected(n.self))
	}
}

// emit delivers a session event unless the node is cancelled — the
// same ctx-guarded discipline as the real session layer's emitEvent.
func (n *Node) emit(ev transport.SessionEvent) {
	select {
	case n.outEvents <- ev:
	case <-n.ctx.Done():
	}
}
