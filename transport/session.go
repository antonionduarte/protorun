package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// SessionLayer sits between the runtime and a concrete Layer.
// It is responsible for:
//   - Performing a simple handshake to associate ephemeral transport
//     connections with stable logical Hosts.
//   - Emitting session-level events (connected / disconnected / failed).
//   - Framing application payloads with a LayerIdentifier so receivers can
//     distinguish handshake vs. application messages.
//
// It is also the single translation point between the two identity
// worlds: the transport below addresses peers by Address (an ephemeral
// endpoint), while the runtime above only ever sees the stable logical
// Host bound by the handshake. Every transport Address that crosses up
// to the runtime is resolved to its logical Host here.
type (
	SessionLayer struct {
		self             Host                // Our own higher-level Host identity
		connectChan      chan Host           // Requests to connect come in as logical Host
		disconnectChan   chan Host           // Requests to disconnect come in as logical Host
		sendChan         chan SessionMessage // Requests to send a SessionMessage
		outChannelEvents chan SessionEvent   // Outgoing events (connected, disconnected, etc.)
		outMessages      chan SessionMessage // Outgoing messages at the session/application level

		// handshakeTimeoutChan delivers expired client-handshake timers
		// back onto the handler goroutine, so timeout transitions run on
		// the same goroutine as every other FSM transition.
		handshakeTimeoutChan chan Address
		handshakeTimeout     time.Duration

		// sessions holds per-peer session state machines, keyed by the
		// transport Address's String() (Address is not comparable in
		// general, so the map key is its stable string form).
		sessions map[string]*sessionConn

		// transport-level details and host mappings.
		transport          Layer            // Underlying transport layer
		transportToLogical map[string]Host  // transport Address key -> logical Host
		logicalToTransport map[Host]Address // logical Host -> transport Address

		ctx        context.Context
		cancelFunc context.CancelFunc

		logger *slog.Logger

		mu sync.Mutex // guards session maps and host mappings
	}

	// SessionState represents the lifecycle state of a single session with a peer.
	SessionState int

	// HandshakeType identifies the type of a session-layer handshake message.
	HandshakeType byte

	// sessionConn models a single logical session with a peer and owns the
	// handshake state machine for that peer.
	sessionConn struct {
		logicalHost   Host
		transportAddr Address
		state         SessionState

		lastErr error

		// handshakeTimer bounds the client-side wait for the server's
		// Ack (or Reject). Armed when the Hello is sent, stopped when
		// the handshake resolves. Only touched from the handler
		// goroutine; the timer's own callback just forwards onto
		// handshakeTimeoutChan.
		handshakeTimer *time.Timer

		layer *SessionLayer
	}

	SessionMessage struct {
		host  Host            // The logical Host identity of the remote node
		layer LayerIdentifier // Session vs Application
		Msg   bytes.Buffer    // The raw data or serialized content
	}

	// SessionEvent signals connection success/failure or disconnection at the session level.
	//
	// When adding a new concrete event type, give it a route in
	// protorun's Runtime.dispatchSessionEvent and add it to that
	// package's event-coverage test — the runtime treats unrouted
	// event kinds as a bug and reports them loudly.
	SessionEvent interface {
		Host() Host
	}

	SessionDisconnected struct {
		host Host
	}
	SessionFailed struct {
		host Host
	}
	// SessionConnected announces an Established session: both sides
	// accepted the handshake. The dialing side emits it only after the
	// server's Ack arrives.
	SessionConnected struct {
		host Host
	}
	// SessionVersionMismatch is emitted when a handshake fails because
	// the peer speaks a different wire-format version. The connection
	// is closed either way; inbound says which side detected it (see
	// Inbound).
	SessionVersionMismatch struct {
		host        Host
		peerVersion uint8
		inbound     bool
	}

	// LayerIdentifier differentiates between application-level payloads and
	// session/handshake payloads carried over the same transport.
	LayerIdentifier int
)

// LayerIdentifier constants. These values are written on the wire as the first
// byte of the session payload coming from the transport layer.
const (
	Application LayerIdentifier = iota
	Session
)

// SessionState values.
const (
	SessionStateIdle SessionState = iota
	SessionStateHandshakingClient
	SessionStateHandshakingServer
	SessionStateEstablished
	SessionStateFailed
	SessionStateClosing
)

// HandshakeType values. Session-layer payloads always start with a single
// byte HandshakeType followed by type-specific data (if any).
const (
	HandshakeHello HandshakeType = iota + 1
	HandshakeAck
	HandshakeReject
)

// Accessor methods to implement the SessionEvent interface:
func (s *SessionConnected) Host() Host       { return s.host }
func (s *SessionDisconnected) Host() Host    { return s.host }
func (s *SessionFailed) Host() Host          { return s.host }
func (s *SessionVersionMismatch) Host() Host { return s.host }

// Event constructors, for adapters at the runtime's Sessions seam
// (in-memory meshes, test fakes) that emit session events themselves.
func NewSessionConnected(host Host) *SessionConnected       { return &SessionConnected{host: host} }
func NewSessionDisconnected(host Host) *SessionDisconnected { return &SessionDisconnected{host: host} }
func NewSessionFailed(host Host) *SessionFailed             { return &SessionFailed{host: host} }
func NewSessionVersionMismatch(host Host, peerVersion uint8, inbound bool) *SessionVersionMismatch {
	return &SessionVersionMismatch{host: host, peerVersion: peerVersion, inbound: inbound}
}

// PeerVersion is the wire-format version the offending peer
// announced. Useful for logs / metrics to spot which side needs the
// upgrade.
func (s *SessionVersionMismatch) PeerVersion() uint8 { return s.peerVersion }

// Inbound reports which side of the handshake detected the mismatch.
// True: a peer dialed us with a version we don't speak; Host() is the
// ephemeral transport host of that inbound connection. False: our own
// dial was Rejected by the peer; Host() is the logical Host we dialed,
// and further retries against it cannot succeed.
func (s *SessionVersionMismatch) Inbound() bool { return s.inbound }

// defaultHandshakeTimeout bounds how long a dialing session waits for
// the server's Ack (or Reject) after sending its Hello.
const defaultHandshakeTimeout = 5 * time.Second

// SessionOption customizes a SessionLayer at construction time.
type SessionOption func(*SessionLayer)

// WithHandshakeTimeout overrides how long a dialing session waits for
// the server's Ack (or Reject) before the handshake is failed.
func WithHandshakeTimeout(d time.Duration) SessionOption {
	return func(s *SessionLayer) {
		if d > 0 {
			s.handshakeTimeout = d
		}
	}
}

func NewSessionLayer(
	transport Layer, self Host, ctx context.Context,
	eventsBuf, msgsBuf int, opts ...SessionOption,
) *SessionLayer {
	ctx, cancel := context.WithCancel(ctx)
	logger := slog.Default().With("component", "session")
	if eventsBuf <= 0 {
		eventsBuf = defaultSessionEventsBuffer
	}
	if msgsBuf <= 0 {
		msgsBuf = defaultSessionMessagesBuffer
	}
	session := &SessionLayer{
		self:                 self,
		connectChan:          make(chan Host),
		disconnectChan:       make(chan Host),
		sendChan:             make(chan SessionMessage),
		outChannelEvents:     make(chan SessionEvent, eventsBuf),
		outMessages:          make(chan SessionMessage, msgsBuf),
		handshakeTimeoutChan: make(chan Address),
		handshakeTimeout:     defaultHandshakeTimeout,
		sessions:             make(map[string]*sessionConn),
		transport:            transport,
		transportToLogical:   make(map[string]Host),
		logicalToTransport:   make(map[Host]Address),
		ctx:                  ctx,
		cancelFunc:           cancel,
		logger:               logger,
	}
	for _, opt := range opts {
		opt(session)
	}
	go session.handler(ctx)
	return session
}

// --- sessionConn state machine methods ---

// handleClientConnectRequested is called when the local runtime requests that
// we establish a session to logicalHost. The SessionLayer is expected to have
// already set logicalHost and transportAddr appropriately when constructing
// the sessionConn.
func (s *sessionConn) handleClientConnectRequested() {
	switch s.state {
	case SessionStateIdle, SessionStateFailed, SessionStateClosing:
		s.state = SessionStateHandshakingClient
		s.layer.logger.Debug("session FSM: client connect requested",
			"logical", s.logicalHost.String(),
			"transport", s.transportAddr.String())
		// Initiate underlying transport connection.
		s.layer.transport.Connect(s.transportAddr)
	default:
		// Connect requested in an unexpected state; log and ignore.
		s.layer.logger.Warn("session FSM: connect requested in non-idle state",
			"state", s.state,
			"logical", s.logicalHost.String(),
			"transport", s.transportAddr.String())
	}
}

// handleConnected is invoked when the underlying transport connects
// for this session's transportAddr.
func (s *sessionConn) handleConnected() {
	switch s.state {
	case SessionStateHandshakingClient:
		// Client side: send Hello with our logical host, then wait for
		// the server's Ack (or Reject) before treating the session as
		// Established — see handleClientHandshake.
		s.layer.logger.Debug("session FSM: transport connected (client), sending Hello",
			"logical", s.logicalHost.String(),
			"transport", s.transportAddr.String())
		helloPayload, err := encodeHello(s.layer.self)
		if err != nil {
			s.layer.logger.Error("session FSM: encodeHello failed",
				"transport", s.transportAddr.String(),
				"err", err)
			s.lastErr = err
			s.state = SessionStateFailed
			s.layer.emitEvent(&SessionFailed{host: s.logicalHost})
			return
		}
		msg := frameFor(Session, helloPayload, s.transportAddr)
		s.layer.transport.Send(msg, s.transportAddr)
		s.layer.logger.Debug("session FSM: client Hello sent, awaiting Ack",
			"transport", s.transportAddr.String())

		// Bound the wait: a peer that accepts the connection but never
		// answers the Hello must not park the session in handshaking.
		s.armHandshakeTimer()

	case SessionStateHandshakingServer:
		// Server side: just record that the transport is up; we wait for Hello.
		s.layer.logger.Debug("session FSM: transport connected (server)",
			"transport", s.transportAddr.String())

	default:
		// In other states, just log; this likely indicates a reconnect or race.
		s.layer.logger.Warn("session FSM: transport connected in unexpected state",
			"state", s.state,
			"transport", s.transportAddr.String())
	}
}

// handleHandshakeMessage processes a session-layer handshake message
// (Hello, Ack, or Reject) carried in body (the payload after the
// LayerIdentifier byte).
func (s *sessionConn) handleHandshakeMessage(body bytes.Buffer) {
	// Work on a copy so we don't mutate shared buffers.
	buf := bytes.NewBuffer(body.Bytes())
	p, err := parseHandshakePayload(buf)
	if err != nil {
		s.lastErr = err
		s.layer.logger.Error("session FSM: failed to parse handshake payload",
			"transport", s.transportAddr.String(),
			"err", err)
		// Version mismatch gets its own treatment so the dialer can
		// distinguish "peer speaks the wrong dialect" from "peer
		// crashed mid-handshake": we tell it why with a Reject before
		// closing the connection.
		if errors.Is(err, ErrVersionMismatch) {
			s.rejectHandshake(p.version)
		}
		return
	}

	switch s.state {
	case SessionStateHandshakingServer:
		s.handleServerHandshake(p)
	case SessionStateHandshakingClient:
		s.handleClientHandshake(p)
	default:
		s.layer.logger.Warn("session FSM: handshake message in unexpected state",
			"state", s.state,
			"type", p.typ,
			"transport", s.transportAddr.String())
	}
}

// rejectHandshake refuses an inbound Hello whose wire-format version
// this build does not speak: it sends a Reject carrying our version so
// the dialer can stop retrying, emits an inbound SessionVersionMismatch
// for observability, and tears the connection down. The state moves to
// Failed so the subsequent transport Disconnected event does not emit a
// spurious SessionDisconnected.
func (s *sessionConn) rejectHandshake(peerVersion uint8) {
	rejectMsg := frameFor(Session, encodeReject(), s.transportAddr)
	s.layer.transport.Send(rejectMsg, s.transportAddr)
	s.state = SessionStateFailed
	s.layer.emitEvent(&SessionVersionMismatch{
		host:        s.logicalHostOrTransport(),
		peerVersion: peerVersion,
		inbound:     true,
	})
	s.layer.transport.Disconnect(s.transportAddr)
}

// logicalHostOrTransport returns the bound logical Host if the handshake
// got that far, otherwise a best-effort Host derived from the transport
// Address (an inbound peer that fails the version check before its Hello
// is accepted has no logical identity yet). It is only used to label
// observability events.
func (s *sessionConn) logicalHostOrTransport() Host {
	if s.logicalHost.Port != 0 {
		return s.logicalHost
	}
	if h, ok := hostFromAddress(s.transportAddr); ok {
		return h
	}
	return Host{}
}

func (s *sessionConn) handleServerHandshake(p handshakePayload) {
	switch p.typ {
	case HandshakeHello:
		// Server received client's logical host; record mapping and send Ack.
		s.logicalHost = p.host
		s.layer.logger.Debug("session FSM: server received Hello",
			"client", p.host.String(),
			"transport", s.transportAddr.String())

		ackMsg := frameFor(Session, encodeAck(), s.transportAddr)
		s.layer.logger.Debug("session FSM: server sending Ack on transport",
			"transport", s.transportAddr.String())
		s.layer.transport.Send(ackMsg, s.transportAddr)
		s.layer.logger.Debug("session FSM: server Ack sent",
			"transport", s.transportAddr.String())

		// Mark as established and emit event.
		s.state = SessionStateEstablished
		s.layer.setServerMapping(s.transportAddr, s.logicalHost)
		s.layer.logger.Debug("session FSM: server handshake complete",
			"client", s.logicalHost.String(),
			"transport", s.transportAddr.String())
		s.layer.logger.Info("session FSM: server emitting SessionConnected",
			"peer", s.logicalHost.String(),
			"transport", s.transportAddr.String())
		s.layer.emitEvent(&SessionConnected{host: s.logicalHost})

	case HandshakeAck, HandshakeReject:
		// Server receiving Ack or Reject is unexpected.
		s.layer.logger.Warn("session FSM: server received unexpected handshake message",
			"type", p.typ,
			"transport", s.transportAddr.String())
	}
}

func (s *sessionConn) handleClientHandshake(p handshakePayload) {
	switch p.typ {
	case HandshakeAck:
		// The server accepted our Hello: the session is Established on
		// both sides now, and only now do protocols learn about it.
		s.stopHandshakeTimer()
		s.state = SessionStateEstablished
		s.layer.setServerMapping(s.transportAddr, s.logicalHost)
		s.layer.logger.Info("session FSM: client received Ack, emitting SessionConnected",
			"peer", s.logicalHost.String(),
			"transport", s.transportAddr.String())
		s.layer.emitEvent(&SessionConnected{host: s.logicalHost})

	case HandshakeReject:
		// The server speaks a different wire-format version. Terminal:
		// redialing the same peer cannot succeed until one side is
		// upgraded, so the runtime translates this into given-up.
		s.stopHandshakeTimer()
		s.state = SessionStateFailed
		s.lastErr = fmt.Errorf("%w: peer=%s peer_version=%d",
			ErrVersionMismatch, s.logicalHost.String(), p.version)
		s.layer.logger.Warn("session FSM: dial rejected, peer speaks a different wire-format version",
			"peer", s.logicalHost.String(),
			"peer_version", p.version,
			"our_version", ProtocolVersion)
		s.layer.emitEvent(&SessionVersionMismatch{
			host:        s.logicalHost,
			peerVersion: p.version,
			inbound:     false,
		})
		s.layer.transport.Disconnect(s.transportAddr)

	case HandshakeHello:
		// Client receiving Hello is unexpected in the current simple protocol.
		s.layer.logger.Warn("session FSM: client received unexpected Hello",
			"transport", s.transportAddr.String())
	}
}

// armHandshakeTimer starts the bounded wait for the server's handshake
// answer. The callback only forwards onto handshakeTimeoutChan (ctx
// guarded); the actual state transition runs on the handler goroutine
// in dispatchHandshakeTimeout.
func (s *sessionConn) armHandshakeTimer() {
	layer := s.layer
	transportAddr := s.transportAddr
	s.handshakeTimer = time.AfterFunc(layer.handshakeTimeout, func() {
		select {
		case layer.handshakeTimeoutChan <- transportAddr:
		case <-layer.ctx.Done():
		}
	})
}

func (s *sessionConn) stopHandshakeTimer() {
	if s.handshakeTimer != nil {
		s.handshakeTimer.Stop()
		s.handshakeTimer = nil
	}
}

// handleDisconnected handles a disconnect at the transport level.
func (s *sessionConn) handleDisconnected() {
	s.stopHandshakeTimer()
	s.layer.logger.Debug("session FSM: transport disconnected",
		"state", s.state,
		"logical", s.logicalHost.String(),
		"transport", s.transportAddr.String())

	switch s.state {
	case SessionStateEstablished, SessionStateHandshakingClient, SessionStateHandshakingServer:
		s.state = SessionStateClosing
		logical := s.layer.logicalFor(s.transportAddr)
		s.layer.cleanupServerMapping(s.transportAddr)
		s.layer.logger.Info("session FSM: emitting SessionDisconnected",
			"peer", logical.String(),
			"transport", s.transportAddr.String())
		s.layer.emitEvent(&SessionDisconnected{host: logical})
	default:
		// In other states, simply mark as failed.
		s.state = SessionStateFailed
	}
}

// handleFailed handles a failure at the transport level.
func (s *sessionConn) handleFailed() {
	s.stopHandshakeTimer()
	s.layer.logger.Warn("session FSM: transport failed",
		"state", s.state,
		"logical", s.logicalHost.String(),
		"transport", s.transportAddr.String())

	logical := s.layer.logicalFor(s.transportAddr)
	s.layer.cleanupServerMapping(s.transportAddr)
	s.state = SessionStateFailed
	s.layer.logger.Info("session FSM: emitting SessionFailed",
		"peer", logical.String(),
		"transport", s.transportAddr.String())
	s.layer.emitEvent(&SessionFailed{host: logical})
}

// ProtocolVersion is the wire-format version this build advertises in
// every Hello handshake. Receivers refuse Hello messages with a
// mismatched version by answering with a Reject (so the dialer learns
// the incompatibility and stops retrying), emitting a
// SessionVersionMismatch event, and closing the underlying connection.
// Bump this whenever the framing or handshake structure changes in a
// way that prior builds can't handle.
const ProtocolVersion uint8 = 1

// ErrVersionMismatch is wrapped into the parse error returned by
// parseHandshakePayload when the peer's Hello carries a version this
// build doesn't speak. The session layer translates that into a
// SessionVersionMismatch event for upstream observers.
var ErrVersionMismatch = errors.New("session handshake: protocol version mismatch")

// encodeHello builds a session-layer handshake payload announcing the
// sender's logical Host. Layout:
//
//	[HandshakeType(1 byte, HandshakeHello) || Version(1 byte) || WriteHost(self)]
//
// Returns an error if the host cannot be serialized.
func encodeHello(h Host) (bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	buf.WriteByte(byte(HandshakeHello))
	buf.WriteByte(ProtocolVersion)
	if err := WriteHost(buf, h); err != nil {
		return bytes.Buffer{}, fmt.Errorf("encodeHello: %w", err)
	}
	return *buf, nil
}

// encodeAck builds a session-layer handshake ACK payload with no extra
// data: [HandshakeType(1 byte, HandshakeAck)].
func encodeAck() bytes.Buffer {
	buf := bytes.NewBuffer(nil)
	buf.WriteByte(byte(HandshakeAck))
	return *buf
}

// encodeReject builds a session-layer handshake Reject payload carrying
// our own wire-format version, so the rejected dialer can log which
// side needs the upgrade: [HandshakeType(1 byte, HandshakeReject) ||
// Version(1 byte)].
func encodeReject() bytes.Buffer {
	buf := bytes.NewBuffer(nil)
	buf.WriteByte(byte(HandshakeReject))
	buf.WriteByte(ProtocolVersion)
	return *buf
}

// handshakePayload is the parsed form of a session-layer handshake
// message.
type handshakePayload struct {
	typ     HandshakeType
	host    Host  // sender's logical host (Hello only)
	version uint8 // sender's wire-format version (Hello and Reject)
}

// parseHandshakePayload interprets a session-layer handshake payload.
// Layout:
//
//	[HandshakeType(1 byte) || Data...]
//
// where Data is [Version(1 byte) || WriteHost(host)] for HandshakeHello,
// [Version(1 byte)] for HandshakeReject, and empty for HandshakeAck.
// A Hello version-byte mismatch is flagged with ErrVersionMismatch —
// with the offending version still populated in the returned payload —
// so the session layer can Reject the dial instead of dropping the
// connection silently.
func parseHandshakePayload(buf *bytes.Buffer) (handshakePayload, error) {
	var p handshakePayload
	if buf.Len() == 0 {
		return p, fmt.Errorf("handshake payload empty")
	}

	msgType, err := buf.ReadByte()
	if err != nil {
		return p, fmt.Errorf("failed to read handshake type: %w", err)
	}
	p.typ = HandshakeType(msgType)

	switch p.typ {
	case HandshakeHello:
		return parseHello(p, buf)
	case HandshakeAck:
		return p, nil
	case HandshakeReject:
		version, err := buf.ReadByte()
		if err != nil {
			return p, fmt.Errorf("HandshakeReject: read version: %w", err)
		}
		p.version = version
		return p, nil
	default:
		return handshakePayload{}, fmt.Errorf("unknown handshake type: %d", msgType)
	}
}

// parseHello reads a Hello's version byte and sender Host into p,
// flagging a version mismatch with ErrVersionMismatch (the offending
// version stays populated so the caller can Reject with context).
func parseHello(p handshakePayload, buf *bytes.Buffer) (handshakePayload, error) {
	version, err := buf.ReadByte()
	if err != nil {
		return p, fmt.Errorf("HandshakeHello: read version: %w", err)
	}
	p.version = version
	if version != ProtocolVersion {
		return p, fmt.Errorf("%w: got=%d expected=%d", ErrVersionMismatch, version, ProtocolVersion)
	}
	host, err := ReadHost(buf)
	if err != nil {
		return p, fmt.Errorf("HandshakeHello: %w", err)
	}
	p.host = host
	return p, nil
}

// Cancel stops the internal goroutine(s) by cancelling their context.
func (s *SessionLayer) Cancel() {
	if s.cancelFunc != nil {
		s.cancelFunc()
	}
}

// emitEvent pushes a session event onto outChannelEvents, but bails out if the
// session context has been cancelled. This prevents the FSM goroutine from
// blocking forever during shutdown when no consumer is reading.
func (s *SessionLayer) emitEvent(ev SessionEvent) {
	select {
	case s.outChannelEvents <- ev:
	case <-s.ctx.Done():
	}
}

func (s *SessionLayer) Connect(host Host) {
	s.logger.Debug("session connect requested", "host", host.String())
	select {
	case s.connectChan <- host:
	case <-s.ctx.Done():
	}
}

func (s *SessionLayer) Disconnect(host Host) {
	s.logger.Debug("session disconnect requested", "host", host.String())
	select {
	case s.disconnectChan <- host:
	case <-s.ctx.Done():
	}
}

func (s *SessionLayer) Send(msg bytes.Buffer, sendTo Host) {
	s.logger.Debug("session send requested", "to", sendTo.String(), "bytes", msg.Len())
	select {
	case s.sendChan <- SessionMessage{Msg: msg, host: sendTo, layer: Application}:
	case <-s.ctx.Done():
	}
}

func (s *SessionLayer) OutChannelEvents() chan SessionEvent {
	return s.outChannelEvents
}

func (s *SessionLayer) OutMessages() chan SessionMessage {
	return s.outMessages
}

// Host returns the logical host associated with this session message.
func (m SessionMessage) Host() Host {
	return m.host
}

// NewSessionMessage builds an application-level SessionMessage from
// host, for adapters at the runtime's Sessions seam (in-memory meshes,
// test fakes) that deliver messages themselves.
func NewSessionMessage(msg bytes.Buffer, host Host) SessionMessage {
	return SessionMessage{host: host, layer: Application, Msg: msg}
}

func (s *SessionLayer) handler(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case msg := <-s.transport.OutChannel():
			s.transportMessageHandler(msg)

		case event := <-s.transport.OutEvents():
			s.transportEventHandler(event)

		case host := <-s.connectChan:
			s.connectClient(host)

		case host := <-s.disconnectChan:
			s.disconnect(host)

		case msg := <-s.sendChan:
			s.send(msg.Msg, msg.host)

		case addr := <-s.handshakeTimeoutChan:
			s.dispatchHandshakeTimeout(addr)
		}
	}
}

// dispatchHandshakeTimeout fails a client handshake that never received
// the server's Ack (or Reject) in time. Runs on the handler goroutine
// like every other FSM transition; a timeout that lost the race against
// a resolution (Ack, Reject, disconnect) is a no-op.
func (s *SessionLayer) dispatchHandshakeTimeout(transportAddr Address) {
	s.mu.Lock()
	sc, ok := s.sessions[transportAddr.String()]
	s.mu.Unlock()
	if !ok || sc.state != SessionStateHandshakingClient {
		return
	}
	sc.stopHandshakeTimer()
	sc.state = SessionStateFailed
	sc.lastErr = fmt.Errorf("session handshake: timed out waiting for Ack from %s", sc.logicalHost.String())
	s.logger.Warn("session FSM: handshake timed out",
		"peer", sc.logicalHost.String(),
		"transport", transportAddr.String(),
		"timeout", s.handshakeTimeout)
	s.emitEvent(&SessionFailed{host: sc.logicalHost})
	s.transport.Disconnect(transportAddr)
}

func (s *SessionLayer) transportMessageHandler(msg Message) {
	layer, body := splitFrame(msg)
	switch layer {
	case Application:
		// Resolve the transport Address up to the stable logical Host so
		// the runtime only ever sees logical identities.
		logical := s.logicalFor(msg.Peer)
		s.logger.Debug("session application message received",
			"from", logical.String(),
			"bytes", body.Len())
		sessionMsg := SessionMessage{host: logical, layer: Application, Msg: body}
		select {
		case s.outMessages <- sessionMsg:
		case <-s.ctx.Done():
		}

	case Session:
		s.logger.Debug("session handshake message received", "from", msg.Peer.String(), "bytes", body.Len())
		s.dispatchHandshakeMessage(msg.Peer, body)

	default:
		s.logger.Warn("session message with unknown layer identifier dropped",
			"from", msg.Peer.String(),
			"layer", layer,
			"bytes", body.Len(),
		)
	}
}

func (s *SessionLayer) transportEventHandler(event Event) {
	switch e := event.(type) {
	case *Connected:
		s.logger.Info("session inbound transport connected", "host", e.peer.String())
		s.dispatchConnected(e.peer)

	case *Disconnected:
		s.dispatchDisconnected(e.peer)

	case *Failed:
		s.dispatchFailed(e.Peer())
	}
}

func (s *SessionLayer) connectClient(h Host) {
	s.logger.Info("session initiating client handshake", "to", h.String())
	// The dialer addresses the peer by its logical Host, which doubles
	// as the transport Address to dial.
	s.withSession(h, h, func(sc *sessionConn) {
		sc.handleClientConnectRequested()
	})
}

func (s *SessionLayer) disconnect(h Host) {
	addr := s.resolveUnderlyingHost(h)
	s.transport.Disconnect(addr)
	s.cleanupServerMapping(addr)
}

func (s *SessionLayer) send(msg bytes.Buffer, sendTo Host) {
	underlying := s.resolveUnderlyingHost(sendTo)
	transportMsg := frameFor(Application, msg, underlying)
	s.transport.Send(transportMsg, underlying)
}

// withSession looks up or creates a sessionConn for the given transport
// Address. If logicalHost is non-zero (Port != 0), it is recorded as the
// desired logical host for this session.
func (s *SessionLayer) withSession(transportAddr Address, logicalHost Host, fn func(sc *sessionConn)) {
	key := transportAddr.String()
	s.mu.Lock()
	sc, ok := s.sessions[key]
	if !ok {
		sc = &sessionConn{
			logicalHost:   logicalHost,
			transportAddr: transportAddr,
			state:         SessionStateIdle,
			layer:         s,
		}
		s.sessions[key] = sc
	} else if logicalHost.Port != 0 {
		// Update logical host if provided.
		sc.logicalHost = logicalHost
	}
	s.mu.Unlock()

	fn(sc)
}

// dispatchHandshakeMessage routes a session-layer handshake message into the
// appropriate sessionConn state machine.
func (s *SessionLayer) dispatchHandshakeMessage(transportAddr Address, body bytes.Buffer) {
	s.withSession(transportAddr, Host{}, func(sc *sessionConn) {
		// If this is an inbound connection on the server side and we haven't
		// yet entered a state, mark as server handshaking.
		if sc.state == SessionStateIdle {
			sc.state = SessionStateHandshakingServer
		}
		sc.handleHandshakeMessage(body)
	})
}

func (s *SessionLayer) dispatchConnected(transportAddr Address) {
	s.withSession(transportAddr, Host{}, func(sc *sessionConn) {
		// If this session was created by an inbound connection and hasn't
		// been marked yet, treat it as server-handshaking.
		if sc.state == SessionStateIdle {
			sc.state = SessionStateHandshakingServer
		}
		sc.handleConnected()
	})
}

func (s *SessionLayer) dispatchDisconnected(transportAddr Address) {
	s.withSession(transportAddr, Host{}, func(sc *sessionConn) {
		sc.handleDisconnected()
	})
}

func (s *SessionLayer) dispatchFailed(transportAddr Address) {
	s.withSession(transportAddr, Host{}, func(sc *sessionConn) {
		sc.handleFailed()
	})
}

func (s *SessionLayer) setServerMapping(transportAddr Address, logical Host) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transportToLogical[transportAddr.String()] = logical
	s.logicalToTransport[logical] = transportAddr
}

func (s *SessionLayer) cleanupServerMapping(transportAddr Address) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := transportAddr.String()
	if logical, ok := s.transportToLogical[key]; ok {
		delete(s.transportToLogical, key)
		delete(s.logicalToTransport, logical)
	}
}

// resolveUnderlyingHost returns the transport Address associated with the
// given logical host, falling back to the logical host itself (as an
// Address) if no mapping exists.
func (s *SessionLayer) resolveUnderlyingHost(sendTo Host) Address {
	s.mu.Lock()
	defer s.mu.Unlock()

	if addr, ok := s.logicalToTransport[sendTo]; ok {
		return addr
	}
	return sendTo
}

// logicalFor returns the logical Host bound to a transport Address. If no
// mapping exists yet (a message arriving before the handshake completes),
// it falls back to the Address coerced to a Host when possible, so TCP's
// ephemeral endpoints still surface something usable.
func (s *SessionLayer) logicalFor(addr Address) Host {
	s.mu.Lock()
	defer s.mu.Unlock()

	if logical, ok := s.transportToLogical[addr.String()]; ok {
		return logical
	}
	if h, ok := hostFromAddress(addr); ok {
		return h
	}
	return Host{}
}

// frameFor prefixes the LayerIdentifier byte onto body and wraps it as a
// transport Message addressed to peer. peer is only a routing hint on the
// Message itself; the transport's Send takes the authoritative address as
// its second argument.
func frameFor(layer LayerIdentifier, body bytes.Buffer, peer Address) Message {
	buf := bytes.NewBuffer(nil)
	buf.WriteByte(byte(layer))
	buf.Write(body.Bytes())
	return Message{Peer: peer, Msg: *buf}
}

// splitFrame reads the LayerIdentifier byte off an inbound transport
// frame, returning it plus the remaining body. An empty frame is treated
// as an Application payload (see docs/wire-format.md).
func splitFrame(msg Message) (LayerIdentifier, bytes.Buffer) {
	buffer := msg.Msg
	if buffer.Len() == 0 {
		return Application, buffer
	}
	header := buffer.Next(1)
	return LayerIdentifier(header[0]), buffer
}
