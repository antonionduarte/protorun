package protorun

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antonionduarte/protorun/transport"
)

// PanicHandler can be implemented by a protocol that wants to observe
// panics from its own handlers. The framework recovers from every
// handler-level panic so a single bad handler doesn't take down the
// runtime; this hook lets a protocol record the panic somewhere
// (metrics, supervisor signal, error channel) without having to wrap
// each handler in user-side recover().
//
// `where` is an informational tag identifying which handler kind
// panicked (e.g. "message handler", "request handler"); it is intended
// for diagnostics, not pattern-matching, and may change between
// versions.
type PanicHandler interface {
	OnPanic(where string, recovered any)
}

type (
	// Connector is the capability for opening, retrying, and tearing
	// down sessions with peers. Handlers that only need to react to
	// session events can take Connector instead of the full
	// ProtocolContext, making their dependency on the framework
	// explicit at the type level.
	Connector interface {
		Connect(host transport.Host) error
		ConnectWithRetry(host transport.Host) error
		Disconnect(host transport.Host) error
	}

	// Sender is the capability for sending application messages to a
	// peer. Send returns an error synchronously (e.g. ErrNoCodec); the
	// actual delivery is asynchronous and surfaces via SessionFailed
	// events on the receive side.
	Sender interface {
		Send(msg Message, to transport.Host) error
	}

	// Timing is the capability for scheduling one-shot and periodic
	// callbacks. Both fire on the owning protocol's event loop, so the
	// callback may touch protocol state without locking. The payload
	// travels by closure capture; there is no timer value and no
	// user-managed id. The returned TimerHandle cancels the timer.
	Timing interface {
		// After schedules fn to run once after d.
		After(d time.Duration, fn func()) TimerHandle
		// Every schedules fn to run once per d until cancelled or the
		// runtime shuts down.
		Every(d time.Duration, fn func()) TimerHandle
	}

	// Identity is the capability for reading the protocol's view of
	// itself: the local Host and a protocol-scoped logger.
	Identity interface {
		Self() transport.Host
		Logger() *slog.Logger
	}

	// ProtocolContext is the main entry point for protocol implementations.
	// It is provided to Protocol.Start and Protocol.Init. It composes the
	// fine-grained capability interfaces (Connector, Sender, Timing,
	// Identity) so handlers and helpers can declare narrower deps if
	// they want: a function that only needs to send messages can take
	// Sender, a function that only needs the local Host can take
	// Identity, and so on.
	//
	// Codec and message-handler registration is done via the typed
	// generic helpers RegisterCodec[M] / RegisterHandler[M] / the IPC
	// helpers in ipc.go, which reach the framework through the
	// binding() hook below.
	ProtocolContext interface {
		Connector
		Sender
		Timing
		Identity

		// binding returns the concrete per-protocol binding that anchors
		// the typed generic helpers (RegisterCodec[M], RegisterHandler[M],
		// SendRequest[Req, Rep], ...). Generic methods aren't allowed on
		// Go interfaces, so the helpers take a ProtocolContext and reach
		// the framework's registration and IPC plumbing through this
		// single unexported hook — which also seals the interface to
		// this package.
		binding() *protocolContext
	}

	// Protocol describes a user protocol that can be hosted by the
	// runtime. Implementations should use the provided ProtocolContext
	// in Start/Init to interact with the system.
	Protocol interface {
		Start(ctx ProtocolContext)
		Init(ctx ProtocolContext)
	}

	protoProtocol struct {
		protocol Protocol
		runtime  *Runtime

		// name is fmt.Sprintf("%T", protocol), cached for the metric,
		// dead-letter, and log fields that report it on hot paths.
		name string

		// mailbox is the single ordered event queue that replaced the
		// former six per-kind channels. Arrival order is delivery order
		// across message / timer / session / IPC events.
		mailbox mailbox

		// lastMailboxWarn is the unix-nano timestamp of the last
		// strict-mode high-occupancy warning, used to rate-limit it to
		// once per second per protocol.
		lastMailboxWarn atomic.Int64

		handlers map[uint64]func(Message, transport.Host)

		// timers holds every live TimerHandle this protocol owns, keyed
		// by the runtime-monotonic timer id. Used to cancel them all on
		// shutdown. Guarded by timersMu.
		timers   map[uint64]*timerHandle
		timersMu sync.Mutex

		// pending tracks outstanding SendRequest calls awaiting a reply
		// or timeout. Indexed by per-protocol monotonic request ID.
		// Guarded by pendingMu so SendRequest can be called from any
		// goroutine, consistent with the rest of the ProtocolContext
		// surface (Connect, Send, etc.).
		pending       map[uint64]pendingRequest
		pendingMu     sync.Mutex
		nextRequestID atomic.Uint64

		// phase tracks the per-protocol lifecycle phase for strict-mode
		// invariant checks. See strict.go. Read on dispatch hot paths
		// when strict is on; never read when strict is off (so the
		// atomic load never even happens for production runs).
		phase atomic.Int32

		ctx ProtocolContext
	}

	protocolContext struct {
		proto   *protoProtocol
		runtime *Runtime
		logger  *slog.Logger
	}
)

// newProtoProtocol wraps a user Protocol in a protoProtocol envelope
// with an OverflowBlock mailbox of the given capacity (defaulting when
// non-positive). It is the convenience form used by in-package tests;
// Runtime.Register goes through newProtoProtocolMailbox with a Mailbox
// assembled from the caller's RegisterOptions.
func newProtoProtocol(protocol Protocol, capacity int) *protoProtocol {
	return newProtoProtocolMailbox(protocol, Mailbox{Capacity: capacity, Overflow: OverflowBlock})
}

// newProtoProtocolMailbox wraps a user Protocol in a protoProtocol
// envelope backed by a mailbox built from m.
func newProtoProtocolMailbox(protocol Protocol, m Mailbox) *protoProtocol {
	return &protoProtocol{
		protocol: protocol,
		name:     fmt.Sprintf("%T", protocol),
		mailbox:  newMailbox(m),
		handlers: make(map[uint64]func(Message, transport.Host)),
		timers:   make(map[uint64]*timerHandle),
		pending:  make(map[uint64]pendingRequest),
	}
}

// bindRuntime is called by Runtime.registerProtocol so the protocol can
// resolve its hosting runtime when ensureContext fires.
func (p *protoProtocol) bindRuntime(r *Runtime) { p.runtime = r }

func (p *protoProtocol) Start(ctx context.Context, wg *sync.WaitGroup) {
	p.ensureContext()
	p.setPhase(phaseRegistering)
	// wg.Add happens AFTER protocol.Start returns. If user's Start
	// panics (e.g. a strict-mode invariant fires), no eventHandler
	// goroutine is created, so the WG counter must not have been
	// incremented; otherwise Cancel's wg.Wait() would block forever.
	p.protocol.Start(p.ctx)
	p.setPhase(phaseRegistered)
	wg.Add(1)
	go p.eventHandler(ctx, wg)
}

func (p *protoProtocol) Init() {
	p.ensureContext()
	p.setPhase(phaseInitializing)
	p.protocol.Init(p.ctx)
	p.setPhase(phaseRunning)
}

// ensureContext lazily initializes the ProtocolContext. The protocol must
// have been registered with a Runtime via registerProtocol or Register
// before Start or Init is called.
func (p *protoProtocol) ensureContext() {
	if p.ctx != nil {
		return
	}
	if p.runtime == nil {
		panic("runtime: protocol used before being registered with a Runtime")
	}
	baseLogger := p.runtime.Logger()
	p.ctx = &protocolContext{
		proto:   p,
		runtime: p.runtime,
		logger: baseLogger.With(
			"component", "protocol",
			"self", p.runtime.self.String(),
		),
	}
}

// --- ProtocolContext implementation ---

func (c *protocolContext) Connect(host transport.Host) error {
	c.proto.requireActivePhase("Connect")
	return c.runtime.connect(host)
}
func (c *protocolContext) ConnectWithRetry(host transport.Host) error {
	c.proto.requireActivePhase("ConnectWithRetry")
	return c.runtime.connectWithRetry(host)
}
func (c *protocolContext) Disconnect(host transport.Host) error {
	c.proto.requireActivePhase("Disconnect")
	return c.runtime.disconnect(host)
}

func (c *protocolContext) Send(msg Message, to transport.Host) error {
	c.proto.requireActivePhase("Send")
	return c.runtime.sendMessage(msg, to)
}

func (c *protocolContext) After(d time.Duration, fn func()) TimerHandle {
	c.proto.requireActivePhase("After")
	return c.runtime.after(c.proto, d, fn)
}
func (c *protocolContext) Every(d time.Duration, fn func()) TimerHandle {
	c.proto.requireActivePhase("Every")
	return c.runtime.every(c.proto, d, fn)
}

func (c *protocolContext) Self() transport.Host { return c.runtime.self }
func (c *protocolContext) Logger() *slog.Logger { return c.logger }

func (c *protocolContext) registerCodec(wireID uint64, codec codec) {
	c.proto.requireRegisterPhase("RegisterCodec")
	if c.runtime.strict {
		if _, exists := c.runtime.codecs.Get(wireID); exists {
			strictPanic("RegisterCodec for wireID=%#x called twice", wireID)
		}
	}
	// Make this protocol the routing target for the wire id on the
	// runtime-level lookup table, together with the codec — one atomic
	// registration step. RegisterCodec should be called once per
	// (protocol, message-type) pair; if two protocols register the
	// same wire id, the last one wins (and the operator should investigate).
	c.runtime.codecs.Set(wireID, c.proto, codec)
}

func (c *protocolContext) registerHandler(wireID uint64, fn func(Message, transport.Host)) {
	c.proto.requireRegisterPhase("RegisterHandler")
	if c.runtime.strict {
		if _, exists := c.proto.handlers[wireID]; exists {
			strictPanic("RegisterHandler for wireID=%#x called twice on the same protocol", wireID)
		}
	}
	c.proto.handlers[wireID] = fn
}

// binding anchors the typed generic helpers: they take a
// ProtocolContext and reach the framework's plumbing (registration,
// IPC, panic reporting) through the concrete binding rather than
// through interface methods.
func (c *protocolContext) binding() *protocolContext { return c }

// registerRequestHandler installs fn as the request handler for wireID
// runtime-wide and points the runtime's request-routing table at this
// protocol. A second registration for the same wireID logs a warning
// and replaces the prior route. The framework allows it for hot-
// reload scenarios but it is almost always a programming error in
// production code.
func (c *protocolContext) registerRequestHandler(wireID uint64, fn func(Request, replyToken)) {
	c.proto.requireRegisterPhase("RegisterRequestHandler")
	prev, hadPrev := c.runtime.ipc.RegisterRequestRoute(wireID, c.proto, fn)
	if hadPrev && prev.proto != c.proto {
		if c.runtime.strict {
			strictPanic("RegisterRequestHandler for wireID=%#x already owned by another protocol", wireID)
		}
		c.logger.Warn("protorun: replacing existing request handler",
			"wireID", wireID,
		)
	}
}

// sendRequest is the requester-side entry point. Routes through the
// runtime so cross-protocol delivery and timeout management have a
// single owner.
func (c *protocolContext) sendRequest(wireID uint64, req Request, timeout time.Duration, onReply func(Reply, error)) {
	c.proto.requireActivePhase("SendRequest")
	c.runtime.sendRequest(c.proto, wireID, req, timeout, onReply)
}

// subscribeNotification adds this protocol to the runtime's fan-out
// table for wireID and stashes the captured handler closure. The
// closure is invoked from this protocol's event loop when a
// notification arrives.
func (c *protocolContext) subscribeNotification(wireID uint64, fn func(Notification)) {
	c.proto.requireRegisterPhase("SubscribeNotification")
	c.runtime.subscribeNotification(c.proto, wireID, fn)
}

func (c *protocolContext) unsubscribeNotification(wireID uint64) {
	c.runtime.unsubscribeNotification(c.proto, wireID)
}

func (c *protocolContext) publishNotification(wireID uint64, n Notification) {
	c.proto.requireActivePhase("PublishNotification")
	c.runtime.publishNotification(wireID, n)
}

// deliverReplyToToken forwards a synthetic reply (typically an error
// produced inside the type-assertion guard in RegisterRequestHandler's
// closure) back to the requester. Reuses the same path as a normal
// responder Reply / Fail.
func (c *protocolContext) deliverReplyToToken(token replyToken, rep Reply, err error) {
	c.runtime.deliverReply(token, rep, err)
}

func (c *protocolContext) reportPanic(where string, rec any, stack []byte) {
	c.proto.reportPanic(where, rec, stack)
}

func (p *protoProtocol) eventHandler(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		ev, ok := p.mailbox.next(ctx)
		if !ok {
			return
		}
		p.dispatch(ev)
	}
}

// dispatch routes one mailbox event to the right handler. Every kind
// travels the same ordered queue, so the order handlers observe here is
// exactly the order producers enqueued in.
func (p *protoProtocol) dispatch(ev protoEvent) {
	switch ev.kind {
	case evMessage:
		p.handleMessage(ev.msg, ev.from)
	case evTimer:
		// Recheck cancellation at dispatch time: the fire may have been
		// queued before the user cancelled the handle. This is what
		// guarantees a callback never runs after a same-loop Cancel.
		if ev.timer.cancelled.Load() {
			return
		}
		p.safeCall("timer handler", ev.timer.fn)
	case evSession:
		p.safeCall("session event handler", func() { p.deliverSessionEvent(ev.aux.session) })
	case evRequest:
		// The IPC closure created in RegisterRequestHandler has its own
		// recover() that auto-fails the responder; safeCall here is
		// belt-and-suspenders for the (impossible by construction) case
		// where the closure itself panics before installing its defer.
		req := ev.aux.request
		p.safeCall("request handler", func() { req.handler(req.req, req.token) })
	case evReply:
		p.safeCall("reply handler", func() { p.deliverReply(ev.aux.reply) })
	case evNotification:
		notif := ev.aux.notif
		p.safeCall("notification handler", func() { notif.handler(notif.n) })
	}
}

func (p *protoProtocol) handleMessage(msg Message, from transport.Host) {
	h := p.handlers[wireIDOf(msg)]
	if h == nil {
		return
	}
	p.safeCall("message handler", func() { h(msg, from) })
}

// enqueue pushes ev onto this protocol's mailbox, samples the depth
// histogram, and — when a drop policy evicts or rejects an event —
// increments the dropped counter and routes the loss to the runtime's
// dead-letter hook. It returns false only when a blocking mailbox was
// aborted by ctx (runtime shutdown), which the fanout paths use to stop
// early. Called from every producer: the dispatcher, IPC delivery,
// notification fanout, and timer fires.
func (p *protoProtocol) enqueue(ctx context.Context, ev protoEvent) bool {
	dropped, didDrop, ok := p.mailbox.push(ctx, ev)
	if !ok {
		return false
	}
	depth := p.mailbox.depth()
	p.runtime.metrics.Histogram("protorun.mailbox.depth", float64(depth),
		Attr{Key: "protocol", Value: p.name})
	if didDrop {
		p.runtime.metrics.Counter("protorun.mailbox.dropped", 1,
			Attr{Key: "protocol", Value: p.name},
			Attr{Key: "kind", Value: dropped.kind.String()},
			Attr{Key: "policy", Value: p.mailbox.policy().String()})
		p.runtime.emitDeadLetter(DeadLetter{
			Protocol: p.name,
			Kind:     dropped.kind.String(),
			Peer:     dropped.peer(),
		})
	}
	p.strictMailboxOccupancy(depth)
	return true
}

// trackTimer records a live timer handle for shutdown cleanup.
func (p *protoProtocol) trackTimer(h *timerHandle) {
	p.timersMu.Lock()
	p.timers[h.id] = h
	p.timersMu.Unlock()
}

// forgetTimer drops a timer handle from the table (on cancel or on a
// one-shot fire). Idempotent.
func (p *protoProtocol) forgetTimer(id uint64) {
	p.timersMu.Lock()
	delete(p.timers, id)
	p.timersMu.Unlock()
}

// cancelAllTimers cancels every timer this protocol still owns. Called
// during runtime shutdown so no timer outlives the runtime.
func (p *protoProtocol) cancelAllTimers() {
	p.timersMu.Lock()
	handles := make([]*timerHandle, 0, len(p.timers))
	for _, h := range p.timers {
		handles = append(handles, h)
	}
	p.timersMu.Unlock()
	for _, h := range handles {
		h.cancel()
	}
}

// safeCall wraps a handler invocation in defer/recover so that a panic
// in user code is logged (with stack), surfaced to the protocol's
// optional PanicHandler, and the event loop continues. Without this
// guard a single bad handler would take down the protocol's event
// loop and break every other handler that protocol owns.
//
// In strict mode, also arms a watchdog that fires a counter + warn
// log if the handler exceeds the configured threshold (default 5s).
// Stop is called on completion regardless of panic / normal return.
func (p *protoProtocol) safeCall(where string, fn func()) {
	stopWatchdog := p.strictWatchdog(where)
	defer stopWatchdog()
	defer func() {
		if rec := recover(); rec != nil {
			p.reportPanic(where, rec, debug.Stack())
		}
	}()
	fn()
}

// reportPanic logs the panic with structured fields and notifies an
// optional PanicHandler implementation on the protocol. Used by both
// safeCall (for general handler dispatch) and the IPC request-handler
// closure (which recovers earlier so it can auto-fail the responder
// before reporting).
func (p *protoProtocol) reportPanic(where string, rec any, stack []byte) {
	logger := slog.Default()
	if p.runtime != nil {
		logger = p.runtime.Logger()
		p.runtime.metrics.Counter("protorun.handler.panic", 1,
			Attr{Key: "where", Value: where},
			Attr{Key: "protocol", Value: fmt.Sprintf("%T", p.protocol)},
		)
	}
	logger.Error("protocol handler panicked",
		"protocol", fmt.Sprintf("%T", p.protocol),
		"where", where,
		"recovered", fmt.Sprintf("%v", rec),
		"stack", string(stack),
	)
	if h, ok := p.protocol.(PanicHandler); ok {
		// Defensive recover around the user's PanicHandler. If it
		// also panics we don't want an infinite loop, just drop it.
		func() {
			defer func() { _ = recover() }()
			h.OnPanic(where, rec)
		}()
	}
}

// deliverReply matches an inbound reply (or timeout) to its pending
// SendRequest entry and invokes the callback on the requester's event
// loop. If no pending entry is found the reply is dropped: the
// most common cause is a real reply landing after the timeout
// already fired (or vice versa). First-arrival wins, second-arrival
// is a silent no-op.
func (p *protoProtocol) deliverReply(rep inboundReply) {
	p.pendingMu.Lock()
	pending, ok := p.pending[rep.requestID]
	if ok {
		delete(p.pending, rep.requestID)
	}
	p.pendingMu.Unlock()
	if !ok {
		// Late arrival: the other branch (timeout vs reply) already
		// claimed this requestID. Counter so operators can see how
		// often this happens; in strict mode also a warn-level log.
		if p.runtime != nil {
			p.runtime.metrics.Counter("protorun.ipc.reply.dropped_late", 1)
		}
		p.strictReplyWithoutHandler()
		return
	}
	if p.runtime != nil {
		wireIDAttr := Attr{Key: "wireID", Value: fmt.Sprintf("%#x", pending.wireID)}
		resultAttr := Attr{Key: "result", Value: replyResultLabel(rep.err)}
		p.runtime.metrics.Counter("protorun.ipc.request.completed", 1, wireIDAttr, resultAttr)
		p.runtime.metrics.Histogram("protorun.ipc.request.latency_ms",
			float64(p.runtime.clock.Now().Sub(pending.startedAt).Microseconds())/1000.0,
			wireIDAttr, resultAttr)
	}
	pending.cb(rep.rep, rep.err)
}

// replyResultLabel maps a reply's error (or nil for success) to the
// "result" attribute value used in IPC metrics.
func replyResultLabel(err error) string {
	switch {
	case err == nil:
		return "success"
	case errors.Is(err, ErrRequestTimeout):
		return "timeout"
	case errors.Is(err, ErrNoRequestHandler):
		return "no_handler"
	case errors.Is(err, ErrHandlerPanicked):
		return "handler_panicked"
	case errors.Is(err, ErrResponderFailed):
		return "responder_failed"
	default:
		return "error"
	}
}

// deliverSessionEvent invokes the protocol's optional session-event
// handlers (OnSessionConnected / OnSessionDisconnected / OnSessionGivenUp)
// when implemented.
func (p *protoProtocol) deliverSessionEvent(ev sessionEvent) {
	switch ev.kind {
	case sessionConnectedEvent:
		if h, ok := p.protocol.(SessionConnectedHandler); ok {
			h.OnSessionConnected(ev.host)
		}
	case sessionDisconnectedEvent:
		if h, ok := p.protocol.(SessionDisconnectedHandler); ok {
			h.OnSessionDisconnected(ev.host)
		}
	case sessionGivenUpEvent:
		if h, ok := p.protocol.(SessionGivenUpHandler); ok {
			h.OnSessionGivenUp(ev.host, ev.attempts)
		}
	}
}

// RegisterCodec registers a Codec[M] under M's wire identifier on the
// supplied ProtocolContext. Free function rather than a method because
// Go interfaces can't have generic methods; the typed registration
// flows through ctx.registerCodec internally. Wire id derives from
// M's Go type name (or M.WireName() if implemented).
func RegisterCodec[M Message](ctx ProtocolContext, c Codec[M]) {
	ctx.binding().registerCodec(WireID[M](), codecAdapter[M]{c: c})
}

// RegisterHandler registers fn as the handler for messages of type M
// on the supplied ProtocolContext. Handlers receive both the decoded
// message and the host that sent it; sender info doesn't need to be
// encoded on the wire. Free function for the same reason as
// RegisterCodec, since generic methods aren't allowed on interfaces.
// The framework performs the type assertion before invoking fn.
func RegisterHandler[M Message](ctx ProtocolContext, fn func(M, transport.Host)) {
	ctx.binding().registerHandler(WireID[M](), func(raw Message, from transport.Host) {
		fn(raw.(M), from)
	})
}
