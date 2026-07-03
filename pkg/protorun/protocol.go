package protorun

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antonionduarte/protorun/pkg/transport"
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

	// Sender is the capability for sending application messages to a peer.
	//
	// IMPORTANT — Send's error is split across two channels, on purpose:
	//
	//   - The returned error is SYNCHRONOUS and LOCAL ONLY: it means the
	//     send was rejected before a byte left this process (ErrNoCodec —
	//     no codec registered for msg's type — or the session layer being
	//     absent/cancelled). A nil error does not mean the peer received
	//     anything; it means the local half of the send succeeded.
	//   - Whether the message actually reached the peer is an ASYNCHRONOUS
	//     property of the session, not of this call. If the connection is
	//     down, drops mid-flight, or the peer never acks at the transport
	//     level, Send still returns nil — the failure surfaces later, to
	//     everyone watching this peer, as SessionDisconnected /
	//     SessionFailed / SessionGivenUp (see SessionDisconnectedHandler /
	//     SessionGivenUpHandler). There is no per-message delivery receipt.
	//
	// Do not treat a nil return as "delivered". If a protocol needs
	// delivery confirmation, build it at the application level (an ack
	// message, a request/reply) — Send only promises the local half.
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
		// across message / timer / session / IPC events. Held behind an
		// atomic pointer because a restart swaps in a fresh mailbox while
		// external producers may still be reading the current one on the
		// enqueue path; the atomic makes that swap race-free. Read via
		// currentMailbox, written via setMailbox.
		mailbox atomic.Pointer[mailboxCell]

		// mailboxCfg is the Mailbox config the mailbox was built from,
		// retained so a restart can build the fresh instance a fresh
		// mailbox with the same capacity and overflow policy.
		mailboxCfg Mailbox

		// factory builds a fresh instance on restart; nil for a
		// singleton Register. Restart requires a non-nil factory (see
		// buildProtocol).
		factory func() Protocol

		// supervisor drives non-Resume panic directives off the event
		// loop; nil when OnPanic is Resume (the default), so unsupervised
		// protocols pay nothing on the enqueue and panic paths.
		supervisor *supervisor

		// quarantined, when set by the supervisor during a restart,
		// makes enqueue drop every event to dead-letter (non-blocking,
		// kind preserved) so no producer stalls on a protocol whose loop
		// is being torn down and rebuilt.
		quarantined atomic.Bool

		// exitLoop, set by safeCall after a non-Resume panic, tells the
		// event loop to return after the current dispatch so the
		// supervisor can rebuild on a clean slate.
		exitLoop atomic.Bool

		// loopCancel / loopDone track the current event-loop incarnation
		// so the supervisor can stop the old loop and wait for it to
		// exit before swapping in a fresh mailbox and instance. Touched
		// only by boot and by the (single) supervisor goroutine.
		loopCancel func()
		loopDone   chan struct{}

		// lastMailboxWarn is the unix-nano timestamp of the last
		// strict-mode high-occupancy warning, used to rate-limit it to
		// once per second per protocol.
		lastMailboxWarn atomic.Int64

		// inFlight counts events that have been accepted into the mailbox
		// but not yet fully dispatched. It is incremented by the producer
		// BEFORE the mailbox push (so no window exists where an event is
		// live but uncounted) and decremented by the event loop AFTER
		// dispatch returns. Runtime.Quiescent sums it across protocols;
		// see that method for the memory-model argument. Zero cost on
		// the happy path (one atomic add per enqueue/dispatch).
		inFlight atomic.Int64

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
	p := &protoProtocol{
		protocol:   protocol,
		name:       fmt.Sprintf("%T", protocol),
		mailboxCfg: m,
		handlers:   make(map[uint64]func(Message, transport.Host)),
		timers:     make(map[uint64]*timerHandle),
		pending:    make(map[uint64]pendingRequest),
	}
	p.setMailbox(newMailbox(m))
	return p
}

// mailboxCell boxes a mailbox interface value so it can live behind an
// atomic.Pointer (an interface can't be swapped atomically directly).
type mailboxCell struct{ mb mailbox }

// currentMailbox returns the protocol's live mailbox. Loaded once per
// event-loop incarnation and once per enqueue so a restart's mailbox
// swap is observed atomically.
func (p *protoProtocol) currentMailbox() mailbox { return p.mailbox.Load().mb }

// setMailbox installs mb as the live mailbox. Called at construction
// and on restart (with a fresh mailbox of the same config).
func (p *protoProtocol) setMailbox(mb mailbox) { p.mailbox.Store(&mailboxCell{mb: mb}) }

// bindRuntime is called by Runtime.registerProtocol so the protocol can
// resolve its hosting runtime when ensureContext fires.
func (p *protoProtocol) bindRuntime(r *Runtime) { p.runtime = r }

func (p *protoProtocol) Start(ctx context.Context, wg *sync.WaitGroup) {
	p.ensureContext()
	if p.supervisor == nil {
		p.setPhase(phaseRegistering)
		// wg.Add happens AFTER protocol.Start returns. If user's Start
		// panics (e.g. a strict-mode invariant fires), no eventHandler
		// goroutine is created, so the WG counter must not have been
		// incremented; otherwise Cancel's wg.Wait() would block forever.
		p.protocol.Start(p.ctx)
		p.setPhase(phaseRegistered)
		p.startLoop(ctx, wg)
		return
	}
	// Supervised: a Start panic is not fatal — recover it, quarantine
	// the protocol, and hand it to the supervisor (which starts after
	// boot) to rebuild. The loop is not started; the fresh instance
	// gets its own.
	if rec, panicked := p.callStart(); panicked {
		p.quarantined.Store(true)
		p.supervisor.signalPanic("Start", rec)
		return
	}
	p.startLoop(ctx, wg)
}

func (p *protoProtocol) Init() {
	p.ensureContext()
	if p.supervisor == nil {
		p.setPhase(phaseInitializing)
		p.protocol.Init(p.ctx)
		p.setPhase(phaseRunning)
		return
	}
	if p.quarantined.Load() {
		// Boot Start already failed for this protocol; the supervisor
		// will rebuild it. Nothing to initialize.
		return
	}
	if rec, panicked := p.callInit(); panicked {
		p.quarantined.Store(true)
		p.exitLoop.Store(true)
		p.supervisor.signalPanic("Init", rec)
	}
}

// startLoop launches an event-loop incarnation for p and records its
// cancel func and done channel so the supervisor can stop it on a
// restart. The loop context is a child of parent (the runtime context)
// so runtime shutdown still stops every loop; the per-incarnation
// cancel lets the supervisor stop just this one.
func (p *protoProtocol) startLoop(parent context.Context, wg *sync.WaitGroup) {
	loopCtx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	p.loopCancel = cancel
	p.loopDone = done
	wg.Add(1)
	go p.eventHandler(loopCtx, wg, done)
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

func (c *protocolContext) handPanicToSupervisor(where string, rec any) {
	c.proto.handPanicToSupervisor(where, rec)
}

func (p *protoProtocol) eventHandler(ctx context.Context, wg *sync.WaitGroup, done chan struct{}) {
	defer close(done)
	defer wg.Done()
	// Bind the mailbox for this incarnation once. A restart starts a new
	// loop against the fresh mailbox; this loop keeps draining the one it
	// was born with until it exits.
	mailbox := p.currentMailbox()
	for {
		ev, ok := mailbox.next(ctx)
		if !ok {
			return
		}
		p.dispatch(ev)
		// Balance the pre-increment done in enqueue: the event is now
		// fully handled. Ordered after dispatch so Quiescent can only
		// read zero once the handler has actually returned.
		p.inFlight.Add(-1)
		// A non-Resume panic during dispatch asks the loop to exit so
		// the supervisor can rebuild on a fresh mailbox and instance.
		if p.exitLoop.Load() {
			return
		}
	}
}

// dispatch routes one mailbox event to the right handler. Every kind
// travels the same ordered queue, so the order handlers observe here is
// exactly the order producers enqueued in.
func (p *protoProtocol) dispatch(ev protoEvent) {
	switch ev.kind {
	case evMessage:
		p.handleMessage(ev.payload.(Message), ev.from)
	case evNotification:
		n := ev.payload.(Notification)
		fn := ev.notifFn
		p.safeCall("notification handler", func() { fn(n) })
	case evTimer:
		// Recheck cancellation at dispatch time: the fire may have been
		// queued before the user cancelled the handle. This is what
		// guarantees a callback never runs after a same-loop Cancel.
		if ev.timer.cancelled.Load() {
			return
		}
		p.safeCall("timer handler", ev.timer.fn)
	default:
		// Session and IPC events all carry their payload behind aux;
		// they share a helper so the hot inline kinds above stay cheap.
		p.dispatchAux(ev)
	}
}

// dispatchAux handles the aux-carrying event kinds (session, IPC, and
// the internal lifecycle callback). Split out of dispatch so the
// hot-path message/timer branches stay simple.
func (p *protoProtocol) dispatchAux(ev protoEvent) {
	switch ev.kind {
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
	case evCallback:
		// Runtime-internal lifecycle callback (RestartHandler.OnRestart),
		// run on the loop like any handler so it observes protocol state
		// without locking.
		if ev.aux != nil && ev.aux.run != nil {
			p.safeCall("restart handler", ev.aux.run)
		}
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
	// A quarantined protocol is mid-restart: its loop is gone and its
	// mailbox is being replaced. Drop every event to dead-letter
	// (non-blocking, even for OverflowBlock) and report success so no
	// producer ever stalls on it. The quarantined flag is only ever set
	// on supervised protocols, so unsupervised ones skip the atomic
	// load entirely.
	if p.supervisor != nil && p.quarantined.Load() {
		p.deadLetterEvent(ev)
		return true
	}
	// Pre-increment before the push so an event is never observable as
	// live-but-uncounted (the ordering Quiescent relies on).
	p.inFlight.Add(1)
	mailbox := p.currentMailbox()
	dropped, didDrop, ok := mailbox.push(ctx, ev)
	if !ok {
		// Aborted by ctx (shutdown); nothing entered the mailbox.
		p.inFlight.Add(-1)
		return false
	}
	if didDrop {
		// Exactly one event left the pipeline without dispatch: for
		// DropNewest it is the incoming ev we just counted; for
		// DropOldest it is an older event counted when it was enqueued.
		// Either way one dispatch will not happen, so undo one count.
		p.inFlight.Add(-1)
	}
	// depth() takes the deque lock for the drop/unbounded policies, and
	// the histogram's Attr list heap-escapes on every enqueue — only pay
	// for either when someone (metrics or strict mode) will look.
	if p.runtime.metricsEnabled || p.runtime.strict {
		depth := mailbox.depth()
		if p.runtime.metricsEnabled {
			p.runtime.metrics.Histogram("protorun.mailbox.depth", float64(depth),
				Attr{Key: "protocol", Value: p.name})
		}
		p.strictMailboxOccupancy(mailbox, depth)
	}
	if didDrop {
		p.runtime.metrics.Counter("protorun.mailbox.dropped", 1,
			Attr{Key: "protocol", Value: p.name},
			Attr{Key: "kind", Value: dropped.kind.String()},
			Attr{Key: "policy", Value: mailbox.policy().String()})
		p.runtime.emitDeadLetter(DeadLetter{
			Protocol: p.name,
			Kind:     dropped.kind.String(),
			Peer:     dropped.peer(),
		})
	}
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
			p.handPanicToSupervisor(where, rec)
		}
	}()
	fn()
}

// handPanicToSupervisor routes a recovered handler panic to the
// protocol's supervisor and asks the event loop to exit so the
// directive is applied off the loop. Resume (the default) has no
// supervisor, so this is a no-op and the loop keeps running exactly
// as before. Called by safeCall and by the request-handler closure in
// RegisterRequestHandler, which recovers before safeCall can (to
// auto-fail its responder) and must not swallow the panic from the
// supervisor's point of view.
func (p *protoProtocol) handPanicToSupervisor(where string, rec any) {
	if p.supervisor == nil {
		return
	}
	p.supervisor.signalPanic(where, rec)
	p.exitLoop.Store(true)
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
	if p.runtime != nil && p.runtime.metricsEnabled {
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
	b := ctx.binding()
	b.strictWireNameNudge(WireID[M](), typeNameOf[M](), implementsWireNamer[M]())
	b.registerCodec(WireID[M](), codecAdapter[M]{c: c})
}

// Handle registers a message type M and its handler in one call, picking
// the codec automatically: SelfCodec[M] when M implements SelfMarshaler,
// otherwise the reflective WireCodec[M]. It is exactly
// RegisterCodec(ctx, codec) followed by RegisterHandler(ctx, fn), so
// strict-mode double-registration behaves identically to doing the two
// calls by hand.
//
// Handle is the default path for message types. Keep the explicit
// two-call form (RegisterCodec + RegisterHandler) when you need a custom
// Codec[M] — a hand-rolled encoding, a foreign format, a shared codec
// instance.
//
// For the WireCodec path, Handle compiles the type's plan eagerly and
// panics if a field kind is unsupported (platform-sized int/uint,
// interface, chan, func, complex): that is a programming error, so it
// surfaces at Start rather than silently on the first Send. Types with a
// SelfMarshaler are not plan-checked — their encoding is their own.
func Handle[M Message](ctx ProtocolContext, fn func(M, transport.Host)) {
	var codec Codec[M]
	var zero M
	if _, ok := any(zero).(SelfMarshaler); ok {
		codec = SelfCodec[M]{}
	} else {
		if t := reflect.TypeOf(zero); t != nil && t.Kind() == reflect.Pointer &&
			t.Elem().Kind() == reflect.Struct {
			if _, err := wirePlanFor(t.Elem()); err != nil {
				panic(fmt.Sprintf("protorun: Handle[%s]: WireCodec cannot encode this type: %v",
					typeNameOf[M](), err))
			}
		}
		codec = WireCodec[M]{}
	}
	RegisterCodec(ctx, codec)
	RegisterHandler(ctx, fn)
}

// implementsWireNamer reports whether M (or *M's element) implements
// WireNamer, mirroring WireID's probe so a pointer-receiver WireName is
// detected even off a typed-nil M.
func implementsWireNamer[M Message]() bool {
	var zero M
	t := reflect.TypeOf(zero)
	if t != nil && t.Kind() == reflect.Pointer {
		_, ok := reflect.New(t.Elem()).Interface().(WireNamer)
		return ok
	}
	_, ok := any(zero).(WireNamer)
	return ok
}

// typeNameOf returns M's Go type name for diagnostics.
func typeNameOf[M Message]() string {
	if t := reflect.TypeOf(*new(M)); t != nil {
		return t.String()
	}
	return "<nil>"
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
