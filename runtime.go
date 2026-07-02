package protorun

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/antonionduarte/protorun/transport"
)

// Public sentinel errors. The runtime returns these (or wraps them with
// fmt.Errorf("...: %w", Err...)) so callers can use errors.Is to test
// for specific failure conditions instead of string-matching.
var (
	// ErrNoSessionLayer is returned by Connect/Disconnect/ConnectWithRetry
	// when no SessionLayer has been registered (typically because
	// WithTCPTransport was not supplied to New).
	ErrNoSessionLayer = errors.New("protorun: session layer not registered")

	// ErrNoNetworkLayer is returned by Run when no transport.Layer has
	// been registered.
	ErrNoNetworkLayer = errors.New("protorun: network layer not registered")

	// ErrAlreadyCancelled is returned by Run when Cancel has already
	// been called on the runtime instance.
	ErrAlreadyCancelled = errors.New("protorun: cannot start a runtime that has been cancelled")

	// ErrNoCodec is returned by Send when no Codec has been registered
	// for the message type's wire identifier.
	ErrNoCodec = errors.New("protorun: no codec registered for message type")
)

type Runtime struct {
	self       transport.Host
	ctx        context.Context
	cancelFunc func()
	wg         sync.WaitGroup

	// clock is the time source for the timer table, retry backoff,
	// request-timeout arming, the strict watchdog, and IPC latency. The
	// default is realClock; WithClock swaps it for deterministic tests.
	clock Clock

	// nextTimerID mints the runtime-monotonic ids that key timer
	// handles. No user-managed ids, so there is no uniqueness contract
	// and no silent replacement.
	nextTimerID atomic.Uint64

	// deadLetter, when set via WithDeadLetter, receives events dropped
	// by drop-policy mailboxes. Called synchronously on the enqueuing
	// goroutine.
	deadLetter func(DeadLetter)

	// protocols is the live protocol set. Written at registration (before
	// start) and, once supervision can remove a Stopped/Escalated
	// protocol at run time, mutated by supervisor goroutines — so it is
	// guarded by protocolsMu and read through snapshotProtocols.
	protocols   []*protoProtocol
	protocolsMu sync.Mutex

	codecs *codecRegistry

	// established is the set of peers with a live session, updated from
	// session-event handling (SessionConnected adds, SessionDisconnected
	// removes). A restart replays a synthetic SessionConnected for each,
	// so the fresh instance rebuilds peer state the way it did at boot.
	established   map[transport.Host]struct{}
	establishedMu sync.Mutex

	// fatalErr is set (once) by a supervisor that escalates; Run and
	// Shutdown surface it. shutdownOnce guards teardown so Cancel is
	// idempotent and safe to call from several paths.
	fatalErr     error
	fatalMu      sync.Mutex
	shutdownOnce sync.Once

	retryPolicy       RetryPolicy
	retryMu           sync.Mutex
	connectionRetries map[transport.Host]*retryState

	// IPC routing: request/handler map plus many-subscriber notification
	// fanout. See ipc_router.go.
	ipc *ipcRouter

	defaultRequestTimeout time.Duration

	// networkLayer is held only for lifecycle teardown (Cancel); all
	// runtime traffic flows through the Sessions seam. It may be nil
	// when the Sessions adapter owns its own transport (or has none,
	// like prototest's in-memory mesh).
	networkLayer transport.Layer
	sessionLayer Sessions

	logger  *slog.Logger
	metrics Metrics

	// Strict-mode toggles. See strict.go for the full list of checks
	// they enable. strict=false (the default) makes them all no-ops.
	strict               bool
	strictHandlerTimeout time.Duration

	// wireNameWarned dedups the strict-mode "message type has no
	// WireName()" nudge to once per wire id. map[uint64]struct{}, only
	// ever touched when strict is on. See strictWireNameNudge.
	wireNameWarned sync.Map
}

// SessionConnectedHandler can be implemented by a protocol that wants to be
// notified whenever a session is established with some Host.
type SessionConnectedHandler interface {
	OnSessionConnected(transport.Host)
}

// SessionDisconnectedHandler can be implemented by a protocol that wants to be
// notified whenever a session is torn down with some Host.
type SessionDisconnectedHandler interface {
	OnSessionDisconnected(transport.Host)
}

// Internal representation of session events delivered to each protoProtocol.
type sessionEventType int

const (
	sessionConnectedEvent sessionEventType = iota
	sessionDisconnectedEvent
	sessionGivenUpEvent
)

type sessionEvent struct {
	kind     sessionEventType
	host     transport.Host
	attempts int // populated for sessionGivenUpEvent
}

// Option configures a Runtime at construction.
type Option func(*Runtime)

// WithLogger overrides the slog.Logger used by the runtime. The runtime
// rebinds the logger with component=runtime so users don't need to do it
// themselves.
func WithLogger(logger *slog.Logger) Option {
	return func(r *Runtime) {
		if logger == nil {
			return
		}
		r.logger = logger.With("component", "runtime")
	}
}

// New constructs a Runtime bound to the given local Host. Protocols, the
// transport layer, and the session layer must be registered before calling
// Start. The runtime context is created here, so Cancel works regardless
// of whether Start has been called.
func New(self transport.Host, opts ...Option) *Runtime {
	ctx, cancel := context.WithCancel(context.Background())
	r := &Runtime{
		self:              self,
		ctx:               ctx,
		cancelFunc:        cancel,
		clock:             realClock{},
		codecs:            newCodecRegistry(),
		connectionRetries: make(map[transport.Host]*retryState),
		established:       make(map[transport.Host]struct{}),
		ipc:               newIPCRouter(),
		logger:            slog.Default().With("component", "runtime"),
		metrics:           noopMetrics{},
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func (r *Runtime) Self() transport.Host { return r.self }

func (r *Runtime) Logger() *slog.Logger {
	if r.logger == nil {
		return slog.Default()
	}
	return r.logger
}

// start launches all long-lived goroutines owned by the runtime
// (protocol event loops, session event pump, and the main dispatcher)
// and returns immediately. It is the synchronous half of Run: tests
// inside this package use start() directly when they need "register
// then verify" without blocking; user code should call Run instead.
func (r *Runtime) start() error {
	if r.sessionLayer == nil {
		// The runtime speaks only to the Sessions seam; a bare
		// transport.Layer isn't enough. Distinguish the errors so the
		// operator knows which half of the stack is missing.
		if r.networkLayer == nil {
			return ErrNoNetworkLayer
		}
		return ErrNoSessionLayer
	}
	if err := r.ctx.Err(); err != nil {
		return ErrAlreadyCancelled
	}

	r.Logger().Info("runtime starting")

	r.startProtocols(r.ctx)
	r.initProtocols()
	// Supervisors start after boot so any Start/Init panic during boot
	// is already buffered on their signal channel and handled the moment
	// they come up — without racing the boot sequence itself.
	r.startSupervisors(r.ctx)
	r.startSessionEvents(r.ctx)

	r.wg.Add(1)
	go r.eventHandler(r.ctx)
	return nil
}

// startSupervisors launches one goroutine per supervised protocol.
// Each parks on its signal channel until a panic is reported (a boot
// panic may already be buffered) and then applies the directive.
func (r *Runtime) startSupervisors(ctx context.Context) {
	for _, p := range r.protocols {
		if p.supervisor == nil {
			continue
		}
		sup := p.supervisor
		r.wg.Go(func() { sup.run(ctx) })
	}
}

// registerNetworkLayer attaches a Layer. Most users wire this
// via WithTCPTransport. Only test code that needs to inject a mock
// transport reaches in here directly.
func (r *Runtime) registerNetworkLayer(networkLayer transport.Layer) {
	r.networkLayer = networkLayer
}

// registerSessionLayer attaches a Sessions adapter. As with the
// transport layer, normal callers wire this via WithTCPTransport or
// WithTransport.
func (r *Runtime) registerSessionLayer(sessionLayer Sessions) {
	r.sessionLayer = sessionLayer
}

// RegisterOption configures a single protocol at registration time.
type RegisterOption func(*registerConfig)

// registerConfig accumulates per-protocol registration settings. Its
// zero value is not valid; Register seeds the defaults before applying
// options.
type registerConfig struct {
	mailbox        Mailbox
	supervision    Supervision
	hasSupervision bool
}

// WithMailbox overrides the registering protocol's mailbox capacity and
// overflow policy. Without it, the protocol gets an OverflowBlock
// mailbox of defaultMailboxCapacity.
func WithMailbox(m Mailbox) RegisterOption {
	return func(c *registerConfig) { c.mailbox = m }
}

// Register wraps a single Protocol instance in a protoProtocol envelope
// and attaches it to this runtime. Without options the protocol gets a
// default mailbox (capacity defaultMailboxCapacity, OverflowBlock) and
// Resume supervision (panics are recovered and logged, the instance
// keeps running); WithMailbox and WithSupervision tune those.
//
// Restart supervision needs a fresh instance per restart, which a
// singleton can't provide — use RegisterFactory for that. Configuring
// OnPanic: Restart here panics in strict mode and downgrades to Resume
// (with a warning) otherwise.
func (r *Runtime) Register(impl Protocol, opts ...RegisterOption) {
	r.buildProtocol(impl, nil, opts)
}

// RegisterFactory attaches a protocol built from a factory. The factory
// is invoked once immediately for the initial instance and again for
// each restart, so every incarnation starts from fresh state — the
// property Restart supervision depends on.
func (r *Runtime) RegisterFactory(factory func() Protocol, opts ...RegisterOption) {
	r.buildProtocol(factory(), factory, opts)
}

// buildProtocol is the shared registration path for Register and
// RegisterFactory. It seeds mailbox and supervision defaults, enforces
// that Restart has a factory, wires a supervisor when the policy is not
// Resume, and appends the envelope.
func (r *Runtime) buildProtocol(impl Protocol, factory func() Protocol, opts []RegisterOption) {
	cfg := registerConfig{mailbox: Mailbox{Capacity: defaultMailboxCapacity, Overflow: OverflowBlock}}
	for _, opt := range opts {
		opt(&cfg)
	}

	p := newProtoProtocolMailbox(impl, cfg.mailbox)
	p.factory = factory

	spec := cfg.supervision
	if spec.OnPanic == Restart && factory == nil {
		// Restart with no factory would re-run a corrupted singleton —
		// the exact Erlang mistake supervision exists to avoid.
		if r.strict {
			strictPanic("WithSupervision{OnPanic: Restart} requires RegisterFactory; %T registered as a singleton", impl)
		}
		r.Logger().Warn("protorun: Restart supervision requires RegisterFactory; downgrading to Resume",
			"protocol", fmt.Sprintf("%T", impl))
		spec.OnPanic = Resume
	}
	if cfg.hasSupervision && spec.OnPanic != Resume {
		p.supervisor = newSupervisor(p, r, spec.withDefaults())
	}

	r.registerProtocol(p)
}

// registerProtocol attaches an already-constructed protoProtocol to this
// runtime. Reserved for tests that want to inspect the envelope.
func (r *Runtime) registerProtocol(protocol *protoProtocol) {
	protocol.bindRuntime(r)
	r.protocolsMu.Lock()
	r.protocols = append(r.protocols, protocol)
	r.protocolsMu.Unlock()
}

// snapshotProtocols copies the live protocol set under lock so callers
// can iterate without holding protocolsMu (fanout enqueues can block on
// a slow consumer, and a supervisor may remove a protocol concurrently).
func (r *Runtime) snapshotProtocols() []*protoProtocol {
	r.protocolsMu.Lock()
	out := make([]*protoProtocol, len(r.protocols))
	copy(out, r.protocols)
	r.protocolsMu.Unlock()
	return out
}

// removeProtocol drops p from the live set. Called by a supervisor when
// a protocol is Stopped or Escalated; its codecs and IPC routes are
// already deregistered, so removal makes it invisible to session
// fanout too.
func (r *Runtime) removeProtocol(p *protoProtocol) {
	r.protocolsMu.Lock()
	for i, x := range r.protocols {
		if x == p {
			r.protocols = append(r.protocols[:i], r.protocols[i+1:]...)
			break
		}
	}
	r.protocolsMu.Unlock()
}

// publishProtocolFailed fans a ProtocolFailed notification out to any
// subscriber. Notifications are local-only and codec-free, so this is a
// plain publish through the IPC fanout.
func (r *Runtime) publishProtocolFailed(protocol, outcome string, attempt int) {
	r.publishNotification(WireID[ProtocolFailed](), ProtocolFailed{
		Protocol: protocol,
		Outcome:  outcome,
		Attempt:  attempt,
	})
}

// escalate records the fatal error (first writer wins) and cancels the
// runtime context so Run unblocks and returns ErrProtocolFailed. It
// only cancels the context — full teardown (layer Cancel, WaitGroup
// wait) is left to Cancel, because escalate runs on a supervisor
// goroutine that Cancel's WaitGroup wait would otherwise deadlock on.
func (r *Runtime) escalate(protocol, desc string) {
	r.fatalMu.Lock()
	if r.fatalErr == nil {
		r.fatalErr = fmt.Errorf("%w: protocol %s: %s", ErrProtocolFailed, protocol, desc)
	}
	r.fatalMu.Unlock()
	r.cancelFunc()
}

// fatalError returns the escalation error, if any.
func (r *Runtime) fatalError() error {
	r.fatalMu.Lock()
	defer r.fatalMu.Unlock()
	return r.fatalErr
}

// markEstablished / markDisconnected maintain the established-peers set
// consulted by session replay on restart.
func (r *Runtime) markEstablished(host transport.Host) {
	r.establishedMu.Lock()
	r.established[host] = struct{}{}
	r.establishedMu.Unlock()
}

func (r *Runtime) markDisconnected(host transport.Host) {
	r.establishedMu.Lock()
	delete(r.established, host)
	r.establishedMu.Unlock()
}

// snapshotEstablished copies the established-peers set under lock.
func (r *Runtime) snapshotEstablished() []transport.Host {
	r.establishedMu.Lock()
	out := make([]transport.Host, 0, len(r.established))
	for h := range r.established {
		out = append(out, h)
	}
	r.establishedMu.Unlock()
	return out
}

// connect / disconnect are the runtime-internal entry points used by
// ProtocolContext implementations. They validate that the runtime is
// usable and return a sync error if not; transport-level failures still
// surface asynchronously through SessionFailed events.
//
// disconnect also clears any tracked retry intent so a user-initiated
// disconnect halts further reconnect attempts.
func (r *Runtime) connect(host transport.Host) error {
	if r.sessionLayer == nil {
		return ErrNoSessionLayer
	}
	if err := r.ctx.Err(); err != nil {
		return err
	}
	r.sessionLayer.Connect(host)
	return nil
}

func (r *Runtime) disconnect(host transport.Host) error {
	if r.sessionLayer == nil {
		return ErrNoSessionLayer
	}
	r.stopRetryFor(host)
	if err := r.ctx.Err(); err != nil {
		return err
	}
	r.sessionLayer.Disconnect(host)
	return nil
}

// --- IPC routing ---
//
// These helpers are the runtime-side glue behind the public IPC API in
// ipc.go. They follow the same pattern as session-event fanout: cross-
// protocol channel writes are ctx-guarded so a slow consumer or a
// shutdown in flight can't block the caller indefinitely.

// deliverReply pushes a reply (or terminal error) onto the requester's
// replyEvents channel. Safe to call from any goroutine: responder
// methods (Reply / Fail) and time.AfterFunc timeout callbacks both
// end up here.
func (r *Runtime) deliverReply(token replyToken, rep Reply, err error) {
	token.requester.enqueue(r.ctx, protoEvent{
		kind: evReply,
		aux:  &eventAux{reply: inboundReply{requestID: token.requestID, rep: rep, err: err}},
	})
}

// sendRequest is the requester-side internal entry point. It allocates
// a request ID, registers a pending entry, then either routes the
// request to its handler (and arms a timeout) or delivers
// ErrNoRequestHandler immediately. Pending is registered before any
// delivery path so deliverReply can always find the entry. Without
// it, the no-handler error would be dropped on the floor.
func (r *Runtime) sendRequest(
	requester *protoProtocol,
	wireID uint64,
	req Request,
	timeout time.Duration,
	onReply func(Reply, error),
) {
	requestID := requester.nextRequestID.Add(1)
	token := replyToken{requester: requester, requestID: requestID}

	wireIDAttr := Attr{Key: "wireID", Value: fmt.Sprintf("%#x", wireID)}
	r.metrics.Counter("protorun.ipc.request.sent", 1, wireIDAttr)

	now := r.clock.Now()
	requester.pendingMu.Lock()
	requester.pending[requestID] = pendingRequest{
		cb:        onReply,
		deadline:  now.Add(timeout),
		startedAt: now,
		wireID:    wireID,
	}
	requester.pendingMu.Unlock()

	route, ok := r.ipc.Route(wireID)
	if !ok {
		r.deliverReply(token, nil, ErrNoRequestHandler)
		return
	}

	r.clock.AfterFunc(timeout, func() {
		r.deliverReply(token, nil, ErrRequestTimeout)
	})

	route.proto.enqueue(r.ctx, protoEvent{
		kind: evRequest,
		aux:  &eventAux{request: inboundRequest{req: req, token: token, handler: route.handler}},
	})
}

// subscribeNotification / unsubscribeNotification / publishNotification
// are thin pass-throughs to the IPC router. The runtime keeps the
// publish path here only so it can emit the published / delivered
// counters at the right moments.
func (r *Runtime) subscribeNotification(proto *protoProtocol, wireID uint64, fn func(Notification)) {
	r.ipc.Subscribe(wireID, proto, fn)
}

func (r *Runtime) unsubscribeNotification(proto *protoProtocol, wireID uint64) {
	r.ipc.Unsubscribe(wireID, proto)
}

// publishNotification fans n out to every subscriber of wireID. Each
// subscriber's notificationEvents channel write is ctx-guarded; if the
// runtime is shutting down or a single subscriber's mailbox is full we
// stop fanning out and return. The publisher should not block on
// subscribers it doesn't know about.
func (r *Runtime) publishNotification(wireID uint64, n Notification) {
	wireIDAttr := Attr{Key: "wireID", Value: fmt.Sprintf("%#x", wireID)}
	r.metrics.Counter("protorun.notification.published", 1, wireIDAttr)

	for _, sub := range r.ipc.SnapshotSubscribers(wireID) {
		if !sub.proto.enqueue(r.ctx, protoEvent{
			kind: evNotification,
			aux:  &eventAux{notif: inboundNotification{n: n, handler: sub.fn}},
		}) {
			return
		}
		r.metrics.Counter("protorun.notification.delivered", 1, wireIDAttr)
	}
}

// emitDeadLetter hands dl to the runtime's dead-letter hook, if one was
// configured via WithDeadLetter. Called synchronously from the enqueue
// path when a drop-policy mailbox evicts or rejects an event.
func (r *Runtime) emitDeadLetter(dl DeadLetter) {
	if r.deadLetter != nil {
		r.deadLetter(dl)
	}
}

// Cancel tears the runtime down and blocks until every goroutine it
// owns has exited. It is idempotent (guarded by shutdownOnce) so the
// several paths that may reach it — a user call, the SIGINT handler in
// Run, and Run's own post-cancellation cleanup — never double-tear-down.
//
// Do not call Cancel from inside a protocol handler or a supervisor
// callback: it waits on the WaitGroup those goroutines belong to. An
// escalating supervisor uses escalate (context-only cancel) instead;
// Run performs the WaitGroup wait once it unblocks.
func (r *Runtime) Cancel() {
	r.shutdownOnce.Do(r.teardown)
}

// teardown is the one-time body behind Cancel:
//  1. Mark every protocol cancelled (strict-mode phase tracking)
//     and cancel every timer it owns.
//  2. Cancel the runtime context (used by all internal goroutines).
//  3. Stop session and transport layers.
//  4. Tear down the retry table.
//  5. Wait for all goroutines to finish via the WaitGroup.
func (r *Runtime) teardown() {
	for _, p := range r.snapshotProtocols() {
		p.setPhase(phaseCancelled)
		p.cancelAllTimers()
	}
	r.cancelFunc()
	if r.sessionLayer != nil {
		r.sessionLayer.Cancel()
	}
	if r.networkLayer != nil {
		r.networkLayer.Cancel()
	}

	r.retryTeardown()

	r.wg.Wait()
	r.Logger().Info("runtime stopped")
}

func (r *Runtime) eventHandler(ctx context.Context) {
	defer r.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case sessionMsg := <-r.sessionLayer.OutMessages():
			r.processMessage(sessionMsg.Msg, sessionMsg.Host())
		}
	}
}

func (r *Runtime) startProtocols(ctx context.Context) {
	for _, protocol := range r.protocols {
		r.Logger().Info("starting protocol", "protocols", len(r.protocols))
		protocol.Start(ctx, &r.wg)
	}
}

func (r *Runtime) initProtocols() {
	for _, protocol := range r.protocols {
		r.Logger().Info("initializing protocol")
		protocol.Init()
	}
}

func (r *Runtime) startSessionEvents(ctx context.Context) {
	if r.sessionLayer == nil {
		return
	}

	r.wg.Go(func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-r.sessionLayer.OutChannelEvents():
				if !r.dispatchSessionEvent(ctx, ev) {
					return
				}
			}
		}
	})
}

// dispatchSessionEvent translates a SessionLayer event into the runtime's
// internal sessionEvent kind, updates retry bookkeeping if applicable, and
// fans the event out to every registered protocol. Returns false if the
// runtime context fired during fanout.
func (r *Runtime) dispatchSessionEvent(ctx context.Context, ev transport.SessionEvent) bool {
	switch e := ev.(type) {
	case *transport.SessionConnected:
		r.metrics.Counter("protorun.session.connected", 1, Attr{Key: "host", Value: e.Host().String()})
		r.markEstablished(e.Host())
		r.onSessionUpForRetry(e.Host())
		return r.fanoutSessionEvent(ctx, sessionEvent{kind: sessionConnectedEvent, host: e.Host()})
	case *transport.SessionDisconnected:
		r.metrics.Counter("protorun.session.disconnected", 1, Attr{Key: "host", Value: e.Host().String()})
		r.markDisconnected(e.Host())
		giveUp, attempts := r.onSessionDownForRetry(e.Host())
		if giveUp {
			r.metrics.Counter("protorun.session.given_up", 1,
				Attr{Key: "host", Value: e.Host().String()},
				Attr{Key: "attempts", Value: attempts})
			if !r.fanoutSessionEvent(ctx, sessionEvent{kind: sessionGivenUpEvent, host: e.Host(), attempts: attempts}) {
				return false
			}
		}
		return r.fanoutSessionEvent(ctx, sessionEvent{kind: sessionDisconnectedEvent, host: e.Host()})
	case *transport.SessionFailed:
		r.metrics.Counter("protorun.session.failed", 1, Attr{Key: "host", Value: e.Host().String()})
		giveUp, attempts := r.onSessionDownForRetry(e.Host())
		if giveUp {
			r.metrics.Counter("protorun.session.given_up", 1,
				Attr{Key: "host", Value: e.Host().String()},
				Attr{Key: "attempts", Value: attempts})
			return r.fanoutSessionEvent(ctx, sessionEvent{kind: sessionGivenUpEvent, host: e.Host(), attempts: attempts})
		}
		// SessionFailed with retry-in-progress is suppressed from fanout:
		// protocols only see the eventual SessionConnected (success) or
		// SessionGivenUp (terminal failure). Without a retry policy in
		// effect this branch is unreachable because no retry state existed.
		return true
	case *transport.SessionVersionMismatch:
		r.metrics.Counter("protorun.session.version_mismatch", 1,
			Attr{Key: "host", Value: e.Host().String()},
			Attr{Key: "peer_version", Value: e.PeerVersion()},
			Attr{Key: "inbound", Value: e.Inbound()})
		if e.Inbound() {
			// A peer dialed us with a version we don't speak; the
			// session layer already answered with a Reject and closed
			// the connection. Nothing to retry and nothing protocols
			// can act on — log loudly for the operator and move on.
			r.Logger().Error("rejected inbound handshake: peer speaks a different wire-format version",
				"transport_host", e.Host().String(),
				"peer_version", e.PeerVersion(),
				"our_version", transport.ProtocolVersion)
			return true
		}
		// Our own dial was Rejected: terminal. Burn no more retry
		// budget on a peer that cannot accept us, and tell protocols
		// to stop via the given-up surface they already implement.
		r.Logger().Error("dial rejected: peer speaks a different wire-format version",
			"host", e.Host().String(),
			"peer_version", e.PeerVersion(),
			"our_version", transport.ProtocolVersion)
		attempts := r.giveUpRetryNow(e.Host())
		r.metrics.Counter("protorun.session.given_up", 1,
			Attr{Key: "host", Value: e.Host().String()},
			Attr{Key: "attempts", Value: attempts})
		return r.fanoutSessionEvent(ctx, sessionEvent{kind: sessionGivenUpEvent, host: e.Host(), attempts: attempts})
	default:
		// Every SessionEvent kind must have an explicit route here (and
		// in TestDispatchSessionEvent_CoversAllEventKinds). Hitting this
		// default means the transport grew an event kind the runtime
		// doesn't know about — a bug, not a condition to swallow.
		r.Logger().Error("unhandled session event kind",
			"type", fmt.Sprintf("%T", ev),
			"host", ev.Host().String())
		r.metrics.Counter("protorun.session.unhandled_event", 1,
			Attr{Key: "type", Value: fmt.Sprintf("%T", ev)})
		return true
	}
}

// fanoutSessionEvent delivers ev into every protocol's sessionEvents
// channel, ctx-guarded so a slow consumer or shutdown doesn't block the
// caller. Returns false on ctx cancellation.
func (r *Runtime) fanoutSessionEvent(ctx context.Context, ev sessionEvent) bool {
	for _, proto := range r.snapshotProtocols() {
		if !proto.enqueue(ctx, protoEvent{kind: evSession, aux: &eventAux{session: ev}}) {
			return false
		}
	}
	return true
}

// processMessage decodes the wire id from the application-layer payload,
// looks up the owning protocol via the codecRegistry, decodes the
// message, and pushes a (msg, from) envelope onto that protocol's
// messageChannel. Sender info is delivered to handlers via the envelope's
// from field, not via fields on the message itself.
func (r *Runtime) processMessage(buffer bytes.Buffer, from transport.Host) {
	logger := r.Logger()

	var wireID uint64
	if err := binary.Read(&buffer, binary.LittleEndian, &wireID); err != nil {
		logger.Error("failed to read wireID header",
			"from", from.String(),
			"err", err,
		)
		r.metrics.Counter("protorun.message.dropped_header", 1)
		return
	}
	wireIDAttr := Attr{Key: "wireID", Value: fmt.Sprintf("%#x", wireID)}

	entry, exists := r.codecs.Get(wireID)
	if !exists {
		logger.Warn("received message for unknown wireID",
			"from", from.String(),
			"wireID", fmt.Sprintf("%#x", wireID),
		)
		r.metrics.Counter("protorun.message.dropped_unknown_id", 1, wireIDAttr)
		return
	}
	protocol := entry.proto

	message, err := entry.codec.unmarshal(buffer.Bytes())
	if err != nil {
		logger.Error("failed to decode message",
			"from", from.String(),
			"wireID", fmt.Sprintf("%#x", wireID),
			"err", err,
		)
		r.metrics.Counter("protorun.message.dropped_decode_error", 1, wireIDAttr)
		return
	}

	logger.Debug("dispatching message",
		"from", from.String(),
		"wireID", fmt.Sprintf("%#x", wireID),
	)
	r.metrics.Counter("protorun.message.dispatched", 1, wireIDAttr)
	protocol.enqueue(r.ctx, protoEvent{kind: evMessage, msg: message, from: from})
}

// WithTCPTransport wires the runtime's transport + session layers with
// the framework's TCP+Hello/Ack stack. It is the typical way to set up
// a runtime; most users never need to construct TCPLayer or
// SessionLayer themselves.
//
// The supplied ctx becomes the parent for both layers' internal
// goroutines.
func WithTCPTransport(ctx context.Context) Option {
	return func(r *Runtime) {
		tcp := transport.NewTCPLayer(r.self, ctx, 0)
		session := transport.NewSessionLayer(tcp, r.self, ctx, 0, 0)
		r.networkLayer = tcp
		r.sessionLayer = session
	}
}

// WithTransport injects a pre-constructed transport stack. Use this
// when you need to plug in a non-TCP transport (UDP, in-memory, mock
// for tests), tune buffer sizes / timeouts on the existing layers, or
// otherwise own the construction yourself. layer may be nil when the
// Sessions adapter owns its own transport (or has none, like
// prototest's in-memory mesh); it is held only so runtime teardown
// can Cancel it.
//
// For the default TCP setup, prefer WithTCPTransport(ctx).
func WithTransport(layer transport.Layer, sessions Sessions) Option {
	return func(r *Runtime) {
		if layer != nil {
			r.networkLayer = layer
		}
		if sessions != nil {
			r.sessionLayer = sessions
		}
	}
}

// Start launches the runtime's goroutines (protocol event loops,
// session event pump, main dispatcher) and returns immediately. The
// caller owns the lifecycle: pair it with Cancel or Shutdown. Most
// main functions should prefer Run, which adds signal handling and
// blocks; Start is for embedding the runtime in something that
// already owns its lifecycle (a server, a test fixture).
func (r *Runtime) Start() error { return r.start() }

// Run starts the runtime, installs SIGINT/SIGTERM handlers that call
// Cancel on receipt, and blocks until the runtime context is done.
// Returns the error from start if any. Use Run from cmd/main instead of
// orchestrating Start + signal handling + select{} yourself.
func (r *Runtime) Run() error {
	if err := r.start(); err != nil {
		return err
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		select {
		case <-sigCh:
			r.Logger().Info("signal received, cancelling runtime")
			r.Cancel()
		case <-r.ctx.Done():
		}
		signal.Stop(sigCh)
	}()

	<-r.ctx.Done()
	// The context can fire from a user Cancel, a signal, or a supervisor
	// escalation (which cancels the context but leaves full teardown to
	// us, to avoid deadlocking on its own goroutine). Run the teardown
	// here — idempotent via shutdownOnce — then surface any fatal error.
	r.Cancel()
	return r.fatalError()
}
