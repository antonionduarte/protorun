package protorun

import (
	"errors"
	"fmt"
	"runtime/debug"
	"sync/atomic"
	"time"
)

// IPC: same-runtime inter-protocol communication. Three primitives:
//
//   - Request / Reply: point-to-point ask, async response. Each Request
//     type has at most one handler runtime-wide (the protocol that
//     called RegisterRequestHandler). Replies are routed back via a
//     framework-generated request ID; users never see the ID.
//
//   - Notification: pub/sub fan-out. Multiple protocols can subscribe
//     to a Notification type; the publisher emits and forgets.
//
// IPC is intentionally local-only: there is no `to peer` parameter.
// Cross-node coordination flows through the existing peer-message path
// (Send / RegisterHandler over a transport.Host). This is the design
// line that distinguishes protorun from network-transparent actor
// frameworks like Ergo: the layering only buys you something if "ask
// the membership protocol for the active view" is cheap and "send a
// Pong to peer X" is the expensive thing.

// Request is the marker interface for IPC request types. Embed
// BaseRequest to satisfy it.
type Request interface{ isRequest() }

// Reply is the marker interface for IPC reply types. Embed BaseReply
// to satisfy it.
type Reply interface{ isReply() }

// Notification is the marker interface for IPC notification types.
// Embed BaseNotification to satisfy it.
type Notification interface{ isNotification() }

// BaseRequest is a zero-byte embeddable that satisfies Request.
type BaseRequest struct{}

func (BaseRequest) isRequest() {}

// BaseReply is a zero-byte embeddable that satisfies Reply.
type BaseReply struct{}

func (BaseReply) isReply() {}

// BaseNotification is a zero-byte embeddable that satisfies Notification.
type BaseNotification struct{}

func (BaseNotification) isNotification() {}

// Responder is the per-request handle the framework hands to a request
// handler. The handler calls Reply (success) or Fail (error) at most
// once; subsequent calls are silently dropped. Handlers may capture
// the responder to reply asynchronously (e.g. after consulting another
// protocol or waiting on a peer message).
type Responder[Rep Reply] interface {
	Reply(rep Rep)
	Fail(err error)
}

// Sentinel errors. Wrap with errors.Is to detect specific IPC failures.
var (
	// ErrNoRequestHandler is delivered to SendRequest's callback when
	// no protocol on this runtime has registered a handler for the
	// request type.
	ErrNoRequestHandler = errors.New("protorun: no handler registered for request type")

	// ErrRequestTimeout is delivered to SendRequest's callback when
	// the responder did not call Reply or Fail within the timeout.
	ErrRequestTimeout = errors.New("protorun: request timed out")

	// ErrResponderFailed wraps the error a responder passed to Fail.
	// Use errors.Unwrap or errors.Is on the wrapped cause to inspect.
	ErrResponderFailed = errors.New("protorun: responder reported failure")

	// ErrHandlerPanicked is delivered to SendRequest's callback when
	// the request handler panics before calling Reply or Fail. Wrapped
	// by ErrResponderFailed, so requesters see both sentinels via
	// errors.Is.
	ErrHandlerPanicked = errors.New("protorun: request handler panicked")
)

// defaultRequestTimeout is the framework default when no per-runtime
// override is supplied via WithDefaultRequestTimeout.
const defaultRequestTimeout = 30 * time.Second

// WithDefaultRequestTimeout overrides the runtime's default request
// timeout used by SendRequest. Per-call timeouts via
// SendRequestWithTimeout still take precedence.
func WithDefaultRequestTimeout(d time.Duration) Option {
	return func(r *Runtime) {
		if d > 0 {
			r.defaultRequestTimeout = d
		}
	}
}

// --- Internal IPC types ---

// replyToken correlates a reply back to the originating request:
// which protocol issued it and a per-protocol-monotonic request ID.
type replyToken struct {
	requester *protoProtocol
	requestID uint64
}

// inboundRequest is the envelope pushed onto a target protocol's
// requestEvents channel when a request needs to be dispatched there.
// The handler closure travels with the envelope so the dispatcher
// doesn't have to repeat the runtime-level lookup.
type inboundRequest struct {
	req     Request
	token   replyToken
	handler func(req Request, token replyToken)
}

// inboundReply is the envelope pushed onto the requester's replyEvents
// channel when a reply (or terminal error) is ready.
type inboundReply struct {
	requestID uint64
	rep       Reply
	err       error
}

// inboundNotification is the envelope pushed onto a subscriber's
// notificationEvents channel. The handler closure is captured at
// Subscribe time and travels with the envelope.
type inboundNotification struct {
	n       Notification
	handler func(Notification)
}

// pendingRequest is the per-outstanding-request state on the requester
// side. Stored in protoProtocol.pending until either a reply lands or
// the timeout fires (whichever first).
type pendingRequest struct {
	cb        func(Reply, error)
	deadline  time.Time
	startedAt time.Time
	wireID    uint64
}

// requestRoute tells the runtime which protocol owns a given request
// type's handler.
type requestRoute struct {
	proto   *protoProtocol
	handler func(req Request, token replyToken)
}

// responder[Rep] is the typed Responder implementation. The done flag
// guards against double-reply; the runtime is captured so Reply / Fail
// can route the response back through the requester's replyEvents.
type responder[Rep Reply] struct {
	runtime *Runtime
	token   replyToken
	done    atomic.Bool
}

func (r *responder[Rep]) Reply(rep Rep) {
	if !r.done.CompareAndSwap(false, true) {
		return
	}
	r.runtime.deliverReply(r.token, rep, nil)
}

func (r *responder[Rep]) Fail(err error) {
	if !r.done.CompareAndSwap(false, true) {
		return
	}
	r.runtime.deliverReply(r.token, nil, fmt.Errorf("%w: %w", ErrResponderFailed, err))
}

// --- Public free functions ---

// RegisterRequestHandler installs a handler for Req on the supplied
// ProtocolContext. Each Req type has at most one handler runtime-wide
// (registering a second time replaces the first and logs a warning).
//
// The handler receives the typed request and a Responder[Rep] that it
// must call exactly once with either Reply or Fail. Sync and async
// shapes both work: replying inline is a "synchronous" request from
// the caller's perspective, while capturing the responder and
// replying later (e.g. after a peer round-trip) makes it asynchronous.
func RegisterRequestHandler[Req Request, Rep Reply](
	ctx ProtocolContext,
	fn func(Req, Responder[Rep]),
) {
	ctx.binding().registerRequestHandler(WireID[Req](), func(raw Request, token replyToken) {
		typed, ok := raw.(Req)
		if !ok {
			// Type mismatch should be unreachable since wire ID
			// derives from the type, but defend against bug-introduced
			// misroutes by failing the responder rather than panicking.
			ctx.binding().deliverReplyToToken(token, nil,
				fmt.Errorf("protorun: request type mismatch: expected %T, got %T", *new(Req), raw))
			return
		}
		r := &responder[Rep]{runtime: ctx.binding().runtime, token: token}
		// Recover here (not just at the event-loop dispatcher) so we
		// can auto-fail the responder before the panic escapes; that
		// way the requester sees ErrHandlerPanicked immediately
		// instead of blocking until the request timeout fires. The
		// outer dispatcher's safeCall is a fallback for the (by-
		// construction unreachable) case where the panic bypasses
		// this defer.
		defer func() {
			if rec := recover(); rec != nil {
				// Report first, fail second. reportPanic runs the
				// PanicHandler hook synchronously on the handler
				// protocol's event loop; r.Fail wakes the requester's
				// event loop. Doing them in this order guarantees
				// "if the requester saw ErrHandlerPanicked, then
				// OnPanic has already been called", which is the
				// natural assumption a supervisor / metrics hook
				// will make.
				ctx.binding().reportPanic("request handler", rec, debug.Stack())
				r.Fail(fmt.Errorf("%w: %v", ErrHandlerPanicked, rec))
			}
		}()
		fn(typed, r)
	})
}

// SendRequest sends req to the protocol that handles its type and
// invokes onReply on the requester's protocol event loop when the
// reply or a terminal error arrives. Uses the runtime's default
// request timeout; see SendRequestWithTimeout to override per call.
func SendRequest[Req Request, Rep Reply](
	ctx ProtocolContext,
	req Req,
	onReply func(Rep, error),
) {
	SendRequestWithTimeout(ctx, req, 0, onReply)
}

// SendRequestWithTimeout is SendRequest with an explicit per-call
// timeout. A non-positive timeout falls back to the runtime default
// (set via WithDefaultRequestTimeout, ultimately defaultRequestTimeout).
func SendRequestWithTimeout[Req Request, Rep Reply](
	ctx ProtocolContext,
	req Req,
	timeout time.Duration,
	onReply func(Rep, error),
) {
	rt := ctx.binding().runtime
	if timeout <= 0 {
		timeout = rt.defaultRequestTimeout
		if timeout <= 0 {
			timeout = defaultRequestTimeout
		}
	}
	wrapped := func(rep Reply, err error) {
		if err != nil {
			var zero Rep
			onReply(zero, err)
			return
		}
		typed, ok := rep.(Rep)
		if !ok {
			var zero Rep
			onReply(zero, fmt.Errorf("protorun: reply type mismatch: expected %T, got %T", *new(Rep), rep))
			return
		}
		onReply(typed, nil)
	}
	ctx.binding().sendRequest(WireID[Req](), req, timeout, wrapped)
}

// SubscribeNotification installs fn as a subscriber for notifications
// of type N on the supplied ProtocolContext. Multiple subscribers per
// N type are supported. Notifications are delivered on the
// subscriber's protocol event loop. A second Subscribe call from the
// same protocol for the same N replaces the prior handler.
func SubscribeNotification[N Notification](ctx ProtocolContext, fn func(N)) {
	ctx.binding().subscribeNotification(WireID[N](), func(raw Notification) {
		typed, ok := raw.(N)
		if !ok {
			return
		}
		fn(typed)
	})
}

// UnsubscribeNotification removes the calling protocol's subscription
// for notifications of type N. Idempotent; a no-op if the protocol
// wasn't subscribed.
func UnsubscribeNotification[N Notification](ctx ProtocolContext) {
	ctx.binding().unsubscribeNotification(WireID[N]())
}

// PublishNotification fans the notification out to every protocol that
// has subscribed to N's wire id. Fire-and-forget; no acknowledgement,
// no error path. Subscribers receive on their own event loops.
func PublishNotification[N Notification](ctx ProtocolContext, n N) {
	ctx.binding().publishNotification(WireID[N](), n)
}
