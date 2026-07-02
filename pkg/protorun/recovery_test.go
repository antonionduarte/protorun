package protorun

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- Test types & helpers ---

type panicEvent struct {
	where string
	rec   any
}

// recordingPanicHandler is a test-side mixin: any protocol that
// embeds it satisfies PanicHandler and accumulates the panics the
// runtime reported. The channel is buffered enough that a chatty test
// won't block the runtime.
type recordingPanicHandler struct {
	mu       sync.Mutex
	events   []panicEvent
	notified chan panicEvent
}

func newRecordingPanicHandler() *recordingPanicHandler {
	return &recordingPanicHandler{notified: make(chan panicEvent, 16)}
}

func (h *recordingPanicHandler) OnPanic(where string, rec any) {
	h.mu.Lock()
	h.events = append(h.events, panicEvent{where, rec})
	h.mu.Unlock()
	select {
	case h.notified <- panicEvent{where, rec}:
	default:
	}
}

func (h *recordingPanicHandler) waitFor(d time.Duration) (panicEvent, bool) {
	select {
	case ev := <-h.notified:
		return ev, true
	case <-time.After(d):
		return panicEvent{}, false
	}
}

// --- Test protocols ---

// panicReqServer registers a request handler that panics on every
// request. Embeds recordingPanicHandler so the test can verify the
// runtime called OnPanic with the right "where" tag.
type panicReqServer struct {
	*recordingPanicHandler
}

func (s *panicReqServer) Start(ctx ProtocolContext) {
	RegisterRequestHandler(ctx, func(_ *echoReq, _ Responder[*echoRep]) {
		panic("request boom")
	})
}
func (s *panicReqServer) Init(_ ProtocolContext) {}

// panicReqServerNoHandler is panicReqServer minus the PanicHandler
// implementation, used to verify the framework's default recovery
// path works when no protocol-level hook is provided.
type panicReqServerNoHandler struct{}

func (panicReqServerNoHandler) Start(ctx ProtocolContext) {
	RegisterRequestHandler(ctx, func(_ *echoReq, _ Responder[*echoRep]) {
		panic("silent boom")
	})
}
func (panicReqServerNoHandler) Init(_ ProtocolContext) {}

// panicNotifSub panics on every notification it receives. It still
// records receipts via a counter so the test can confirm the dispatch
// reached it before the panic.
type panicNotifSub struct {
	*recordingPanicHandler
	delivered atomic.Int32
}

func (s *panicNotifSub) Start(ctx ProtocolContext) {
	SubscribeNotification(ctx, func(_ tickNotif) {
		s.delivered.Add(1)
		panic("notif boom")
	})
}
func (s *panicNotifSub) Init(_ ProtocolContext) {}

// recursivePanicHandler implements PanicHandler but its OnPanic itself
// panics. Used to verify the framework's nested-recover guard.
type recursivePanicHandler struct {
	hookCalled atomic.Int32
}

func (r *recursivePanicHandler) OnPanic(_ string, _ any) {
	r.hookCalled.Add(1)
	panic("hook boom")
}

type recursivePanicProto struct {
	*recursivePanicHandler
}

func (p *recursivePanicProto) Start(ctx ProtocolContext) {
	RegisterRequestHandler(ctx, func(_ *echoReq, _ Responder[*echoRep]) {
		panic("user boom")
	})
}
func (p *recursivePanicProto) Init(_ ProtocolContext) {}

// secondCallProto registers a request handler that panics on the
// first request and replies normally on subsequent ones. Used to
// verify the event loop survives a panic.
type secondCallProto struct {
	calls atomic.Int32
}

func (s *secondCallProto) Start(ctx ProtocolContext) {
	RegisterRequestHandler(ctx, func(req *echoReq, r Responder[*echoRep]) {
		if s.calls.Add(1) == 1 {
			panic("first-call boom")
		}
		r.Reply(&echoRep{Got: req.Msg})
	})
}
func (s *secondCallProto) Init(_ ProtocolContext) {}

// pairOfRequestors sends two requests in sequence; the second one waits
// for the first reply, so the event loop must stay alive through the
// first (panicking) handler invocation.
type pairOfRequestors struct {
	first  *echoReplyResult
	second *echoReplyResult
}

func (p *pairOfRequestors) Start(_ ProtocolContext) {}
func (p *pairOfRequestors) Init(ctx ProtocolContext) {
	SendRequest(ctx, &echoReq{Msg: "one"}, func(rep *echoRep, err error) {
		p.first.set(rep, err)
		// Chain: send the second request from inside the first reply's
		// callback so it lands on the same event loop *after* the first
		// reply is processed.
		SendRequest(ctx, &echoReq{Msg: "two"}, func(rep *echoRep, err error) {
			p.second.set(rep, err)
		})
	})
}

// --- Tests ---

// TestRecovery_RequestHandler_PanicAutoFails verifies the
// auto-fail-on-panic guarantee for request handlers: the requester
// gets ErrHandlerPanicked (wrapped in ErrResponderFailed) immediately
// instead of waiting for the timeout.
func TestRecovery_RequestHandler_PanicAutoFails(t *testing.T) {
	rec := newRecordingPanicHandler()
	server := &panicReqServer{recordingPanicHandler: rec}
	result := newEchoReplyResult()

	startIPCRuntime(t, server, &timeoutRequestor{
		target:  result,
		timeout: 5 * time.Second, // long timeout; auto-fail must arrive first
	})

	if !result.wait(2 * time.Second) {
		t.Fatalf("auto-fail never arrived; requester was waiting on the timeout instead")
	}
	if !errors.Is(result.err, ErrResponderFailed) {
		t.Fatalf("expected ErrResponderFailed, got %v", result.err)
	}
	if !errors.Is(result.err, ErrHandlerPanicked) {
		t.Fatalf("expected ErrHandlerPanicked, got %v", result.err)
	}

	ev, ok := rec.waitFor(time.Second)
	if !ok {
		t.Fatalf("PanicHandler.OnPanic was never invoked")
	}
	if ev.where != "request handler" {
		t.Fatalf("expected where=request handler, got %q", ev.where)
	}
	if ev.rec != "request boom" {
		t.Fatalf("expected recovered=request boom, got %v", ev.rec)
	}
}

// TestRecovery_RequestHandler_NoPanicHandler verifies the framework's
// default recovery path works when the protocol does not implement
// PanicHandler. The runtime must still recover and the requester must
// still get ErrHandlerPanicked.
func TestRecovery_RequestHandler_NoPanicHandler(t *testing.T) {
	result := newEchoReplyResult()
	startIPCRuntime(t,
		panicReqServerNoHandler{},
		&timeoutRequestor{target: result, timeout: 2 * time.Second},
	)

	if !result.wait(2 * time.Second) {
		t.Fatalf("auto-fail never arrived")
	}
	if !errors.Is(result.err, ErrHandlerPanicked) {
		t.Fatalf("expected ErrHandlerPanicked, got %v", result.err)
	}
}

// TestRecovery_EventLoopSurvives verifies that after a request
// handler panics, the same protocol's event loop continues to process
// subsequent requests.
func TestRecovery_EventLoopSurvives(t *testing.T) {
	server := &secondCallProto{}
	first, second := newEchoReplyResult(), newEchoReplyResult()
	startIPCRuntime(t, server, &pairOfRequestors{first: first, second: second})

	if !first.wait(2 * time.Second) {
		t.Fatalf("first reply never arrived")
	}
	if !errors.Is(first.err, ErrHandlerPanicked) {
		t.Fatalf("expected first request to fail with ErrHandlerPanicked, got %v", first.err)
	}

	if !second.wait(2 * time.Second) {
		t.Fatalf("second reply never arrived; event loop appears stuck after panic")
	}
	if second.err != nil {
		t.Fatalf("second request unexpectedly failed: %v", second.err)
	}
	if second.rep == nil || second.rep.Got != "two" {
		t.Fatalf("second reply: expected Got=two, got %+v", second.rep)
	}
}

// TestRecovery_NotificationHandlerPanic verifies that a panicking
// notification subscriber doesn't break fan-out to its peers. It
// also verifies the panic is reported via OnPanic with the right
// where tag.
func TestRecovery_NotificationHandlerPanic(t *testing.T) {
	rec := newRecordingPanicHandler()
	bad := &panicNotifSub{recordingPanicHandler: rec}
	good := newNotifSubscriber()
	startIPCRuntime(t, bad, good, &publisher{tick: 42})

	select {
	case <-good.gotOne:
	case <-time.After(2 * time.Second):
		t.Fatalf("non-panicking subscriber never received the notification")
	}

	// Wait for the panic to be reported by the runtime; this is the
	// synchronization point that guarantees bad's handler ran. Just
	// reading bad.delivered here would race against bad's event loop.
	ev, ok := rec.waitFor(2 * time.Second)
	if !ok {
		t.Fatalf("PanicHandler.OnPanic was never invoked")
	}
	if ev.where != "notification handler" {
		t.Fatalf("expected where=notification handler, got %q", ev.where)
	}
	if got := bad.delivered.Load(); got != 1 {
		t.Fatalf("expected panicking subscriber to receive 1 notification, got %d", got)
	}
}

// TestRecovery_PanicHandler_ItselfPanics verifies that if the user's
// OnPanic also panics, the framework swallows it instead of looping
// forever or taking down the runtime.
func TestRecovery_PanicHandler_ItselfPanics(t *testing.T) {
	hook := &recursivePanicHandler{}
	server := &recursivePanicProto{recursivePanicHandler: hook}
	result := newEchoReplyResult()

	startIPCRuntime(t, server, &timeoutRequestor{
		target:  result,
		timeout: 2 * time.Second,
	})

	if !result.wait(2 * time.Second) {
		t.Fatalf("auto-fail never arrived; runtime may have stalled on PanicHandler-panic")
	}
	if !errors.Is(result.err, ErrHandlerPanicked) {
		t.Fatalf("expected ErrHandlerPanicked, got %v", result.err)
	}
	if hook.hookCalled.Load() == 0 {
		t.Fatalf("recursivePanicHandler.OnPanic was never called")
	}
}
