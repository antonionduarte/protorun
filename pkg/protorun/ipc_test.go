package protorun

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/transport"
)

// --- Test types ---

type echoReq struct {
	BaseRequest
	Msg string
}

type echoRep struct {
	BaseReply
	Got string
}

type orphanReq struct {
	BaseRequest
}

type tickNotif struct {
	BaseNotification
	Tick int
}

// --- Test helpers ---

// startIPCRuntime spins up a runtime with a mock network/session pair
// (IPC is local-only, so no real wires are involved) and registers the
// supplied protocols. Cancel is registered via t.Cleanup so individual
// tests don't have to remember to tear down.
func startIPCRuntime(t *testing.T, protocols ...Protocol) *Runtime {
	t.Helper()
	self := transport.NewHost(0, "127.0.0.1")
	rt := New(self)

	_ = registerMockStack(rt, self)

	for _, p := range protocols {
		rt.Register(p)
	}

	if err := rt.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(rt.Cancel)
	return rt
}

// echoReplyResult is a tiny reply latch a test protocol writes once and
// the test goroutine waits on. Channel-based so wait can race against a
// timeout cleanly.
type echoReplyResult struct {
	rep  *echoRep
	err  error
	done chan struct{}
}

func newEchoReplyResult() *echoReplyResult {
	return &echoReplyResult{done: make(chan struct{})}
}

func (r *echoReplyResult) set(rep *echoRep, err error) {
	r.rep = rep
	r.err = err
	close(r.done)
}

func (r *echoReplyResult) wait(d time.Duration) bool {
	select {
	case <-r.done:
		return true
	case <-time.After(d):
		return false
	}
}

// --- Test protocols ---

// echoServer handles echoReq inline by replying with the same string.
type echoServer struct{}

func (echoServer) Start(ctx ProtocolContext) {
	RegisterRequestHandler(ctx, func(req *echoReq, r Responder[*echoRep]) {
		r.Reply(&echoRep{Got: req.Msg})
	})
}
func (echoServer) Init(_ ProtocolContext) {}

// asyncEchoServer captures the responder and replies on a background
// goroutine, exercising the async shape.
type asyncEchoServer struct{}

func (asyncEchoServer) Start(ctx ProtocolContext) {
	RegisterRequestHandler(ctx, func(req *echoReq, r Responder[*echoRep]) {
		go func() {
			time.Sleep(10 * time.Millisecond)
			r.Reply(&echoRep{Got: req.Msg})
		}()
	})
}
func (asyncEchoServer) Init(_ ProtocolContext) {}

// silentServer registers a handler that captures the responder and
// never replies. Used by the timeout test.
type silentServer struct {
	captured chan Responder[*echoRep]
}

func (s *silentServer) Start(ctx ProtocolContext) {
	RegisterRequestHandler(ctx, func(_ *echoReq, r Responder[*echoRep]) {
		s.captured <- r
	})
}
func (s *silentServer) Init(_ ProtocolContext) {}

// failingServer replies via Fail with a sentinel error.
var errServerBoom = errors.New("server boom")

type failingServer struct{}

func (failingServer) Start(ctx ProtocolContext) {
	RegisterRequestHandler(ctx, func(_ *echoReq, r Responder[*echoRep]) {
		r.Fail(errServerBoom)
	})
}
func (failingServer) Init(_ ProtocolContext) {}

// requestor sends one echoReq from Init and writes the reply into target.
type requestor struct {
	target *echoReplyResult
	msg    string
}

func (r *requestor) Start(_ ProtocolContext) {}
func (r *requestor) Init(ctx ProtocolContext) {
	SendRequest(ctx, &echoReq{Msg: r.msg}, func(rep *echoRep, err error) {
		r.target.set(rep, err)
	})
}

// timeoutRequestor is requestor with an explicit, short timeout.
type timeoutRequestor struct {
	target  *echoReplyResult
	timeout time.Duration
}

func (r *timeoutRequestor) Start(_ ProtocolContext) {}
func (r *timeoutRequestor) Init(ctx ProtocolContext) {
	SendRequestWithTimeout(ctx, &echoReq{Msg: "ignored"}, r.timeout,
		func(rep *echoRep, err error) {
			r.target.set(rep, err)
		})
}

// orphanRequestor sends a request type with no registered handler.
type orphanRequestor struct {
	gotErr chan error
}

func (r *orphanRequestor) Start(_ ProtocolContext) {}
func (r *orphanRequestor) Init(ctx ProtocolContext) {
	SendRequest(ctx, &orphanReq{}, func(_ *echoRep, err error) {
		r.gotErr <- err
	})
}

// selfHandler both handles echoReq and sends one to itself from Init.
type selfHandler struct {
	target *echoReplyResult
}

func (s *selfHandler) Start(ctx ProtocolContext) {
	RegisterRequestHandler(ctx, func(req *echoReq, r Responder[*echoRep]) {
		r.Reply(&echoRep{Got: req.Msg + "-self"})
	})
}
func (s *selfHandler) Init(ctx ProtocolContext) {
	SendRequest(ctx, &echoReq{Msg: "hi"}, func(rep *echoRep, err error) {
		s.target.set(rep, err)
	})
}

// notifSubscriber records every notification it receives.
type notifSubscriber struct {
	mu       sync.Mutex
	received []tickNotif
	gotOne   chan struct{}
	once     sync.Once
}

func newNotifSubscriber() *notifSubscriber {
	return &notifSubscriber{gotOne: make(chan struct{})}
}

func (s *notifSubscriber) Start(ctx ProtocolContext) {
	SubscribeNotification(ctx, func(n tickNotif) {
		s.mu.Lock()
		s.received = append(s.received, n)
		s.mu.Unlock()
		s.once.Do(func() { close(s.gotOne) })
	})
}
func (s *notifSubscriber) Init(_ ProtocolContext) {}
func (s *notifSubscriber) snapshot() []tickNotif {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]tickNotif, len(s.received))
	copy(out, s.received)
	return out
}

// unsubscriber subscribes in Start, unsubscribes in Init.
type unsubscriber struct {
	called atomic.Int32
}

func (u *unsubscriber) Start(ctx ProtocolContext) {
	SubscribeNotification(ctx, func(_ tickNotif) {
		u.called.Add(1)
	})
}
func (u *unsubscriber) Init(ctx ProtocolContext) {
	UnsubscribeNotification[tickNotif](ctx)
}

// publisher publishes a single notification from Init.
type publisher struct {
	tick int
}

func (p *publisher) Start(_ ProtocolContext) {}
func (p *publisher) Init(ctx ProtocolContext) {
	PublishNotification(ctx, tickNotif{Tick: p.tick})
}

// --- Tests ---

// TestIPC_RequestReply_HappyPath verifies a sync reply round-trip:
// requester sends, server replies inline, requester receives.
func TestIPC_RequestReply_HappyPath(t *testing.T) {
	result := newEchoReplyResult()
	startIPCRuntime(t, echoServer{}, &requestor{target: result, msg: "hello"})

	if !result.wait(2 * time.Second) {
		t.Fatalf("never received reply")
	}
	if result.err != nil {
		t.Fatalf("unexpected error: %v", result.err)
	}
	if result.rep == nil || result.rep.Got != "hello" {
		t.Fatalf("expected reply.Got=hello, got %+v", result.rep)
	}
}

// TestIPC_RequestReply_Async verifies the async shape: handler captures
// the responder and replies later from a background goroutine.
func TestIPC_RequestReply_Async(t *testing.T) {
	result := newEchoReplyResult()
	startIPCRuntime(t, asyncEchoServer{}, &requestor{target: result, msg: "later"})

	if !result.wait(2 * time.Second) {
		t.Fatalf("never received reply")
	}
	if result.err != nil {
		t.Fatalf("unexpected error: %v", result.err)
	}
	if result.rep.Got != "later" {
		t.Fatalf("expected reply.Got=later, got %q", result.rep.Got)
	}
}

// TestIPC_NoHandler verifies that SendRequest errors with
// ErrNoRequestHandler when no protocol registered for the request type.
func TestIPC_NoHandler(t *testing.T) {
	gotErr := make(chan error, 1)
	startIPCRuntime(t, &orphanRequestor{gotErr: gotErr})

	select {
	case err := <-gotErr:
		if !errors.Is(err, ErrNoRequestHandler) {
			t.Fatalf("expected ErrNoRequestHandler, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("never received error")
	}
}

// TestIPC_Timeout verifies that an unanswered request fires the
// timeout path with ErrRequestTimeout.
func TestIPC_Timeout(t *testing.T) {
	result := newEchoReplyResult()
	silent := &silentServer{captured: make(chan Responder[*echoRep], 1)}
	startIPCRuntime(t, silent, &timeoutRequestor{target: result, timeout: 50 * time.Millisecond})

	if !result.wait(2 * time.Second) {
		t.Fatalf("timeout never fired")
	}
	if !errors.Is(result.err, ErrRequestTimeout) {
		t.Fatalf("expected ErrRequestTimeout, got %v", result.err)
	}
	// Drain the captured responder so the silent server doesn't leak it.
	select {
	case <-silent.captured:
	default:
	}
}

// TestIPC_Timeout_LateReplyDropped verifies that if the responder
// finally replies after the timeout, the reply is silently dropped
// (no double-callback to the requester).
func TestIPC_Timeout_LateReplyDropped(t *testing.T) {
	result := newEchoReplyResult()
	silent := &silentServer{captured: make(chan Responder[*echoRep], 1)}
	startIPCRuntime(t, silent, &timeoutRequestor{target: result, timeout: 30 * time.Millisecond})

	// Wait for timeout to land.
	if !result.wait(2 * time.Second) {
		t.Fatalf("timeout never fired")
	}
	if !errors.Is(result.err, ErrRequestTimeout) {
		t.Fatalf("expected ErrRequestTimeout, got %v", result.err)
	}

	// Now have the captured responder reply. The framework must drop it.
	select {
	case r := <-silent.captured:
		r.Reply(&echoRep{Got: "too late"})
	case <-time.After(time.Second):
		t.Fatalf("server never captured the responder")
	}

	// If the framework double-called the requester's onReply, our
	// echoReplyResult would panic on close-of-closed-channel. Sleep a
	// beat to catch any racing late delivery.
	time.Sleep(50 * time.Millisecond)
}

// TestIPC_Fail verifies that responder.Fail wraps the user error in
// ErrResponderFailed.
func TestIPC_Fail(t *testing.T) {
	result := newEchoReplyResult()
	startIPCRuntime(t, failingServer{}, &requestor{target: result, msg: "boom"})

	if !result.wait(2 * time.Second) {
		t.Fatalf("never received error")
	}
	if !errors.Is(result.err, ErrResponderFailed) {
		t.Fatalf("expected ErrResponderFailed, got %v", result.err)
	}
	if !errors.Is(result.err, errServerBoom) {
		t.Fatalf("expected wrapped errServerBoom, got %v", result.err)
	}
}

// TestIPC_SelfRequest verifies a single protocol can request a type it
// itself handles.
func TestIPC_SelfRequest(t *testing.T) {
	result := newEchoReplyResult()
	startIPCRuntime(t, &selfHandler{target: result})

	if !result.wait(2 * time.Second) {
		t.Fatalf("self-request never completed")
	}
	if result.err != nil {
		t.Fatalf("unexpected error: %v", result.err)
	}
	if result.rep.Got != "hi-self" {
		t.Fatalf("expected reply.Got=hi-self, got %q", result.rep.Got)
	}
}

// TestIPC_Notification_Fanout verifies a single PublishNotification
// reaches every subscriber.
func TestIPC_Notification_Fanout(t *testing.T) {
	subA := newNotifSubscriber()
	subB := newNotifSubscriber()
	subC := newNotifSubscriber()
	startIPCRuntime(t, subA, subB, subC, &publisher{tick: 7})

	for i, s := range []*notifSubscriber{subA, subB, subC} {
		select {
		case <-s.gotOne:
		case <-time.After(2 * time.Second):
			t.Fatalf("subscriber %d never received the notification", i)
		}
		got := s.snapshot()
		if len(got) != 1 || got[0].Tick != 7 {
			t.Fatalf("subscriber %d got %+v, want one tick=7", i, got)
		}
	}
}

// TestIPC_Notification_Unsubscribe verifies that an unsubscribed
// protocol does not receive subsequent publishes.
func TestIPC_Notification_Unsubscribe(t *testing.T) {
	u := &unsubscriber{}
	sub := newNotifSubscriber()
	// publisher fires from its Init; both u.Init (which unsubscribes) and
	// sub.Start (which subscribes) run before any Init. So at publish
	// time u is unsubscribed and sub is subscribed.
	startIPCRuntime(t, u, sub, &publisher{tick: 1})

	select {
	case <-sub.gotOne:
	case <-time.After(2 * time.Second):
		t.Fatalf("subscriber never got the publish; wiring broken before assertion is meaningful")
	}

	// Give the runtime a moment in case u was about to receive (it shouldn't).
	time.Sleep(50 * time.Millisecond)

	if got := u.called.Load(); got != 0 {
		t.Fatalf("unsubscribed protocol still got %d notifications", got)
	}
}

// TestIPC_Notification_NoSubscribers verifies that publishing with no
// subscribers is a clean no-op.
func TestIPC_Notification_NoSubscribers(t *testing.T) {
	startIPCRuntime(t, &publisher{tick: 99})
	// No assertion beyond "doesn't panic / hang / leak goroutine".
	time.Sleep(20 * time.Millisecond)
}
