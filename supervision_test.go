package protorun

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/transport"
)

// --- Shared supervision test fixtures ---

// supProbe is the observation surface shared across a factory's
// instances. Every channel is buffered so a chatty protocol never
// blocks its event loop on a slow test goroutine.
type supProbe struct {
	mu        sync.Mutex
	instances int
	nextID    int

	startedCh   chan int     // Init reached (instance id)
	restartedCh chan int     // OnRestart (attempt)
	connectedCh chan connSig // OnSessionConnected (instance id + host)
	okCh        chan int     // okMsg handled (instance id)
	timerCh     chan int     // one-shot timer fired (instance id)
}

type connSig struct {
	id   int
	host transport.Host
}

func newSupProbe() *supProbe {
	return &supProbe{
		startedCh:   make(chan int, 32),
		restartedCh: make(chan int, 32),
		connectedCh: make(chan connSig, 32),
		okCh:        make(chan int, 32),
		timerCh:     make(chan int, 32),
	}
}

func (pr *supProbe) register(p *supProto) {
	pr.mu.Lock()
	pr.instances++
	pr.nextID++
	p.id = pr.nextID
	pr.mu.Unlock()
}

func (pr *supProbe) instanceCount() int {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	return pr.instances
}

// supProto is the supervised protocol under test. onPoison panics (the
// crash trigger); onOK records that the live instance handled a benign
// message; the session/restart hooks feed the probe.
type supProto struct {
	probe         *supProbe
	id            int
	panicOnInit   bool
	scheduleTimer bool
}

func (p *supProto) Start(ctx ProtocolContext) {
	RegisterCodec(ctx, poisonCodec{})
	RegisterHandler(ctx, p.onPoison)
	RegisterCodec(ctx, okCodec{})
	RegisterHandler(ctx, p.onOK)
}

func (p *supProto) Init(ctx ProtocolContext) {
	if p.panicOnInit {
		panic("init boom")
	}
	if p.scheduleTimer {
		id := p.id
		ctx.After(100*time.Millisecond, func() { send(p.probe.timerCh, id) })
	}
	send(p.probe.startedCh, p.id)
}

func (p *supProto) onPoison(_ *poisonMsg, _ transport.Host) { panic("poison") }
func (p *supProto) onOK(_ *okMsg, _ transport.Host)         { send(p.probe.okCh, p.id) }

func (p *supProto) OnSessionConnected(h transport.Host) {
	send(p.probe.connectedCh, connSig{id: p.id, host: h})
}
func (p *supProto) OnSessionDisconnected(transport.Host) {}
func (p *supProto) OnRestart(attempt int)                { send(p.probe.restartedCh, attempt) }

// supFactory returns a factory that registers each new instance with
// the probe (bumping the instance counter and assigning an id).
func supFactory(pr *supProbe, panicOnInit, scheduleTimer bool) func() Protocol {
	return func() Protocol {
		p := &supProto{probe: pr, panicOnInit: panicOnInit, scheduleTimer: scheduleTimer}
		pr.register(p)
		return p
	}
}

type poisonMsg struct{ BaseMessage }
type okMsg struct{ BaseMessage }

type poisonCodec struct{}

func (poisonCodec) Marshal(*poisonMsg) ([]byte, error)   { return nil, nil }
func (poisonCodec) Unmarshal([]byte) (*poisonMsg, error) { return &poisonMsg{}, nil }

type okCodec struct{}

func (okCodec) Marshal(*okMsg) ([]byte, error)   { return nil, nil }
func (okCodec) Unmarshal([]byte) (*okMsg, error) { return &okMsg{}, nil }

// send pushes v onto a buffered probe channel without ever blocking.
func send[T any](ch chan T, v T) {
	select {
	case ch <- v:
	default:
	}
}

func waitInt(t *testing.T, ch chan int, d time.Duration) int {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(d):
		t.Fatalf("timed out waiting on int channel")
		return 0
	}
}

func waitConn(t *testing.T, ch chan connSig, d time.Duration) connSig {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(d):
		t.Fatalf("timed out waiting on conn channel")
		return connSig{}
	}
}

// spamAdvance drives a manualClock forward continuously so that
// clock-based backoff waits complete without real sleeping. The
// returned func stops the driver and blocks until it exits (goleak).
func spamAdvance(clock *manualClock) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				clock.Advance(time.Second)
				time.Sleep(time.Millisecond)
			}
		}
	}()
	return func() {
		close(stop)
		<-done
	}
}

// --- Tests ---

// TestSupervision_RestartHappyPath drives the full restart contract: a
// poison message panics the handler, the supervisor rebuilds a fresh
// instance, old timers are cancelled, established peers are replayed,
// OnRestart fires on the new instance, and the new instance handles
// subsequent messages.
func TestSupervision_RestartHappyPath(t *testing.T) {
	self := transport.NewHost(0, "127.0.0.1")
	pr := newSupProbe()
	clock := newManualClock()

	rt := New(self, WithClock(clock))
	_ = registerMockStack(rt, self)
	rt.RegisterFactory(supFactory(pr, false, true), WithSupervision(Supervision{
		OnPanic: Restart,
		Backoff: func(int) time.Duration { return 0 },
	}))
	if err := rt.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(rt.Cancel)

	// Instance 1 up.
	if id := waitInt(t, pr.startedCh, time.Second); id != 1 {
		t.Fatalf("first Init on instance %d, want 1", id)
	}

	// Establish a peer; instance 1 sees it.
	peer := transport.NewHost(5001, "127.0.0.1")
	if !rt.dispatchSessionEvent(context.Background(), transport.NewSessionConnected(peer)) {
		t.Fatalf("dispatchSessionEvent reported cancellation")
	}
	if c := waitConn(t, pr.connectedCh, time.Second); c.id != 1 || c.host != peer {
		t.Fatalf("OnSessionConnected on instance %d host %v, want 1 %v", c.id, c.host, peer)
	}

	// Poison the handler → restart.
	rt.processMessage(processFrame(t, WireID[*poisonMsg](), nil), peer)

	// Session replay reaches the fresh instance...
	if c := waitConn(t, pr.connectedCh, 2*time.Second); c.id != 2 || c.host != peer {
		t.Fatalf("replay OnSessionConnected on instance %d host %v, want 2 %v", c.id, c.host, peer)
	}
	// ...and OnRestart fires with attempt 1.
	if a := waitInt(t, pr.restartedCh, 2*time.Second); a != 1 {
		t.Fatalf("OnRestart attempt %d, want 1", a)
	}
	if got := pr.instanceCount(); got != 2 {
		t.Fatalf("instance count %d, want 2 (one restart)", got)
	}

	// The old instance's one-shot timer was cancelled; only the fresh
	// instance's timer should fire when the clock advances past it.
	clock.Advance(200 * time.Millisecond)
	if id := waitInt(t, pr.timerCh, time.Second); id != 2 {
		t.Fatalf("timer fired for instance %d, want only the fresh instance 2", id)
	}

	// The fresh instance is live and handles subsequent messages.
	rt.processMessage(processFrame(t, WireID[*okMsg](), nil), peer)
	if id := waitInt(t, pr.okCh, 2*time.Second); id != 2 {
		t.Fatalf("okMsg handled by instance %d, want 2", id)
	}
}

// TestSupervision_PendingRequestFails verifies that a SendRequest still
// outstanding when its protocol restarts fails with ErrProtocolRestarting
// rather than hanging until its timeout.
func TestSupervision_PendingRequestFails(t *testing.T) {
	self := transport.NewHost(0, "127.0.0.1")
	rt := New(self)
	_ = registerMockStack(rt, self)

	rt.Register(&silentSrv{captured: make(chan Responder[*slowRep], 4)})

	result := make(chan error, 4)
	rt.RegisterFactory(func() Protocol { return &reqProto{result: result} },
		WithSupervision(Supervision{OnPanic: Restart, Backoff: func(int) time.Duration { return 0 }}))

	if err := rt.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(rt.Cancel)

	// Init issued a request that will never be answered; poison the
	// requester so its restart auto-fails the pending call.
	rt.processMessage(processFrame(t, WireID[*poisonMsg](), nil), transport.NewHost(1, "127.0.0.1"))

	select {
	case err := <-result:
		if !errors.Is(err, ErrProtocolRestarting) {
			t.Fatalf("pending request failed with %v, want ErrProtocolRestarting", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("pending request was never failed")
	}
}

type reqProto struct {
	result chan error
}

func (p *reqProto) Start(ctx ProtocolContext) {
	RegisterCodec(ctx, poisonCodec{})
	RegisterHandler(ctx, func(_ *poisonMsg, _ transport.Host) { panic("poison") })
}
func (p *reqProto) Init(ctx ProtocolContext) {
	SendRequestWithTimeout(ctx, &slowReq{}, 10*time.Second, func(_ *slowRep, err error) {
		p.result <- err
	})
}

// slowReq / slowRep (declared in request_timeout_test.go) are an IPC
// pair whose handler never replies, reused here so the pending call is
// still outstanding when the requester restarts.
type silentSrv struct{ captured chan Responder[*slowRep] }

func (s *silentSrv) Start(ctx ProtocolContext) {
	RegisterRequestHandler(ctx, func(_ *slowReq, r Responder[*slowRep]) {
		send(s.captured, r) // hold the responder; never reply
	})
}
func (s *silentSrv) Init(ProtocolContext) {}

// TestSupervision_PoisonNotRedeliveredAndDeadLetters verifies two
// things at once: the poison message is not redelivered to the fresh
// instance (no crash loop), and events arriving while the protocol is
// quarantined go to the dead-letter hook instead of blocking the
// producer.
func TestSupervision_PoisonNotRedeliveredAndDeadLetters(t *testing.T) {
	self := transport.NewHost(0, "127.0.0.1")
	pr := newSupProbe()

	var dlmu sync.Mutex
	var deadLetters []DeadLetter

	var rt *Runtime
	gate := make(chan struct{})
	// A backoff that parks the supervisor mid-restart (quarantine
	// active) until the test releases the gate, so the dead-letter
	// window is deterministic. Selects on the runtime context so
	// shutdown can never deadlock it.
	backoff := func(int) time.Duration {
		select {
		case <-gate:
		case <-rt.ctx.Done():
		}
		return 0
	}

	rt = New(self, WithDeadLetter(func(dl DeadLetter) {
		dlmu.Lock()
		deadLetters = append(deadLetters, dl)
		dlmu.Unlock()
	}))
	_ = registerMockStack(rt, self)
	rt.RegisterFactory(supFactory(pr, false, false),
		WithSupervision(Supervision{OnPanic: Restart, Backoff: backoff}))
	if err := rt.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(rt.Cancel)

	proto := rt.protocols[0]
	waitInt(t, pr.startedCh, time.Second) // instance 1 up

	peer := transport.NewHost(5002, "127.0.0.1")
	rt.processMessage(processFrame(t, WireID[*poisonMsg](), nil), peer) // → panic → quarantine

	// Wait until the protocol is quarantined (supervisor parked in
	// backoff), then inject an event and confirm it dead-letters.
	deadline := time.Now().Add(2 * time.Second)
	for !proto.quarantined.Load() {
		if time.Now().After(deadline) {
			t.Fatalf("protocol never entered quarantine")
		}
		time.Sleep(time.Millisecond)
	}
	other := transport.NewHost(5003, "127.0.0.1")
	if !rt.dispatchSessionEvent(context.Background(), transport.NewSessionConnected(other)) {
		t.Fatalf("dispatchSessionEvent reported cancellation")
	}

	dlmu.Lock()
	sawSession := false
	for _, dl := range deadLetters {
		if dl.Kind == "session" {
			sawSession = true
		}
	}
	dlmu.Unlock()
	if !sawSession {
		t.Fatalf("expected a session event to dead-letter during quarantine, got %v", deadLetters)
	}

	// Release the restart.
	close(gate)

	if id := waitInt(t, pr.startedCh, 2*time.Second); id != 2 {
		t.Fatalf("fresh instance Init on %d, want 2", id)
	}
	// No crash loop: the poison was not redelivered, so no third instance.
	rt.processMessage(processFrame(t, WireID[*okMsg](), nil), peer)
	if id := waitInt(t, pr.okCh, 2*time.Second); id != 2 {
		t.Fatalf("okMsg handled by instance %d, want 2", id)
	}
	if got := pr.instanceCount(); got != 2 {
		t.Fatalf("instance count %d, want 2 (no crash loop)", got)
	}
}

// TestSupervision_BudgetEscalate verifies that a protocol panicking in
// Init restarts MaxRestarts times and then, with OnGiveUp: Escalate,
// makes Run return an ErrProtocolFailed-wrapped error.
func TestSupervision_BudgetEscalate(t *testing.T) {
	self := transport.NewHost(0, "127.0.0.1")
	pr := newSupProbe()
	clock := newManualClock()

	rt := New(self, WithClock(clock))
	_ = registerMockStack(rt, self)
	rt.RegisterFactory(supFactory(pr, true, false), WithSupervision(Supervision{
		OnPanic:     Restart,
		MaxRestarts: 3,
		Window:      time.Hour,
		Backoff:     func(int) time.Duration { return 10 * time.Millisecond },
		OnGiveUp:    Escalate,
	}))

	stopClock := spamAdvance(clock)
	defer stopClock()

	done := make(chan error, 1)
	go func() { done <- rt.Run() }()

	select {
	case err := <-done:
		if !errors.Is(err, ErrProtocolFailed) {
			t.Fatalf("Run returned %v, want ErrProtocolFailed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Run did not return after escalation")
	}

	// One boot instance + three restart rebuilds, all panicking in Init.
	if got := pr.instanceCount(); got != 4 {
		t.Fatalf("instance count %d, want 4 (1 boot + 3 restarts)", got)
	}
}

// TestSupervision_BudgetStop verifies that OnGiveUp: Stop removes the
// protocol after the budget is exhausted, and that a sibling learns of
// it via a ProtocolFailed notification.
func TestSupervision_BudgetStop(t *testing.T) {
	self := transport.NewHost(0, "127.0.0.1")
	pr := newSupProbe()
	clock := newManualClock()

	rt := New(self, WithClock(clock))
	_ = registerMockStack(rt, self)

	watcher := &failWatcher{ch: make(chan ProtocolFailed, 8)}
	rt.Register(watcher)
	rt.RegisterFactory(supFactory(pr, true, false), WithSupervision(Supervision{
		OnPanic:     Restart,
		MaxRestarts: 2,
		Window:      time.Hour,
		Backoff:     func(int) time.Duration { return 10 * time.Millisecond },
		OnGiveUp:    Stop,
	}))
	proto := rt.protocols[1]

	if err := rt.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(rt.Cancel)

	stopClock := spamAdvance(clock)
	defer stopClock()

	select {
	case pf := <-watcher.ch:
		if pf.Outcome != "stopped" {
			t.Fatalf("ProtocolFailed outcome %q, want stopped", pf.Outcome)
		}
		if pf.Protocol != proto.name {
			t.Fatalf("ProtocolFailed protocol %q, want %q", pf.Protocol, proto.name)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("sibling never saw the stop notification")
	}

	if got := pr.instanceCount(); got != 3 {
		t.Fatalf("instance count %d, want 3 (1 boot + 2 restarts)", got)
	}

	// The protocol was removed from the live set (only the watcher left).
	protos := rt.snapshotProtocols()
	if len(protos) != 1 || protos[0] != watcher.protoOf(rt) {
		t.Fatalf("expected the stopped protocol to be removed, live set = %d", len(protos))
	}
}

type failWatcher struct{ ch chan ProtocolFailed }

func (w *failWatcher) Start(ctx ProtocolContext) {
	SubscribeNotification(ctx, func(pf ProtocolFailed) { send(w.ch, pf) })
}
func (w *failWatcher) Init(ProtocolContext) {}

// protoOf returns the envelope wrapping w, for the "still registered"
// assertion.
func (w *failWatcher) protoOf(rt *Runtime) *protoProtocol {
	for _, p := range rt.snapshotProtocols() {
		if p.protocol == w {
			return p
		}
	}
	return nil
}

// TestSupervision_SiblingSeesRestart verifies that a successful restart
// publishes a ProtocolFailed{Outcome: "restarted"} notification that a
// sibling can subscribe to.
func TestSupervision_SiblingSeesRestart(t *testing.T) {
	self := transport.NewHost(0, "127.0.0.1")
	pr := newSupProbe()

	rt := New(self)
	_ = registerMockStack(rt, self)

	watcher := &failWatcher{ch: make(chan ProtocolFailed, 8)}
	rt.Register(watcher)
	rt.RegisterFactory(supFactory(pr, false, false),
		WithSupervision(Supervision{OnPanic: Restart, Backoff: func(int) time.Duration { return 0 }}))
	proto := rt.protocols[1]

	if err := rt.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(rt.Cancel)

	waitInt(t, pr.startedCh, time.Second)
	rt.processMessage(processFrame(t, WireID[*poisonMsg](), nil), transport.NewHost(1, "127.0.0.1"))

	select {
	case pf := <-watcher.ch:
		if pf.Outcome != "restarted" {
			t.Fatalf("ProtocolFailed outcome %q, want restarted", pf.Outcome)
		}
		if pf.Protocol != proto.name {
			t.Fatalf("ProtocolFailed protocol %q, want %q", pf.Protocol, proto.name)
		}
		if pf.Attempt != 1 {
			t.Fatalf("ProtocolFailed attempt %d, want 1", pf.Attempt)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("sibling never saw the restart notification")
	}
}

// TestSupervision_RestartOnSingleton_StrictPanics verifies that
// configuring Restart on a plain Register panics at registration time
// in strict mode.
func TestSupervision_RestartOnSingleton_StrictPanics(t *testing.T) {
	self := transport.NewHost(0, "127.0.0.1")
	rt := New(self, WithStrict(true))

	defer func() {
		if recover() == nil {
			t.Fatalf("expected Register to panic on Restart-without-factory in strict mode")
		}
	}()
	rt.Register(&MockProtocol{}, WithSupervision(Supervision{OnPanic: Restart}))
}

// TestSupervision_RestartOnSingleton_NonStrictDowngrades verifies that
// the same misconfiguration downgrades to Resume (no supervisor) in
// non-strict mode instead of panicking.
func TestSupervision_RestartOnSingleton_NonStrictDowngrades(t *testing.T) {
	self := transport.NewHost(0, "127.0.0.1")
	rt := New(self)

	rt.Register(&MockProtocol{}, WithSupervision(Supervision{OnPanic: Restart}))
	if rt.protocols[0].supervisor != nil {
		t.Fatalf("expected Restart-without-factory to downgrade to Resume (no supervisor)")
	}
}

// panicReqSrv is a supervised protocol whose request handler always
// panics. Its Init reports the live instance id so tests can observe
// the rebuild.
type panicReqSrv struct {
	id      int
	started chan int
}

func (s *panicReqSrv) Start(ctx ProtocolContext) {
	RegisterRequestHandler(ctx, func(_ *slowReq, _ Responder[*slowRep]) { panic("request boom") })
}
func (s *panicReqSrv) Init(ProtocolContext) { send(s.started, s.id) }

// reqAsker fires one request at Init and reports the callback error.
type reqAsker struct{ result chan error }

func (a *reqAsker) Start(ProtocolContext) {}
func (a *reqAsker) Init(ctx ProtocolContext) {
	SendRequestWithTimeout(ctx, &slowReq{}, 10*time.Second, func(_ *slowRep, err error) {
		a.result <- err
	})
}

// TestSupervision_RequestHandlerPanicRestarts guards the hand-off from
// the request-handler recover to the supervisor. That recover runs
// before safeCall can see the panic (it must auto-fail the responder
// first), so it has to forward the panic explicitly — otherwise a
// supervised protocol would keep running with half-mutated state after
// a request-handler panic. The requester must see ErrHandlerPanicked
// AND the supervised server must be rebuilt.
func TestSupervision_RequestHandlerPanicRestarts(t *testing.T) {
	self := transport.NewHost(0, "127.0.0.1")
	rt := New(self)
	_ = registerMockStack(rt, self)

	started := make(chan int, 4)
	var instances atomic.Int32
	rt.RegisterFactory(func() Protocol {
		return &panicReqSrv{id: int(instances.Add(1)), started: started}
	}, WithSupervision(Supervision{
		OnPanic: Restart,
		Backoff: func(int) time.Duration { return 0 },
	}))

	result := make(chan error, 4)
	rt.Register(&reqAsker{result: result})

	if err := rt.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(rt.Cancel)

	if id := waitInt(t, started, time.Second); id != 1 {
		t.Fatalf("first Init on instance %d, want 1", id)
	}

	// The asker's Init request hits the panicking handler: the
	// responder is auto-failed...
	select {
	case err := <-result:
		if !errors.Is(err, ErrHandlerPanicked) {
			t.Fatalf("request failed with %v, want ErrHandlerPanicked", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("request callback never invoked")
	}

	// ...and the panic still reaches the supervisor: a fresh instance
	// comes up.
	if id := waitInt(t, started, 2*time.Second); id != 2 {
		t.Fatalf("rebuilt Init on instance %d, want 2", id)
	}
	if got := int(instances.Load()); got != 2 {
		t.Fatalf("instance count %d, want 2 (one restart)", got)
	}
}
