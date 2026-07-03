package protorun

import (
	"sync"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/transport"
)

// recordingTracer captures every TraceEvent for assertions. Safe for
// concurrent use because the runtime emits from many goroutines.
type recordingTracer struct {
	mu     sync.Mutex
	events []TraceEvent
}

func (t *recordingTracer) Trace(ev TraceEvent) {
	t.mu.Lock()
	t.events = append(t.events, ev)
	t.mu.Unlock()
}

func (t *recordingTracer) kinds() map[string]int {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := map[string]int{}
	for _, ev := range t.events {
		out[ev.Kind]++
	}
	return out
}

// tracerProbe is a protocol that registers a codec+handler for
// localMessage and, on Init, sends one message to self so the runtime
// runs both its send and deliver trace emit points.
type tracerProbe struct {
	self transport.Host
	got  chan struct{}
}

func (p *tracerProbe) Start(ctx ProtocolContext) {
	RegisterCodec(ctx, localCodec{})
	RegisterHandler(ctx, func(*localMessage, transport.Host) {
		select {
		case p.got <- struct{}{}:
		default:
		}
	})
}

func (p *tracerProbe) Init(ctx ProtocolContext) {
	_ = ctx.Send(&localMessage{}, p.self)
}

// TestTracer_RecordsSendAndDeliver installs a recording tracer and drives
// a self-send through a real runtime, asserting the tracer saw both the
// "send" and the "deliver" event with the local host as the peer.
func TestTracer_RecordsSendAndDeliver(t *testing.T) {
	tr := &recordingTracer{}
	self := transport.NewHost(0, "127.0.0.1")
	rt := New(self, WithTracer(tr))
	_ = registerMockStack(rt, self)

	probe := &tracerProbe{self: self, got: make(chan struct{}, 1)}
	rt.Register(probe)

	if err := rt.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer rt.Cancel()

	select {
	case <-probe.got:
	case <-time.After(time.Second):
		t.Fatal("self-message never delivered")
	}

	kinds := tr.kinds()
	if kinds["send"] != 1 {
		t.Errorf("send events: got %d, want 1", kinds["send"])
	}
	if kinds["deliver"] != 1 {
		t.Errorf("deliver events: got %d, want 1", kinds["deliver"])
	}
}

// TestTracer_RecordsSessionEvents drives a session-connected event
// through the runtime's dispatch path and asserts the tracer observed the
// live "session-connected" kind carrying the peer.
func TestTracer_RecordsSessionEvents(t *testing.T) {
	tr := &recordingTracer{}
	self := transport.NewHost(0, "127.0.0.1")
	rt := New(self, WithTracer(tr))
	_ = registerMockStack(rt, self)
	rt.Register(&MockProtocol{})

	if err := rt.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer rt.Cancel()

	peer := transport.NewHost(9999, "127.0.0.1")
	if !rt.dispatchSessionEvent(rt.ctx, transport.NewSessionConnected(peer)) {
		t.Fatal("dispatchSessionEvent returned false")
	}

	found := false
	for range 50 {
		if tr.kinds()["session-connected"] == 1 {
			found = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !found {
		t.Errorf("session-connected events: got %d, want 1", tr.kinds()["session-connected"])
	}
}

// TestTracer_NoneWhenNotInstalled proves the emit sites are silent — and,
// with no WithTracer option, tracerEnabled stays false — so a run without
// a tracer produces no events at all.
func TestTracer_NoneWhenNotInstalled(t *testing.T) {
	self := transport.NewHost(0, "127.0.0.1")
	rt := New(self)
	if rt.tracerEnabled {
		t.Fatal("tracerEnabled should be false with no WithTracer option")
	}
	_ = registerMockStack(rt, self)

	probe := &tracerProbe{self: self, got: make(chan struct{}, 1)}
	rt.Register(probe)

	if err := rt.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer rt.Cancel()

	select {
	case <-probe.got:
	case <-time.After(time.Second):
		t.Fatal("self-message never delivered")
	}
	// Nothing to assert on a tracer we never installed; the point is the
	// run above must not panic on a nil tracer, which the guards ensure.
}
