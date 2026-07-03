package protorun

import (
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/transport"
)

// selfEcho exercises Send-to-self: onTrigger sends a selfNote to the
// local Host and records markers around it so the test can assert the
// loopback's three contract points — fresh instance, correct routing,
// and FIFO ordering behind the sending handler.
type selfEcho struct {
	self transport.Host
	ctx  ProtocolContext

	sent *selfNote // the exact instance handed to Send

	order    chan string    // "trigger-done" then "self"
	received chan *selfNote // the instance the handler observed
}

type selfTrigger struct{ BaseMessage }
type selfNote struct {
	BaseMessage
	V uint64
}

func (p *selfEcho) Start(ctx ProtocolContext) {
	p.ctx = ctx
	Handle(ctx, p.onTrigger)
	Handle(ctx, p.onNote)
}
func (p *selfEcho) Init(ProtocolContext) {}

func (p *selfEcho) onTrigger(_ *selfTrigger, _ transport.Host) {
	p.sent = &selfNote{V: 42}
	if err := p.ctx.Send(p.sent, p.self); err != nil {
		panic(err)
	}
	// Mutate AFTER Send: the handler must still observe 42 (the loopback
	// decodes a fresh instance; it must not alias this struct).
	p.sent.V = 99
	p.order <- "trigger-done"
}

func (p *selfEcho) onNote(n *selfNote, from transport.Host) {
	if from != p.self {
		panic("self-delivered message must carry from=self")
	}
	p.order <- "self"
	p.received <- n
}

// TestSend_SelfDelivery covers the Send-to-self loopback contract:
// routed to the registering protocol, decoded fresh (no aliasing),
// delivered FIFO after the sending handler returns, from=self.
func TestSend_SelfDelivery(t *testing.T) {
	self := transport.NewHost(9100, "127.0.0.1")
	rt := New(self)
	_ = registerMockStack(rt, self)

	p := &selfEcho{
		self:     self,
		order:    make(chan string, 4),
		received: make(chan *selfNote, 1),
	}
	rt.Register(p)
	if err := rt.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(rt.Cancel)

	rt.processMessage(processFrame(t, WireID[*selfTrigger](), nil), transport.NewHost(1, "127.0.0.1"))

	waitOrder := func(want string) {
		t.Helper()
		select {
		case got := <-p.order:
			if got != want {
				t.Fatalf("event order: got %q, want %q", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for %q", want)
		}
	}
	// FIFO: the sending handler finishes before the self-message runs.
	waitOrder("trigger-done")
	waitOrder("self")

	got := <-p.received
	if got == p.sent {
		t.Fatalf("handler received the sender's instance; loopback must decode a fresh one")
	}
	if got.V != 42 {
		t.Fatalf("handler observed V=%d; want 42 (post-Send mutation of the sent struct leaked through)", got.V)
	}
}
