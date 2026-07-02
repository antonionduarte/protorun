package protorun

import (
	"context"
	"sync/atomic"

	"github.com/antonionduarte/go-simple-protocol-runtime/pkg/transport"
)

// registerMockStack wires rt with the standard in-package fixture: a
// MockNetworkLayer wrapped in a real SessionLayer bound to self. This
// is the one place that knows how a test runtime's transport stack is
// assembled. (Out-of-package protocol tests should use pkg/prototest
// instead; this package can't import it without a cycle.)
func registerMockStack(rt *Runtime, self transport.Host) *MockNetworkLayer {
	mock := NewMockNetworkLayer()
	rt.registerNetworkLayer(mock)
	rt.registerSessionLayer(transport.NewSessionLayer(mock, self, context.Background(), 0, 0))
	return mock
}

// MockProtocol is a stub Protocol used in tests. It does no work itself;
// tests mostly care that Start/Init were called and that the protocol
// can be wrapped, registered, and torn down through the runtime APIs.
type MockProtocol struct {
	StartCalled bool
	InitCalled  bool

	HandledMessages []Message
}

func (m *MockProtocol) Start(_ ProtocolContext) { m.StartCalled = true }
func (m *MockProtocol) Init(_ ProtocolContext)  { m.InitCalled = true }

func (m *MockProtocol) RecordMessageHandler(msg Message) {
	m.HandledMessages = append(m.HandledMessages, msg)
}

type MockNetworkLayer struct {
	ConnectCalled    bool
	DisconnectCalled bool
	SendCalled       bool
	CancelCalled     bool

	outChannel chan transport.Message
	outEvents  chan transport.Event
}

func NewMockNetworkLayer() *MockNetworkLayer {
	return &MockNetworkLayer{
		outChannel: make(chan transport.Message, 1),
		outEvents:  make(chan transport.Event, 1),
	}
}

func (m *MockNetworkLayer) Connect(_ transport.Host)                   { m.ConnectCalled = true }
func (m *MockNetworkLayer) Disconnect(_ transport.Host)                { m.DisconnectCalled = true }
func (m *MockNetworkLayer) Send(_ transport.Message, _ transport.Host) { m.SendCalled = true }
func (m *MockNetworkLayer) OutChannel() chan transport.Message         { return m.outChannel }
func (m *MockNetworkLayer) OutEvents() chan transport.Event            { return m.outEvents }
func (m *MockNetworkLayer) Cancel()                                    { m.CancelCalled = true }

// localMessage is the canonical test message: just a sender, no payload.
// It uses BaseMessage so Sender/SetSender are inherited.
type localMessage struct {
	BaseMessage
}

var _ Message = (*localMessage)(nil)

// localCodec is a no-payload Codec[*localMessage] used by tests that don't
// care about message contents, only routing.
type localCodec struct{}

func (localCodec) Marshal(_ *localMessage) ([]byte, error)   { return nil, nil }
func (localCodec) Unmarshal(_ []byte) (*localMessage, error) { return &localMessage{}, nil }

// failingCodec returns the supplied error from Encode and Decode. Used to
// test that the runtime propagates codec errors instead of swallowing them.
type failingCodec struct{}

func (failingCodec) Marshal(_ *failingMessageBM) ([]byte, error)   { return nil, assertError{} }
func (failingCodec) Unmarshal(_ []byte) (*failingMessageBM, error) { return nil, assertError{} }

type failingMessageBM struct{ BaseMessage }

// assertError is a sentinel error used by failing test codecs.
type assertError struct{}

func (assertError) Error() string { return "expected codec error" }

// twoSidedProtocol is used by the two-runtime integration test. It
// connects to its peer on Init, replies once on inbound, and records
// receipt for the test to observe.
type twoSidedProtocol struct {
	Peer transport.Host

	ctx      ProtocolContext
	received atomic.Bool
}

func (p *twoSidedProtocol) Start(ctx ProtocolContext) {
	p.ctx = ctx
	RegisterCodec(ctx, localCodec{})
	RegisterHandler(ctx, p.handle)
}

func (p *twoSidedProtocol) Init(ctx ProtocolContext) {
	p.ctx = ctx
	ctx.Connect(p.Peer)
}

func (p *twoSidedProtocol) OnSessionConnected(h transport.Host) {
	if h != p.Peer || p.ctx == nil {
		return
	}
	_ = p.ctx.Send(&localMessage{}, p.Peer)
}

func (p *twoSidedProtocol) OnSessionDisconnected(_ transport.Host) {}

func (p *twoSidedProtocol) handle(_ *localMessage, _ transport.Host) {
	p.received.Store(true)
}
