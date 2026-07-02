package protorun

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/antonionduarte/go-simple-protocol-runtime/pkg/transport"
)

// TestProtocolContext_NarrowInterfaces is a compile-time check that
// ProtocolContext satisfies each capability interface. If someone
// removes a method from one of the splits, this stops compiling.
func TestProtocolContext_NarrowInterfaces(t *testing.T) {
	t.Helper()

	// Compile-time assertions.
	var _ Connector = (*protocolContext)(nil)
	var _ Sender = (*protocolContext)(nil)
	var _ Timing = (*protocolContext)(nil)
	var _ Identity = (*protocolContext)(nil)
	var _ ProtocolContext = (*protocolContext)(nil)

	// And the inverse: a function declared in terms of a single
	// capability accepts a full ProtocolContext.
	use := func(c Connector) { _ = c }
	rt := New(transport.NewHost(0, "127.0.0.1"))
	_ = registerMockStack(rt, rt.self)
	defer rt.Cancel()

	proto := newProtoProtocol(&MockProtocol{}, 0)
	rt.registerProtocol(proto)
	proto.ensureContext()
	use(proto.ctx)
}

// strictPanicProtocol is a generic strict-misuse Protocol whose
// Start/Init bodies are caller-supplied. Lets each strict test
// declare exactly the misuse it wants to provoke.
type strictPanicProtocol struct {
	startBody func(ctx ProtocolContext)
	initBody  func(ctx ProtocolContext)
}

func (p *strictPanicProtocol) Start(ctx ProtocolContext) {
	if p.startBody != nil {
		p.startBody(ctx)
	}
}
func (p *strictPanicProtocol) Init(ctx ProtocolContext) {
	if p.initBody != nil {
		p.initBody(ctx)
	}
}

// runStrict starts a runtime in strict mode, with mock transport,
// registering proto. Returns any panic recovered during start as a
// string. The strict-mode panics this test exercises happen synchronously
// inside protocol.Start / protocol.Init, so they bubble up through
// Runtime.start (which is called on the caller's goroutine).
//
// rt.Cancel is deferred *before* the recover so cleanup runs even when
// the strict-mode invariant fires. Defers are LIFO: recover catches
// the panic, then Cancel tears down the SessionLayer goroutines.
func runStrict(t *testing.T, proto Protocol) (panicked string) {
	t.Helper()

	self := transport.NewHost(0, "127.0.0.1")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt := New(self, WithStrict(true))
	mock := NewMockNetworkLayer()
	rt.registerNetworkLayer(mock)
	rt.registerSessionLayer(transport.NewSessionLayer(mock, self, ctx, 0, 0))
	rt.Register(proto)

	defer rt.Cancel()
	defer func() {
		if rec := recover(); rec != nil {
			panicked, _ = rec.(string)
		}
	}()

	if err := rt.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	return ""
}

// expectStrictPanic runs the supplied protocol and asserts a strict-
// mode panic with the given substring.
func expectStrictPanic(t *testing.T, proto Protocol, wantSubstr string) {
	t.Helper()
	got := runStrict(t, proto)
	if got == "" {
		t.Fatalf("expected strict panic containing %q, got no panic", wantSubstr)
	}
	if !strings.Contains(got, wantSubstr) {
		t.Fatalf("expected strict panic containing %q, got %q", wantSubstr, got)
	}
}

// TestStrict_DoubleRegistration_Codec verifies registering the same
// codec twice on the same protocol panics in strict mode.
func TestStrict_DoubleRegistration_Codec(t *testing.T) {
	expectStrictPanic(t, &strictPanicProtocol{
		startBody: func(ctx ProtocolContext) {
			RegisterCodec(ctx, BinaryCodec[*benchMsg]{})
			RegisterCodec(ctx, BinaryCodec[*benchMsg]{})
		},
	}, "RegisterCodec for wireID")
}

// TestStrict_DoubleRegistration_Handler verifies registering the same
// handler twice panics in strict mode.
func TestStrict_DoubleRegistration_Handler(t *testing.T) {
	expectStrictPanic(t, &strictPanicProtocol{
		startBody: func(ctx ProtocolContext) {
			RegisterCodec(ctx, BinaryCodec[*benchMsg]{})
			RegisterHandler(ctx, func(_ *benchMsg, _ transport.Host) {})
			RegisterHandler(ctx, func(_ *benchMsg, _ transport.Host) {})
		},
	}, "RegisterHandler for wireID")
}

// TestStrict_PhaseOrdering_RegisterInInit verifies registering a
// codec from Init (instead of Start) panics in strict mode.
func TestStrict_PhaseOrdering_RegisterInInit(t *testing.T) {
	expectStrictPanic(t, &strictPanicProtocol{
		initBody: func(ctx ProtocolContext) {
			RegisterCodec(ctx, BinaryCodec[*benchMsg]{})
		},
	}, "RegisterCodec must be called from Start")
}

// TestStrict_PhaseOrdering_ConnectInStart verifies calling Connect
// from Start (instead of Init or later) panics in strict mode.
func TestStrict_PhaseOrdering_ConnectInStart(t *testing.T) {
	expectStrictPanic(t, &strictPanicProtocol{
		startBody: func(ctx ProtocolContext) {
			_ = ctx.Connect(transport.NewHost(0, "127.0.0.1"))
		},
	}, "Connect must be called from Init")
}

// TestStrict_Disabled_NoPanic verifies the same misuse that panics
// in strict mode is a quiet no-op (just a counter increment) when
// strict is off.
func TestStrict_Disabled_NoPanic(t *testing.T) {
	defer func() {
		if rec := recover(); rec != nil {
			t.Fatalf("expected no panic with strict off, got %v", rec)
		}
	}()

	self := transport.NewHost(0, "127.0.0.1")
	rt := New(self) // no WithStrict
	_ = registerMockStack(rt, self)
	rt.Register(&strictPanicProtocol{
		startBody: func(ctx ProtocolContext) {
			RegisterCodec(ctx, BinaryCodec[*benchMsg]{})
			RegisterCodec(ctx, BinaryCodec[*benchMsg]{}) // double, would panic in strict mode
		},
	})

	if err := rt.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	rt.Cancel()
}

// TestStrict_SlowHandlerWatchdog_Logs verifies the watchdog fires a
// counter when a handler exceeds the configured threshold.
func TestStrict_SlowHandlerWatchdog_Logs(t *testing.T) {
	rec := newRecordingMetrics()
	self := transport.NewHost(0, "127.0.0.1")
	rt := New(self,
		WithStrict(true),
		WithStrictHandlerTimeout(50*time.Millisecond),
		WithMetrics(rec),
	)
	_ = registerMockStack(rt, self)

	// Register a request handler that sleeps past the threshold.
	server := newProtoProtocol(&MockProtocol{}, 16)
	rt.registerProtocol(server)
	server.ensureContext()
	server.setPhase(phaseRegistering) // bypass the strict-phase check for ad-hoc setup
	RegisterRequestHandler(server.ctx, func(_ *benchReq, r Responder[*benchRep]) {
		time.Sleep(150 * time.Millisecond)
		r.Reply(&benchRep{})
	})
	server.setPhase(phaseRunning)

	client := newProtoProtocol(&MockProtocol{}, 16)
	rt.registerProtocol(client)
	client.ensureContext()
	client.setPhase(phaseRunning)

	if err := rt.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer rt.Cancel()

	done := make(chan struct{})
	SendRequest(client.ctx, &benchReq{}, func(_ *benchRep, _ error) { close(done) })
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reply never arrived")
	}

	if got := rec.totalCounter("protorun.strict.slow_handler"); got == 0 {
		t.Errorf("expected at least one strict.slow_handler counter, got 0")
	}
}
