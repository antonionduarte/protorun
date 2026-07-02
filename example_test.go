package protorun_test

import (
	"context"
	"fmt"
	"time"

	"github.com/antonionduarte/protorun"
	"github.com/antonionduarte/protorun/transport"
)

// myMessage is a tiny fixed-size message used by the examples below.
type myMessage struct {
	protorun.BaseMessage
	Seq uint64
}

// echoProtocol is a minimal Protocol implementation suitable for example
// snippets. It does no work, but registers a codec and a handler so the
// snippet compiles.
type echoProtocol struct{}

func (echoProtocol) Start(ctx protorun.ProtocolContext) {
	protorun.RegisterCodec(ctx, protorun.BinaryCodec[*myMessage]{})
	protorun.RegisterHandler(ctx, func(_ *myMessage, _ transport.Host) {})
}
func (echoProtocol) Init(_ protorun.ProtocolContext) {}

// Example shows the canonical setup: construct a runtime with a TCP
// transport, register a protocol, and Run() until SIGINT/SIGTERM.
func Example() {
	self := transport.NewHost(7000, "127.0.0.1")
	rt := protorun.New(self,
		protorun.WithTCPTransport(context.Background()),
	)
	rt.Register(echoProtocol{})

	// In real code, call rt.Run() here. The example just sketches the
	// shape; Run blocks on signals, so we substitute a manual cancel.
	go func() {
		time.Sleep(10 * time.Millisecond)
		rt.Cancel()
	}()
	_ = rt.Run()
	fmt.Println("done")
	// Output: done
}

// ExampleRegisterHandler shows the typed handler signature: a handler
// receives the decoded message and the transport.Host that sent it.
// No type assertions, no manual ID tracking.
func ExampleRegisterHandler() {
	var ctx protorun.ProtocolContext // supplied by the framework in Start
	if ctx == nil {                  // illustrative guard for the godoc snippet
		return
	}
	protorun.RegisterHandler(ctx, func(m *myMessage, from transport.Host) {
		fmt.Printf("got seq=%d from=%s\n", m.Seq, from.String())
	})
}

// ExampleRegisterCodec shows registering BinaryCodec for a fixed-size
// message. BaseMessage is a zero-byte marker so encoding/binary can
// size the struct.
func ExampleRegisterCodec() {
	var ctx protorun.ProtocolContext
	if ctx == nil {
		return
	}
	protorun.RegisterCodec(ctx, protorun.BinaryCodec[*myMessage]{})
}

// ExampleWithRetryPolicy configures opt-in reconnect on a runtime. The
// policy is used by every ConnectWithRetry call; plain Connect is
// unaffected.
func ExampleWithRetryPolicy() {
	self := transport.NewHost(0, "127.0.0.1")
	rt := protorun.New(self,
		protorun.WithTCPTransport(context.Background()),
		protorun.WithRetryPolicy(protorun.RetryPolicy{
			Initial:     200 * time.Millisecond,
			Max:         10 * time.Second,
			Multiplier:  2.0,
			MaxAttempts: 0, // 0 = unbounded
			Jitter:      0.2,
		}),
	)
	_ = rt
}
