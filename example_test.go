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
// snippets. It does no work, but registers a message type and handler so
// the snippet compiles. protorun.Handle picks the codec (here the
// reflective WireCodec) and registers the handler in one call.
type echoProtocol struct{}

func (echoProtocol) Start(ctx protorun.ProtocolContext) {
	protorun.Handle(ctx, func(_ *myMessage, _ transport.Host) {})
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

// ExampleHandle shows the one-line message registration: Handle infers
// the message type from the handler, picks a codec (SelfCodec if the
// type implements SelfMarshaler, otherwise the reflective WireCodec),
// and registers both the codec and the handler.
func ExampleHandle() {
	var ctx protorun.ProtocolContext // supplied by the framework in Start
	if ctx == nil {                  // illustrative guard for the godoc snippet
		return
	}
	protorun.Handle(ctx, func(m *myMessage, from transport.Host) {
		fmt.Printf("got seq=%d from=%s\n", m.Seq, from.String())
	})
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

// ExampleProtocolContext_After schedules one-shot and periodic work on
// the protocol's own event loop. Both return a TimerHandle whose Cancel
// is idempotent and safe to call from inside a handler.
func ExampleProtocolContext_After() {
	var ctx protorun.ProtocolContext // supplied by the framework in Init
	if ctx == nil {                  // illustrative guard for the godoc snippet
		return
	}
	once := ctx.After(500*time.Millisecond, func() {
		fmt.Println("fired once")
	})
	ticker := ctx.Every(time.Second, func() {
		fmt.Println("tick")
	})
	// Later, from any handler of this protocol:
	once.Cancel()
	ticker.Cancel()
}

// ExampleWithMailbox tunes a protocol's mailbox: a bounded queue that
// drops the oldest event under overload, with a dead-letter hook to
// observe the loss.
func ExampleWithMailbox() {
	self := transport.NewHost(0, "127.0.0.1")
	rt := protorun.New(self,
		protorun.WithTCPTransport(context.Background()),
		protorun.WithDeadLetter(func(dl protorun.DeadLetter) {
			fmt.Printf("dropped %s from %s\n", dl.Kind, dl.Protocol)
		}),
	)
	rt.Register(echoProtocol{}, protorun.WithMailbox(protorun.Mailbox{
		Capacity: 256,
		Overflow: protorun.OverflowDropOldest,
	}))
	_ = rt
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
