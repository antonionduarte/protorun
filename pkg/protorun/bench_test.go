package protorun

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/transport"
)

// discardLogger silences the runtime's default slog output for
// benchmarks that spin up a real Runtime: setup/teardown logging is
// otherwise real I/O that has nothing to do with what each benchmark
// measures, and it garbles `go test -bench` output.
var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// benchMsg is a fixed-size message used across the codec / dispatch
// benchmarks. Embeds BaseMessage so BinaryCodec[*benchMsg] can size it.
type benchMsg struct {
	BaseMessage
	Seq uint64
}

// BenchmarkBinaryCodec_Marshal measures the marshal path for a fixed-
// size message: the hottest cost on the send side.
func BenchmarkBinaryCodec_Marshal(b *testing.B) {
	codec := BinaryCodec[*benchMsg]{}
	msg := &benchMsg{Seq: 42}

	for b.Loop() {
		if _, err := codec.Marshal(msg); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBinaryCodec_Unmarshal measures the unmarshal path: the
// hottest cost on the receive side.
func BenchmarkBinaryCodec_Unmarshal(b *testing.B) {
	codec := BinaryCodec[*benchMsg]{}
	payload, err := codec.Marshal(&benchMsg{Seq: 42})
	if err != nil {
		b.Fatal(err)
	}

	for b.Loop() {
		if _, err := codec.Unmarshal(payload); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWireCodec_Marshal measures the reflective codec's marshal
// path on the same fixed-size message as BinaryCodec, so the plan-walk
// overhead versus encoding/binary is directly comparable.
func BenchmarkWireCodec_Marshal(b *testing.B) {
	codec := WireCodec[*benchMsg]{}
	msg := &benchMsg{Seq: 42}

	for b.Loop() {
		if _, err := codec.Marshal(msg); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWireCodec_Unmarshal is the receive-side counterpart to
// BenchmarkWireCodec_Marshal.
func BenchmarkWireCodec_Unmarshal(b *testing.B) {
	codec := WireCodec[*benchMsg]{}
	payload, err := codec.Marshal(&benchMsg{Seq: 42})
	if err != nil {
		b.Fatal(err)
	}

	for b.Loop() {
		if _, err := codec.Unmarshal(payload); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWireID measures the cost of computing a wire identifier:
// per-call overhead for the lookup-by-type path.
func BenchmarkWireID(b *testing.B) {

	for b.Loop() {
		_ = WireID[*benchMsg]()
	}
}

// benchTimedMsg carries a send timestamp so a benchmark can measure the
// latency between a push and the consumer observing it, rather than
// just push/pop throughput.
type benchTimedMsg struct {
	BaseMessage
	Sent time.Time
}

// BenchmarkMailbox_EnqueueDispatchLatency measures the mailbox's own
// scheduling latency: the time from push to a consumer goroutine
// observing the event via next(). Producer and consumer run on
// separate goroutines and the producer waits for each event to be
// observed before pushing the next one, so the number reflects the
// channel-park/wake cost the unified mailbox adds on the delivery
// path — distinct from BenchmarkProcessMessage, which measures
// sustained push throughput under a free-running drainer.
func BenchmarkMailbox_EnqueueDispatchLatency(b *testing.B) {
	mb := newMailbox(Mailbox{Capacity: defaultMailboxCapacity, Overflow: OverflowBlock})
	ctx := b.Context()

	latencies := make(chan time.Duration)
	go func() {
		for {
			ev, ok := mb.next(ctx)
			if !ok {
				return
			}
			latencies <- time.Since(ev.msg.(*benchTimedMsg).Sent)
		}
	}()

	var total time.Duration
	for b.Loop() {
		mb.push(ctx, protoEvent{kind: evMessage, msg: &benchTimedMsg{Sent: time.Now()}})
		total += <-latencies
	}
	b.ReportMetric(float64(total.Nanoseconds())/float64(b.N), "ns/dispatch")
}

// BenchmarkProcessMessage measures end-to-end inbound dispatch:
// encode the wireID, decode, push onto the protocol's mailbox.
// A drainer goroutine keeps the mailbox from filling up so we measure
// the dispatch path itself, not back-pressure.
func BenchmarkProcessMessage(b *testing.B) {
	self := transport.NewHost(0, "127.0.0.1")
	rt := New(self, WithLogger(discardLogger))

	proto := newProtoProtocol(&MockProtocol{}, 1024)
	rt.registerProtocol(proto)
	proto.ensureContext()
	RegisterCodec(proto.ctx, BinaryCodec[*benchMsg]{})
	RegisterHandler(proto.ctx, func(_ *benchMsg, _ transport.Host) {})

	payload, err := BinaryCodec[*benchMsg]{}.Marshal(&benchMsg{Seq: 42})
	if err != nil {
		b.Fatal(err)
	}
	var frame bytes.Buffer
	if err := binary.Write(&frame, binary.LittleEndian, WireID[*benchMsg]()); err != nil {
		b.Fatal(err)
	}
	frame.Write(payload)
	frameBytes := frame.Bytes()

	sender := transport.NewHost(0, "127.0.0.1")

	drainCtx, stopDrain := context.WithCancel(context.Background())
	var drained sync.WaitGroup
	drained.Go(func() {
		for {
			if _, ok := proto.currentMailbox().next(drainCtx); !ok {
				return
			}
		}
	})

	for b.Loop() {
		buf := bytes.NewBuffer(frameBytes)
		rt.processMessage(*buf, sender)
	}
	b.StopTimer()
	stopDrain()
	drained.Wait()
}

// BenchmarkPublishNotification_Fanout measures notification fanout
// with varying subscriber counts. Subscriber's handler is empty; the
// cost is the runtime's per-subscriber mailbox enqueue.
func BenchmarkPublishNotification_Fanout(b *testing.B) {
	for _, n := range []int{1, 10, 100} {
		b.Run(fmtCount(n), func(b *testing.B) {
			benchPublish(b, n)
		})
	}
}

func benchPublish(b *testing.B, subscribers int) {
	self := transport.NewHost(0, "127.0.0.1")
	mock := NewMockNetworkLayer()
	session := transport.NewSessionLayer(mock, self, b.Context(), 0, 0)
	rt := New(self,
		WithTransport(mock, session),
		WithLogger(discardLogger),
	)

	publisher := newProtoProtocol(&MockProtocol{}, b.N+1)
	rt.registerProtocol(publisher)
	publisher.ensureContext()

	for range subscribers {
		sub := newProtoProtocol(&MockProtocol{}, b.N+1)
		rt.registerProtocol(sub)
		sub.ensureContext()
		SubscribeNotification(sub.ctx, func(_ *benchNotif) {})
	}

	if err := rt.start(); err != nil {
		b.Fatal(err)
	}
	defer rt.Cancel()

	notif := &benchNotif{}
	b.ResetTimer()
	for range b.N {
		PublishNotification(publisher.ctx, notif)
	}
}

type benchNotif struct{ BaseNotification }

func fmtCount(n int) string {
	switch n {
	case 1:
		return "1subscriber"
	case 10:
		return "10subscribers"
	case 100:
		return "100subscribers"
	}
	return "N"
}

// BenchmarkSendRequest measures end-to-end same-runtime IPC: requester
// fires a SendRequest, handler replies inline, requester's onReply
// runs. Throughput is the round-trip rate.
func BenchmarkSendRequest(b *testing.B) {
	self := transport.NewHost(0, "127.0.0.1")
	mock := NewMockNetworkLayer()
	session := transport.NewSessionLayer(mock, self, b.Context(), 0, 0)
	rt := New(self,
		WithTransport(mock, session),
		WithLogger(discardLogger),
	)

	server := newProtoProtocol(&MockProtocol{}, b.N+1)
	rt.registerProtocol(server)
	server.ensureContext()
	RegisterRequestHandler(server.ctx, func(_ *benchReq, r Responder[*benchRep]) {
		r.Reply(&benchRep{})
	})

	client := newProtoProtocol(&MockProtocol{}, b.N+1)
	rt.registerProtocol(client)
	client.ensureContext()

	if err := rt.start(); err != nil {
		b.Fatal(err)
	}
	defer rt.Cancel()

	done := make(chan struct{}, b.N)
	cb := func(_ *benchRep, _ error) { done <- struct{}{} }

	b.ResetTimer()
	for range b.N {
		SendRequest(client.ctx, &benchReq{}, cb)
	}
	for range b.N {
		<-done
	}
}

// tcpBenchPing / tcpBenchPong are the wire messages BenchmarkTCP_
// RoundTrip exchanges over a real TCP session. WireName freezes the
// id so the benchmark doesn't depend on package-internal naming.
type tcpBenchPing struct {
	BaseMessage
	Seq uint64
}

func (tcpBenchPing) WireName() string { return "bench.tcpPing" }

type tcpBenchPong struct {
	BaseMessage
	Seq uint64
}

func (tcpBenchPong) WireName() string { return "bench.tcpPong" }

// tcpBenchResponder answers every Ping with a Pong carrying the same
// sequence number.
type tcpBenchResponder struct {
	ctx ProtocolContext
}

func (p *tcpBenchResponder) Start(ctx ProtocolContext) {
	p.ctx = ctx
	Handle(ctx, p.onPing)
	// Send marshals with the sender's own codec registry, so the
	// responder needs a Pong codec too even though it never handles
	// one inbound.
	RegisterCodec(ctx, WireCodec[*tcpBenchPong]{})
}
func (p *tcpBenchResponder) Init(ProtocolContext) {}
func (p *tcpBenchResponder) onPing(m *tcpBenchPing, from transport.Host) {
	_ = p.ctx.Send(&tcpBenchPong{Seq: m.Seq}, from)
}

// tcpBenchRequester dials peer on Init, signals connected once the
// session comes up, and hands every Pong's Seq to replies.
type tcpBenchRequester struct {
	ctx       ProtocolContext
	peer      transport.Host
	connected chan struct{}
	replies   chan uint64
}

func (p *tcpBenchRequester) Start(ctx ProtocolContext) {
	p.ctx = ctx
	Handle(ctx, p.onPong)
	// Send marshals with the sender's own codec registry, so the
	// requester needs a Ping codec too even though it never handles
	// one inbound.
	RegisterCodec(ctx, WireCodec[*tcpBenchPing]{})
}
func (p *tcpBenchRequester) Init(ctx ProtocolContext) {
	if err := ctx.Connect(p.peer); err != nil {
		panic(err)
	}
}
func (p *tcpBenchRequester) OnSessionConnected(h transport.Host) {
	if h == p.peer {
		close(p.connected)
	}
}
func (p *tcpBenchRequester) onPong(m *tcpBenchPong, _ transport.Host) {
	p.replies <- m.Seq
}

// BenchmarkTCP_RoundTrip is the one benchmark in this file that
// exercises the full stack: two real Runtimes, each with its own
// TCPLayer + SessionLayer, on localhost. It measures wall-clock
// latency for one Ping -> Pong round trip over an established TCP
// session — everything BenchmarkProcessMessage / BenchmarkSendRequest
// measure in-process, plus socket I/O, framing, and OS scheduling.
// Session establishment happens once, before the timed loop.
func BenchmarkTCP_RoundTrip(b *testing.B) {
	// Ports 7310/7311: outside the 7300-7304 band this package's own
	// integration tests use and the 7400+ band cmd/gossip's multi-
	// runtime tests reserve (see CLAUDE.md Testing notes). Only bound
	// when this benchmark actually runs (go test -bench), so it never
	// collides with a concurrent `go test ./...`.
	hostA := transport.NewHost(7310, "127.0.0.1")
	hostB := transport.NewHost(7311, "127.0.0.1")

	// transport.NewTCPLayer / NewSessionLayer log through slog.Default()
	// (there is no per-layer logger option); swap it out for the
	// duration of this benchmark so socket/handshake logging doesn't
	// mix into `go test -bench` output.
	prevDefault := slog.Default()
	slog.SetDefault(discardLogger)
	defer slog.SetDefault(prevDefault)

	requester := &tcpBenchRequester{peer: hostB, connected: make(chan struct{}), replies: make(chan uint64, 1)}
	responder := &tcpBenchResponder{}

	rtA := New(hostA, WithLogger(discardLogger), WithTCPTransport(b.Context()))
	rtA.Register(requester)
	rtB := New(hostB, WithLogger(discardLogger), WithTCPTransport(b.Context()))
	rtB.Register(responder)

	if err := rtA.start(); err != nil {
		b.Fatal(err)
	}
	defer rtA.Cancel()
	if err := rtB.start(); err != nil {
		b.Fatal(err)
	}
	defer rtB.Cancel()

	select {
	case <-requester.connected:
	case <-time.After(5 * time.Second):
		b.Fatal("timed out waiting for the TCP session to establish")
	}

	for b.Loop() {
		if err := requester.ctx.Send(&tcpBenchPing{Seq: 1}, hostB); err != nil {
			b.Fatal(err)
		}
		<-requester.replies
	}
}

type benchReq struct{ BaseRequest }
type benchRep struct{ BaseReply }
