package protorun

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync"
	"testing"

	"github.com/antonionduarte/protorun/transport"
)

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

// BenchmarkWireID measures the cost of computing a wire identifier:
// per-call overhead for the lookup-by-type path.
func BenchmarkWireID(b *testing.B) {

	for b.Loop() {
		_ = WireID[*benchMsg]()
	}
}

// BenchmarkProcessMessage measures end-to-end inbound dispatch:
// encode the wireID, decode, push onto the protocol's mailbox.
// A drainer goroutine keeps the mailbox from filling up so we measure
// the dispatch path itself, not back-pressure.
func BenchmarkProcessMessage(b *testing.B) {
	self := transport.NewHost(0, "127.0.0.1")
	rt := New(self)

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

type benchReq struct{ BaseRequest }
type benchRep struct{ BaseReply }
