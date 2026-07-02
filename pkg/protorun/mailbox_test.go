package protorun

import (
	"context"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/transport"
)

// drainKinds pops every immediately-available event from a mailbox and
// returns their kinds in order. It stops as soon as next would block.
func drainKinds(mb mailbox) []protoEventKind {
	var kinds []protoEventKind
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		ev, ok := mb.next(ctx)
		cancel()
		if !ok {
			return kinds
		}
		kinds = append(kinds, ev.kind)
	}
}

// TestMailbox_FIFOAcrossKinds is the core ordering guarantee: a single
// queue means arrival order is delivery order regardless of event kind.
// This is the property the old six-channel select could not provide.
func TestMailbox_FIFOAcrossKinds(t *testing.T) {
	mb := newMailbox(Mailbox{Capacity: 8, Overflow: OverflowBlock})
	ctx := context.Background()

	want := []protoEventKind{evSession, evMessage, evTimer, evReply, evNotification, evRequest}
	for _, k := range want {
		if _, _, ok := mb.push(ctx, protoEvent{kind: k}); !ok {
			t.Fatalf("push(%v) aborted", k)
		}
	}

	got := drainKinds(mb)
	if len(got) != len(want) {
		t.Fatalf("drained %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("event %d = %v, want %v (full: %v)", i, got[i], want[i], got)
		}
	}
}

// TestMailbox_DropOldest evicts the oldest event and keeps the newest.
func TestMailbox_DropOldest(t *testing.T) {
	mb := newMailbox(Mailbox{Capacity: 2, Overflow: OverflowDropOldest})
	ctx := context.Background()

	push := func(k protoEventKind) (protoEvent, bool) {
		dropped, didDrop, ok := mb.push(ctx, protoEvent{kind: k})
		if !ok {
			t.Fatalf("push aborted")
		}
		return dropped, didDrop
	}

	push(evMessage) // [msg]
	push(evTimer)   // [msg, timer]
	dropped, didDrop := push(evReply)
	if !didDrop {
		t.Fatalf("expected a drop on the third push")
	}
	if dropped.kind != evMessage {
		t.Fatalf("evicted %v, want oldest evMessage", dropped.kind)
	}
	if got := drainKinds(mb); len(got) != 2 || got[0] != evTimer || got[1] != evReply {
		t.Fatalf("remaining = %v, want [timer reply]", got)
	}
}

// TestMailbox_DropNewest rejects the incoming event when full.
func TestMailbox_DropNewest(t *testing.T) {
	mb := newMailbox(Mailbox{Capacity: 2, Overflow: OverflowDropNewest})
	ctx := context.Background()

	mb.push(ctx, protoEvent{kind: evMessage})
	mb.push(ctx, protoEvent{kind: evTimer})
	dropped, didDrop, ok := mb.push(ctx, protoEvent{kind: evReply})
	if !ok || !didDrop {
		t.Fatalf("expected the third push to be rejected (didDrop=%v ok=%v)", didDrop, ok)
	}
	if dropped.kind != evReply {
		t.Fatalf("rejected %v, want the incoming evReply", dropped.kind)
	}
	if got := drainKinds(mb); len(got) != 2 || got[0] != evMessage || got[1] != evTimer {
		t.Fatalf("remaining = %v, want [message timer]", got)
	}
}

// TestMailbox_Unbounded never drops even far past a nominal capacity.
func TestMailbox_Unbounded(t *testing.T) {
	mb := newMailbox(Mailbox{Capacity: 4, Overflow: OverflowUnbounded})
	ctx := context.Background()

	const n = 100
	for range n {
		if _, didDrop, _ := mb.push(ctx, protoEvent{kind: evMessage}); didDrop {
			t.Fatalf("unbounded mailbox dropped an event")
		}
	}
	if d := mb.depth(); d != n {
		t.Fatalf("depth = %d, want %d", d, n)
	}
	if c := mb.capacity(); c != 0 {
		t.Fatalf("unbounded capacity() = %d, want 0", c)
	}
}

// TestMailbox_BlockingPushAbortsOnCtx verifies a full OverflowBlock
// mailbox unblocks the producer when the context is cancelled, so
// shutdown never deadlocks.
func TestMailbox_BlockingPushAbortsOnCtx(t *testing.T) {
	mb := newMailbox(Mailbox{Capacity: 1, Overflow: OverflowBlock})
	ctx, cancel := context.WithCancel(context.Background())

	mb.push(context.Background(), protoEvent{kind: evMessage}) // fills it

	done := make(chan bool, 1)
	go func() {
		_, _, ok := mb.push(ctx, protoEvent{kind: evTimer})
		done <- ok
	}()

	select {
	case <-done:
		t.Fatalf("push should be blocking on a full mailbox")
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	select {
	case ok := <-done:
		if ok {
			t.Fatalf("aborted push should report ok=false")
		}
	case <-time.After(time.Second):
		t.Fatalf("cancel did not unblock the producer")
	}
}

// TestRegister_WithMailbox verifies the WithMailbox RegisterOption
// installs the requested capacity and overflow policy on the protocol.
func TestRegister_WithMailbox(t *testing.T) {
	self := transport.NewHost(0, "127.0.0.1")
	rt := New(self)
	rt.Register(&MockProtocol{}, WithMailbox(Mailbox{Capacity: 4, Overflow: OverflowDropOldest}))

	mb := rt.protocols[0].currentMailbox()
	if mb.policy() != OverflowDropOldest {
		t.Errorf("policy = %v, want drop_oldest", mb.policy())
	}
	if mb.capacity() != 4 {
		t.Errorf("capacity = %d, want 4", mb.capacity())
	}
}

// TestRegister_DefaultMailbox verifies the default is an OverflowBlock
// mailbox of defaultMailboxCapacity.
func TestRegister_DefaultMailbox(t *testing.T) {
	self := transport.NewHost(0, "127.0.0.1")
	rt := New(self)
	rt.Register(&MockProtocol{})

	mb := rt.protocols[0].currentMailbox()
	if mb.policy() != OverflowBlock {
		t.Errorf("default policy = %v, want block", mb.policy())
	}
	if mb.capacity() != defaultMailboxCapacity {
		t.Errorf("default capacity = %d, want %d", mb.capacity(), defaultMailboxCapacity)
	}
}

// TestEnqueue_DropRoutesToDeadLetterAndMetrics drives the runtime-level
// enqueue path (not the bare mailbox) to verify a drop increments the
// counter and routes a fully-populated DeadLetter to the hook.
func TestEnqueue_DropRoutesToDeadLetterAndMetrics(t *testing.T) {
	self := transport.NewHost(0, "127.0.0.1")
	metrics := newRecordingMetrics()

	var got []DeadLetter
	rt := New(self, WithMetrics(metrics), WithDeadLetter(func(dl DeadLetter) {
		got = append(got, dl)
	}))
	proto := newProtoProtocolMailbox(&MockProtocol{}, Mailbox{Capacity: 1, Overflow: OverflowDropNewest})
	rt.registerProtocol(proto)

	peerA := transport.NewHost(1, "127.0.0.1")
	peerB := transport.NewHost(2, "127.0.0.1")
	proto.enqueue(rt.ctx, protoEvent{kind: evMessage, msg: &localMessage{}, from: peerA})
	proto.enqueue(rt.ctx, protoEvent{kind: evMessage, msg: &localMessage{}, from: peerB})

	if len(got) != 1 {
		t.Fatalf("expected 1 dead letter, got %d", len(got))
	}
	if got[0].Kind != "message" {
		t.Errorf("dead letter kind = %q, want message", got[0].Kind)
	}
	if got[0].Peer != peerB {
		t.Errorf("dead letter peer = %v, want the rejected event's peer %v", got[0].Peer, peerB)
	}
	if got[0].Protocol != proto.name {
		t.Errorf("dead letter protocol = %q, want %q", got[0].Protocol, proto.name)
	}
	if n := metrics.totalCounter("protorun.mailbox.dropped"); n != 1 {
		t.Errorf("mailbox.dropped counter = %d, want 1", n)
	}
}
