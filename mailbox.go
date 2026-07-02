package protorun

import (
	"context"
	"sync"

	"github.com/antonionduarte/protorun/transport"
)

// OverflowPolicy selects what a protocol's mailbox does when a producer
// enqueues into a full mailbox. Chosen per protocol at registration via
// WithMailbox.
type OverflowPolicy int

const (
	// OverflowBlock makes the producer block until the event loop frees
	// a slot. This is the safest policy — no event is ever lost — but a
	// slow protocol exerts backpressure on whoever enqueues to it. The
	// block is guarded by the runtime context, so shutdown always
	// unblocks the producer. Default.
	OverflowBlock OverflowPolicy = iota

	// OverflowDropOldest evicts the oldest queued event to make room for
	// the new one; the evicted event goes to the dead-letter hook.
	// Enqueue never blocks.
	OverflowDropOldest

	// OverflowDropNewest rejects the incoming event when the mailbox is
	// full; the rejected event goes to the dead-letter hook. Enqueue
	// never blocks.
	OverflowDropNewest

	// OverflowUnbounded never blocks and never drops: the mailbox grows
	// without limit. Use only when dropping an event is unacceptable and
	// the producer must not block — and accept the memory risk, since a
	// permanently slow event loop lets the queue grow until the process
	// runs out of memory. Strict mode warns on sustained high occupancy.
	OverflowUnbounded
)

func (o OverflowPolicy) String() string {
	switch o {
	case OverflowBlock:
		return "block"
	case OverflowDropOldest:
		return "drop_oldest"
	case OverflowDropNewest:
		return "drop_newest"
	case OverflowUnbounded:
		return "unbounded"
	}
	return "unknown"
}

// Mailbox configures a protocol's event mailbox. Capacity is the queue
// depth (default defaultMailboxCapacity when non-positive); Overflow is
// the policy applied when a bounded mailbox fills. Ignored capacity for
// OverflowUnbounded, which never bounds.
type Mailbox struct {
	Capacity int
	Overflow OverflowPolicy
}

// defaultMailboxCapacity is the queue depth used when WithMailbox is not
// supplied or Capacity is non-positive.
const defaultMailboxCapacity = 1024

// DeadLetter describes an event that a protocol's mailbox dropped under
// a drop overflow policy. It is handed to the runtime's dead-letter hook
// (WithDeadLetter) so operators can observe or record loss.
type DeadLetter struct {
	// Protocol is the concrete protocol type, formatted as fmt.Sprintf
	// "%T", whose mailbox dropped the event.
	Protocol string

	// Kind is the dropped event's kind ("message", "timer", "session",
	// "request", "reply", "notification").
	Kind string

	// Peer is the peer Host the event concerned, when applicable
	// (message and session events). The zero Host otherwise.
	Peer transport.Host
}

// WithDeadLetter registers a hook invoked once per event dropped by a
// drop-policy mailbox (OverflowDropOldest / OverflowDropNewest). The
// hook is called synchronously on the producer's goroutine from inside
// the enqueue path: it MUST be fast and MUST NOT call back into the
// runtime (Send, SendRequest, Connect, ...), or it will deadlock the
// enqueuing goroutine. Use it to count, log, or signal — nothing more.
func WithDeadLetter(fn func(DeadLetter)) Option {
	return func(r *Runtime) {
		if fn != nil {
			r.deadLetter = fn
		}
	}
}

// protoEventKind tags the protoEvent union.
type protoEventKind uint8

const (
	evMessage protoEventKind = iota
	evTimer
	evSession
	evRequest
	evReply
	evNotification
	// evCallback carries a runtime-internal lifecycle callback (today,
	// RestartHandler.OnRestart) so it runs on the owning protocol's
	// event loop like every other handler. Kept last so the existing
	// kinds keep their values.
	evCallback
)

func (k protoEventKind) String() string {
	switch k {
	case evMessage:
		return "message"
	case evTimer:
		return "timer"
	case evSession:
		return "session"
	case evRequest:
		return "request"
	case evReply:
		return "reply"
	case evNotification:
		return "notification"
	case evCallback:
		return "callback"
	}
	return "unknown"
}

// protoEvent is the single tagged union carried by a protocol's
// mailbox. One ordered queue across all kinds is what makes arrival
// order equal delivery order: a message and the SessionDisconnected
// that preceded it are handled in the order they were enqueued, not in
// the order a multi-channel select happened to pick.
//
// The hot-path payloads (message, timer) are inline. The rarer session
// and IPC payloads live behind aux so the value copied through the
// mailbox stays small; those kinds already allocate a handler closure,
// so one more small allocation on their path is free.
//
// Only the fields named by kind are meaningful; the rest are zero.
type protoEvent struct {
	kind protoEventKind

	msg   Message        // evMessage
	from  transport.Host // evMessage
	timer *timerHandle   // evTimer
	aux   *eventAux      // evSession, evRequest, evReply, evNotification, evCallback
}

// eventAux holds the payload for the non-hot event kinds. Exactly one
// field is set, chosen by protoEvent.kind.
type eventAux struct {
	session sessionEvent
	request inboundRequest
	reply   inboundReply
	notif   inboundNotification
	run     func() // evCallback
}

// peer returns the Host this event concerns, or the zero Host when the
// kind carries no peer. Used to populate DeadLetter.Peer.
func (e *protoEvent) peer() transport.Host {
	switch e.kind {
	case evMessage:
		return e.from
	case evSession:
		if e.aux != nil {
			return e.aux.session.host
		}
	}
	return transport.Host{}
}

// mailbox is a protocol's ordered event queue. Two implementations back
// it: blockingMailbox (a plain buffered channel, for OverflowBlock) and
// dequeMailbox (a mutex-guarded deque plus a signal channel, for the
// drop and unbounded policies). The event loop only ever sees next().
type mailbox interface {
	// push enqueues ev. It returns the event evicted or rejected by the
	// overflow policy (didDrop true) so the caller can dead-letter it,
	// and ok=false only when a blocking push was aborted by ctx (runtime
	// shutdown) without enqueuing.
	push(ctx context.Context, ev protoEvent) (dropped protoEvent, didDrop, ok bool)

	// next blocks until an event is available or ctx is done. ok=false
	// means ctx fired and the loop should exit.
	next(ctx context.Context) (ev protoEvent, ok bool)

	// drain removes and returns every currently-queued event without
	// blocking. Used by the supervisor to empty a quarantined mailbox
	// into the dead-letter hook before a restart. The caller must
	// guarantee no event loop is concurrently draining (the loop has
	// already exited by the time the supervisor calls this).
	drain() []protoEvent

	// depth is the current occupancy, sampled for metrics on enqueue.
	depth() int

	// capacity is the configured bound, or 0 for OverflowUnbounded.
	capacity() int

	// policy is the overflow policy this mailbox enforces.
	policy() OverflowPolicy
}

// newMailbox builds the mailbox implementation for a Mailbox config,
// normalizing a non-positive capacity to defaultMailboxCapacity.
func newMailbox(m Mailbox) mailbox {
	capacity := m.Capacity
	if capacity <= 0 {
		capacity = defaultMailboxCapacity
	}
	if m.Overflow == OverflowBlock {
		return &blockingMailbox{ch: make(chan protoEvent, capacity), cap: capacity}
	}
	return &dequeMailbox{
		pol:    m.Overflow,
		cap:    capacity,
		signal: make(chan struct{}, 1),
	}
}

// blockingMailbox is the OverflowBlock implementation: a single
// buffered channel. One channel trivially preserves FIFO across kinds,
// and a full-channel send blocks the producer — the desired
// backpressure — until the event loop drains a slot or ctx fires.
type blockingMailbox struct {
	ch  chan protoEvent
	cap int
}

func (b *blockingMailbox) push(ctx context.Context, ev protoEvent) (protoEvent, bool, bool) {
	select {
	case b.ch <- ev:
		return protoEvent{}, false, true
	case <-ctx.Done():
		return protoEvent{}, false, false
	}
}

func (b *blockingMailbox) next(ctx context.Context) (protoEvent, bool) {
	select {
	case ev := <-b.ch:
		return ev, true
	case <-ctx.Done():
		return protoEvent{}, false
	}
}

func (b *blockingMailbox) drain() []protoEvent {
	var out []protoEvent
	for {
		select {
		case ev := <-b.ch:
			out = append(out, ev)
		default:
			return out
		}
	}
}

func (b *blockingMailbox) depth() int             { return len(b.ch) }
func (b *blockingMailbox) capacity() int          { return b.cap }
func (b *blockingMailbox) policy() OverflowPolicy { return OverflowBlock }

// dequeMailbox backs the drop and unbounded policies: a mutex-guarded
// slice used as a FIFO deque, plus a 1-buffered signal channel the event
// loop selects on alongside ctx.Done(). Producers never block.
type dequeMailbox struct {
	pol OverflowPolicy
	cap int

	mu     sync.Mutex
	queue  []protoEvent
	signal chan struct{}
}

func (d *dequeMailbox) push(_ context.Context, ev protoEvent) (protoEvent, bool, bool) {
	d.mu.Lock()
	dropped, didDrop := d.enqueueLocked(ev)
	d.mu.Unlock()
	// Wake the event loop. The channel is 1-buffered and coalescing: a
	// pending signal already tells the loop "queue non-empty", so a
	// non-blocking send is enough and never blocks the producer.
	select {
	case d.signal <- struct{}{}:
	default:
	}
	return dropped, didDrop, true
}

// enqueueLocked applies the overflow policy. Caller holds d.mu.
func (d *dequeMailbox) enqueueLocked(ev protoEvent) (dropped protoEvent, didDrop bool) {
	switch d.pol {
	case OverflowUnbounded:
		d.queue = append(d.queue, ev)
		return protoEvent{}, false
	case OverflowDropNewest:
		if len(d.queue) >= d.cap {
			return ev, true // reject the incoming event
		}
		d.queue = append(d.queue, ev)
		return protoEvent{}, false
	case OverflowDropOldest:
		if len(d.queue) >= d.cap {
			old := d.queue[0]
			d.queue = d.queue[1:]
			d.queue = append(d.queue, ev)
			return old, true // evict the oldest
		}
		d.queue = append(d.queue, ev)
		return protoEvent{}, false
	}
	d.queue = append(d.queue, ev)
	return protoEvent{}, false
}

func (d *dequeMailbox) next(ctx context.Context) (protoEvent, bool) {
	for {
		d.mu.Lock()
		if len(d.queue) > 0 {
			ev := d.queue[0]
			// Release the drained slot back to the GC; reslicing keeps
			// the backing array pinned otherwise.
			d.queue[0] = protoEvent{}
			d.queue = d.queue[1:]
			if len(d.queue) == 0 {
				d.queue = nil
			}
			d.mu.Unlock()
			return ev, true
		}
		d.mu.Unlock()
		select {
		case <-d.signal:
			// Re-check the queue: the signal is coalescing, so a wake
			// may not correspond one-to-one with an enqueue.
		case <-ctx.Done():
			return protoEvent{}, false
		}
	}
}

func (d *dequeMailbox) drain() []protoEvent {
	d.mu.Lock()
	out := d.queue
	d.queue = nil
	d.mu.Unlock()
	return out
}

func (d *dequeMailbox) depth() int {
	d.mu.Lock()
	n := len(d.queue)
	d.mu.Unlock()
	return n
}

// capacity reports 0 for the unbounded policy so occupancy checks that
// divide by capacity can skip it.
func (d *dequeMailbox) capacity() int {
	if d.pol == OverflowUnbounded {
		return 0
	}
	return d.cap
}

func (d *dequeMailbox) policy() OverflowPolicy { return d.pol }
