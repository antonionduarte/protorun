package protorun

import (
	"context"
	"sync/atomic"
	"time"
)

// TimerHandle is returned by ProtocolContext.After and Every. Cancel
// stops the timer. It is idempotent (double Cancel is a no-op), safe
// after the timer has already fired, and safe to call from inside any
// handler of the owning protocol. When Cancel is called from the
// protocol's own event loop, the callback is guaranteed not to run
// afterwards even if the fire is already sitting in the mailbox: the
// dispatcher rechecks the cancelled flag before invoking it.
type TimerHandle struct{ h *timerHandle }

// Cancel stops the timer. See TimerHandle.
func (t TimerHandle) Cancel() {
	if t.h != nil {
		t.h.cancel()
	}
}

// timerHandle is the runtime-internal timer state, keyed by a
// runtime-monotonic id. fn is the user closure; the payload travels by
// capture, so there is no Timer value and no user-managed id. cancelled
// is checked both when the underlying clock fires and again at dispatch
// time, so a cancel that races a queued fire still suppresses the
// callback.
type timerHandle struct {
	id        uint64
	owner     *protoProtocol
	fn        func()
	cancelled atomic.Bool
	stop      func() // stops the underlying clock timer / ticker goroutine
}

// cancel marks the timer cancelled, drops it from the owner's table,
// and stops the underlying clock resource. Idempotent via the atomic
// swap.
func (h *timerHandle) cancel() {
	if h.cancelled.Swap(true) {
		return
	}
	h.owner.forgetTimer(h.id)
	if h.stop != nil {
		h.stop()
	}
}

// after schedules fn to fire once after d on owner's event loop. The
// one-shot fire drops the handle from owner's table but does not mark
// it cancelled — dispatch still runs fn unless the user cancels first.
func (r *Runtime) after(owner *protoProtocol, d time.Duration, fn func()) TimerHandle {
	h := &timerHandle{id: r.nextTimerID.Add(1), owner: owner, fn: fn}
	owner.trackTimer(h)
	ctx := r.ctx
	ct := r.clock.AfterFunc(d, func() {
		owner.forgetTimer(h.id)
		if h.cancelled.Load() {
			return
		}
		owner.enqueue(ctx, protoEvent{kind: evTimer, timer: h})
	})
	h.stop = func() { ct.Stop() }
	return TimerHandle{h: h}
}

// every schedules fn to fire on owner's event loop once per d until the
// handle is cancelled or the runtime shuts down. A per-timer goroutine
// (tracked by the runtime WaitGroup) drives a Clock ticker; cancelling
// the handle cancels its context, which stops the goroutine and the
// ticker.
func (r *Runtime) every(owner *protoProtocol, d time.Duration, fn func()) TimerHandle {
	h := &timerHandle{id: r.nextTimerID.Add(1), owner: owner, fn: fn}
	owner.trackTimer(h)
	ctx, cancel := context.WithCancel(r.ctx)
	ticker := r.clock.NewTicker(d)
	h.stop = cancel
	r.wg.Go(func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C():
				if h.cancelled.Load() {
					return
				}
				owner.enqueue(ctx, protoEvent{kind: evTimer, timer: h})
			}
		}
	})
	return TimerHandle{h: h}
}
