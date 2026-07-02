package protorun

import (
	"sync"
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

	// stop stops the clock timer currently backing this handle. For a
	// periodic timer (Every) the backing clock timer is replaced on
	// every fire, so stop is updated under stopMu each time it re-arms.
	stopMu sync.Mutex
	stop   func()
}

// setStop installs fn as the current underlying-timer stopper. Called
// once for a one-shot and after each re-arm for a periodic timer.
func (h *timerHandle) setStop(fn func()) {
	h.stopMu.Lock()
	h.stop = fn
	h.stopMu.Unlock()
}

// cancel marks the timer cancelled, drops it from the owner's table,
// and stops the underlying clock resource. Idempotent via the atomic
// swap.
func (h *timerHandle) cancel() {
	if h.cancelled.Swap(true) {
		return
	}
	h.owner.forgetTimer(h.id)
	h.stopMu.Lock()
	stop := h.stop
	h.stopMu.Unlock()
	if stop != nil {
		stop()
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
	h.setStop(func() { ct.Stop() })
	return TimerHandle{h: h}
}

// every schedules fn to fire on owner's event loop once per d until the
// handle is cancelled or the runtime shuts down. It is built on the
// same one-shot AfterFunc seam as after: each fire enqueues the tick and
// re-arms the next AfterFunc, so there is no background ticker
// goroutine. That matters for two reasons — production pays for no extra
// goroutine per periodic timer, and under a virtual clock the fire (and
// its enqueue) happens synchronously on the goroutine that advances the
// clock, which is what keeps periodic timers deterministic inside the
// simulation harness.
//
// The next fire is scheduled d after the current deadline (read from the
// clock inside the fire), so a virtual clock stays perfectly periodic
// and a real clock only drifts by handler latency, not cumulatively.
func (r *Runtime) every(owner *protoProtocol, d time.Duration, fn func()) TimerHandle {
	h := &timerHandle{id: r.nextTimerID.Add(1), owner: owner, fn: fn}
	owner.trackTimer(h)
	ctx := r.ctx

	var arm func()
	arm = func() {
		if h.cancelled.Load() {
			return
		}
		owner.enqueue(ctx, protoEvent{kind: evTimer, timer: h})
		if h.cancelled.Load() {
			return
		}
		ct := r.clock.AfterFunc(d, arm)
		h.setStop(func() { ct.Stop() })
		// Lost the race with a concurrent cancel that ran between the
		// check above and setStop: stop the timer we just armed so no
		// fire outlives the cancel.
		if h.cancelled.Load() {
			ct.Stop()
		}
	}

	ct := r.clock.AfterFunc(d, arm)
	h.setStop(func() { ct.Stop() })
	if h.cancelled.Load() {
		ct.Stop()
	}
	return TimerHandle{h: h}
}
