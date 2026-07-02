package prototest

import (
	"sync"
	"time"

	"github.com/antonionduarte/protorun"
)

// FakeClock is a virtual protorun.Clock: time advances only when the
// test calls Advance. It is the foundation for deterministic simulation
// (Phase 4): with timer order controlled by the test, timers, retries,
// and request timeouts become reproducible.
//
// Advance fires every due AfterFunc callback and ticker tick in
// deadline order (ties broken by arm order), stepping Now to each
// deadline before invoking its callback, so a callback that reads Now
// sees its own scheduled time. Callbacks run without the clock's lock
// held, so they may re-enter the clock (arm more timers, Stop tickers)
// without deadlocking.
//
// FakeClock is safe for concurrent use. Note that Phase 0 does not yet
// switch prototest.NewRuntime onto it; it is exported now for direct
// use via protorun.WithClock and unit testing.
type FakeClock struct {
	mu    sync.Mutex
	now   time.Time
	seq   uint64
	items []*fakeEntry
}

// fakeEntry is one scheduled fire: a one-shot AfterFunc (period == 0) or
// a recurring ticker (period > 0).
type fakeEntry struct {
	deadline time.Time
	seq      uint64         // arm order, breaks deadline ties
	period   time.Duration  // 0 for a one-shot timer
	fn       func()         // one-shot callback; nil for a ticker
	ch       chan time.Time // ticker delivery channel; nil for a timer
	stopped  bool
}

// NewFakeClock returns a FakeClock whose Now is start.
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

// Now returns the current virtual time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// AfterFunc schedules fn to run when the clock advances to now+d.
func (c *FakeClock) AfterFunc(d time.Duration, fn func()) protorun.ClockTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := &fakeEntry{deadline: c.now.Add(d), seq: c.nextSeq(), fn: fn}
	c.items = append(c.items, e)
	return &fakeTimer{clock: c, entry: e}
}

// NewTicker returns a ticker that fires every d of virtual time. Its
// channel is buffered with capacity 1 and Advance uses a non-blocking
// send, mirroring time.Ticker's tick-coalescing under a slow receiver.
func (c *FakeClock) NewTicker(d time.Duration) protorun.ClockTicker {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := &fakeEntry{
		deadline: c.now.Add(d),
		seq:      c.nextSeq(),
		period:   d,
		ch:       make(chan time.Time, 1),
	}
	c.items = append(c.items, e)
	return &fakeTicker{clock: c, entry: e}
}

func (c *FakeClock) nextSeq() uint64 {
	c.seq++
	return c.seq
}

// Advance moves virtual time forward by d, firing every callback and
// tick whose deadline falls in (now, now+d] in deadline-then-arm order.
// Now steps to each deadline before its callback runs; after the last
// due entry Now lands exactly on the old now+d.
func (c *FakeClock) Advance(d time.Duration) {
	if d <= 0 {
		return
	}
	c.mu.Lock()
	target := c.now.Add(d)
	for {
		e := c.earliestDueLocked(target)
		if e == nil {
			c.now = target
			c.mu.Unlock()
			return
		}
		c.now = e.deadline
		if e.period > 0 {
			// Ticker: reschedule the next tick and deliver this one
			// (non-blocking, coalescing) without holding the lock.
			e.deadline = e.deadline.Add(e.period)
			tick := c.now
			ch := e.ch
			c.mu.Unlock()
			select {
			case ch <- tick:
			default:
			}
			c.mu.Lock()
			continue
		}
		// One-shot: remove and fire without the lock.
		e.stopped = true
		c.removeLocked(e)
		fn := e.fn
		c.mu.Unlock()
		fn()
		c.mu.Lock()
	}
}

// earliestDueLocked returns the earliest non-stopped entry with a
// deadline <= target, ties broken by arm sequence. Caller holds c.mu.
func (c *FakeClock) earliestDueLocked(target time.Time) *fakeEntry {
	var best *fakeEntry
	for _, e := range c.items {
		if e.stopped || e.deadline.After(target) {
			continue
		}
		if best == nil || e.deadline.Before(best.deadline) ||
			(e.deadline.Equal(best.deadline) && e.seq < best.seq) {
			best = e
		}
	}
	return best
}

// removeLocked deletes e from the pending slice. Caller holds c.mu.
func (c *FakeClock) removeLocked(e *fakeEntry) {
	for i, x := range c.items {
		if x == e {
			c.items = append(c.items[:i], c.items[i+1:]...)
			return
		}
	}
}

// stop marks an entry stopped and removes it. Reports whether it was
// still pending (mirrors time.Timer.Stop).
func (c *FakeClock) stop(e *fakeEntry) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e.stopped {
		return false
	}
	e.stopped = true
	c.removeLocked(e)
	return true
}

// pendingCount reports how many entries are still scheduled. Test-only
// introspection.
func (c *FakeClock) pendingCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

// fakeTimer is the ClockTimer returned by FakeClock.AfterFunc.
type fakeTimer struct {
	clock *FakeClock
	entry *fakeEntry
}

func (t *fakeTimer) Stop() bool { return t.clock.stop(t.entry) }

// fakeTicker is the ClockTicker returned by FakeClock.NewTicker.
type fakeTicker struct {
	clock *FakeClock
	entry *fakeEntry
}

func (t *fakeTicker) C() <-chan time.Time { return t.entry.ch }
func (t *fakeTicker) Stop()               { t.clock.stop(t.entry) }

// Compile-time assertion that FakeClock satisfies the runtime seam.
var _ protorun.Clock = (*FakeClock)(nil)
