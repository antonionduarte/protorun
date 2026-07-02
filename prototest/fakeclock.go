package prototest

import (
	"sync"
	"time"

	"github.com/antonionduarte/protorun"
)

// FakeClock is a virtual protorun.Clock: time advances only when the
// clock is told to, via Advance. It is the foundation for deterministic
// simulation — with timer order controlled instead of raced, timers,
// retries, and request timeouts become reproducible.
//
// Advance fires every due AfterFunc callback in deadline order (ties
// broken by arm order), stepping Now to each deadline before invoking
// its callback, so a callback that reads Now sees its own scheduled
// time. Callbacks run without the clock's lock held, so they may
// re-enter the clock (arm more timers, stop timers) without deadlocking.
// Periodic timers (protorun ctx.Every) are built on AfterFunc, so they
// too fire synchronously on the goroutine that calls Advance — there is
// no background ticker goroutine to race.
//
// FakeClock is safe for concurrent use. Under a Sim the mesh owns one
// FakeClock shared by every node, and the Sim scheduler is the only
// caller of Advance; direct use via protorun.WithClock is also fine.
type FakeClock struct {
	mu    sync.Mutex
	now   time.Time
	seq   uint64
	items []*fakeEntry
}

// fakeEntry is one scheduled one-shot AfterFunc fire.
type fakeEntry struct {
	deadline time.Time
	seq      uint64 // arm order, breaks deadline ties
	fn       func()
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

func (c *FakeClock) nextSeq() uint64 {
	c.seq++
	return c.seq
}

// Advance moves virtual time forward by d, firing every callback whose
// deadline falls in (now, now+d] in deadline-then-arm order. Now steps
// to each deadline before its callback runs; after the last due entry
// Now lands exactly on the old now+d.
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

// fireDue fires every entry whose deadline is at or before the current
// Now — in deadline-then-arm order — without moving Now, and reports
// whether any fired. The Sim uses it to flush timers armed for the
// current instant (for example a handler arming After(0), or a re-arm
// landing exactly on Now) so "advance to the next deadline" always makes
// forward progress instead of spinning on a zero-length step.
func (c *FakeClock) fireDue() bool {
	fired := false
	c.mu.Lock()
	for {
		e := c.earliestDueLocked(c.now)
		if e == nil {
			c.mu.Unlock()
			return fired
		}
		e.stopped = true
		c.removeLocked(e)
		fn := e.fn
		c.mu.Unlock()
		fn()
		fired = true
		c.mu.Lock()
	}
}

// nextDeadline reports the earliest pending fire time, if any. Used by
// the Sim scheduler to decide how far to advance virtual time.
func (c *FakeClock) nextDeadline() (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var best time.Time
	has := false
	for _, e := range c.items {
		if e.stopped {
			continue
		}
		if !has || e.deadline.Before(best) {
			best = e.deadline
			has = true
		}
	}
	return best, has
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

// fakeTimer is the ClockTimer returned by FakeClock.AfterFunc.
type fakeTimer struct {
	clock *FakeClock
	entry *fakeEntry
}

func (t *fakeTimer) Stop() bool { return t.clock.stop(t.entry) }

// Compile-time assertion that FakeClock satisfies the runtime seam.
var _ protorun.Clock = (*FakeClock)(nil)
