package prototest

import (
	"testing"
	"time"
)

var epoch = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

// TestFakeClock_NowAdvances verifies Now only moves when Advance is
// called, and lands exactly on the requested offset.
func TestFakeClock_NowAdvances(t *testing.T) {
	c := NewFakeClock(epoch)
	if !c.Now().Equal(epoch) {
		t.Fatalf("Now = %v, want %v", c.Now(), epoch)
	}
	c.Advance(3 * time.Second)
	if got := c.Now(); !got.Equal(epoch.Add(3 * time.Second)) {
		t.Fatalf("Now = %v, want %v", got, epoch.Add(3*time.Second))
	}
}

// TestFakeClock_AfterFuncFiresOnAdvance verifies a callback fires only
// once its deadline is crossed, and that Now inside the callback equals
// the deadline.
func TestFakeClock_AfterFuncFiresOnAdvance(t *testing.T) {
	c := NewFakeClock(epoch)

	var fired bool
	var seenNow time.Time
	c.AfterFunc(100*time.Millisecond, func() {
		fired = true
		seenNow = c.Now()
	})

	c.Advance(50 * time.Millisecond)
	if fired {
		t.Fatalf("callback fired before its deadline")
	}
	c.Advance(50 * time.Millisecond)
	if !fired {
		t.Fatalf("callback did not fire at its deadline")
	}
	if want := epoch.Add(100 * time.Millisecond); !seenNow.Equal(want) {
		t.Fatalf("Now inside callback = %v, want deadline %v", seenNow, want)
	}
}

// TestFakeClock_FiresInDeadlineOrder verifies callbacks fire in deadline
// order regardless of arm order, with ties broken by arm order.
func TestFakeClock_FiresInDeadlineOrder(t *testing.T) {
	c := NewFakeClock(epoch)

	var order []int
	c.AfterFunc(30*time.Millisecond, func() { order = append(order, 3) })
	c.AfterFunc(10*time.Millisecond, func() { order = append(order, 1) })
	c.AfterFunc(20*time.Millisecond, func() { order = append(order, 2) })
	// Same deadline as the "2" entry: must fire after it (arm order).
	c.AfterFunc(20*time.Millisecond, func() { order = append(order, 4) })

	c.Advance(time.Second)

	want := []int{1, 2, 4, 3}
	if len(order) != len(want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
}

// TestFakeClock_StopPreventsFire verifies Stop cancels a pending timer
// and reports whether it was still pending.
func TestFakeClock_StopPreventsFire(t *testing.T) {
	c := NewFakeClock(epoch)

	var fired bool
	timer := c.AfterFunc(10*time.Millisecond, func() { fired = true })
	if !timer.Stop() {
		t.Fatalf("Stop on a pending timer should report true")
	}
	if timer.Stop() {
		t.Fatalf("second Stop should report false")
	}
	c.Advance(time.Second)
	if fired {
		t.Fatalf("stopped timer must not fire")
	}
}

// TestFakeClock_TickerFiresPerPeriod verifies a ticker delivers a tick
// each period and stops on Stop.
func TestFakeClock_TickerFiresPerPeriod(t *testing.T) {
	c := NewFakeClock(epoch)
	ticker := c.NewTicker(10 * time.Millisecond)

	ticks := 0
	for range 3 {
		c.Advance(10 * time.Millisecond)
		select {
		case got := <-ticker.C():
			ticks++
			if want := c.Now(); !got.Equal(want) {
				t.Fatalf("tick value = %v, want %v", got, want)
			}
		default:
			t.Fatalf("expected a tick after advancing one period")
		}
	}
	if ticks != 3 {
		t.Fatalf("ticks = %d, want 3", ticks)
	}

	ticker.Stop()
	if n := c.pendingCount(); n != 0 {
		t.Fatalf("pending after ticker Stop = %d, want 0", n)
	}
	c.Advance(50 * time.Millisecond)
	select {
	case <-ticker.C():
		t.Fatalf("stopped ticker must not deliver")
	default:
	}
}
