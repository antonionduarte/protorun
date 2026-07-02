package protorun

import (
	"testing"
	"time"

	"github.com/antonionduarte/protorun/transport"
)

type testTimer struct {
	id int
}

func (t *testTimer) TimerID() int { return t.id }

// TestCancelTimerStopsTimer schedules a timer and cancels it before it
// fires; nothing should arrive on the owning protocol's timerChannel.
func TestCancelTimerStopsTimer(t *testing.T) {
	rt := New(transport.NewHost(7400, "127.0.0.1"))
	owner := newProtoProtocol(&MockProtocol{}, 0)
	rt.registerProtocol(owner)

	timer := &testTimer{id: 1}
	rt.setupTimer(owner, timer, 10*time.Millisecond)
	rt.cancelTimer(timer.id)

	select {
	case <-owner.TimerChannel():
		t.Fatalf("expected timer to be cancelled and not fire")
	case <-time.After(30 * time.Millisecond):
	}
}
