package hyparview

import (
	"sync"
	"time"

	"github.com/antonionduarte/protorun"
	"github.com/antonionduarte/protorun/transport"
)

// stateProbe is a test-only protocol co-located with a HyParView instance
// on the same runtime. It polls DebugState on a fast timer and keeps the
// latest snapshot under a mutex, so the test goroutine can read a node's
// active and passive views at a quiescent point without racing the
// event loop (the reply is delivered and stored on the probe's own loop;
// the mutex only guards the test-goroutine read).
type stateProbe struct {
	ctx protorun.ProtocolContext

	mu      sync.Mutex
	active  []transport.Host
	passive []transport.Host
}

func (s *stateProbe) Start(ctx protorun.ProtocolContext) { s.ctx = ctx }

func (s *stateProbe) Init(ctx protorun.ProtocolContext) {
	poll := func() {
		protorun.SendRequest(ctx, &DebugState{}, func(rep *DebugStateReply, err error) {
			if err != nil {
				return
			}
			s.mu.Lock()
			s.active, s.passive = rep.Active, rep.Passive
			s.mu.Unlock()
		})
	}
	poll()
	ctx.Every(250*time.Millisecond, poll)
}

func (s *stateProbe) snapshotActive() []transport.Host {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]transport.Host, len(s.active))
	copy(out, s.active)
	return out
}

func (s *stateProbe) snapshotPassive() []transport.Host {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]transport.Host, len(s.passive))
	copy(out, s.passive)
	return out
}

