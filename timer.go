package protorun

import (
	"context"
	"time"
)

// Timer represents a logical timer scheduled through the runtime. The
// TimerID must be stable and unique within the protocol that scheduled
// it. Protocols don't share timer IDs, since timers are routed back to
// the owning protocol via the ProtocolContext that scheduled them.
// Reusing the same TimerID inside a single protocol replaces the
// earlier timer.
type Timer interface {
	TimerID() int
}

// shutdownCtx returns the runtime's shutdown context, falling back to
// context.Background() if Start has not yet been called.
func (r *Runtime) shutdownCtx() context.Context {
	r.timerMutex.Lock()
	ctx := r.ctx
	r.timerMutex.Unlock()
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// setupTimer schedules a single-shot timer that fires after duration and
// pushes the Timer onto the owning protoProtocol's timerChannel.
func (r *Runtime) setupTimer(owner *protoProtocol, timer Timer, duration time.Duration) {
	ctx := r.shutdownCtx()
	goTimer := time.AfterFunc(duration, func() {
		r.timerMutex.Lock()
		delete(r.ongoingTimers, timer.TimerID())
		r.timerMutex.Unlock()
		select {
		case owner.timerChannel <- timer:
		case <-ctx.Done():
		}
	})
	r.timerMutex.Lock()
	if existing, ok := r.ongoingTimers[timer.TimerID()]; ok && existing != nil {
		existing.Stop()
	}
	r.ongoingTimers[timer.TimerID()] = goTimer
	r.timerMutex.Unlock()
}

func (r *Runtime) cancelTimer(timerID int) {
	r.timerMutex.Lock()
	if goTimer, ok := r.ongoingTimers[timerID]; ok && goTimer != nil {
		goTimer.Stop()
		delete(r.ongoingTimers, timerID)
	}
	if cancel, ok := r.ongoingPeriodicTimers[timerID]; ok && cancel != nil {
		cancel()
		delete(r.ongoingPeriodicTimers, timerID)
	}
	r.timerMutex.Unlock()
}

// setupPeriodicTimer schedules a recurring timer that fires every duration
// and pushes the Timer onto the owning protoProtocol's timerChannel.
func (r *Runtime) setupPeriodicTimer(owner *protoProtocol, timer Timer, duration time.Duration) {
	parent := r.shutdownCtx()
	ctx, cancel := context.WithCancel(parent)

	r.timerMutex.Lock()
	if existing, ok := r.ongoingPeriodicTimers[timer.TimerID()]; ok && existing != nil {
		existing()
	}
	r.ongoingPeriodicTimers[timer.TimerID()] = cancel
	r.timerMutex.Unlock()

	r.wg.Go(func() {
		defer cancel()
		ticker := time.NewTicker(duration)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				select {
				case owner.timerChannel <- timer:
				case <-ctx.Done():
					return
				}
			}
		}
	})
}
