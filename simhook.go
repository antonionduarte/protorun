package protorun

import (
	"bytes"

	"github.com/antonionduarte/protorun/transport"
)

// This file holds the small, clearly-scoped introspection and delivery
// surface the deterministic-simulation harness (prototest.Sim) needs
// from the core. Everything here is for test harnesses and diagnostics;
// production code and production Sessions adapters neither implement nor
// call it.

// InboundSink receives inbound traffic that a Sessions adapter chooses
// to deliver synchronously, in place of the async OutMessages /
// OutChannelEvents pumps the runtime would otherwise run. The runtime
// hands one to any Sessions adapter that implements SyncDeliverer; the
// adapter (prototest's mesh under a Sim) calls these on the goroutine of
// its choosing, so delivery is a plain synchronous call and the runtime
// reaches quiescence observably after each one.
//
// The methods mirror what the async pumps do: DeliverMessage is the
// processMessage path (decode wire id, route, enqueue), DeliverSessionEvent
// is the dispatchSessionEvent path (retry bookkeeping + fan-out).
type InboundSink interface {
	DeliverMessage(payload bytes.Buffer, from transport.Host)
	DeliverSessionEvent(ev transport.SessionEvent)
}

// SyncDeliverer is an optional capability a Sessions adapter can
// implement to take over inbound delivery. At start the runtime offers
// the adapter an InboundSink; if UseSyncInbound returns true, the runtime
// installs that sink as the sole inbound path and does not start its
// async pump goroutines. Returning false leaves the async pumps in place
// (the adapter may still keep the sink unused).
//
// Intended solely for deterministic-simulation harnesses. The production
// Sessions adapter (transport.SessionLayer) does not implement it, so the
// async path stays the only path in production.
type SyncDeliverer interface {
	UseSyncInbound(sink InboundSink) bool
}

// runtimeInboundSink adapts a Runtime to InboundSink, exposing the two
// inbound entry points the async pumps use.
type runtimeInboundSink struct{ r *Runtime }

func (s runtimeInboundSink) DeliverMessage(payload bytes.Buffer, from transport.Host) {
	s.r.processMessage(payload, from)
}

func (s runtimeInboundSink) DeliverSessionEvent(ev transport.SessionEvent) {
	// Ignore the ctx-cancelled return: a simulation never advances after
	// shutdown, and the real fan-out result is observed via mailboxes.
	s.r.dispatchSessionEvent(s.r.ctx, ev)
}

// installSyncInbound offers the Sessions adapter a synchronous inbound
// sink and records whether it accepted. Called once at start.
func (r *Runtime) installSyncInbound() {
	sd, ok := r.sessionLayer.(SyncDeliverer)
	if !ok {
		return
	}
	if sd.UseSyncInbound(runtimeInboundSink{r: r}) {
		r.syncInbound = true
	}
}

// Quiescent reports whether the runtime has fully settled: every live
// protocol has zero events in flight — nothing queued in a mailbox and
// nothing being dispatched. It exists for deterministic test harnesses
// (prototest.Sim polls it between scheduling decisions) and for
// diagnostics; production code should not build behavior on it.
//
// Memory-model contract. Each protocol keeps an inFlight counter that a
// producer increments BEFORE pushing an event onto the mailbox and the
// event loop decrements AFTER the handler returns (see protocol.go).
// Because the increment precedes the push, there is no instant at which
// an event is live yet uncounted; because the decrement follows dispatch,
// the count stays positive until the handler is truly done. sync/atomic
// operations are sequentially consistent in Go, so a Quiescent reader on
// another goroutine that observes zero for a protocol has observed all of
// that protocol's increments matched by decrements — i.e. no event is
// queued or mid-dispatch.
//
// This makes Quiescent sound as a settle probe ONLY when every enqueue
// into a protocol's mailbox happens either from the harness's own
// (counted) delivery or from within a handler that is itself still
// in-flight (its own increment not yet balanced). The Sim guarantees
// exactly that: cross-node sends are captured by the mesh scheduler
// rather than pushed straight into a peer mailbox, and the runtime's
// async pumps are turned off under a Sim. A caller polling Quiescent must
// yield (runtime.Gosched) between reads to let event loops run.
func (r *Runtime) Quiescent() bool {
	for _, p := range r.snapshotProtocols() {
		if p.inFlight.Load() != 0 {
			return false
		}
	}
	return true
}
