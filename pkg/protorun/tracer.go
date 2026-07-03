package protorun

import (
	"time"

	"github.com/antonionduarte/protorun/pkg/transport"
)

// Tracer is the runtime's per-event observability hook, a sibling of
// Metrics: where Metrics reports aggregate counters and histograms, a
// Tracer receives one TraceEvent per interesting thing the runtime does
// (a message sent or delivered, a session lifecycle change, a
// supervision outcome, a dropped event). It exists to feed a live trace
// stream to protoviz (cmd/protoviz) without a protocol lifting a finger:
// the events are the same ones prototest's Sim recorder writes for
// post-run traces, so a live stream is format-compatible with a file.
//
// Plug an implementation in via WithTracer; the default is nil (no
// tracer), and every emit site guards on Runtime.tracerEnabled so a run
// with no tracer installed pays nothing — no event is built, no method
// is called. This mirrors the metrics fast path (see metrics.go): the
// interface call is cheap, but the TraceEvent literal and the Host /
// wire formatting that fill it escape to the heap, so the guard, not a
// no-op default, is what keeps the hot paths allocation-free.
//
// Trace is called SYNCHRONOUSLY on the goroutine that produced the event
// (a protocol event loop, the inbound dispatcher, a supervisor). An
// implementation MUST be fast and non-blocking: buffer and return, drop
// on overflow, never stall. The runtime does not protect itself from a
// slow Tracer beyond this contract — a blocking Trace backpressures the
// very path it is observing. cmd/protoviz's NewHTTPTracer honors this
// with a bounded drop-oldest ring and a background POSTer.
//
// Implementations MUST be safe for concurrent use: the runtime calls
// Trace from many goroutines.
type Tracer interface {
	Trace(ev TraceEvent)
}

// TraceEvent is one thing the runtime did, from THIS runtime's point of
// view — the local node is implicit (a live server tags each stream by
// node). It is deliberately flat and small so building one inside the
// tracerEnabled guard is a single struct literal.
//
// Field meaning depends on Kind:
//
//	Kind                    Peer            Wire   Bytes  Detail
//	"send"                  destination     wireID body   —
//	"deliver"               source          wireID body   —
//	"session-connected"     peer            —      —       —
//	"session-disconnected"  peer            —      —       —
//	"session-failed"        peer            —      —       —
//	"session-givenup"       peer            —      —       —
//	"restart"               —               —      —      protocol type
//	"stop"                  —               —      —      protocol type
//	"escalate"              —               —      —      protocol type
//	"dead-letter"           peer (if any)   —      —      "protocol/kind"
type TraceEvent struct {
	Kind   string
	Peer   transport.Host
	Wire   uint64
	Bytes  int
	Detail string
	At     time.Time
}

// WithTracer installs a Tracer and flips the tracerEnabled fast-path
// flag. Passing a nil Tracer (or omitting the option) leaves tracing off
// so the emit sites stay free.
func WithTracer(t Tracer) Option {
	return func(r *Runtime) {
		if t == nil {
			return
		}
		r.tracer = t
		r.tracerEnabled = true
	}
}

// trace stamps ev with the wall-clock instant and hands it to the
// installed tracer. Callers guard on r.tracerEnabled first so the
// TraceEvent literal is never built when tracing is off; this helper is
// only ever reached with a non-nil tracer. ev is taken by pointer only to
// avoid copying the (heavy) value into this helper — it is passed to the
// Tracer by value as the interface prescribes.
func (r *Runtime) trace(ev *TraceEvent) {
	ev.At = time.Now()
	r.tracer.Trace(*ev)
}

// traceSession emits a session-lifecycle TraceEvent, guarding on the
// fast-path flag itself so call sites stay a single branch-free statement
// (which keeps dispatchSessionEvent's cyclomatic complexity in check).
func (r *Runtime) traceSession(kind string, host transport.Host) {
	if r.tracerEnabled {
		r.trace(&TraceEvent{Kind: kind, Peer: host})
	}
}
