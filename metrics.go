package protorun

// Metrics is the observability hook the runtime uses to report
// counters and histograms about its own behavior. Plug an
// implementation in via WithMetrics; the default is a no-op so the
// hot paths pay nothing for instrumentation that no one is consuming.
//
// Counters are for cumulative monotone counts (messages dispatched,
// sessions established, panics recovered). Histograms are for
// distributions where the tail matters (IPC request latency, ...).
//
// Implementations MUST be safe for concurrent use: the runtime calls
// Metrics methods from many goroutines (per-protocol event loops,
// transport pumps, retry timers, IPC dispatch).
//
// Attribute keys the runtime emits today:
//
//	wireID    the message / IPC type, formatted as %#x.
//	protocol  concrete protocol Go type, formatted as %T.
//	host      peer Host involved, formatted via fmt.Stringer.
//	where     panic call-site tag ("message handler", ...).
//	result    terminal status of an IPC request: "completed",
//	          "timeout", "no_handler", "responder_failed".
//
// Names use the prefix "protorun." for everything the framework
// emits so users can route them with a single label/regex.
type Metrics interface {
	Counter(name string, delta int64, attrs ...Attr)
	Histogram(name string, value float64, attrs ...Attr)
}

// Attr is a structured attribute attached to a metric sample. Maps
// 1:1 onto Prometheus labels or OpenTelemetry attributes.
type Attr struct {
	Key   string
	Value any
}

// WithMetrics replaces the runtime's default no-op Metrics with the
// supplied implementation. Pass a nil value (or omit the option
// entirely) to leave metrics disabled; the no-op default is cheap.
func WithMetrics(m Metrics) Option {
	return func(r *Runtime) {
		if m == nil {
			return
		}
		r.metrics = m
	}
}

// noopMetrics is the zero-cost default Metrics implementation. Picked
// up by New when no WithMetrics option is supplied. Methods are
// inlinable, so a no-op runtime pays roughly nothing per Counter call.
type noopMetrics struct{}

func (noopMetrics) Counter(_ string, _ int64, _ ...Attr)     {}
func (noopMetrics) Histogram(_ string, _ float64, _ ...Attr) {}
