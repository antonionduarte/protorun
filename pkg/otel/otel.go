// Package otel adapts protorun's Metrics interface onto an
// OpenTelemetry metric.Meter. It is a nested module (its own go.mod)
// so the core protorun module keeps its zero-dependency guarantee:
// only programs that opt into OpenTelemetry pull in
// go.opentelemetry.io/otel.
//
//	meter := otel.Meter("my-app") // from your chosen MeterProvider
//	rt := protorun.New(self, protorun.WithMetrics(otelmetrics.Metrics(meter)))
//
// slog, protorun's logging library, already has community OTel bridges
// (e.g. go.opentelemetry.io/contrib/bridges/otelslog) — this package
// covers the Metrics half only.
//
// # Instrument caching
//
// protorun.Metrics.Counter/Histogram are called by name on every hot
// path (message dispatch, IPC, mailbox events); creating an OTel
// instrument on every call would be both wasteful and against the
// OTel API's own guidance (instruments are meant to be created once
// and reused). Metrics resolves each distinct name to an instrument
// exactly once (guarded by sync.Once per name, cached in a sync.Map)
// and reuses it for every subsequent call with that name.
//
// # Errors never panic the metrics path
//
// If the underlying meter.Int64Counter / meter.Float64Histogram call
// returns an error (a misconfigured MeterProvider, an invalid name),
// the error is reported once, via OpenTelemetry's own global error
// handler (otel.Handle), and every subsequent Counter/Histogram call
// for that name becomes a no-op. Metrics never panics and never
// returns an error itself — matching protorun.Metrics's own
// fire-and-forget shape.
package otel

import (
	"context"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/antonionduarte/protorun/pkg/protorun"
)

// Metrics adapts meter into a protorun.Metrics. Pass it to
// protorun.WithMetrics.
func Metrics(meter metric.Meter) protorun.Metrics {
	return &otelMetrics{meter: meter}
}

type otelMetrics struct {
	meter metric.Meter

	// counters and histograms cache one instrument per distinct metric
	// name, keyed by name -> *counterEntry / *histogramEntry. Populated
	// lazily and only ever grows (metric names are a small, static set
	// the framework itself defines), so an unbounded sync.Map is fine.
	counters   sync.Map
	histograms sync.Map
}

type counterEntry struct {
	once sync.Once
	inst metric.Int64Counter // nil after a failed creation: permanent no-op
}

type histogramEntry struct {
	once sync.Once
	inst metric.Float64Histogram // nil after a failed creation: permanent no-op
}

func (m *otelMetrics) counter(name string) metric.Int64Counter {
	v, _ := m.counters.LoadOrStore(name, &counterEntry{})
	e, _ := v.(*counterEntry)
	e.once.Do(func() {
		inst, err := m.meter.Int64Counter(name)
		if err != nil {
			otel.Handle(fmt.Errorf("protorun/otel: create counter %q: %w", name, err))
			return
		}
		e.inst = inst
	})
	return e.inst
}

func (m *otelMetrics) histogram(name string) metric.Float64Histogram {
	v, _ := m.histograms.LoadOrStore(name, &histogramEntry{})
	e, _ := v.(*histogramEntry)
	e.once.Do(func() {
		inst, err := m.meter.Float64Histogram(name)
		if err != nil {
			otel.Handle(fmt.Errorf("protorun/otel: create histogram %q: %w", name, err))
			return
		}
		e.inst = inst
	})
	return e.inst
}

// Counter implements protorun.Metrics.
func (m *otelMetrics) Counter(name string, delta int64, attrs ...protorun.Attr) {
	inst := m.counter(name)
	if inst == nil {
		return
	}
	inst.Add(context.Background(), delta, metric.WithAttributes(toAttributes(attrs)...))
}

// Histogram implements protorun.Metrics.
func (m *otelMetrics) Histogram(name string, value float64, attrs ...protorun.Attr) {
	inst := m.histogram(name)
	if inst == nil {
		return
	}
	inst.Record(context.Background(), value, metric.WithAttributes(toAttributes(attrs)...))
}

// toAttributes maps protorun's structured Attr (a string key plus an
// any value) onto attribute.KeyValue. protorun's own attributes are
// always strings, bools, or ints (see the Attr key catalogue on
// protorun.Metrics); the fmt.Stringer and default branches are a safety
// net for callers of protorun.Metrics that pass other Go types.
func toAttributes(attrs []protorun.Attr) []attribute.KeyValue {
	if len(attrs) == 0 {
		return nil
	}
	kvs := make([]attribute.KeyValue, len(attrs))
	for i, a := range attrs {
		kvs[i] = toAttribute(a)
	}
	return kvs
}

func toAttribute(a protorun.Attr) attribute.KeyValue {
	switch v := a.Value.(type) {
	case string:
		return attribute.String(a.Key, v)
	case bool:
		return attribute.Bool(a.Key, v)
	case int:
		return attribute.Int(a.Key, v)
	case int64:
		return attribute.Int64(a.Key, v)
	case float64:
		return attribute.Float64(a.Key, v)
	case fmt.Stringer:
		return attribute.String(a.Key, v.String())
	default:
		return attribute.String(a.Key, fmt.Sprintf("%v", v))
	}
}
