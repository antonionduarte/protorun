package otel_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/antonionduarte/protorun"
	protorunotel "github.com/antonionduarte/protorun/otel"
)

func newTestMeter(t *testing.T) (protorun.Metrics, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	meter := provider.Meter("protorun/otel/test")
	return protorunotel.Metrics(meter), reader
}

func collect(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	return rm
}

func findMetric(rm metricdata.ResourceMetrics, name string) (metricdata.Metrics, bool) {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m, true
			}
		}
	}
	return metricdata.Metrics{}, false
}

func TestMetrics_CounterAdds(t *testing.T) {
	m, reader := newTestMeter(t)

	m.Counter("protorun.message.dispatched", 1, protorun.Attr{Key: "wireID", Value: "0x1"})
	m.Counter("protorun.message.dispatched", 2, protorun.Attr{Key: "wireID", Value: "0x1"})

	rm := collect(t, reader)
	got, ok := findMetric(rm, "protorun.message.dispatched")
	if !ok {
		t.Fatalf("metric protorun.message.dispatched not found in %+v", rm)
	}

	sum, ok := got.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("got.Data = %T, want metricdata.Sum[int64]", got.Data)
	}
	if len(sum.DataPoints) != 1 {
		t.Fatalf("len(DataPoints) = %d, want 1 (same attrs should collapse)", len(sum.DataPoints))
	}
	if sum.DataPoints[0].Value != 3 {
		t.Errorf("DataPoints[0].Value = %d, want 3", sum.DataPoints[0].Value)
	}
}

func TestMetrics_HistogramRecordsWithAttributes(t *testing.T) {
	m, reader := newTestMeter(t)

	m.Histogram("protorun.ipc.request.latency", 1.5, protorun.Attr{Key: "result", Value: "completed"})
	m.Histogram("protorun.ipc.request.latency", 2.5, protorun.Attr{Key: "result", Value: "completed"})

	rm := collect(t, reader)
	got, ok := findMetric(rm, "protorun.ipc.request.latency")
	if !ok {
		t.Fatalf("metric protorun.ipc.request.latency not found in %+v", rm)
	}

	hist, ok := got.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("got.Data = %T, want metricdata.Histogram[float64]", got.Data)
	}
	if len(hist.DataPoints) != 1 {
		t.Fatalf("len(DataPoints) = %d, want 1", len(hist.DataPoints))
	}
	dp := hist.DataPoints[0]
	if dp.Count != 2 {
		t.Errorf("Count = %d, want 2", dp.Count)
	}
	if dp.Sum != 4.0 {
		t.Errorf("Sum = %v, want 4.0", dp.Sum)
	}

	want := attribute.NewSet(attribute.String("result", "completed"))
	if !dp.Attributes.Equals(&want) {
		t.Errorf("Attributes = %v, want %v", dp.Attributes.ToSlice(), want.ToSlice())
	}
}

func TestMetrics_DistinctNamesAreDistinctInstruments(t *testing.T) {
	m, reader := newTestMeter(t)

	m.Counter("protorun.session.connected", 1)
	m.Counter("protorun.session.disconnected", 1)

	rm := collect(t, reader)
	if _, ok := findMetric(rm, "protorun.session.connected"); !ok {
		t.Error("protorun.session.connected missing")
	}
	if _, ok := findMetric(rm, "protorun.session.disconnected"); !ok {
		t.Error("protorun.session.disconnected missing")
	}
}

func TestMetrics_NoAttrsIsValid(t *testing.T) {
	m, reader := newTestMeter(t)

	m.Counter("protorun.handler.panic", 1)

	rm := collect(t, reader)
	got, ok := findMetric(rm, "protorun.handler.panic")
	if !ok {
		t.Fatalf("metric protorun.handler.panic not found")
	}
	sum, ok := got.Data.(metricdata.Sum[int64])
	if !ok || len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 1 {
		t.Errorf("got = %+v, want a single data point with value 1", got.Data)
	}
}
