package protorun

import (
	"sync"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/transport"
)

// recordingMetrics captures every Counter / Histogram call into
// in-memory slices so tests can assert what the runtime reported.
// Goroutine-safe because the runtime emits from many goroutines.
type recordingMetrics struct {
	mu         sync.Mutex
	counters   []recordedCounter
	histograms []recordedHistogram
}

type recordedCounter struct {
	Name  string
	Delta int64
	Attrs []Attr
}

type recordedHistogram struct {
	Name  string
	Value float64
	Attrs []Attr
}

func newRecordingMetrics() *recordingMetrics { return &recordingMetrics{} }

func (m *recordingMetrics) Counter(name string, delta int64, attrs ...Attr) {
	m.mu.Lock()
	m.counters = append(m.counters, recordedCounter{Name: name, Delta: delta, Attrs: attrs})
	m.mu.Unlock()
}

func (m *recordingMetrics) Histogram(name string, value float64, attrs ...Attr) {
	m.mu.Lock()
	m.histograms = append(m.histograms, recordedHistogram{Name: name, Value: value, Attrs: attrs})
	m.mu.Unlock()
}

func (m *recordingMetrics) totalCounter(name string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var total int64
	for _, c := range m.counters {
		if c.Name == name {
			total += c.Delta
		}
	}
	return total
}

func (m *recordingMetrics) histogramSamples(name string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, h := range m.histograms {
		if h.Name == name {
			n++
		}
	}
	return n
}

// TestMetrics_NoopDefault verifies the framework runs cleanly with
// no WithMetrics option supplied. The default noopMetrics drops
// everything. Proves no nil-deref, no init-order issue.
func TestMetrics_NoopDefault(t *testing.T) {
	self := transport.NewHost(0, "127.0.0.1")
	rt := New(self)
	_ = registerMockStack(rt, self)

	if err := rt.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	rt.Cancel()
}

// TestMetrics_RecordsIPC drives a same-runtime IPC round trip with
// recordingMetrics installed and asserts the sent / completed
// counters fired and the latency histogram captured one sample.
func TestMetrics_RecordsIPC(t *testing.T) {
	rec := newRecordingMetrics()

	self := transport.NewHost(0, "127.0.0.1")
	rt := New(self, WithMetrics(rec))
	_ = registerMockStack(rt, self)

	server := newProtoProtocol(&MockProtocol{}, 16)
	rt.registerProtocol(server)
	server.ensureContext()
	RegisterRequestHandler(server.ctx, func(_ *benchReq, r Responder[*benchRep]) {
		r.Reply(&benchRep{})
	})

	client := newProtoProtocol(&MockProtocol{}, 16)
	rt.registerProtocol(client)
	client.ensureContext()

	if err := rt.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer rt.Cancel()

	done := make(chan struct{})
	SendRequest(client.ctx, &benchReq{}, func(_ *benchRep, _ error) { close(done) })
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reply never arrived")
	}

	if got := rec.totalCounter("protorun.ipc.request.sent"); got != 1 {
		t.Errorf("ipc.request.sent: got %d, want 1", got)
	}
	if got := rec.totalCounter("protorun.ipc.request.completed"); got != 1 {
		t.Errorf("ipc.request.completed: got %d, want 1", got)
	}
	if got := rec.histogramSamples("protorun.ipc.request.latency_ms"); got != 1 {
		t.Errorf("ipc latency histogram: got %d samples, want 1", got)
	}
}

// TestMetrics_RecordsLatencyHistogram fires 100 SendRequests through
// recordingMetrics and asserts 100 latency samples landed.
func TestMetrics_RecordsLatencyHistogram(t *testing.T) {
	rec := newRecordingMetrics()

	self := transport.NewHost(0, "127.0.0.1")
	rt := New(self, WithMetrics(rec))
	_ = registerMockStack(rt, self)

	server := newProtoProtocol(&MockProtocol{}, 256)
	rt.registerProtocol(server)
	server.ensureContext()
	RegisterRequestHandler(server.ctx, func(_ *benchReq, r Responder[*benchRep]) {
		r.Reply(&benchRep{})
	})

	client := newProtoProtocol(&MockProtocol{}, 256)
	rt.registerProtocol(client)
	client.ensureContext()

	if err := rt.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer rt.Cancel()

	const total = 100
	done := make(chan struct{}, total)
	for range total {
		SendRequest(client.ctx, &benchReq{}, func(_ *benchRep, _ error) {
			done <- struct{}{}
		})
	}
	deadline := time.After(2 * time.Second)
	for range total {
		select {
		case <-done:
		case <-deadline:
			t.Fatal("not all replies arrived in time")
		}
	}

	if got := rec.histogramSamples("protorun.ipc.request.latency_ms"); got != total {
		t.Errorf("histogram samples: got %d, want %d", got, total)
	}
}

// TestMetrics_RecordsPanic verifies the handler.panic counter fires
// when a request handler panics.
func TestMetrics_RecordsPanic(t *testing.T) {
	rec := newRecordingMetrics()

	self := transport.NewHost(0, "127.0.0.1")
	rt := New(self, WithMetrics(rec))
	_ = registerMockStack(rt, self)

	server := newProtoProtocol(&MockProtocol{}, 16)
	rt.registerProtocol(server)
	server.ensureContext()
	RegisterRequestHandler(server.ctx, func(_ *benchReq, _ Responder[*benchRep]) {
		panic("boom")
	})

	client := newProtoProtocol(&MockProtocol{}, 16)
	rt.registerProtocol(client)
	client.ensureContext()

	if err := rt.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer rt.Cancel()

	done := make(chan struct{})
	SendRequest(client.ctx, &benchReq{}, func(_ *benchRep, _ error) { close(done) })
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected auto-fail to arrive after handler panic")
	}

	if got := rec.totalCounter("protorun.handler.panic"); got != 1 {
		t.Errorf("handler.panic: got %d, want 1", got)
	}
}
