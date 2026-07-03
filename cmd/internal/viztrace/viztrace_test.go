package viztrace

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/protorun"
	"github.com/antonionduarte/protorun/pkg/transport"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// TestToTraceLine renders each field group correctly and omits a zero peer.
func TestToTraceLine(t *testing.T) {
	peer := transport.NewHost(6001, "10.0.0.2")
	line := ToTraceLine(&protorun.TraceEvent{Kind: "deliver", Peer: peer, Bytes: 42, At: time.Now()})
	if line.Kind != "deliver" || line.Peer != "10.0.0.2:6001" || line.Bytes != 42 {
		t.Errorf("deliver line wrong: %+v", line)
	}
	// A lifecycle event has no peer: the zero Host must not render.
	lc := ToTraceLine(&protorun.TraceEvent{Kind: "restart", Detail: "raft.Raft"})
	if lc.Peer != "" {
		t.Errorf("zero peer should render empty, got %q", lc.Peer)
	}
	if lc.Detail != "raft.Raft" {
		t.Errorf("detail: got %q", lc.Detail)
	}
}

// TestHTTPTracer_PostsBatches drives events through the tracer and asserts
// the background poster delivers them to a fake /ingest endpoint as JSONL.
func TestHTTPTracer_PostsBatches(t *testing.T) {
	var (
		mu    sync.Mutex
		lines []TraceLine
		node  string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		node = r.URL.Query().Get("node")
		sc := bufio.NewScanner(r.Body)
		for sc.Scan() {
			var l TraceLine
			if err := json.Unmarshal(sc.Bytes(), &l); err == nil {
				lines = append(lines, l)
			}
		}
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	tr := NewHTTPTracer(srv.URL, "10.0.0.1:6000")
	tr.Trace(protorun.TraceEvent{Kind: "send", Peer: transport.NewHost(6001, "10.0.0.2")})
	tr.Trace(protorun.TraceEvent{Kind: "session-connected", Peer: transport.NewHost(6001, "10.0.0.2")})
	tr.Close() // flushes the final batch

	mu.Lock()
	defer mu.Unlock()
	if node != "10.0.0.1:6000" {
		t.Errorf("node tag: got %q", node)
	}
	if len(lines) != 2 {
		t.Fatalf("posted lines: got %d, want 2", len(lines))
	}
	if lines[0].Kind != "send" || lines[1].Kind != "session-connected" {
		t.Errorf("wrong kinds: %+v", lines)
	}
}

// TestHTTPTracer_TraceNeverBlocks fills far past the ring capacity; Trace
// must stay non-blocking and simply drop the oldest.
func TestHTTPTracer_TraceNeverBlocks(t *testing.T) {
	tr := &HTTPTracer{done: make(chan struct{})} // no poster goroutine
	for range ringCap * 2 {
		tr.Trace(protorun.TraceEvent{Kind: "send"})
	}
	tr.mu.Lock()
	n := len(tr.ring)
	tr.mu.Unlock()
	if n > ringCap {
		t.Errorf("ring exceeded cap: %d > %d", n, ringCap)
	}
}
