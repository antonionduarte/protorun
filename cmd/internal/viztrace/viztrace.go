// Package viztrace is the shared cmd-internal glue between a running
// protorun node and the protoviz live server: the HTTP tracer a cluster
// process installs (NewHTTPTracer) and the TraceLine wire contract the
// server's /ingest handler decodes. It lives here, rather than in
// cmd/protoviz, because a main package can't be imported — cmd/broadcast
// needs the tracer and cmd/protoviz needs the wire shape.
package viztrace

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/antonionduarte/protorun/pkg/protorun"
	"github.com/antonionduarte/protorun/pkg/transport"
)

// TraceLine is the on-the-wire JSON an HTTPTracer POSTs to /ingest: one
// pkg/protorun TraceEvent rendered in string form (Peer as "ip:port", Wire
// as its registered type label). The server decodes into this shape and
// converts it to protoviz/1.
type TraceLine struct {
	Kind   string `json:"kind"`
	Peer   string `json:"peer,omitempty"`
	Wire   string `json:"wire,omitempty"`
	Bytes  int    `json:"bytes,omitempty"`
	Detail string `json:"detail,omitempty"`
	At     string `json:"at,omitempty"`
}

// ringCap bounds how many un-POSTed events an HTTPTracer holds. Past it the
// oldest are dropped: Trace must never block or grow without bound.
const ringCap = 8192

// flushEvery is the batch cadence: the background goroutine POSTs whatever
// has accumulated this often.
const flushEvery = 250 * time.Millisecond

// HTTPTracer is a protorun.Tracer that ships a runtime's trace events to a
// protoviz live server. It honors the Tracer contract — Trace is fast and
// never blocks — by buffering into a bounded, drop-oldest ring and letting
// a background goroutine POST batches to /ingest. A failed POST drops its
// batch: live tracing is best-effort and must not perturb the cluster.
//
// Wire it in with protorun.WithTracer(NewHTTPTracer(server, self)); call
// Close on shutdown to flush the final batch.
type HTTPTracer struct {
	endpoint string
	client   *http.Client

	mu   sync.Mutex
	ring [][]byte

	done   chan struct{}
	closed sync.Once
	wg     sync.WaitGroup
}

// NewHTTPTracer builds a tracer that POSTs to server's /ingest endpoint,
// tagging its stream with node. server is a base URL like
// "http://localhost:7777".
func NewHTTPTracer(server, node string) *HTTPTracer {
	endpoint := strings.TrimRight(server, "/") + "/ingest?node=" + url.QueryEscape(node)
	t := &HTTPTracer{
		endpoint: endpoint,
		client:   &http.Client{Timeout: 5 * time.Second},
		done:     make(chan struct{}),
	}
	t.wg.Add(1)
	go t.loop()
	return t
}

// Trace buffers ev for the next batch. Non-blocking: it marshals the event
// and appends to the ring, evicting the oldest entry when full.
//
//nolint:gocritic // ev is by value because the protorun.Tracer interface dictates it.
func (t *HTTPTracer) Trace(ev protorun.TraceEvent) {
	line, err := json.Marshal(ToTraceLine(&ev))
	if err != nil {
		return
	}
	t.mu.Lock()
	t.ring = append(t.ring, line)
	if len(t.ring) > ringCap {
		t.ring = t.ring[len(t.ring)-ringCap:]
	}
	t.mu.Unlock()
}

// Close stops the background poster after a final flush.
func (t *HTTPTracer) Close() {
	t.closed.Do(func() { close(t.done) })
	t.wg.Wait()
}

func (t *HTTPTracer) loop() {
	defer t.wg.Done()
	ticker := time.NewTicker(flushEvery)
	defer ticker.Stop()
	for {
		select {
		case <-t.done:
			t.flush()
			return
		case <-ticker.C:
			t.flush()
		}
	}
}

// flush POSTs the buffered batch as line-delimited JSONL. On any error the
// batch is dropped — best-effort by design.
func (t *HTTPTracer) flush() {
	t.mu.Lock()
	if len(t.ring) == 0 {
		t.mu.Unlock()
		return
	}
	batch := t.ring
	t.ring = nil
	t.mu.Unlock()

	var buf bytes.Buffer
	for _, line := range batch {
		buf.Write(line)
		buf.WriteByte('\n')
	}
	resp, err := t.client.Post(t.endpoint, "application/x-ndjson", &buf)
	if err != nil {
		return
	}
	_ = resp.Body.Close()
}

// ToTraceLine renders a protorun.TraceEvent into the JSON the server's
// /ingest handler expects: Peer as "ip:port" (omitted for a zero Host),
// Wire as its registered type label (omitted when there is none).
func ToTraceLine(ev *protorun.TraceEvent) TraceLine {
	line := TraceLine{
		Kind:   ev.Kind,
		Bytes:  ev.Bytes,
		Detail: ev.Detail,
	}
	if ev.Peer != (transport.Host{}) {
		line.Peer = ev.Peer.String()
	}
	if ev.Wire != 0 {
		line.Wire = WireLabel(ev.Wire)
	}
	if !ev.At.IsZero() {
		line.At = ev.At.Format("15:04:05.000")
	}
	return line
}

// WireLabel resolves a wire id to its registered human-readable type name,
// falling back to a hex rendering for an id no codec has registered.
func WireLabel(id uint64) string {
	if name, ok := protorun.WireNameOf(id); ok {
		return name
	}
	return fmt.Sprintf("0x%016x", id)
}
