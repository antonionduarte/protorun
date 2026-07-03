package main

import (
	"bufio"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// hub is the fan-out core of the live server: a bounded replay ring plus a
// set of connected SSE clients. Every trace line — whether normalized from
// an /ingest POST or paced out of a -replay file — is handed to publish,
// which appends it to the ring (evicting the oldest past the cap) and
// forwards it to every live client. A newly connected client is first sent
// the whole current ring, then the live tail, with no gap and no duplicate
// (subscribe registers the client and snapshots the ring under one lock, so
// any publish is wholly before or wholly after that critical section).
//
// The lock is held across the non-blocking client sends, which is safe
// because a slow client's send hits the select default and drops rather
// than stalling the publisher — tracing is lossy by design so it can never
// backpressure a real cluster.
type hub struct {
	mu      sync.Mutex
	ring    [][]byte
	ringMax int
	clients map[*sseClient]struct{}
	step    uint64
}

// sseClient is one connected /events reader. Its channel is buffered so a
// briefly busy reader does not immediately drop; past the buffer, publish
// drops lines for that client rather than blocking everyone.
type sseClient struct {
	ch chan []byte
}

func newHub(ringMax int) *hub {
	if ringMax <= 0 {
		ringMax = 1
	}
	return &hub{
		ringMax: ringMax,
		clients: make(map[*sseClient]struct{}),
	}
}

// publish records line in the replay ring and forwards it to every client.
func (h *hub) publish(line []byte) {
	h.mu.Lock()
	h.ring = append(h.ring, line)
	if len(h.ring) > h.ringMax {
		h.ring = h.ring[len(h.ring)-h.ringMax:]
	}
	for c := range h.clients {
		select {
		case c.ch <- line:
		default:
			// Slow client: drop this line for it. Lossy by design.
		}
	}
	h.mu.Unlock()
}

// nextStep returns the server-side monotonic step used to give ingested
// events (interleaved from many nodes) a single total order for the
// viewer's fold.
func (h *hub) nextStep() uint64 {
	h.mu.Lock()
	h.step++
	s := h.step
	h.mu.Unlock()
	return s
}

// subscribe registers a new client and returns it together with a snapshot
// of the current ring to replay before the live tail.
func (h *hub) subscribe() (*sseClient, [][]byte) {
	c := &sseClient{ch: make(chan []byte, 1024)}
	h.mu.Lock()
	snap := make([][]byte, len(h.ring))
	copy(snap, h.ring)
	h.clients[c] = struct{}{}
	h.mu.Unlock()
	return c, snap
}

func (h *hub) unsubscribe(c *sseClient) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
}

// heartbeatInterval bounds how long an idle SSE connection goes without a
// byte, so proxies and the browser keep it open.
const heartbeatInterval = 15 * time.Second

// handleEvents streams the replay ring then the live tail as Server-Sent
// Events. One-way streaming needs nothing more than SSE — no websocket.
func (h *hub) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	c, snap := h.subscribe()
	defer h.unsubscribe(c)

	for _, line := range snap {
		writeSSE(w, line)
	}
	flusher.Flush()

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case line := <-c.ch:
			writeSSE(w, line)
			flusher.Flush()
		case <-ticker.C:
			_, _ = fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		}
	}
}

// writeSSE writes one trace line as an SSE data frame.
func writeSSE(w http.ResponseWriter, line []byte) {
	// A trace line is a single JSON object with no embedded newline, so a
	// one-line data frame is always well-formed.
	_, _ = fmt.Fprintf(w, "data: %s\n\n", line)
}

// handleIngest accepts a line-delimited JSONL stream pushed by a cluster
// process's HTTP tracer, tags each line with the ?node= param and a
// server-side step, converts pkg/protorun TraceEvent kinds into the
// protoviz/1 kinds the viewer parses, and publishes the result.
func (h *hub) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	node := r.URL.Query().Get("node")
	if node == "" {
		node = "unknown"
	}
	scanner := bufio.NewScanner(r.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		raw := scanner.Bytes()
		if len(raw) == 0 {
			continue
		}
		out, ok := normalizeIngest(raw, node, h.nextStep())
		if ok {
			h.publish(out)
		}
	}
	w.WriteHeader(http.StatusNoContent)
}
