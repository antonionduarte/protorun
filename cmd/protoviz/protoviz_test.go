package main

import (
	"encoding/json"
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// decode unmarshals a normalized line into a generic map for assertions.
func decode(t *testing.T, line []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(line, &m); err != nil {
		t.Fatalf("normalized line is not JSON: %v\n%s", err, line)
	}
	return m
}

// TestNormalizeIngest_Deliver checks a receiver-side deliver becomes an
// authoritative {kind:deliver, from:peer, to:node}.
func TestNormalizeIngest_Deliver(t *testing.T) {
	raw := []byte(`{"kind":"deliver","peer":"10.0.0.2:6001","wire":"plumtree.Gossip","bytes":74,"at":"12:00:00.000"}`)
	line, ok := normalizeIngest(raw, "10.0.0.1:6000", 7)
	if !ok {
		t.Fatal("normalizeIngest returned ok=false")
	}
	m := decode(t, line)
	if m["kind"] != "deliver" || m["from"] != "10.0.0.2:6001" || m["to"] != "10.0.0.1:6000" {
		t.Errorf("deliver mapping wrong: %v", m)
	}
	if m["step"].(float64) != 7 {
		t.Errorf("step: got %v, want 7", m["step"])
	}
	if m["wire"] != "plumtree.Gossip" {
		t.Errorf("wire: got %v", m["wire"])
	}
}

// TestNormalizeIngest_Send keeps sends as kind "send" (from this node to the
// peer) so the topology/sequence lenses can ignore them but nothing is lost.
func TestNormalizeIngest_Send(t *testing.T) {
	raw := []byte(`{"kind":"send","peer":"10.0.0.2:6001","wire":"plumtree.Gossip","bytes":74}`)
	line, ok := normalizeIngest(raw, "10.0.0.1:6000", 1)
	if !ok {
		t.Fatal("ok=false")
	}
	m := decode(t, line)
	if m["kind"] != "send" || m["from"] != "10.0.0.1:6000" || m["to"] != "10.0.0.2:6001" {
		t.Errorf("send mapping wrong: %v", m)
	}
}

// TestNormalizeIngest_Session maps the runtime's session-* kinds onto the
// viewer's {kind:session, event, node, peer}.
func TestNormalizeIngest_Session(t *testing.T) {
	cases := map[string]string{
		"session-connected":    "connected",
		"session-disconnected": "disconnected",
		"session-failed":       "failed",
		"session-givenup":      "failed",
	}
	for kind, want := range cases {
		raw := []byte(`{"kind":"` + kind + `","peer":"10.0.0.2:6001"}`)
		line, ok := normalizeIngest(raw, "10.0.0.1:6000", 1)
		if !ok {
			t.Fatalf("%s: ok=false", kind)
		}
		m := decode(t, line)
		if m["kind"] != "session" || m["event"] != want || m["peer"] != "10.0.0.2:6001" || m["node"] != "10.0.0.1:6000" {
			t.Errorf("%s: mapping wrong: %v", kind, m)
		}
	}
}

// TestNormalizeIngest_BadLine rejects unparseable input and unknown kinds.
func TestNormalizeIngest_BadLine(t *testing.T) {
	if _, ok := normalizeIngest([]byte("not json"), "n", 1); ok {
		t.Error("expected ok=false for bad JSON")
	}
	if _, ok := normalizeIngest([]byte(`{"kind":"mystery"}`), "n", 1); ok {
		t.Error("expected ok=false for unknown kind")
	}
}

// TestHub_ReplayThenLive proves a subscriber gets the current ring first,
// then live publishes, with no gap or duplicate.
func TestHub_ReplayThenLive(t *testing.T) {
	h := newHub(10)
	h.publish([]byte("a"))
	h.publish([]byte("b"))

	c, snap := h.subscribe()
	defer h.unsubscribe(c)
	if len(snap) != 2 || string(snap[0]) != "a" || string(snap[1]) != "b" {
		t.Fatalf("replay snapshot wrong: %q", snap)
	}

	h.publish([]byte("c"))
	select {
	case line := <-c.ch:
		if string(line) != "c" {
			t.Errorf("live line: got %q, want c", line)
		}
	default:
		t.Error("expected live line c on the channel")
	}
}

// TestHub_RingEvicts caps the ring at its max, dropping the oldest.
func TestHub_RingEvicts(t *testing.T) {
	h := newHub(2)
	h.publish([]byte("a"))
	h.publish([]byte("b"))
	h.publish([]byte("c"))
	_, snap := h.subscribe()
	if len(snap) != 2 || string(snap[0]) != "b" || string(snap[1]) != "c" {
		t.Errorf("ring did not evict oldest: %q", snap)
	}
}

// TestHub_NextStepMonotonic checks the server-side total-order counter.
func TestHub_NextStepMonotonic(t *testing.T) {
	h := newHub(10)
	if a, b := h.nextStep(), h.nextStep(); a != 1 || b != 2 {
		t.Errorf("nextStep not monotonic from 1: %d, %d", a, b)
	}
}
