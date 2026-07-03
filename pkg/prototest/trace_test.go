package prototest

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/transport"
)

// runFlood builds the standard flood ring, converges a broadcast, and
// returns the concatenated per-node protocol trace — the same delivery-
// order witness the determinism tests use. Extra options let a caller
// attach a recorder. The protocol trace is a pure function of the schedule,
// so it is unchanged by an observing recorder.
//
// It drives the Sim through a stub TB and runs the harness cleanups before
// returning, so the recorder's buffered writer is flushed and a caller can
// read a WithTrace buffer immediately (a real test would read it only after
// its own Cleanup ran).
func runFlood(t *testing.T, seed int64, opts ...Option) string {
	t.Helper()
	stub := &stubTB{name: t.Name()}
	all := append([]Option{WithSeed(seed)}, opts...)
	sim := NewSim(stub, all...)
	const n = 6
	protos := make([]*floodProtocol, n)
	for i := range n {
		left := floodHost((i - 1 + n) % n)
		right := floodHost((i + 1) % n)
		p := newFloodProtocol(floodHost(i), left, right)
		protos[i] = p
		sim.Node(floodHost(i), p)
	}
	sim.Run(time.Second)
	protos[0].Broadcast(42, 16)
	sim.RunUntil(func() bool {
		for _, p := range protos {
			if !p.hasDelivered(42) {
				return false
			}
		}
		return true
	}, 30*time.Second)
	sim.Run(2 * time.Second)

	var out strings.Builder
	for i, p := range protos {
		out.WriteString("=== node ")
		out.WriteByte(byte('0' + i))
		out.WriteString(" ===\n")
		out.WriteString(traceString(p))
	}
	stub.runCleanups() // flush the recorder and shut the runtimes down
	if stub.failed {
		t.Fatal("flood run reported a harness failure")
	}
	return out.String()
}

// TestTrace_DoesNotPerturbSchedule is the recorder's core invariant: a run
// observed by a recorder delivers exactly the same schedule as an untraced
// run of the same seed, and two traced runs emit byte-identical JSONL. If
// tracing perturbed the schedule (extra sends, altered RNG draws) either
// assertion would fail.
func TestTrace_DoesNotPerturbSchedule(t *testing.T) {
	const seed = 0x7ACE

	plain := runFlood(t, seed)

	var buf1, buf2 bytes.Buffer
	traced1 := runFlood(t, seed, WithTrace(&buf1))
	traced2 := runFlood(t, seed, WithTrace(&buf2))

	if plain != traced1 {
		t.Fatalf("tracing perturbed the schedule:\n--- untraced ---\n%s\n--- traced ---\n%s", plain, traced1)
	}
	if traced1 != traced2 {
		t.Fatal("two traced runs produced different protocol traces")
	}
	if buf1.String() != buf2.String() {
		t.Fatal("same-seed traces are not byte-identical")
	}
	if buf1.Len() == 0 {
		t.Fatal("empty trace")
	}
}

// TestTrace_WellFormed checks the emitted stream is a valid protoviz/1
// trace: meta first, every line JSON, the core event kinds present, and
// message wire ids resolved to type names (not hex fallbacks).
func TestTrace_WellFormed(t *testing.T) {
	var buf bytes.Buffer
	runFlood(t, 0xF00D, WithTrace(&buf))

	kinds := map[string]int{}
	var first map[string]any
	scan := bufio.NewScanner(&buf)
	scan.Buffer(make([]byte, 0, 1<<20), 1<<20)
	line := 0
	sawWire := false
	for scan.Scan() {
		var ev map[string]any
		if err := json.Unmarshal(scan.Bytes(), &ev); err != nil {
			t.Fatalf("line %d not valid JSON: %v", line+1, err)
		}
		if line == 0 {
			first = ev
		}
		if k, _ := ev["kind"].(string); k != "" {
			kinds[k]++
		}
		if w, ok := ev["wire"].(string); ok {
			sawWire = true
			if strings.HasPrefix(w, "0x") {
				t.Errorf("unresolved wire id %q — WireNameOf reverse mapping not populated?", w)
			}
		}
		line++
	}
	if first["kind"] != "meta" || first["format"] != "protoviz/1" {
		t.Errorf("first line is not a protoviz/1 meta header: %v", first)
	}
	for _, want := range []string{"node", "session", "deliver", "clock"} {
		if kinds[want] == 0 {
			t.Errorf("missing %q events", want)
		}
	}
	if !sawWire {
		t.Error("no deliver carried a wire label")
	}
}

// TestTrace_StateSampling checks that WithTraceSampler + WithTraceStateEvery
// produce state events. The sampler here is trivial (constant state) — the
// point is the cadence hook, not a real protocol probe.
func TestTrace_StateSampling(t *testing.T) {
	var buf bytes.Buffer
	sampler := func(_ *Sim, emit func(transport.Host, string, any)) {
		emit(floodHost(0), "flood", map[string]int{"seen": 1})
	}
	runFlood(t, 0x5A3, WithTrace(&buf), WithTraceStateEvery(5), WithTraceSampler(sampler))

	states := 0
	scan := bufio.NewScanner(&buf)
	scan.Buffer(make([]byte, 0, 1<<20), 1<<20)
	for scan.Scan() {
		var ev map[string]any
		if json.Unmarshal(scan.Bytes(), &ev) == nil && ev["kind"] == "state" {
			states++
		}
	}
	if states == 0 {
		t.Fatal("no state events despite a sampler and cadence")
	}
}

// stubTB is a minimal prototest.TB for exercising the recorder outside a
// real test — here, the on-failure artifact path, which keys off Failed().
type stubTB struct {
	name     string
	failed   bool
	cleanups []func()
}

func (s *stubTB) Helper()               {}
func (s *stubTB) Name() string          { return s.name }
func (s *stubTB) Logf(string, ...any)   {}
func (s *stubTB) Errorf(string, ...any) { s.failed = true }
func (s *stubTB) Fatalf(string, ...any) { s.failed = true }
func (s *stubTB) Failed() bool          { return s.failed }
func (s *stubTB) Cleanup(fn func())     { s.cleanups = append(s.cleanups, fn) }
func (s *stubTB) runCleanups() {
	for _, fn := range slices.Backward(s.cleanups) {
		fn()
	}
}

// TestTrace_OnFailureArtifact drives the on-failure recorder with a stub TB
// reporting failure, then runs the harness cleanups and asserts a trace
// artifact was written to PROTOTEST_TRACE_DIR and is a valid protoviz/1
// stream. This is the failure-wiring mechanics; a real test wires it with
// WithTraceOnFailure() and the harness's own Cleanup/Failed.
func TestTrace_OnFailureArtifact(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PROTOTEST_TRACE_DIR", dir)

	stub := &stubTB{name: "OnFailureCase"}
	sim := NewSim(stub, WithSeed(1), WithTraceOnFailure())
	a, b := floodHost(0), floodHost(1)
	pa := newFloodProtocol(a, b)
	pb := newFloodProtocol(b, a)
	sim.Node(a, pa)
	sim.Node(b, pb)
	sim.Run(2 * time.Second)

	stub.failed = true // simulate a failed test
	stub.runCleanups() // flush + write the artifact + shut runtimes down

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 artifact, got %d", len(entries))
	}
	data, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	if err != nil {
		t.Fatalf("read artifact: %v", err)
	}
	firstLine, _, _ := bytes.Cut(data, []byte("\n"))
	var meta map[string]any
	if err := json.Unmarshal(firstLine, &meta); err != nil {
		t.Fatalf("artifact first line not JSON: %v", err)
	}
	if meta["kind"] != "meta" || meta["format"] != "protoviz/1" {
		t.Errorf("artifact is not a protoviz/1 trace: %v", meta)
	}
}

// TestTrace_OnFailureSuppressedOnSuccess confirms the on-failure recorder
// writes nothing when the test did not fail.
func TestTrace_OnFailureSuppressedOnSuccess(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PROTOTEST_TRACE_DIR", dir)

	stub := &stubTB{name: "SuccessCase"}
	sim := NewSim(stub, WithSeed(1), WithTraceOnFailure())
	a, b := floodHost(0), floodHost(1)
	sim.Node(a, newFloodProtocol(a, b))
	sim.Node(b, newFloodProtocol(b, a))
	sim.Run(time.Second)
	stub.runCleanups() // never marked failed

	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatalf("expected no artifact on success, got %d", len(entries))
	}
}
