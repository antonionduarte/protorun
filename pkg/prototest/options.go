package prototest

import (
	"hash/fnv"
	"io"

	"github.com/antonionduarte/protorun/pkg/transport"
)

// Option configures a Mesh or Sim at construction. WithSeed pins the
// pseudo-random source; WithRealClock opts a plain Mesh out of virtual
// time; the WithTrace* options attach a trace recorder.
type Option func(*meshConfig)

type meshConfig struct {
	seed      int64
	realClock bool

	// Trace recorder configuration; see trace.go. traceWriter and
	// traceOnFailure are the two mutually-exclusive sinks (an explicit
	// writer streams; on-failure buffers to memory and dumps only if the
	// test failed). traceSampler + traceStateEvery drive periodic state
	// capture. All zero by default: no recorder, no observation cost.
	traceWriter     io.Writer
	traceOnFailure  bool
	traceSampler    func(sim *Sim, emit func(node transport.Host, protocol string, state any))
	traceStateEvery int
}

// WithSeed pins the mesh's single pseudo-random source (loss decisions,
// delay jitter, and delivery-order tie-breaking all flow from it). The
// same seed reproduces the exact schedule. Without it the seed is
// derived deterministically from the test name, so a bare test is stable
// run-to-run; the chosen seed is always logged at construction.
func WithSeed(seed int64) Option {
	return func(c *meshConfig) { c.seed = seed }
}

// WithRealClock keeps a plain Mesh (and the runtimes NewRuntime builds on
// it) on wall-clock time instead of the virtual clock that is now the
// default. Intended for the rare protocol test that genuinely needs real
// time. It is ignored by NewSim, which requires virtual time to drive the
// schedule.
func WithRealClock() Option {
	return func(c *meshConfig) { c.realClock = true }
}

// WithTrace attaches a protoviz trace recorder that writes a JSONL event
// stream to w as the mesh (and, under a Sim, its scheduler) runs: node and
// session events, message deliveries and fault drops, clock advances, and
// fault mutations. The recorder only observes the schedule the simulator
// already produces — it never changes delivery order or timing, so a
// traced run and an untraced run of the same seed deliver identically. The
// writer is buffered and flushed on the test's Cleanup. See trace.go for
// the format (protoviz/1).
func WithTrace(w io.Writer) Option {
	return func(c *meshConfig) { c.traceWriter = w }
}

// WithTraceOnFailure attaches a recorder that buffers the trace in memory
// and, in a Cleanup, writes it to a file only when the test failed. The
// destination directory is $PROTOTEST_TRACE_DIR if set, else the OS temp
// dir; the filename derives from the test name. The path is logged via the
// harness so a failing CI run surfaces a ready-to-open artifact. It is a
// no-op on success. Ignored if WithTrace is also set (that streams
// instead).
func WithTraceOnFailure() Option {
	return func(c *meshConfig) { c.traceOnFailure = true }
}

// WithTraceStateEvery sets the state-sampling cadence: every n units of
// simulator progress (deliveries and clock advances) the recorder invokes
// the WithTraceSampler callback at a quiescent point to capture per-node
// protocol state. n <= 0 disables sampling. Without a sampler it has no
// effect.
func WithTraceStateEvery(n int) Option {
	return func(c *meshConfig) { c.traceStateEvery = n }
}

// WithTraceSampler registers the state-capture callback the recorder runs
// every WithTraceStateEvery steps, at a quiescent point on the scheduler
// goroutine. The callback receives the Sim and an emit function; it reads
// protocol state through its own probe protocols (the established
// DebugState-over-IPC pattern — no new protocol-side API) and calls
// emit(node, protocolName, state) for each snapshot, which the recorder
// marshals to JSON as a "state" event. Protocol lenses decode the shape.
func WithTraceSampler(fn func(sim *Sim, emit func(node transport.Host, protocol string, state any))) Option {
	return func(c *meshConfig) { c.traceSampler = fn }
}

// defaultSeed derives a stable seed from a test name so bare tests are
// reproducible run-to-run without the author picking a number.
func defaultSeed(name string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(name))
	return int64(h.Sum64())
}
