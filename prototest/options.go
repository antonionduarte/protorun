package prototest

import "hash/fnv"

// Option configures a Mesh or Sim at construction. WithSeed pins the
// pseudo-random source; WithRealClock opts a plain Mesh out of virtual
// time.
type Option func(*meshConfig)

type meshConfig struct {
	seed      int64
	realClock bool
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

// defaultSeed derives a stable seed from a test name so bare tests are
// reproducible run-to-run without the author picking a number.
func defaultSeed(name string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(name))
	return int64(h.Sum64())
}
