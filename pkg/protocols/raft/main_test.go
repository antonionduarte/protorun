package raft

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain wires goleak into the raft test suite so any leaked goroutine
// after teardown fails at the package boundary — load-bearing for a
// framework whose correctness hinges on goroutine lifecycle, and the
// guard that keeps the load benchmarks' per-iteration clusters honest
// about shutting down cleanly.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
