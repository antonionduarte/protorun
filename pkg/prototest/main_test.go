package prototest

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain wires goleak into the prototest suite: the whole point of
// the mesh is leak-free in-process runtimes, so leaked goroutines
// here fail loudly.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
