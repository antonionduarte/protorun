package protorun

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain wires goleak into the protorun test suite. Any test that
// leaks a goroutine after teardown fails at the package boundary,
// which is load-bearing for a framework whose correctness hinges on
// goroutine lifecycle.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
