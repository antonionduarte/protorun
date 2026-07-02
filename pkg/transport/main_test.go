package transport

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain wires goleak into the transport test suite. See the parent
// package's main_test.go for rationale.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
