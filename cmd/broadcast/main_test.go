package main

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain wires goleak into the broadcast example's test suite so any
// leaked goroutine after a test's Cleanup callbacks fails the package.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
