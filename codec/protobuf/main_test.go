package protobuf

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain wires goleak in, matching the standard every other test
// package in the repo holds itself to.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
