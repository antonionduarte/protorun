package quic

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs every test under goleak so a leaked goroutine (ours or a
// quic-go connection that outlives Cancel) fails the package loudly.
//
// quic-go parks a few long-lived background goroutines that are not tied
// to any single connection and are not cleaned up synchronously by
// Listener.Close; they are ignored here by top function so they don't mask
// real leaks in this package's own code.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreTopFunction("github.com/quic-go/quic-go.(*Transport).listen"),
		goleak.IgnoreAnyFunction("github.com/quic-go/quic-go.(*connIDGenerator).SetHandshakeComplete"),
	)
}
