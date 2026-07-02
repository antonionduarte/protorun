package protorun

import (
	"testing"
	"time"

	"github.com/antonionduarte/protorun/transport"
)

// TestRuntime_RegisterAndRun verifies that Register adds the protocol and
// Run blocks until Cancel is invoked from another goroutine.
func TestRuntime_RegisterAndRun(t *testing.T) {
	self := transport.NewHost(0, "127.0.0.1")
	rt := New(self)

	_ = registerMockStack(rt, self)

	impl := &MockProtocol{}
	rt.Register(impl)
	if len(rt.protocols) != 1 {
		t.Fatalf("Register did not append: got %d protocols", len(rt.protocols))
	}

	done := make(chan error, 1)
	go func() {
		done <- rt.Run()
	}()

	// Give Run a moment to enter its blocking wait, then cancel.
	time.Sleep(50 * time.Millisecond)
	rt.Cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Run did not return within 2s of Cancel")
	}

	if !impl.StartCalled {
		t.Errorf("Start was not called on the registered protocol")
	}
	if !impl.InitCalled {
		t.Errorf("Init was not called on the registered protocol")
	}
}
