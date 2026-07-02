package prototest

import (
	"testing"
	"time"

	"github.com/antonionduarte/go-simple-protocol-runtime/pkg/protorun"
	"github.com/antonionduarte/go-simple-protocol-runtime/pkg/transport"
)

// shutdownTimeout bounds the fixture's cleanup so a wedged runtime
// fails the test instead of hanging the suite.
const shutdownTimeout = 5 * time.Second

// NewRuntime stands up a runnable runtime for self on the mesh: it
// creates (or reuses) the mesh node, wires it in at the Sessions seam,
// registers the given protocols, starts the runtime, and registers a
// bounded shutdown on t.Cleanup. Extra runtime options (WithMetrics,
// WithStrict, ...) go through opts.
func NewRuntime(
	t testing.TB,
	mesh *Mesh,
	self transport.Host,
	protocols []protorun.Protocol,
	opts ...protorun.Option,
) *protorun.Runtime {
	t.Helper()

	node := mesh.Node(self)
	opts = append(opts, protorun.WithTransport(nil, node))
	rt := protorun.New(self, opts...)
	for _, p := range protocols {
		rt.Register(p)
	}
	if err := rt.Start(); err != nil {
		t.Fatalf("prototest: runtime for %s failed to start: %v", self.String(), err)
	}
	t.Cleanup(func() {
		if err := rt.Shutdown(shutdownTimeout); err != nil {
			t.Errorf("prototest: runtime for %s failed to shut down: %v", self.String(), err)
		}
	})
	return rt
}
