package prototest

import (
	"time"

	"github.com/antonionduarte/protorun/pkg/protorun"
	"github.com/antonionduarte/protorun/pkg/transport"
)

// shutdownTimeout bounds the fixture's cleanup so a wedged runtime
// fails the test instead of hanging the suite.
const shutdownTimeout = 5 * time.Second

// NewRuntime stands up a runnable runtime for self on the mesh: it
// creates (or reuses) the mesh node, wires it in at the Sessions seam,
// registers the given protocols, starts the runtime, and registers a
// bounded shutdown on t.Cleanup. Extra runtime options (WithMetrics,
// WithStrict, ...) go through opts.
//
// The runtime runs on the mesh's shared virtual clock by default (all
// nodes on one mesh share one timeline); build the mesh with
// prototest.WithRealClock for wall time. A caller-supplied
// protorun.WithClock in opts still wins, since options apply in order.
func NewRuntime(
	t TB,
	mesh *Mesh,
	self transport.Host,
	protocols []protorun.Protocol,
	opts ...protorun.Option,
) *protorun.Runtime {
	t.Helper()

	node := mesh.Node(self)
	full := make([]protorun.Option, 0, len(opts)+2)
	if mesh.clock != nil {
		full = append(full, protorun.WithClock(mesh.clock))
	}
	full = append(full, protorun.WithTransport(nil, node))
	full = append(full, opts...)

	rt := protorun.New(self, full...)
	node.rt = rt
	for _, p := range protocols {
		rt.Register(p)
	}
	mesh.recorder.node(self, protocols)
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
