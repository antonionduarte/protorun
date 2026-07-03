package paxos

import (
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/protorun"
	"github.com/antonionduarte/protorun/pkg/prototest"
	"github.com/antonionduarte/protorun/pkg/transport"
)

// These benchmarks measure single-decree Paxos on wall-clock time. Because
// a synod decides ONE value per instance, "throughput" is meaningless —
// the honest unit is decision latency per fresh instance. Each iteration
// therefore stands up a FRESH n-node cluster, proposes once, and waits for
// every node to publish Decided.
//
// # What is (and isn't) in ns/op
//
// Cluster construction, session establishment, and teardown are wrapped in
// b.StopTimer/b.StartTimer, so ns/op is the pure Propose->Decided-on-all
// latency, not the (much larger, and uninteresting) setup cost. This is the
// least-lying framing for a decide-once protocol: measuring construction
// would swamp the decision it is meant to time, and reusing one instance
// across iterations is impossible (the decree is decided forever after the
// first round — a second Propose fails with AlreadyDecidedError, by design,
// with no public Reset).
//
// startRound() (protocol.go) sends Prepare to all peers immediately on
// Propose, and every acceptor broadcasts Accepted to all peers when it
// accepts, so each node learns the decision independently within one
// round (~2 message hops): Prepare/Promise, then Accept/Accepted. No timer
// is on the happy path — the retry backoff only fires on a stalled round —
// so the uncontended number is a genuine two-round-trip latency. The
// benchmark waits for full connectivity before proposing precisely so a
// dropped first round cannot inject a retry-backoff into the measurement.

// benchDiscardLogger silences runtime logging so it does not garble
// `go test -bench` output or count as measured I/O.
var benchDiscardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// benchDriver is a test-only probe co-located with the Paxos protocol on
// every node. It signals once when the node publishes Decided (the
// completion the benchmarks count), counts live sessions (for the
// connectivity gate before proposing), and exposes its ProtocolContext so
// the benchmark goroutine can drive Propose through the supported IPC path.
type benchDriver struct {
	ctx       protorun.ProtocolContext
	decided   chan struct{}
	connected atomic.Int64
}

func (d *benchDriver) Start(ctx protorun.ProtocolContext) {
	d.ctx = ctx
	protorun.SubscribeNotification(ctx, func(Decided) { d.decided <- struct{}{} })
}
func (*benchDriver) Init(protorun.ProtocolContext)          {}
func (d *benchDriver) OnSessionConnected(transport.Host)    { d.connected.Add(1) }
func (d *benchDriver) OnSessionDisconnected(transport.Host) { d.connected.Add(-1) }
func (d *benchDriver) OnSessionGivenUp(transport.Host, int) {}

// buildPaxosNode stands up one runtime for self on a real-clock mesh,
// mirroring prototest.NewRuntime but WITHOUT its b.Cleanup shutdown hook:
// these benchmarks build a fresh cluster per iteration and must shut each
// one down explicitly (see shutdownAll) rather than accumulating every
// iteration's runtimes until the benchmark ends.
func buildPaxosNode(b *testing.B, mesh *prototest.Mesh, self transport.Host, protocols []protorun.Protocol) *protorun.Runtime {
	b.Helper()
	opts := make([]protorun.Option, 0, 3)
	if mesh.Clock() != nil { // nil under WithRealClock; kept for symmetry with the fixture
		opts = append(opts, protorun.WithClock(mesh.Clock()))
	}
	opts = append(opts, protorun.WithTransport(nil, mesh.Node(self)), protorun.WithLogger(benchDiscardLogger))

	rt := protorun.New(self, opts...)
	for _, p := range protocols {
		rt.Register(p)
	}
	if err := rt.Start(); err != nil {
		b.Fatalf("paxos: runtime for %s failed to start: %v", self.String(), err)
	}
	return rt
}

// buildDecideCluster wires a fresh n-node synod on mesh and returns the
// per-node drivers and runtimes.
func buildDecideCluster(b *testing.B, mesh *prototest.Mesh, n int) ([]*benchDriver, []*protorun.Runtime) {
	b.Helper()
	drivers := make([]*benchDriver, n)
	rts := make([]*protorun.Runtime, n)
	for i := range n {
		d := &benchDriver{decided: make(chan struct{}, 1)}
		drivers[i] = d
		rts[i] = buildPaxosNode(b, mesh, paxosHost(i), []protorun.Protocol{
			New(paxosHost(i), clusterConfig(i, n)),
			d,
		})
	}
	return drivers, rts
}

// waitAllConnected blocks (in unmeasured setup) until every node holds a
// session to all n-1 peers, so the first proposal round is not lost to an
// unestablished session. The poll sleep is off the measured path.
func waitAllConnected(b *testing.B, drivers []*benchDriver, timeout time.Duration) {
	b.Helper()
	want := int64(len(drivers) - 1)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		all := true
		for _, d := range drivers {
			if d.connected.Load() < want {
				all = false
				break
			}
		}
		if all {
			return
		}
		time.Sleep(time.Millisecond)
	}
	b.Fatalf("synod did not fully connect within %s", timeout)
}

// shutdownAll tears down every runtime with a bounded timeout, in
// parallel. Called per iteration (under StopTimer) so goroutines never
// accumulate across b.N.
func shutdownAll(b *testing.B, rts []*protorun.Runtime) {
	b.Helper()
	var wg sync.WaitGroup
	for _, rt := range rts {
		wg.Go(func() {
			if err := rt.Shutdown(5 * time.Second); err != nil {
				b.Errorf("paxos: runtime shutdown failed: %v", err)
			}
		})
	}
	wg.Wait()
}

// BenchmarkPaxos_Decide_5nodes measures uncontended single-decree latency:
// a fresh 5-node synod, one proposer, timed from Propose to every node
// having published Decided. ns/op is that Propose->all-Decided latency
// (setup and teardown are excluded via StopTimer). Roughly two message
// round trips over the in-memory mesh.
func BenchmarkPaxos_Decide_5nodes(b *testing.B) {
	const n = 5
	for b.Loop() {
		b.StopTimer()
		mesh := prototest.NewMesh(b, prototest.WithRealClock())
		drivers, rts := buildDecideCluster(b, mesh, n)
		waitAllConnected(b, drivers, 10*time.Second)
		b.StartTimer()

		protorun.SendRequest(drivers[0].ctx, &Propose{Value: []byte("v0")}, func(*ProposeReply, error) {})
		for _, d := range drivers {
			<-d.decided
		}

		b.StopTimer()
		shutdownAll(b, rts)
		// b.Loop requires the timer running when it is next evaluated; the
		// gap until the next iteration's StopTimer is negligible.
		b.StartTimer()
	}
}

// BenchmarkPaxos_Decide_Contended_5nodes has TWO proposers nominate
// different values at the same instant. Their ballots are disjoint
// (round*N + nodeIndex) so they never collide, but each can interrupt the
// other's round, forcing higher-ballot retries under the randomized
// backoff. ns/op is again Propose->all-Decided latency; expect materially
// higher mean AND variance than the uncontended case — that duel is the
// point of the benchmark, not noise.
func BenchmarkPaxos_Decide_Contended_5nodes(b *testing.B) {
	const n = 5
	for b.Loop() {
		b.StopTimer()
		mesh := prototest.NewMesh(b, prototest.WithRealClock())
		drivers, rts := buildDecideCluster(b, mesh, n)
		waitAllConnected(b, drivers, 10*time.Second)
		b.StartTimer()

		// Two proposers race with different values.
		protorun.SendRequest(drivers[0].ctx, &Propose{Value: []byte("v0")}, func(*ProposeReply, error) {})
		protorun.SendRequest(drivers[1].ctx, &Propose{Value: []byte("v1")}, func(*ProposeReply, error) {})
		for _, d := range drivers {
			<-d.decided
		}

		b.StopTimer()
		shutdownAll(b, rts)
		// b.Loop requires the timer running when it is next evaluated; the
		// gap until the next iteration's StopTimer is negligible.
		b.StartTimer()
	}
}
