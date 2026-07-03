package raft

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/protorun"
	"github.com/antonionduarte/protorun/pkg/prototest"
	"github.com/antonionduarte/protorun/pkg/transport"
)

// These benchmarks measure Raft commit performance on wall-clock time.
// They deliberately opt the prototest mesh OUT of its default virtual
// clock (prototest.WithRealClock) so ns/op is real elapsed time, not a
// simulated schedule that would collapse to zero.
//
// # What "commit latency" means here, and the heartbeat-coupling finding
//
// handlePropose appends locally and then calls broadcastAppendEntries()
// immediately (protocol.go) — it does NOT wait for the next heartbeat
// tick to replicate. So the commit path for one proposal is:
//
//	Propose -> local append -> immediate AppendEntries to all peers
//	        -> a majority of AppendEntriesReply -> commitIndex advances
//	        -> Applied published on the LEADER
//
// i.e. one message round trip, independent of HeartbeatInterval. The
// benchmarks wait for the PROPOSER (leader) to observe its own Applied,
// so the serial number is a genuine replication-round latency, not a
// "sleep until the next heartbeat" artifact. The _fastHeartbeat variant
// (1ms heartbeat vs the harness's 30ms) exists to make this visible: if
// commit were heartbeat-bound the two would differ by ~an order of
// magnitude; in practice they track each other, which is the evidence
// that replication is driven by Propose, not by the tick.
//
// (Followers still learn a commit only on the NEXT AppendEntries they
// receive, so a follower's Applied lags by up to one heartbeat — but the
// proposer, which is what we time, does not.)

// benchDiscardLogger silences runtime + transport logging so it does not
// garble `go test -bench` output or count as measured I/O.
var benchDiscardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// benchDriver is a test-only probe co-located with the Raft protocol on
// every node. It records every Applied index onto a buffered channel
// (the completion signal the benchmarks count) and exposes its
// ProtocolContext so the benchmark goroutine can drive Propose /
// DebugState through the supported IPC path.
type benchDriver struct {
	ctx     protorun.ProtocolContext
	applied chan uint64
}

func (d *benchDriver) Start(ctx protorun.ProtocolContext) {
	d.ctx = ctx
	protorun.SubscribeNotification(ctx, func(ev Applied) {
		// Non-blocking: EVERY node applies committed entries, but only the
		// leader's channel is ever drained by a benchmark. A blocking send
		// on a follower's (unread) channel would fill its buffer and wedge
		// that follower's event loop, cascading into a mesh-wide deadlock.
		// The buffer is sized well above the max in-flight proposals, so
		// the watched (leader) channel is drained faster than it fills and
		// never drops a completion.
		select {
		case d.applied <- ev.Index:
		default:
		}
	})
}
func (*benchDriver) Init(protorun.ProtocolContext) {}

// benchAppliedBuffer sizes each driver's completion channel. It must
// exceed the largest pipelining depth any benchmark uses (K below) with
// generous slack so a burst of commits landing in one AppendEntries
// round never blocks the probe.
const benchAppliedBuffer = 4096

// buildMeshCluster stands up an n-node Raft cluster on a real-clock
// in-memory mesh and returns the per-node drivers. Every runtime is
// registered for bounded shutdown on b.Cleanup by prototest.NewRuntime.
func buildMeshCluster(b *testing.B, mesh *prototest.Mesh, n int, cfg func(self, n int) Config) []*benchDriver {
	b.Helper()
	drivers := make([]*benchDriver, n)
	for i := range n {
		d := &benchDriver{applied: make(chan uint64, benchAppliedBuffer)}
		drivers[i] = d
		prototest.NewRuntime(b, mesh, raftHost(i), []protorun.Protocol{
			New(raftHost(i), cfg(i, n)),
			d,
		}, protorun.WithLogger(benchDiscardLogger))
	}
	return drivers
}

// benchDebugState fetches a node's Raft state snapshot through IPC,
// bounded by timeout. Used only during (unmeasured) setup to locate the
// leader.
func benchDebugState(d *benchDriver, timeout time.Duration) (DebugStateReply, bool) {
	type res struct {
		rep DebugStateReply
		ok  bool
	}
	ch := make(chan res, 1)
	protorun.SendRequest(d.ctx, &DebugState{}, func(rep *DebugStateReply, err error) {
		if err != nil || rep == nil {
			ch <- res{}
			return
		}
		ch <- res{rep: *rep, ok: true}
	})
	select {
	case r := <-ch:
		return r.rep, r.ok
	case <-time.After(timeout):
		return DebugStateReply{}, false
	}
}

// waitBenchLeader polls the cluster until some node reports itself leader
// and returns its index. Runs entirely in setup, so the poll sleep is
// off the measured path.
func waitBenchLeader(b *testing.B, drivers []*benchDriver, timeout time.Duration) int {
	b.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, d := range drivers {
			if rep, ok := benchDebugState(d, 500*time.Millisecond); ok && rep.Role == Leader {
				return i
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	b.Fatalf("no leader elected within %s", timeout)
	return -1
}

// commitBytes builds a unique command payload for proposal seq.
func commitBytes(seq uint64) []byte { return strconv.AppendUint([]byte("cmd-"), seq, 10) }

// runSerialCommit is the shared body of the serial-commit benchmarks: one
// proposal in flight at a time, timed from Propose to the leader's own
// Applied. driverOf(ld) proposes; ld.applied is the completion signal.
func runSerialCommit(b *testing.B, ld *benchDriver) {
	// A failed Propose (e.g. the leader lost leadership) would otherwise
	// hang the applied-wait; close(failed) turns that into a clean fatal
	// on the test goroutine. No per-iteration allocation on the hot path.
	failed := make(chan struct{})
	var failOnce sync.Once
	var failMsg atomic.Pointer[string]
	markFail := func(err error) {
		msg := err.Error()
		failMsg.Store(&msg)
		failOnce.Do(func() { close(failed) })
	}

	var seq uint64
	for b.Loop() {
		seq++
		protorun.SendRequest(ld.ctx, &Propose{Command: commitBytes(seq)}, func(_ *ProposeReply, err error) {
			if err != nil {
				markFail(err)
			}
		})
		select {
		case <-ld.applied:
		case <-failed:
			b.Fatalf("propose failed (leader lost?): %s", *failMsg.Load())
		}
	}
}

// BenchmarkRaft_Commit_3nodes measures serial commit latency (one
// in-flight proposal) on a 3-node cluster: ns/op is the time from
// Propose to the proposer observing the command applied — a single
// replication round trip over the in-memory mesh, NOT heartbeat-bound
// (see the file header).
func BenchmarkRaft_Commit_3nodes(b *testing.B) { benchRaftSerial(b, 3, clusterConfig) }

// BenchmarkRaft_Commit_5nodes is the 5-node counterpart: a larger
// majority (3 of 5 vs 2 of 3) to acknowledge each entry.
func BenchmarkRaft_Commit_5nodes(b *testing.B) { benchRaftSerial(b, 5, clusterConfig) }

// BenchmarkRaft_Commit_3nodes_fastHeartbeat is the 3-node serial commit
// with an explicitly-aggressive 1ms heartbeat (vs 30ms in clusterConfig).
// It is labeled loud on purpose: because Propose replicates immediately,
// this should NOT be dramatically faster than BenchmarkRaft_Commit_3nodes
// — the near-parity is the evidence that commit latency is a replication
// round trip, not a wait for the heartbeat tick.
func BenchmarkRaft_Commit_3nodes_fastHeartbeat(b *testing.B) {
	benchRaftSerial(b, 3, fastHeartbeatConfig)
}

func benchRaftSerial(b *testing.B, n int, cfg func(self, n int) Config) {
	mesh := prototest.NewMesh(b, prototest.WithRealClock())
	drivers := buildMeshCluster(b, mesh, n, cfg)
	leader := waitBenchLeader(b, drivers, 15*time.Second)
	runSerialCommit(b, drivers[leader])
}

// BenchmarkRaft_CommitPipelined_5nodes keeps K proposals in flight at
// once (a semaphore of depth K) and measures sustained committed-ops
// throughput on a 5-node cluster. ns/op is per-commit wall time under
// pipelining; the commits/s metric is the headline sustained rate.
func BenchmarkRaft_CommitPipelined_5nodes(b *testing.B) {
	const (
		n = 5
		K = 64
	)
	mesh := prototest.NewMesh(b, prototest.WithRealClock())
	drivers := buildMeshCluster(b, mesh, n, clusterConfig)
	leader := waitBenchLeader(b, drivers, 15*time.Second)
	ld := drivers[leader]

	failed := make(chan struct{})
	var failOnce sync.Once
	var failMsg atomic.Pointer[string]
	markFail := func(err error) {
		msg := err.Error()
		failMsg.Store(&msg)
		failOnce.Do(func() { close(failed) })
	}

	// waitOne blocks for one committed proposal or a clean fatal.
	waitOne := func() {
		select {
		case <-ld.applied:
		case <-failed:
			b.Fatalf("propose failed (leader lost?): %s", *failMsg.Load())
		}
	}

	var seq uint64
	inFlight := 0
	start := time.Now()
	for b.Loop() {
		if inFlight >= K {
			waitOne()
			inFlight--
		}
		seq++
		protorun.SendRequest(ld.ctx, &Propose{Command: commitBytes(seq)}, func(_ *ProposeReply, err error) {
			if err != nil {
				markFail(err)
			}
		})
		inFlight++
	}
	for inFlight > 0 {
		waitOne()
		inFlight--
	}
	elapsed := time.Since(start)
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "commits/s")
	}
}

// --- real TCP -----------------------------------------------------------

// raftTCPBasePort hands out fresh localhost port ranges for the TCP
// benchmark under -count=N. Per the repo convention (see CLAUDE.md
// Testing notes) it stays BELOW 7400 — the 7400+ band is cmd/gossip's
// growing atomic. 7150-7199 is cmd/broadcast, 7300-7311 is pkg/protorun's
// TCP tests; 7330-7399 is an unused gap this benchmark claims. Only bound
// when the benchmark actually runs.
var raftTCPBasePort int32 = 7330

func reserveRaftTCPPorts(n int) int { return int(atomic.AddInt32(&raftTCPBasePort, int32(n))) - n }

// BenchmarkRaft_Commit_TCP_3nodes is the honest end-to-end number: the
// same serial-commit shape as BenchmarkRaft_Commit_3nodes but over REAL
// TCP sessions on localhost — sockets, length-prefix framing, the
// versioned handshake, and OS scheduling, none of which the in-memory
// mesh pays. Session establishment and leader election happen before the
// timed loop.
func BenchmarkRaft_Commit_TCP_3nodes(b *testing.B) {
	const n = 3
	base := reserveRaftTCPPorts(n)

	hosts := make([]transport.Host, n)
	for i := range n {
		hosts[i] = transport.NewHost(base+i, "127.0.0.1")
	}
	cfg := func(i int) Config {
		var peers []transport.Host
		for j := range n {
			if j != i {
				peers = append(peers, hosts[j])
			}
		}
		return Config{
			Peers:              peers,
			HeartbeatInterval:  50 * time.Millisecond,
			ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
			ReconnectInterval:  100 * time.Millisecond,
		}
	}

	// transport.NewTCPLayer / NewSessionLayer log through slog.Default();
	// swap it out for the benchmark so socket/handshake logging stays out
	// of `go test -bench` output.
	prevDefault := slog.Default()
	slog.SetDefault(benchDiscardLogger)
	defer slog.SetDefault(prevDefault)

	drivers := make([]*benchDriver, n)
	rts := make([]*protorun.Runtime, n)
	for i := range n {
		d := &benchDriver{applied: make(chan uint64, benchAppliedBuffer)}
		drivers[i] = d
		rt := protorun.New(hosts[i],
			protorun.WithLogger(benchDiscardLogger),
			protorun.WithTCPTransport(context.Background()),
		)
		rt.Register(New(hosts[i], cfg(i)))
		rt.Register(d)
		rts[i] = rt
	}
	for _, rt := range rts {
		if err := rt.Start(); err != nil {
			b.Fatalf("runtime start: %v", err)
		}
	}
	b.Cleanup(func() {
		var wg sync.WaitGroup
		for _, rt := range rts {
			wg.Go(rt.Cancel)
		}
		wg.Wait()
	})

	leader := waitBenchLeader(b, drivers, 15*time.Second)
	runSerialCommit(b, drivers[leader])
}

// fastHeartbeatConfig is clusterConfig with an aggressive 1ms heartbeat,
// used by BenchmarkRaft_Commit_3nodes_fastHeartbeat to demonstrate that
// commit latency is not heartbeat-coupled. Election bounds stay well
// above the heartbeat so a live leader remains stable.
func fastHeartbeatConfig(self, n int) Config {
	c := clusterConfig(self, n)
	c.HeartbeatInterval = 1 * time.Millisecond
	return c
}
