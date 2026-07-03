# Benchmarks

Every number on this page was measured in this repository, on this
machine, with `make bench` (`go test -bench=. -run=^$ -benchmem ./...`
from the root module). No third-party framework numbers are
reproduced here — see "Comparing with actor frameworks" at the bottom
for why, and for links to where Hollywood's and Ergo's own numbers
live.

**Machine:** Apple M5 (arm64), macOS (Darwin 25.4.0), Go 1.26.3
(`darwin/arm64`), `GOMAXPROCS`-driven default parallelism (10 logical
cores report as `-10` in the benchmark names below — that's `GOMAXPROCS`,
not a benchmark parameter).

Re-run any of this yourself with:

```bash
make bench                                   # everything, matches this page
go test -bench=BenchmarkTCP_RoundTrip -benchmem ./pkg/protorun   # a single benchmark
```

All benchmark source lives in
[`bench_test.go`](../pkg/protorun/bench_test.go) in `pkg/protorun`.

## Methodology

Six things are measured, each isolating one layer of the stack:

1. **Codec cost** — `BinaryCodec` (fixed-size, `encoding/binary`) vs
   `WireCodec` (the reflective default) marshaling and unmarshaling the
   same fixed-size message. This is the per-message encode/decode tax
   before anything touches a socket.
2. **`WireID`** — the cost of hashing a Go type name to its 64-bit wire
   identifier (FNV-1a), the lookup key `Handle` and the codec registry
   use.
3. **Mailbox scheduling latency** — `BenchmarkMailbox_
   EnqueueDispatchLatency` pushes one event at a time onto a bare
   mailbox from one goroutine and times how long a separate consumer
   goroutine takes to observe it via `next()`. This isolates the
   channel-park/wake cost the unified mailbox itself adds, independent
   of decode or handler work.
4. **End-to-end inbound dispatch** — `BenchmarkProcessMessage` decodes
   a wire frame and pushes the resulting event onto a protocol's
   mailbox under a free-running drainer, i.e. sustained throughput of
   the exact code path `processMessage` runs for every inbound TCP
   frame, minus the socket read itself.
5. **In-process IPC** — `BenchmarkPublishNotification_Fanout` (pub/sub
   fanout cost at 1/10/100 subscribers) and `BenchmarkSendRequest`
   (request/reply round trip: fire, handler replies inline, callback
   runs) over an in-memory mock transport — no sockets, so this is
   pure runtime/IPC-router overhead.
6. **Real TCP round trip** — `BenchmarkTCP_RoundTrip` is the only
   benchmark here that goes through real sockets: two `Runtime`s, each
   with its own `TCPLayer` + `SessionLayer`, on `127.0.0.1`. The
   session handshake happens once, before the timed loop; each
   iteration sends one `Ping`, waits for the paired `Pong`, and that
   full round trip (marshal, socket write, socket read, unmarshal,
   dispatch, handler, marshal, socket write, socket read, unmarshal,
   dispatch) is the reported latency.

Every `Runtime` in these benchmarks is built with a discarded `slog`
logger — the default logger is real (blocking) I/O to a terminal/file,
which has nothing to do with what these benchmarks measure and would
otherwise dominate the noise floor.

## Results

### Codec: `WireCodec` vs `BinaryCodec`

Same fixed-size message (`BaseMessage` + one `uint64` field) through
both codecs:

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `BinaryCodec_Marshal` | 50.34 | 120 | 3 |
| `BinaryCodec_Unmarshal` | 49.37 | 64 | 3 |
| `WireCodec_Marshal` | 21.59 | 8 | 1 |
| `WireCodec_Unmarshal` | 28.33 | 8 | 1 |

`WireCodec` — the reflective default `Handle` picks automatically —
is not a slower fallback here: for a fixed-size message its cached
per-type plan beats hand-rolled `encoding/binary` on both time and
allocations. (`BinaryCodec` remains useful for its zero-reflection
code path and as the lowest-overhead option for very hot fixed-size
types; the gap is small enough that it is rarely the reason to pick
one over the other.)

### `WireID`

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `WireID` | 20.35 | 8 | 1 |

One FNV-1a hash of a cached type-name string per call.

### Mailbox scheduling latency

| Benchmark | ns/op (push+consume) | ns/dispatch (custom metric) | B/op | allocs/op |
|---|---:|---:|---:|---:|
| `Mailbox_EnqueueDispatchLatency` | 468.2 | 252.4 | 24 | 1 |

`ns/dispatch` is the time from `push` to the consumer goroutine
observing the event via `next()` — the cross-goroutine channel
park/wake cost, isolated from decode or handler work. `ns/op` also
includes the producer-side loop overhead of recording a timestamp and
waiting on the result channel, which is why it runs higher than the
`ns/dispatch` custom metric alone.

### End-to-end inbound dispatch

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `ProcessMessage` | 268.6 | 120 | 5 |

Decode wireID header, codec lookup, `Unmarshal`, mailbox push — the
complete `processMessage` path minus the socket read.

### In-process IPC

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `PublishNotification_Fanout/1subscriber` | 234.4 | 0 | 0 |
| `PublishNotification_Fanout/10subscribers` | 1655 | 0 | 0 |
| `PublishNotification_Fanout/100subscribers` | 24110 | 0 | 0 |
| `SendRequest` | 718.2 | 546 | 6 |

Publish is fully allocation-free: the notification travels inline in
the mailbox event (no envelope allocation), the subscriber snapshot
is an immutable copy-on-write slice returned without copying, and
metric attributes are only built when a real `Metrics`
implementation is installed (`WithMetrics` sets a flag hot paths
check first). Fanout cost still scales linearly with subscriber
count — one mailbox push each. `SendRequest` is a full local round
trip: request enqueued, handler runs, `Responder.Reply` enqueues the
reply, requester's callback runs.

These figures are the post-allocation-pass run (commit 9a42f43): the
pass cut `ProcessMessage` −58% ns/op, fanout −29% to −49% ns/op, and
`SendRequest` −19% ns/op against the v0.8.0 baseline, at the cost of
a ~7% regression on the raw mailbox enqueue→dispatch micro-benchmark
(the event union grew 64→72 bytes to carry notifications inline — a
copy we accept to delete a heap allocation per delivery).

### Real TCP round trip

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `TCP_RoundTrip` | 30116 (≈30.1 µs) | 1648 | 84 |

One `Ping` → `Pong` round trip over an established localhost TCP
session: two marshal/unmarshal pairs, two socket writes, two socket
reads, two mailbox dispatches, two handler invocations. For scale,
that is roughly 39x the in-process `SendRequest` latency above — the
expected order of magnitude for adding two real socket round trips on
loopback (no NIC, no real network latency) on top of the same
dispatch machinery.

## Consensus protocols

The six benchmarks above isolate the runtime's own machinery. The
consensus batteries (`pkg/protocols/raft`, `pkg/protocols/paxos`) are
benchmarked one layer up — a whole cluster of runtimes reaching
agreement — in each package's `bench_test.go`. Unlike the runtime
benchmarks these run on **wall-clock time**: the prototest mesh is
built with `prototest.WithRealClock()` so `ns/op` is real elapsed time,
not a simulated schedule (which would collapse to zero). Completion is
counted, never slept on — every iteration waits on a channel fed by the
protocol's own `Applied` / `Decided` notification.

### What each benchmark measures

- **`Raft_Commit_3nodes` / `_5nodes`** — *serial commit latency*: one
  in-flight proposal, timed from `Propose` to the proposer observing
  its own command `Applied`. That is one replication round trip (the
  leader appends locally, replicates, and a majority acknowledges).
- **`Raft_Commit_3nodes_fastHeartbeat`** — the same, with an
  explicitly-aggressive 1 ms heartbeat instead of 30 ms. It exists to
  make the heartbeat-coupling finding falsifiable (below).
- **`Raft_CommitPipelined_5nodes`** — *sustained throughput*: K = 64
  proposals kept in flight behind a semaphore, reported as `commits/s`.
- **`Raft_Commit_TCP_3nodes`** — the serial-commit shape over **real
  TCP** on localhost (sockets, length-prefix framing, the versioned
  handshake). The honest end-to-end number.
- **`Paxos_Decide_5nodes`** — single-decree *decision latency*: a
  fresh 5-node synod per iteration, one proposer, timed from `Propose`
  to every node publishing `Decided`. Cluster construction/teardown is
  excluded via `StopTimer`/`StartTimer`; `ns/op` is the ~two-round-trip
  decision alone.
- **`Paxos_Decide_Contended_5nodes`** — two proposers nominate
  different values at once; time to global decision under a proposer
  duel.

### The heartbeat-coupling finding

**Raft replicates on `Propose`, not on the heartbeat tick.**
`handlePropose` appends the entry and calls `broadcastAppendEntries()`
immediately; the heartbeat timer only covers *idle* periods. So the
proposer's commit latency is a replication round trip, independent of
`HeartbeatInterval` — which is exactly why `_fastHeartbeat` (1 ms) and
the default `Commit_3nodes` (30 ms) land within noise of each other
rather than an order of magnitude apart. (Followers still learn a
commit only on the *next* `AppendEntries` they receive, so a
follower's `Applied` can lag by up to one heartbeat — but the
proposer, which is what these benchmarks time, does not.)

### Numbers

> Taken at a system load average of ~6–25 (busy machine), `-count=6`.
> Allocation columns are exact regardless of load; ns/op medians are
> pessimistic. Reproduce on an idle machine with:
>
> ```bash
> go test -bench=. -benchmem -count=6 -run=^$ ./pkg/protocols/raft/
> go test -bench=. -benchmem -count=6 -run=^$ ./pkg/protocols/paxos/
> ```

| Benchmark | ns/op (median) | B/op | allocs/op | note |
|---|---:|---:|---:|---|
| `Raft_Commit_3nodes` | ≈23 µs | 5.3K | 128 | serial commit, 2-of-3 majority |
| `Raft_Commit_5nodes` | ≈41 µs | 9.5K | 244 | serial commit, 3-of-5 majority |
| `Raft_CommitPipelined_5nodes` | — | 36K | ~950 | ≈4.2k commits/s (K=64) |
| `Raft_Commit_TCP_3nodes` | ≈510 µs | 8K | 232 | real TCP on localhost |
| `Paxos_Decide_5nodes` | ≈120 µs | 20K | 520 | uncontended, all 5 decide |
| `Paxos_Decide_Contended_5nodes` | ≈121 µs | 25K | 663 | two-proposer duel |

**The first measurement run caught a real design flaw.** The original
`Storage` seam took the full `PersistentState` per persist, forcing an
O(log) copy on every commit: the bench showed per-commit cost growing
linearly with log length (367 µs, 15,131 allocs, 1.1 MB per 3-node
commit by iteration 10,000). The seam is now incremental —
`SaveTerm` for term/vote changes, `AppendEntries(from, entries)` for
log suffixes — which is also the shape a real durable WAL wants.
Post-fix, per-commit cost is flat in log length: **16x faster, 118x
fewer allocations** on the same bench. This is exactly the class of
bug invariant tests cannot catch and load tests exist for.

Two more observations worth more than the absolute numbers:

- **Pipelining barely beats serial on the in-memory mesh.** At K = 64
  the sustained rate (`commits/s`) is close to `1 / serial-latency`,
  because the mesh has ~zero delivery latency, so there is no round-trip
  time to hide — the bottleneck is the leader's single event loop, which
  does O(peers) marshal+send work *per `Propose`* (`handlePropose`
  broadcasts a fresh `AppendEntries` to every peer on each call rather
  than coalescing pending proposals into one round). Coalescing that
  broadcast is the obvious lever if pipelined throughput ever matters;
  on real TCP, where there *is* latency to amortize, pipelining should
  help more than it does here.
- **Contention barely shows on a lossless, zero-latency mesh.** Paxos's
  randomized retry-backoff (the anti-livelock mechanism) only engages
  when a round is interrupted before it completes; over an instant,
  lossless mesh both proposers finish Phase 1 + Phase 2 before any retry
  timer fires, so `Decide_Contended` is not reliably slower than the
  uncontended case (here it was faster — within the load noise). The
  duel's cost surfaces under injected delay/loss, not on the happy mesh.

## Comparing with actor frameworks

It is tempting to put these numbers next to Hollywood's or Ergo's
published actor-messaging benchmarks. Resist that: **the unit of work
being measured is not the same thing.**

- An actor-framework "message send" benchmark measures handing one
  value to one actor's mailbox — the actor model's smallest unit.
- protorun's numbers above measure a **protocol event**: a value plus
  routing metadata (wire id lookup, codec dispatch, IPC bookkeeping)
  flowing through a per-protocol event loop that also serializes
  timers, session events, and IPC requests/replies with that message,
  by design (see [`docs/concurrency-model.md`](concurrency-model.md)).
  That ordering guarantee costs a little more per event than a bare
  actor mailbox push, and it is not optional — it is the thing that
  makes a whole protocol stack replayable under
  [`prototest.Sim`](simulation.md).

Putting a protorun ns/op next to a Hollywood or Ergo ns/op invites the
reader to conclude one framework is "faster" at the same job, when the
job itself differs. If you want those frameworks' own numbers:

- Hollywood: <https://github.com/anthdm/hollywood#performance>
- Ergo: <https://github.com/ergo-services/ergo#benchmarks>

Use protorun's numbers to reason about protorun (is dispatch fast
enough for your message rate? does fanout cost scale the way your
subscriber count needs it to?), not as a cross-framework leaderboard
entry.
