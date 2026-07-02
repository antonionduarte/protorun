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
go test -bench=BenchmarkTCP_RoundTrip -benchmem .   # a single benchmark
```

All benchmark source lives in [`bench_test.go`](../bench_test.go) at
the module root.

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
| `ProcessMessage` | 642.0 | 344 | 17 |

Decode wireID header, codec lookup, `Unmarshal`, mailbox push — the
complete `processMessage` path minus the socket read.

### In-process IPC

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `PublishNotification_Fanout/1subscriber` | 352.4 | 336 | 9 |
| `PublishNotification_Fanout/10subscribers` | 3045 | 2640 | 45 |
| `PublishNotification_Fanout/100subscribers` | 29959 | 25872 | 405 |
| `SendRequest` | 776.1 | 986 | 20 |

Fanout cost scales close to linearly with subscriber count (roughly
70-300 ns of marginal cost per additional subscriber in this run) —
expected, since publish enqueues one event per subscriber mailbox.
`SendRequest` is a full local round trip: request enqueued, handler
runs, `Responder.Reply` enqueues the reply, requester's callback runs.

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
