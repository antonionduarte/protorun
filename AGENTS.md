# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when
working with code in this repository.

## Project context

A Go protocol runtime for building distributed protocols, heavily
inspired by [Babel](https://github.com/pfouto/babel-core) (Java).
Pre-v1: APIs are settling but breaking changes are still on the
table. `TODO.md` is the source of truth for what is complete vs.
planned. `docs/README.md` is the full docs index (tutorial, how-tos,
reference, explanation — Diátaxis); this file is a map of the code,
not a replacement for those.

## Common commands

```bash
# Build everything
make build

# Run all tests (includes -race)
make test-race

# Lint, modernize, vulnerability scan
make lint
make modernize-check
make vulncheck

# Coverage summary
make coverage

# Benchmarks
make bench

# Run a single test by name
go test -race ./pkg/protorun -run TestIPC_RequestReply_HappyPath

# Run the pingpong example (two terminals, one per node)
go run ./cmd/pingpong \
  -config cmd/pingpong/pingpong.example.yaml \
  -self-ip 127.0.0.1 -self-port 5001 \
  -peer-ip 127.0.0.1 -peer-port 5002

# Run the gossip example as a 10-node localhost cluster
for i in 0 1 2 3 4 5 6 7 8 9; do
  go run ./cmd/gossip \
    -self-port $((7400 + i)) \
    -contact-port $((7400 + (i+1) % 10)) \
    -contact-port $((7400 + (i+5) % 10)) &
done

# 10k-node scale probe (gated)
GOSSIP_SCALE_10K=1 go test -run TestGossip_10000Nodes_Scale \
  -timeout 10m ./cmd/gossip/

# protoviz viewer (viz/ — Node app, NOT part of the Go workspace)
cd viz && npm install && npm run dev   # open a sample trace
cd viz && npm test                     # vitest: parser + fold engine
cd viz && npm run build                # tsc + vite production build

# protoviz live server (cmd/protoviz — stdlib-only Go binary)
cd viz && npm run build                                          # produce viz/dist first
go run ./cmd/protoviz -addr :7777                               # serve viz/dist + live /events
go run ./cmd/protoviz -replay viz/sample-traces/raft-partition.jsonl -pace 50ms  # demo, no cluster
go run ./cmd/broadcast -self-port 8801 -viz http://localhost:7777                # cluster streams itself
```

## protoviz viewer (`viz/`)

`viz/` is the web viewer for `protoviz/1` JSONL traces (emitted by
`pkg/prototest`'s `WithTrace`; schema in `pkg/prototest/trace.go`, design in
`docs/visualizer-design.md`). Static Vite + React + TypeScript + Tailwind app,
default (neutral) shadcn/ui theme — NOT a Go module, excluded from `go.work`,
node v26 + npm (no pnpm/bun). Own CI job (`viz` in `.github/workflows/ci.yml`)
runs `npm ci && npm test && npm run build`. Layout: `src/lib/trace.ts`
(tolerant parser), `src/lib/fold.ts` (keyframed fold engine, snapshots every
500 events → instant backward scrubbing), `src/lib/lenses.ts` (registry),
`src/lenses/*` (topology/sequence/inspector universal; membership/tree/
consensus per protocol). `viz/sample-traces/` is Vite's `publicDir`. Deviation:
the plumtree tree lens derives eager/lazy edges from the Gossip/Prune/Graft/
IHave message stream, since `plumtree.DebugStatsReply` carries only counters.
See `viz/README.md`.

Live mode (Stage 3): `protorun.WithTracer` (`pkg/protorun/tracer.go`) is the
metrics-style fast path (guard on `tracerEnabled`, zero perturbation when off)
emitting a `TraceEvent` per send/deliver/session/supervision/dead-letter.
`cmd/protoviz` (stdlib only) serves the UI + an SSE `/events` stream +
`/ingest?node=<host>`; `cmd/internal/viztrace.NewHTTPTracer` (bounded
drop-oldest ring, batched POSTs) streams a cluster there, wired into
`cmd/broadcast -viz`. Ingest is receiver-authoritative (a receiver's `deliver`
is the record; `send` is forwarded as `kind:"send"`, not folded into topology).
Viewer live path: `parseLiveLine` + `FoldEngine.append` grow the trace
incrementally for follow-mode tailing.

## Architecture

Three layers, stacked top to bottom:

1. **Runtime** (`pkg/protorun`). Per-process value (no singleton),
   constructed via `protorun.New(self, opts...)`. Owns the protocol
   registry, codec lookup, IPC router (request handlers + notification
   fanout), retry table, timer table, and the dispatcher goroutine
   that pulls inbound session messages and routes them to the right
   protocol's mailbox.
2. **SessionLayer** (`pkg/transport/session.go`). Performs a versioned
   Hello/Ack handshake to bind ephemeral transport connections to
   stable logical `Host` identities. Emits session events
   (`SessionConnected`, `SessionDisconnected`, `SessionFailed`,
   `SessionVersionMismatch`). Frames application vs. handshake payloads
   with a `LayerIdentifier` byte and maintains a per-peer FSM
   (`sessionConn`). It is the **single translation point** between the
   transport's `transport.Address` and the logical `Host`: below it the
   world is `Address`, above it (the `Sessions` seam, the runtime) the
   world is `Host`.
3. **TransportLayer** (`transport.Layer` in `pkg/transport/layer.go`,
   TCP impl in `tcp.go`). Addresses peers by `transport.Address`
   (`Connect`/`Disconnect`/`Send`, `Message.Peer`, `Event.Peer()`);
   `Host` (ip:port) implements `Address` and is the endpoint type TCP
   and QUIC use. Opens the listener/dialer, length-prefixes frames
   (4-byte big-endian), and exposes `OutChannel` / `OutEvents`. TLS is a
   construction option: `transport.WithTLS(cfg)` (sugar over
   `tls.Dialer`/`tls.Listen`), or the raw `transport.WithDialFunc` /
   `transport.WithListenFunc` seams; forwarded through
   `WithTCPTransport(ctx, topts...)`. See `docs/how-to-tls.md`. A QUIC
   backend lives in the `pkg/transport/quic` nested module — same framing,
   the SessionLayer runs over it unchanged.

Wire format (top down):

```
TCP frame:        [Length(uint32 BE) || LayerID(1 byte) || Body]
Application body: [WireID(uint64 LE) || Payload]
Session body:     [HandshakeType(1 byte) || HandshakeData]
                  # Hello: [Type=1 || Version(1) || WriteHost(self)]
                  # Ack:   [Type=2]
```

`WireID` is a 64-bit FNV-1a hash of the message Go type's name (or
`WireName()` if implemented). Production protocols should implement
`WireName()` to freeze the ID across renames.

### Codecs

A message's `Payload` bytes come from its registered `Codec[M]`.
`Handle(ctx, fn)` is the default registration path: it infers `M` from
the `func(*M, transport.Host)` handler and registers a codec + the
handler in one call, picking `SelfCodec[M]` when `*M` implements
`SelfMarshaler` (`MarshalWire`/`UnmarshalWire`) and the reflective
`WireCodec[M]` otherwise. Custom codecs keep the explicit
`RegisterCodec` + `RegisterHandler` pair.

`WireCodec[M]` (`wirecodec.go`) is the zero-config default: bool, sized
ints/uints, floats, string, `[]byte`, slices, arrays of fixed-size
kinds, maps (sorted-key deterministic encode), nested structs, and
pointers to structs. It compiles a cached per-type plan via reflection
(same trick as `wireIDCache`); unexported and `wire:"-"` fields are
skipped; platform-sized int/uint, interface, chan, func, and complex are
rejected at plan-compile time. The byte layout is normative in
`docs/wire-format.md`. Other codecs: `BinaryCodec[M]` (fixed-size,
`encoding/binary`), `JSONCodec[M]` (debug/inspection only, not a stable
format), and `pkg/codec/protobuf`'s `ProtoCodec[M proto.Message]` in a
nested module. Strict mode warns once per type when a codec is registered for a
type without `WireName()`.

### Modules and workspace

The core module (root) is deliberately zero-dependency. Third-party
dependencies live in nested modules with their own `go.mod` (cmd/ is
its own module too, so example binaries can consume pkg/config without
the library module growing a YAML dependency): today
`pkg/codec/protobuf` (requires `google.golang.org/protobuf`),
`pkg/transport/quic` (requires `github.com/quic-go/quic-go`), `pkg/config`
(requires `gopkg.in/yaml.v3`), and `pkg/otel` (requires
`go.opentelemetry.io/otel/*`). `go.work` at the root is tracked in git
(only `go.work.sum` is ignored) so a checkout resolves the nested
modules against the in-tree core. The `Makefile` targets loop over
`MODULES := . cmd pkg/codec/protobuf pkg/transport/quic pkg/config pkg/otel`; CI runs each
nested module's tests (and govulncheck) as separate steps. Add new
modules to both `MODULES` and `go.work`. `pkg/config`/`pkg/otel` are two
directories below root (`replace ... => ../..`); `pkg/codec/protobuf`/
`pkg/transport/quic` are three levels down (`replace ... => ../../..`) — get
this wrong and `go work sync` reports conflicting replacements.

### Configuration (`config`) and metrics adapter (`otel`)

`config.Load(path)` parses YAML with a reserved `runtime:` block
(logging level/components/format) plus arbitrary named sections;
`config.Section[T](f, name)` decodes one section into a caller type,
strictly (`yaml.v3` KnownFields). `f.Runtime().Options()` builds the
implied `[]protorun.Option`. Philosophy: protocols get config through
their own constructors, no reflection over `Register`. `cmd/pingpong`
is the worked example. `otel.Metrics(meter) protorun.Metrics` adapts
Counter/Histogram onto OTel instruments, one per name, cached
(`sync.Once`/`sync.Map`); a creation error reports once via
`otel.Handle` and that name becomes a permanent no-op — never panics.

### Protocol authoring contract

Protocols implement `protorun.Protocol` (Start + Init) and receive a
`protorun.ProtocolContext` in both methods. All interaction with the
runtime goes through `ProtocolContext`, never through package-level
helpers. The context carries the protocol-scoped logger and ensures
`Self()` is correct.

Each protocol runs in a single event-loop goroutine draining one
ordered mailbox (`mailbox.go`) that carries a tagged `protoEvent`
union — message, timer, session, request, reply, notification.
Arrival order is delivery order across all kinds (a message from peer
P is never handled before the `SessionDisconnected` for P that
preceded it). Protocol state can be mutated freely from inside any
handler without locking, as long as access stays inside those
callbacks.

Mailbox overflow policy is chosen per protocol at registration:
`Register(p, WithMailbox(Mailbox{Capacity, Overflow}))` (default
capacity 1024, `OverflowBlock`). Policies: `OverflowBlock`
(backpressure the producer, ctx-guarded so shutdown can't deadlock),
`OverflowDropOldest`, `OverflowDropNewest` (non-blocking; the
evicted/rejected event goes to the `WithDeadLetter` hook), and
`OverflowUnbounded` (never blocks, grows — memory risk). Dropped
replies are acceptable: `SendRequest` timeouts cover lost replies.

Optional handlers, detected via interface assertion:

- `SessionConnectedHandler.OnSessionConnected(transport.Host)`
- `SessionDisconnectedHandler.OnSessionDisconnected(transport.Host)`
- `SessionGivenUpHandler.OnSessionGivenUp(transport.Host, attempts int)`
- `SessionFailedHandler.OnSessionFailed(transport.Host)` — plain-Connect
  failures only; retry-managed failures surface as Connected/GivenUp
- `PanicHandler.OnPanic(where string, recovered any)`
- `RestartHandler.OnRestart(attempt int)` — called on a freshly
  restarted instance after session replay (see Supervision).

`Send` to the local Host loops back through the inbound path (fresh
decoded instance, FIFO behind the sender's mailbox) — quorum
protocols need no local-vote special case.

Cross-protocol coordination is IPC, never direct method calls.
`RegisterRequestHandler` + `SendRequest` for ask/answer;
`SubscribeNotification` + `PublishNotification` for fan-out. The
gossip example demonstrates this with `gossip.TriggerBroadcast` as the
public surface plus a tiny `triggerer` test harness.

### Wiring order

In `main`:

```
protorun.New(self, WithLogger, WithTCPTransport, ...)
  -> rt.Register(myProtocol)
  -> rt.Run()
```

`WithTCPTransport(ctx, topts...)` sets up the TCPLayer + SessionLayer
and forwards any `transport.TCPOption` (e.g. `transport.WithTLS(cfg)`).
For custom transport stacks — QUIC (`pkg/transport/quic`), an in-memory
mesh, a mock — use `WithTransport(layer, session)` with pre-built
layers.

### Protocol library

Batteries under `pkg/protocols/`, all in the core module (zero third-party
deps). `pkg/protocols/membership` is a **types-only IPC contract** — the
interchangeability seam — `GetView`/`View{Active}` (request/reply) and
`NeighborUp`/`NeighborDown` (notifications). It has no codecs and no
`WireName` on purpose: IPC is local-only, so it never touches the wire
(contrast a membership protocol's own peer messages, which do).
`pkg/protocols/hyparview` (faithful HyParView) publishes the contract; its
shuffle is routed over active-view links with a path-retracing reply, so
no transient sessions to passive peers (the resolved Phase 5 open
question). `pkg/protocols/plumtree` (faithful Plumtree) consumes the contract:
eager/lazy sets from `NeighborUp`/`Down`, GRAFT/PRUNE tree repair,
sender+seq message ids, bounded GRAFT cache; public surface is a
`Broadcast` request + `Delivered` notification. Both take a `Config` with
sane zero-value defaults; every wire message has `WireName()` +
`SelfMarshaler` (a `transport.Host`'s int port rules out `WireCodec`).
Both are exemplary authoring-contract code (all state in handlers, no
goroutines, per-node RNG seeded from `Host`, sort before every random
pick) so they run deterministically under `prototest.Sim`.
`pkg/protocols/raft` (faithful Raft) is the consensus battery and does NOT
use the membership contract — consensus needs total, stable membership, so
its peer set is static via `Config`. Randomized-timeout election,
`nextIndex` log repair, current-term-only commit (§5.4.2), up-to-date vote
(§5.4.1), higher-term stepdown; `Storage` seam (in-memory default is
loudly non-durable); public surface `Propose`/`Applied`/`LeaderChanged` +
`DebugState`. Same authoring-contract discipline; its Sim suite asserts
Election/State-Machine/Leader-Completeness safety, minority-partition
safety, and byte-identical determinism. `pkg/protocols/paxos` (faithful
single-decree Paxos, "Paxos Made Simple") is the second consensus battery,
also static-membership via `Config`: proposer/acceptor/learner in one
Protocol, per-node **disjoint** ballots (`round*N + index`, so ballots
never collide and need no tie-break), the Phase-2a **value-adoption** rule
(safety's crux), randomized-backoff liveness against dueling proposers,
on-reconnect Accepted re-announce for partition-heal catch-up, and the same
`Storage` seam; public surface `Propose`/`Decided` + `DebugState`. It is
single-decree ONLY — Multi-Paxos/log replication is Raft's job in this
tree. Its Sim suite asserts Agreement/Integrity, the multi-seed dueling
gauntlet, value adoption (engineered via delay timing), minority-cannot-
decide, chosen-is-forever, and byte-identical determinism. `cmd/gossip`
consumes the contract (static membership = the simplest impl);
`cmd/broadcast` stacks Plumtree/HyParView/TCP. Story in
`docs/protocols.md`.

### Supervision

`rt.Register(p)` defaults every protocol to `Resume`: a panicking
handler is recovered, logged, reported to `PanicHandler`, and the loop
keeps running (unchanged from before). `rt.RegisterFactory(newP,
WithSupervision(Supervision{OnPanic, MaxRestarts, Window, Backoff,
OnGiveUp}))` opts into real recovery. `Directive` is
`Resume`/`Restart`/`Stop`/`Escalate`. `Restart` needs a factory (fresh
state per restart) — strict mode panics if it's configured on a
singleton `Register`, non-strict downgrades it to `Resume`.

On a non-`Resume` panic the event loop exits and a per-protocol
supervisor goroutine runs the restart contract off the loop
(`supervisor.go`): quarantine the mailbox (further enqueues dead-letter
and never block) and drain it, cancel timers, auto-fail pending
`SendRequest`s with `ErrProtocolRestarting`, deregister the instance's
codecs / request routes / notification subscriptions (owner-indexed
`RemoveOwner` on `codecRegistry` and `ipcRouter`), wait a cancellable
`ExpBackoff` on the runtime Clock, rebuild from the factory
(`Start` → `Init`), replay a synthetic `SessionConnected` for every
established peer, then invoke `RestartHandler`. Exceeding `MaxRestarts`
within `Window` triggers `OnGiveUp`: `Stop` removes the protocol,
`Escalate` records a fatal error and cancels the runtime so
`Run`/`Shutdown` return an `ErrProtocolFailed`-wrapped error. Every
outcome publishes a local `ProtocolFailed{Protocol, Outcome, Attempt}`
notification (subscribe like any notification; no codec needed).

### Deterministic simulation

`prototest.NewSim(t, prototest.WithSeed(n))` runs a full protocol
stack under a seeded scheduler on the mesh's shared `FakeClock`.
`sim.Node(host, protocols...)` returns the real runtime; `sim.Run(d)`
/ `sim.RunUntil(pred, max)` / `sim.Step()` drive it. The scheduler
delivers network events in seeded order, settles every runtime to
quiescence, then advances the clock to the next timer/delivery
deadline — so timers, request timeouts, and retry backoff are
deterministic and a 30-second convergence test runs in milliseconds.
Faults: `mesh.Cut`/`Heal`/`Isolate`/`SetLoss`/`SetDelay`, all off one
seeded RNG; `Heal` does not auto-reconnect. Determinism holds for
protocols that follow the authoring contract (all state and sends
inside handlers, no own goroutines, no wall-clock reads); Go's
randomized map iteration leaks into send order unless a handler sorts
before ranging peers. Full write-up in `docs/simulation.md`.

`Runtime.Quiescent() bool` is the introspection probe the scheduler
polls: true when every live protocol has no event in flight. Backed by
a per-protocol `inFlight` counter incremented by the producer BEFORE
the mailbox push and decremented by the event loop AFTER dispatch — the
ordering that lets a reader observe zero only once every event has
drained (memory-model note on the method). Delivered synchronously via
the optional `SyncDeliverer`/`InboundSink` seam (`simhook.go`): a
Sessions adapter that implements it takes over inbound delivery and the
runtime skips its async pump goroutines. Only `prototest`'s mesh
implements it; production adapters don't. Documented as
test-harness/diagnostics surface.

### Timers

`ctx.After(d, fn)` (one-shot) and `ctx.Every(d, fn)` (periodic) live
in `timer.go`. Both return a `TimerHandle` whose `Cancel()` is
idempotent, safe after fire, and safe from inside any handler of the
same protocol — a cancel from the protocol's own event loop
guarantees the callback never runs afterwards, because the dispatcher
rechecks a cancelled flag even for a fire already sitting in the
mailbox. Payload travels by closure capture; there are no user-managed
IDs and no silent replacement. Handles are keyed by a runtime-
monotonic `uint64`, tracked per protocol, and all cancelled on
shutdown. Both are built on a single `Clock` primitive, `AfterFunc`
(`clock.go`): `Every` re-arms an `AfterFunc` after each fire rather
than running a background ticker goroutine, so a virtual clock fires
periodic timers synchronously (what the `Sim` needs) and production
pays for no per-timer goroutine. `WithClock` swaps the clock (default
`realClock`); `prototest.FakeClock` drives virtual time and is shared
per-mesh.

### Logging

Uses `log/slog`. Four component tags: `runtime`, `session`,
`transport`, `protocol`. The runtime adds the tag automatically for
the layers it owns. `componentFilterHandler` in `logging.go` filters
records when a `Components` allow-list is configured; otherwise
every record passes. Use `ProtocolContext.Logger()` inside a
protocol; it is pre-scoped with `component=protocol` and `self`.

### Diagnostics

The "received message for unknown wireID" warning in `processMessage`
is rate-limited to once per wireID per minute
(`unknownWireIDWarnWindow`, `unknownWireIDWarned sync.Map`, read via
`r.clock` so it's fake-clock-testable). The
`protorun.message.dropped_unknown_id` counter still increments every
occurrence. `Sender.Send`'s error split (sync return = local failure
only; delivery failure = a later session event) has a boxed doc
comment on `protocol.go`'s `Sender` and a matching README callout —
keep both in sync.

### Strict mode

`WithStrict(true)` enables runtime invariant checks: double-
registration panics, phase-ordering panics
(register-only-during-Start, active-calls-only-after-Init), a
slow-handler watchdog, and a rate-limited (once/sec/protocol) warning
when a mailbox crosses 80% occupancy. Off by default. See `strict.go`.

### Metrics

`Metrics` interface (Counter + Histogram + structured `Attr`) plus
`WithMetrics(m)` option. Default no-op. Instruments core paths:
`protorun.message.*`, `protorun.ipc.*`, `protorun.notification.*`,
`protorun.session.*`, `protorun.mailbox.depth`,
`protorun.mailbox.dropped`, `protorun.handler.panic`,
`protorun.protocol.restart` (outcome:
restarted/stopped/escalated), `protorun.strict.slow_handler`. See
`metrics.go`.

## Testing notes

- The `Runtime` is a per-process value; tests construct fresh ones.
  No singleton, no reset helper needed.
- Test fixtures live in `runtime_test_helpers_test.go`
  (`MockProtocol`, `MockNetworkLayer`, `localMessage`,
  `twoSidedProtocol`).
- Integration tests in `runtime_integration_test.go` spin up two
  real `TCPLayer` + `SessionLayer` pairs on localhost (ports 7300+)
  and exchange real frames; run them under `-race`.
- Every test package has `goleak.VerifyTestMain` so leaked
  goroutines fail loudly.
- Multi-runtime tests use a base port atomic (`reservePorts`) so
  they can run with `-count=N` without colliding. See
  `cmd/gossip/integration_test.go`.
- `prototest` (`pkg/prototest`) tests protocols without real TCP: an
  in-memory mesh at the Sessions seam plus `NewRuntime`. The root
  `pkg/protorun` package's own tests can't import it (cycle); they use
  `registerMockStack` in `runtime_test_helpers_test.go` instead.
  `prototest.Sim` (`pkg/prototest/sim.go`) is the deterministic-simulation
  harness; proof tests in `pkg/prototest/sim_test.go`
  (`TestSim_DeterministicTrace` must stay byte-identical under
  `-race -count=20`, plus partition/heal, loss/delay, virtual-time
  timers) with the flood harness in `pkg/prototest/sim_flood_test.go`.
  `Step()` never returns false while a periodic timer is live — bound
  step loops with a predicate/counter or use `RunUntil`.
- `WireCodec` has two fuzz targets (`FuzzWireCodec_RoundTrip`,
  `FuzzWireCodec_Unmarshal`) that run their seed corpus under a plain
  `go test`; run `-fuzz` explicitly for longer campaigns. The nested
  `pkg/codec/protobuf` module has its own test suite (with goleak), run by
  `make test` / `make test-race` and as a separate CI step.
- The protocol library (`pkg/protocols/hyparview`, `pkg/protocols/plumtree`)
  tests against `prototest.Sim` as its primary vehicle: seeded
  convergence/churn/shuffle and broadcast/duplicate-bound/partition-heal
  suites, stable under `-race -count=5`, plus pure-logic unit tests
  (view sets, seeded sampling, payload cache, wire round-trips). View
  introspection in tests goes through IPC (`hyparview.DebugState`,
  `plumtree.DebugStats`) rather than racing event-loop state. Determinism
  requires the per-node RNG seed and sort-before-iterate discipline these
  protocols follow; a protocol that ranges a `map` to fan out would leak
  nondeterminism (a property of Go, see `docs/simulation.md`).
- `bench_test.go` (`pkg/protorun`) is the only benchmark file; `make bench`
  covers it. Every `Runtime` uses `WithLogger(discardLogger)` so real
  logging I/O doesn't pollute `-bench` output; `BenchmarkTCP_RoundTrip`
  also swaps `slog.Default()` (restored via `defer`) since the
  transport layers log through the package default, not a constructor
  option. See `docs/benchmarks.md` for the numbers.
- `pkg/config`/`pkg/otel` test with plain `go test` (no goleak, no
  goroutines): `config` covers happy path, missing/type-mismatched/
  unknown-field sections, and the typo'd-top-level-key fallback;
  `otel` asserts against `sdk/metric`'s `ManualReader`.
