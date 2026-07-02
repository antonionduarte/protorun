# Changelog

All notable changes to protorun will be documented in this file. The
format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The framework is pre-1.0; minor versions may break API. Wire format
is versioned via the session-layer handshake (`transport.ProtocolVersion`).

## Unreleased

> **Tags below are pending.** Everything since v0.1.0 is still on
> `main`, undtagged. This block is structured by roadmap phase
> (`docs/roadmap.md`) rather than as one flat list specifically so each
> phase can be cut as its own tag (`v0.2.0` … `v0.8.0`) when the time
> comes — the phase headers below are the intended tag boundaries, not
> yet real releases.

### v0.2.0 — module rename, unified mailbox, timers, Clock seam (Phase 0)

An architecture pass driven by a deep-module review, alongside the
four breaking changes Phase 0 planned: handshake truthfulness, an
explicit seam for session management, and a first-class testing
package for protocol authors, so the rest of the roadmap could build
on final shapes.

#### Added

- **Unified per-protocol mailbox with overflow policy.** The six
  per-protocol channels (messages, timers, session events, requests,
  replies, notifications) collapse into one ordered mailbox carrying a
  tagged event union, so arrival order is delivery order across kinds.
  `Register` grows variadic `RegisterOption`s; `WithMailbox(Mailbox{
  Capacity, Overflow})` chooses the policy: `OverflowBlock` (default,
  backpressure the producer, ctx-guarded so shutdown can't deadlock),
  `OverflowDropOldest` / `OverflowDropNewest` (non-blocking; the
  evicted/rejected event goes to a `WithDeadLetter(func(DeadLetter))`
  hook), and `OverflowUnbounded` (never blocks, grows). New metrics
  `protorun.mailbox.depth` (histogram, sampled on enqueue) and
  `protorun.mailbox.dropped` (counter); strict mode warns, rate-
  limited once per second per protocol, above 80% occupancy.
- **`Clock` seam.** A `Clock` interface (`Now` / `AfterFunc` /
  `NewTicker`) now backs the timer table, retry backoff, request-
  timeout arming, the strict watchdog, and IPC latency. `WithClock`
  swaps it; the default is a zero-allocation real-time clock.
  `prototest.FakeClock` (`NewFakeClock` / `Now` / `Advance`) drives
  virtual time for deterministic tests.
- **Handshake Reject.** A server that receives a Hello with an
  unsupported wire-format version now answers with an explicit
  `Reject` (HandshakeType 3, carrying the refuser's version) before
  closing. The dialing runtime translates it into an immediate
  terminal `OnSessionGivenUp` — no retry budget is burned on a peer
  that can never accept — plus a loud log and a
  `protorun.session.version_mismatch` metric. Old builds treat the
  unknown handshake type as a parse error and close, exactly the
  pre-Reject behavior, so no version bump is needed.
- **Handshake timeout.** A dialer whose peer accepts the connection
  but never answers the Hello fails the handshake after a bounded
  wait (default 5s, `transport.WithHandshakeTimeout`).
- **`protorun.Sessions` seam.** The runtime now holds its session
  stack behind an interface derived from exactly what it uses;
  `*transport.SessionLayer` is the production adapter and
  `WithTransport` accepts any adapter. `Runtime.Start()` is exported
  for callers that own their lifecycle.
- **`prototest`.** In-memory mesh (no wire, no handshake,
  deterministic in-process delivery) implementing `Sessions`, plus a
  `NewRuntime` fixture: protocol authors can test full runtimes
  without TCP or port management.
- **Wire-format spec.** `docs/wire-format.md` is the authoritative
  envelope description; per-layer code comments now describe only
  the bytes that layer owns.

#### Changed

- **Module renamed to `github.com/antonionduarte/protorun`.**
  Previously `github.com/antonionduarte/go-simple-protocol-runtime`.
  `pkg/protorun` is promoted to the module root, and `pkg/transport`,
  `pkg/wire`, `pkg/prototest` move to `transport/`, `wire/`,
  `prototest/`. Import paths change accordingly; no behavior change.
- **Timer API redesigned around handles.** `ctx.After(d, fn)` and
  `ctx.Every(d, fn)` replace the old `Timer`/`TimerID` surface. Both
  return a `TimerHandle` with an idempotent `Cancel` that is safe
  after fire and from inside the protocol's own handlers (a same-loop
  cancel guarantees the callback never runs, even if the fire is
  already queued). The payload rides by closure capture; there are no
  user-managed IDs and no silent replacement, and every handle a
  protocol owns is cancelled on shutdown.
- **`SessionConnected` is truthful on the dialing side.** The client
  FSM waits for the server's Ack before marking the session
  Established and emitting `SessionConnected` (previously it emitted
  optimistically on Hello send, which reset the retry counter on
  every doomed attempt — a handshake-rejecting peer caused an
  infinite retry loop with phantom connect/disconnect flaps).
- **Codec registry owns the codec.** wireID lookup is a single
  guarded read returning `{protocol, codec}`; registration is one
  atomic step and the "codec lookup raced" branches are gone.
- **`ProtocolContext` shrank from 20 methods to 11.** The ten
  unexported plumbing hooks collapsed into one sealing `binding()`
  method; the generic helpers reach the framework through the
  concrete binding. Protocol-author code is unaffected.

#### Fixed

- `SessionVersionMismatch` events are now routed by the runtime
  (previously dropped silently) and emitted through the ctx-guarded
  `emitEvent` path (previously a raw channel send that could block
  the session goroutine at shutdown). The runtime's event mapper now
  has a loud default plus a coverage test so new event kinds can't
  be silently unrouted.

#### Removed

- **Old timer surface.** `Timer`, `TimerID()`, `SetupTimer`,
  `SetupPeriodicTimer`, `CancelTimer`, and `RegisterTimerHandler` are
  gone (no deprecation shim); use `After` / `Every` / `TimerHandle`.
- **`WithChannelBuffer`.** The runtime-wide per-channel buffer option
  is replaced by per-protocol `WithMailbox`.

### v0.3.0 — supervision and restart (Phase 1)

#### Added

- **Supervision and restart.** `RegisterFactory(func() Protocol, ...)`
  plus `WithSupervision(Supervision{OnPanic, MaxRestarts, Window,
  Backoff, OnGiveUp})` give a panicking protocol a real recovery
  policy instead of only `Resume`. `Directive` is `Resume` (default,
  today's behavior) / `Restart` / `Stop` / `Escalate`. A per-protocol
  supervisor goroutine runs the restart contract off the event loop:
  quarantine the mailbox (further events dead-letter, never block) and
  drain it, cancel timers, auto-fail pending `SendRequest`s with
  `ErrProtocolRestarting`, deregister the instance's codecs / request
  routes / notification subscriptions, wait a cancellable
  `ExpBackoff(base, max)` (exported `BackoffFunc`), build a fresh
  instance from the factory (Start → Init), replay a synthetic
  `SessionConnected` for every established peer, and invoke the
  optional `RestartHandler.OnRestart(attempt)`. More than
  `MaxRestarts` panics within `Window` triggers `OnGiveUp` (`Stop`
  removes the protocol; `Escalate` cancels the runtime and makes
  `Run`/`Shutdown` return an `ErrProtocolFailed`-wrapped error).
  `Restart` requires a factory: a strict-mode registration panic, or a
  warn-and-downgrade-to-`Resume` otherwise. Observability:
  `protorun.protocol.restart` counter (`outcome` attr) and a runtime-
  published `ProtocolFailed{Protocol, Outcome, Attempt}` notification
  siblings can `SubscribeNotification` to.

### v0.4.0 — codec ergonomics (Phase 2)

#### Added

- **Codec ergonomics.** `Handle(ctx, fn)` registers a message type and
  its handler in one call, inferring the type from the
  `func(*M, transport.Host)` signature and picking the codec: the
  reflective `WireCodec[M]` by default, or `SelfCodec[M]` when `*M`
  implements the new `SelfMarshaler`
  (`MarshalWire`/`UnmarshalWire`). `WireCodec[M]` is a zero-config codec
  for real message types — strings, `[]byte`, slices, maps
  (deterministic sorted-key encoding), arrays, nested structs, and
  pointers to structs, on top of every fixed-size type — compiling a
  cached per-type encode/decode plan via reflection (the trick
  `wireIDCache` uses). Its byte layout is normative in
  `docs/wire-format.md`; platform-sized int/uint, interface, chan, func,
  and complex are rejected at plan-compile time with a clear error.
  `JSONCodec[M]` (core, `encoding/json`) is added for debugging, and a
  new nested module `codec/protobuf` ships `ProtoCodec[M proto.Message]`
  so protobuf shops keep the core zero-dependency. Strict mode gains a
  once-per-type Warn nudge when a message codec is registered for a type
  without `WireName()`. The `cmd/gossip` hand-rolled `Codec` and the
  `cmd/pingpong` `BinaryCodec` registrations are replaced by `Handle`.

### v0.5.0 — transport: TLS hooks, Address migration, QUIC (Phase 3)

#### Added

- **Transport TLS and dial/listen hooks.** `NewTCPLayer` grows
  functional options: `transport.WithDialFunc` /
  `transport.WithListenFunc` expose the raw `net.Conn` / `net.Listener`
  seams, and `transport.WithTLS(cfg)` is sugar that wires both to
  `tls.Dialer` / `tls.Listen`. They are forwarded through
  `protorun.WithTCPTransport(ctx, topts...)`, so TLS and mTLS are a
  one-liner with no fork — the framing and Hello/Ack handshake are
  byte-for-byte unchanged, TLS just terminates below them. How-to with
  server-TLS and mTLS snippets in `docs/how-to-tls.md`. Out-of-tree
  Layer backends get `transport.NewConnected/NewDisconnected/NewFailed`
  constructors for the events whose `peer` field is unexported.
- **QUIC transport backend (`transport/quic` nested module).**
  `quic.NewLayer(self, ctx, tlsConf, ...)` implements `transport.Layer`
  over `github.com/quic-go/quic-go`: one connection per peer pair, one
  bidirectional stream, the exact same 4-byte length framing and
  `LayerIdentifier` byte as TCP, so the SessionLayer and runtime run
  unchanged on top. TLS is mandatory (QUIC has no cleartext mode) with a
  distinct `protorun` ALPN; `quic.DevTLS()` mints a throwaway in-memory
  self-signed config for tests/dev (not for production). Its own go.mod
  + `replace`, tracked in `go.work`, Makefile `MODULES`, and CI — the
  core module stays zero-dependency.

#### Changed

- **`transport.Layer` is addressed by `transport.Address`, not `Host`.**
  `Connect`, `Disconnect`, and `Send` now take `transport.Address`;
  `Message.Host`/`Event.Host()` became `Message.Peer`/`Event.Peer()`
  (both `Address`). `Host` (ip:port) still implements `Address` and
  remains the endpoint type TCP and QUIC use, but the interface no
  longer hard-codes it — a custom backend can address peers by anything.
  The SessionLayer is the single translation point between transport
  `Address`es and the stable logical `Host`s protocols see: the
  `protorun.Sessions` seam, the runtime, and the retry table stay
  `Host`-based and unchanged, and the Hello still carries the dialer's
  logical `Host` unchanged on the wire. As a byproduct the SessionLayer
  now resolves inbound application messages to their logical `Host`
  (previously it surfaced the raw transport endpoint).

### v0.6.0 — prototest deterministic simulation (Phase 4)

#### Added

- **Deterministic simulation (`prototest.Sim`).** A whole protocol
  stack — real runtimes, real protocols, real IPC — runs under a seeded
  scheduler on the mesh's shared virtual clock. `NewSim(t,
  WithSeed(n))`; `sim.Node(host, protocols...)` returns the real
  runtime; `sim.Run(d)` / `sim.RunUntil(pred, max)` / `sim.Step()` drive
  it. The scheduler delivers network events in seeded order, settles
  every runtime to quiescence, then advances the clock to the next
  timer/delivery deadline, so timers, request timeouts, and retry
  backoff are all deterministic and a 30-second convergence test runs in
  milliseconds. A given seed replays byte-identically (proven by
  `TestSim_DeterministicTrace` under `-race -count=20`) for protocols
  that follow the authoring contract. Full write-up in
  `docs/simulation.md`.
- **Mesh network-fault injection.** `mesh.Cut(a,b)` /
  `mesh.Heal(a,b)` / `mesh.Isolate(h)` / `mesh.SetLoss(a,b,p)` /
  `mesh.SetDelay(a,b,d,jitter)`. A Cut tears down any live session
  (SessionDisconnected both sides) and loses in-flight messages; Heal
  makes the link reachable again but does **not** auto-reconnect —
  protocols reconnect themselves, as in production. Loss and jitter draw
  from the mesh's single seeded RNG. `NewMesh(t, opts...)` gains
  `WithSeed(int64)` and `WithRealClock()`, and always logs its seed for
  reproduction.
- **`Runtime.Quiescent() bool`.** A small introspection probe (all
  protocol mailboxes empty and no handler mid-dispatch), backed by a
  per-protocol in-flight counter incremented before enqueue and
  decremented after dispatch. Documented as test-harness/diagnostics
  surface; it is how the Sim detects when to take its next scheduling
  step. A companion `SyncDeliverer` / `InboundSink` seam lets a Sessions
  adapter deliver inbound traffic synchronously (the mesh under a Sim),
  which is what makes quiescence observable; production adapters don't
  implement it.

#### Changed

- **`prototest.NewRuntime` runs on virtual time by default.** Nodes on
  one mesh now share the mesh's `FakeClock` (exposed via `mesh.Clock()`),
  so their timers advance on one controllable timeline. Build the mesh
  with `prototest.WithRealClock()` for wall time. `prototest.NewMesh`
  now takes `testing.TB` (`NewMesh(t, opts...)`), so it can log its seed.
- **`ctx.Every` no longer spawns a per-timer goroutine.** Periodic
  timers are now built on the one-shot `AfterFunc` seam (re-arming after
  each fire), so a virtual clock fires them synchronously on the
  goroutine that advances it — the property the Sim's quiescence
  detection needs — and production pays for no extra goroutine per
  periodic timer. Scheduling is drift-free on a virtual clock and drifts
  only by handler latency (not cumulatively) on the real clock. Public
  API (`ctx.Every` / `TimerHandle`) is unchanged.

#### Removed

- **`Clock.NewTicker` / `ClockTicker`.** The `Clock` seam is now a
  single primitive, `AfterFunc`; `ctx.Every` is built on it (see
  Changed). Removing the ticker keeps the seam small enough that a
  virtual clock can control every scheduled fire — including periodic
  ones — synchronously. `prototest.FakeClock` loses its ticker methods
  accordingly.

### v0.7.0 — protocol library (Phase 5)

#### Added

- **Protocol library (`protocols/`).** Three zero-dependency
  packages in the core module. `protocols/membership` is the
  interchangeability seam: a types-only IPC contract — `GetView` /
  `View{Active}` (request/reply) and `NeighborUp` / `NeighborDown`
  (notifications). It carries no codecs and no `WireName` on purpose:
  IPC is local-only, so the contract never touches the wire. Any
  dissemination protocol written against it runs over any membership
  protocol that publishes it. `protocols/hyparview` is a faithful
  HyParView (Leitão/Pereira/Rodrigues 2007): small symmetric
  session-backed active view + larger passive view, JOIN via contact,
  ForwardJoin ARWL/PRWL random walks, periodic Shuffle, Neighbor
  promotion on active-view failure with the paper's empty-view priority
  rule, and failure detection off the session layer (`OnSessionDisconnected`
  / `OnSessionGivenUp`) with no extra heartbeats. Shuffles are routed
  entirely over active-view links — the request is a TTL walk over active
  views and the reply retraces the walk's recorded path back to the
  origin — so no transient sessions to passive peers are ever opened
  (resolving the roadmap's Phase 5 open question). `protocols/plumtree`
  is a faithful Plumtree ("Epidemic Broadcast Trees", 2007) over the
  contract: eager-push tree links + lazy-push IHAVE announcements
  (batched), duplicate-triggered PRUNE and missing-message-timer GRAFT
  tree repair, sender+seq message ids, and a bounded GRAFT cache
  (`Config.CacheSize`; long partitions beyond the cache are the app's
  problem, by design). Public surface mirrors the gossip example's
  trigger pattern: a `Broadcast` request plus a `Delivered{ID, Payload,
  From}` notification. Every wire message implements `WireName()` and a
  `SelfMarshaler` codec (a `transport.Host`'s platform-int port rules out
  the reflective `WireCodec`). Both protocols expose a `Config` with
  sane zero-value defaults. Primary test suites run on `prototest.Sim`:
  20-node HyParView convergence / churn / shuffle-rotation and 20-node
  Plumtree exactly-once broadcast / spanning-tree duplicate-bound /
  partition-heal, seeded and sub-5s wall time under `-race -count=5`,
  plus pure-logic unit tests. Story in `docs/protocols.md`.
- **`cmd/gossip` rewritten onto the contract; new `cmd/broadcast`.**
  The gossip example's membership protocol now implements
  `protocols/membership` (a static contact list is the simplest possible
  contract *implementation* — a pedagogical baseline), and its eager-push
  gossip subscribes to `NeighborUp`/`NeighborDown` instead of private
  wiring; the 10-node integration and 100/1000-node scale tests are
  unchanged. `cmd/broadcast` is the flagship demo: Plumtree over
  HyParView over TCP, stdin lines broadcast to the cluster and Delivered
  lines printed, with a real-TCP integration test on the 7400+ port band.

### v0.8.0 — launch plumbing: config, otel, benchmarks, docs, diagnostics (Phase 6)

#### Added

- **`config` (nested module).** YAML runtime configuration via
  `gopkg.in/yaml.v3`. `config.Load(path)` parses a document with a
  reserved `runtime:` block (`logging.level`/`components`/`format`,
  mirroring `protorun.LoggingConfig`) plus arbitrary named sections;
  `config.Section[T](f, name)` decodes one named section into a
  caller-defined type, strictly (`yaml.v3` `KnownFields`, documented in
  the package doc alongside the "no framework magic" philosophy:
  protocols receive config through their constructors, never through
  reflection over `Register` calls). `f.Runtime().Options()` builds the
  implied `[]protorun.Option` (today, `WithLogger`). `cmd/pingpong`'s
  private config package is replaced by this module; its example YAML
  gains a `pingpong:` section (`StartSeq`) demonstrating `Section[T]`.
- **`otel` (nested module).** `otel.Metrics(meter metric.Meter)
  protorun.Metrics` adapts Counter/Histogram onto OpenTelemetry
  instruments, caching one instrument per metric name (`sync.Once` per
  name in a `sync.Map`) and mapping `protorun.Attr` to
  `attribute.KeyValue`. An instrument-creation error is reported once
  via OTel's own global error handler and that name becomes a
  permanent no-op afterward — the metrics path never panics. Tested
  against `go.opentelemetry.io/otel/sdk/metric`'s `ManualReader`.
- **Benchmarks.** Two new benchmarks close the roadmap's stated gaps:
  `BenchmarkMailbox_EnqueueDispatchLatency` (cross-goroutine mailbox
  push-to-observe latency, isolated from decode/handler cost) and
  `BenchmarkTCP_RoundTrip` (the only benchmark that exercises a real
  TCP session end to end: two `Runtime`s on localhost, one Ping→Pong
  round trip per iteration). `docs/benchmarks.md` publishes the
  measured numbers (this machine, this run only — no third-party
  numbers), the methodology per benchmark, and an honest "comparing
  with actor frameworks" section explaining why ns/op isn't
  comparable across frameworks with a different unit of work.
- **Diagnostics: rate-limited unknown-wireID warning.** `processMessage`
  now logs "received message for unknown wireID" at most once per
  wireID per minute (`unknownWireIDWarnWindow`), so a stale peer or a
  version-skewed one spamming a bad id doesn't flood the log; the
  `protorun.message.dropped_unknown_id` counter is unaffected and still
  increments on every occurrence.
- **Docs set (Diátaxis).** `docs/README.md` indexes the docs as
  tutorial / how-to / reference / explanation. New: `docs/tutorial.md`
  ("your first protocol", built on `prototest.Sim`, a tiny
  counter/echo protocol from zero to a passing deterministic test),
  `docs/how-to-custom-codec.md` (`SelfMarshaler` vs. a full `Codec[M]`),
  `docs/how-to-custom-transport.md` (the `transport.Layer` contract,
  walked through `transport/quic` as a worked second implementation),
  and `docs/concurrency-model.md` (the event-loop/mailbox/IPC model,
  and why protocol composition is a different model from actors — the
  roadmap's positioning statement, in user-facing prose).

#### Changed

- **README repositioned** around "protocol composition runtime — Babel
  for Go": an early, factual comparison table against Proto.Actor,
  Ergo, GoAkt, and Hollywood (unit of composition, coordination model,
  membership/broadcast batteries, deterministic simulation, zero-dep
  core), the seeded-simulation pitch moved up with a short snippet, new
  Configuration/Metrics subsections documenting the `config`/`otel`
  modules, and the Documentation section now points at the
  `docs/README.md` index. The quick start stays early and unchanged.
- **`Sender.Send`'s error-semantics doc callout made unmissable**: the
  `protocol.go` doc comment and a new README callout both spell out
  that the return value is synchronous/local-only (`ErrNoCodec`, ...)
  and that delivery failure surfaces later as a session event, not as
  this call's return value — previously stated in passing, now a
  boxed callout in both places.

## v0.1.0, 2026-05-02

First tagged release. Established the protocol-runtime core, IPC
plane, recovery semantics, observability hooks, and a runnable
multi-layer example (membership + eager-push gossip with a
10-node integration test).

### Added

- **Core runtime.** `protorun.New(self, ...Option)`, `Register`,
  `Run`, `Cancel`. Per-protocol event-loop concurrency. Component-
  tagged `slog` logging.
- **Type-safe message dispatch.** Wire IDs derived from Go type
  names (FNV-1a) with `WireName()` opt-in to freeze the ID across
  renames. Generic `RegisterCodec[*M]`, `RegisterHandler[*M]`,
  `BinaryCodec[*M]` for fixed-size structs, `wire` for
  variable-length encoding.
- **TCP transport with handshake.** `WithTCPTransport(ctx)` wires a
  TCPLayer + SessionLayer. Hello/Ack handshake binds ephemeral
  connections to stable Host identities. Session events
  (`SessionConnected` / `SessionDisconnected` / `SessionFailed` /
  `SessionGivenUp` / `SessionVersionMismatch`) deliver to optional
  protocol-side handler interfaces.
- **Reconnect policy.** `ConnectWithRetry(host)` with
  `WithRetryPolicy` (exponential backoff + jitter). Defaults are
  unbounded retries; configurable per-runtime.
- **Inter-protocol communication (IPC).** Same-runtime Request/Reply
  via `RegisterRequestHandler[Req,Rep]` + `SendRequest`, and fan-out
  Notifications via `SubscribeNotification[N]` /
  `PublishNotification[N]`. Local-only by design; cross-node
  coordination still flows through the peer-message path.
- **Panic recovery.** Every handler dispatch runs under a `recover`
  so a single bad protocol can't take down its event loop. Optional
  `PanicHandler` interface for metrics / supervision; request
  handlers auto-fail their `Responder` with `ErrHandlerPanicked`.
- **Capability-typed `ProtocolContext`.** Composes finer-grained
  interfaces: `Connector`, `Sender`, `Timing`, `Identity`. Existing
  code is source-compatible; new code can declare narrower deps.
- **Strict mode.** `WithStrict(bool)` opt-in runtime invariant
  checks: double-registration, phase ordering, slow-handler
  watchdog (configurable via `WithStrictHandlerTimeout`),
  reply-without-handler diagnostics.
- **Metrics interface.** `Metrics` (Counter + Histogram with `Attr`
  attributes) + `WithMetrics(m)`. Default no-op. Instruments core
  paths (`protorun.message.*`, `protorun.ipc.*`,
  `protorun.notification.*`, `protorun.session.*`,
  `protorun.handler.panic`, `protorun.strict.slow_handler`).
- **Bounded shutdown.** `Runtime.Shutdown(timeout)` returns
  `ErrShutdownTimeout` if the WaitGroup hasn't drained when the
  timer fires. `Cancel` stays unbounded.
- **Wire-format version negotiation.** `transport.ProtocolVersion`
  is part of every Hello; mismatched peers see a typed
  `SessionVersionMismatch` event. Lock-stepped at v0.1.0; bump
  alongside any framing change.
- **Address interface.** `transport.Address` (`String` + `Equal`)
  defines the abstract peer-identity surface. `Host` satisfies it.
  Future non-TCP transports plug in here. The Layer migration to
  take Address everywhere is deferred (see TODO.md).
- **Custom transport injection.** `WithTransport(layer, session)`
  closes the IoC seam: pre-built transport stacks plug in via the
  public Option, no internal access required.
- **Examples.** `cmd/pingpong` (canonical two-node) and `cmd/gossip`
  (membership + eager-push gossip with a 10-node integration test
  and 100/1000-node scale probes). 10-node test stable under
  `go test -race -count=10`.
- **Benchmarks.** `BenchmarkProcessMessage`, `BenchmarkSendRequest`,
  `BenchmarkBinaryCodec_*`, `BenchmarkPublishNotification_Fanout`
  (1/10/100 subscribers), `BenchmarkWireID`. `make bench` target.
- **Goleak in TestMain.** Every test package is goroutine-leak
  guarded.
- **CI** (build, vet, race tests, lint with new-from-rev gate,
  govulncheck, coverage threshold gate at 60%).
- **`golangci-lint`** config with Go-community-aligned thresholds
  (`gocyclo`, `cyclop`, `gocognit`, `funlen`, `lll`, `dupl`,
  Uber-aligned style rules).
- **`pre-commit` hook** (lint + tests on staged Go files via
  `make hooks-install`).

### Out of scope (post-v0.1.0)

See [`TODO.md`](TODO.md). Highlights: full `Address` migration of
`transport.Layer`; HyParView / SWIM gossip-membership; wire-level
TLS; richer per-protocol configuration; benchmark baseline tracking.
