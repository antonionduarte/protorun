# Changelog

All notable changes to protorun will be documented in this file. The
format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The framework is pre-1.0; minor versions may break API. Wire format
is versioned via the session-layer handshake (`transport.ProtocolVersion`).

## Unreleased

Architecture pass driven by a deep-module review: handshake
truthfulness, an explicit seam for session management, and a
first-class testing package for protocol authors.

### Added

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

### Changed

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
- **Timer API redesigned around handles.** `ctx.After(d, fn)` and
  `ctx.Every(d, fn)` replace the old `Timer`/`TimerID` surface. Both
  return a `TimerHandle` with an idempotent `Cancel` that is safe
  after fire and from inside the protocol's own handlers (a same-loop
  cancel guarantees the callback never runs, even if the fire is
  already queued). The payload rides by closure capture; there are no
  user-managed IDs and no silent replacement, and every handle a
  protocol owns is cancelled on shutdown.
- **Module renamed to `github.com/antonionduarte/protorun`.**
  Previously `github.com/antonionduarte/go-simple-protocol-runtime`.
  `pkg/protorun` is promoted to the module root, and `pkg/transport`,
  `pkg/wire`, `pkg/prototest` move to `transport/`, `wire/`,
  `prototest/`. Import paths change accordingly; no behavior change.
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

### Fixed

- `SessionVersionMismatch` events are now routed by the runtime
  (previously dropped silently) and emitted through the ctx-guarded
  `emitEvent` path (previously a raw channel send that could block
  the session goroutine at shutdown). The runtime's event mapper now
  has a loud default plus a coverage test so new event kinds can't
  be silently unrouted.

### Removed

- **Old timer surface.** `Timer`, `TimerID()`, `SetupTimer`,
  `SetupPeriodicTimer`, `CancelTimer`, and `RegisterTimerHandler` are
  gone (no deprecation shim); use `After` / `Every` / `TimerHandle`.
- **`WithChannelBuffer`.** The runtime-wide per-channel buffer option
  is replaced by per-protocol `WithMailbox`.

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
