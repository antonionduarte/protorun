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
- **`pkg/prototest`.** In-memory mesh (no wire, no handshake,
  deterministic in-process delivery) implementing `Sessions`, plus a
  `NewRuntime` fixture: protocol authors can test full runtimes
  without TCP or port management.
- **Wire-format spec.** `docs/wire-format.md` is the authoritative
  envelope description; per-layer code comments now describe only
  the bytes that layer owns.

### Changed

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
  `BinaryCodec[*M]` for fixed-size structs, `pkg/wire` for
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
