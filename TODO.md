# TODO

Mostly historical: the framework is at the point where the remaining
work is feature-additive rather than foundational. Most checked items
were completed during the polish passes leading up to v0.1.0.

## Pending

- [ ] Migrate `transport.Layer` and downstream APIs to take
      `Address` instead of `Host`. The interface exists (so users
      can declare custom address types), but the concrete Layer
      methods still take Host. Will land once a real non-TCP
      backend (UDP / in-memory mesh / QUIC) needs the abstraction.
- [ ] Configuration file parser at the runtime level (per-protocol
      config). The pingpong example has YAML for logging only.
- [ ] Better diagnostics: net-layer wrapper errors, louder
      "protocol not registered for received wireID" warnings,
      louder error when a runtime starts without a network layer
      registered (already returns `ErrNoNetworkLayer`).

## Done

- [x] Handshake hardening: dialer waits for Ack before Established
      (bounded by a handshake timeout); version mismatch answered
      with an explicit Reject that the runtime translates into an
      immediate terminal `OnSessionGivenUp`. Authoritative wire spec
      in `docs/wire-format.md`.
- [x] `Sessions` seam: the runtime holds its session stack behind
      `protorun.Sessions`; `*transport.SessionLayer` is the
      production adapter.
- [x] `pkg/prototest`: exported in-memory mesh + runtime fixture so
      protocol authors can test protocols without TCP.
- [x] Basic structure (Protocol, Runtime, ProtocolContext).
- [x] TCP transport layer with length-prefixed framing.
- [x] SessionLayer with Hello/Ack handshake binding ephemeral
      connections to stable Host identities.
- [x] Type-hashed message dispatch via `WireID[T]` (FNV-1a on Go
      type name; opt-in `WireName()` override for rename-safety).
- [x] Generic typed handlers (`RegisterHandler[*M]`).
- [x] BinaryCodec for fixed-size messages; `pkg/wire` helpers for
      variable-length.
- [x] Per-protocol event-loop concurrency.
- [x] Timer system: `SetupTimer`, `SetupPeriodicTimer`,
      `CancelTimer`, `RegisterTimerHandler`.
- [x] Reconnect policy with exponential backoff + jitter
      (`ConnectWithRetry`).
- [x] Inter-protocol communication: Request/Reply via
      `RegisterRequestHandler` + `SendRequest`, fan-out
      Notifications via `SubscribeNotification` /
      `PublishNotification`. Local-only.
- [x] Panic recovery for handler dispatch: every handler runs
      under a `recover` so one bad protocol can't take down its
      event loop. Optional `PanicHandler` interface; request
      handlers auto-fail their `Responder` with
      `ErrHandlerPanicked`.
- [x] Pingpong example.
- [x] Multi-layer gossip example with 10-node integration test
      and 100/1000-node scale probes.
- [x] Goleak-verified shutdown.
- [x] Sentinel errors, `errors.Is`-able.
- [x] Component-tagged structured logging via `slog`.
- [x] CI: build, vet, lint, race tests, govulncheck, coverage gate
      on every PR / push.
- [x] LICENSE (MIT).
- [x] README rewrite, CONTRIBUTING.md.
- [x] Capability-typed `ProtocolContext` (composes `Connector`,
      `Sender`, `Timing`, `Identity`).
- [x] Strict mode (`WithStrict(true)`): double-registration,
      phase ordering, slow-handler watchdog, reply-without-handler
      diagnostics.
- [x] Metrics interface (`Metrics` + `WithMetrics(m)`).
- [x] Benchmarks for hot paths (`make bench`).
- [x] Bounded shutdown (`Runtime.Shutdown(timeout)`).
- [x] Wire-format version negotiation in the Hello handshake.
- [x] `transport.Address` interface defined; Host implements it.
- [x] Runtime decomposition (extracted `codecRegistry` and
      `ipcRouter`).
- [x] Tag `v0.1.0`.

## Considered but out of scope

- Multi-node gossip-membership (HyParView / SWIM). The static
  membership in `cmd/gossip` is enough for framework validation.
- Plumtree / lazy-push spanning-tree gossip. Eager push is the
  right baseline.
- Wire-level TLS / authentication. Out of scope for the framework
  itself; layer it on top.
- Connection-pool / multiplexing. One TCP conn per peer pair is
  fine for protorun's scope.
