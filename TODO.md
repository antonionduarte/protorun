# TODO

Pre-launch roadmap lives in [`docs/roadmap.md`](docs/roadmap.md):
full design-level plans, phase by phase, each ending in a tag.
Everything below v1.0 may break API and wire format.

## Pending (see docs/roadmap.md for designs)

- [ ] Phase 1 (v0.3.0): supervision — factory registration, panic
      directives (Resume/Restart/Stop/Escalate), restart with
      session replay, restart budget + backoff.
- [ ] Phase 2 (v0.4.0): `WireCodec[M]` reflective codec for
      variable-length messages, `Handle` one-line registration,
      `codec/protobuf` nested module, `JSONCodec`.
- [ ] Phase 3 (v0.5.0): `Address` migration for `transport.Layer`
      (was pending here already), dial/listen hooks +
      `transport.WithTLS`, `transport/quic` nested module.
- [ ] Phase 4 (v0.6.0): prototest deterministic simulation — mesh
      fault injection (cut/heal/loss/delay), seeded schedules,
      virtual time via `FakeClock`, `prototest.Sim` harness.
- [ ] Phase 5 (v0.7.0): protocol library — `protocols/membership`
      IPC contract, `protocols/hyparview`, `protocols/plumtree`,
      sim-based convergence/churn/partition suites.
- [ ] Phase 6 (v0.8.0+): `config` + `otel` nested modules,
      published benchmarks, Diátaxis docs set, README
      repositioning, diagnostics polish (unknown-wireID warnings,
      Send error-semantics callout), v1.0 freeze.

## Done

- [x] Phase 0 (v0.2.0): module rename to `protorun`, unified
      per-protocol mailbox with overflow policy + dead-letter hook,
      handle-based timer API (`ctx.After`/`ctx.Every`), `Clock` seam
      with `prototest.FakeClock`.

- [x] Handshake hardening: dialer waits for Ack before Established
      (bounded by a handshake timeout); version mismatch answered
      with an explicit Reject that the runtime translates into an
      immediate terminal `OnSessionGivenUp`. Authoritative wire spec
      in `docs/wire-format.md`.
- [x] `Sessions` seam: the runtime holds its session stack behind
      `protorun.Sessions`; `*transport.SessionLayer` is the
      production adapter.
- [x] `prototest`: exported in-memory mesh + runtime fixture so
      protocol authors can test protocols without TCP.
- [x] Basic structure (Protocol, Runtime, ProtocolContext).
- [x] TCP transport layer with length-prefixed framing.
- [x] SessionLayer with Hello/Ack handshake binding ephemeral
      connections to stable Host identities.
- [x] Type-hashed message dispatch via `WireID[T]` (FNV-1a on Go
      type name; opt-in `WireName()` override for rename-safety).
- [x] Generic typed handlers (`RegisterHandler[*M]`).
- [x] BinaryCodec for fixed-size messages; `wire` helpers for
      variable-length.
- [x] Per-protocol event-loop concurrency over one ordered mailbox
      with a per-protocol overflow policy (`WithMailbox`).
- [x] Handle-based timer system: `ctx.After` / `ctx.Every` returning
      a cancellable `TimerHandle`, over a `Clock` seam.
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
