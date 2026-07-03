# TODO

Pre-launch roadmap lives in [`docs/roadmap.md`](docs/roadmap.md):
full design-level plans, phase by phase, each ending in a tag.
Everything below v1.0 may break API and wire format.

## Pending (protoviz — visual protocol debugger)

- [x] Phase A: trace format v1 + `prototest` Sim recorder
      (`WithTrace`) + viewer app (`viz/`, Vite + React + ShadCN
      default) with Topology/Sequence/Inspector lenses and a step
      scrubber. Design: `docs/visualizer-design.md`.
- [x] Phase B: protocol lenses (membership / broadcast-tree /
      consensus) + trace-artifact-on-failure test helper.
      Viewer at `viz/` (see `viz/README.md`); tolerant parser +
      keyframed fold engine, six lenses, ⌘K palette, fault ribbon.
      Deviation: plumtree tree edges are derived from the Gossip/
      Prune/Graft/IHave stream, since `DebugStatsReply` carries only
      counters, not per-peer eager/lazy lists.
- [ ] Phase C: live mode — `Tracer` runtime seam, `cmd/protoviz`
      server, WebSocket streaming.

## Pending (v0.9 API window — consensus-author friction)

- [x] `SessionFailedHandler`: reactive plain-Connect failure
      observation (was silently dropped).
- [x] `Sim.StepUntil` + `DeliveryInfo`: delivery-granular predicate
      stepping for adversarial tests.
- [x] `Responder.Fail` errors.As contract documented.
- [ ] `StaticMembership` helper: hyparview/raft/paxos each hand-roll
      the same connect-peer-set/reconnect-timer/session-tracking
      plumbing (~30 lines); extract it.
- [x] Self-delivery: `Send(msg, self)` now loops back through the
      codec via the normal inbound path (fresh decoded instance, no
      aliasing, FIFO behind the sender's mailbox). `pkg/protocols/
      paxos` still hand-folds its local vote — correct and tested, so
      left alone; simplifying it onto self-delivery is optional
      cleanup for whenever the package is next touched.

## Pending

- [ ] v1.0.0 gate: wire format frozen (version byte already
      negotiated in Hello), API frozen, CHANGELOG discipline
      maintained from v0.2.0 onward. Every phase 0-6 is done; this is
      a release-process gate, not a code deliverable — see
      `docs/roadmap.md`'s intro.

See [Post-launch](#post-launch) below for what's explicitly deferred
past v1.0.

## Done

- [x] Phase 6 (v0.8.0+): nested `config` module (`config.Load`,
      `config.Section[T]`, strict `yaml.v3` decoding, `f.Runtime().
      Options()`; replaces `cmd/pingpong`'s private config package)
      and nested `otel` module (`otel.Metrics(meter)` adapting
      `protorun.Metrics` onto OpenTelemetry instruments, cached per
      name, never panicking on a failed instrument creation).
      `docs/benchmarks.md`: measured numbers (codec cost, `WireID`,
      mailbox scheduling latency, in-process IPC, real-TCP round
      trip) plus methodology and an honest actor-framework comparison
      caveat; two new benchmarks
      (`BenchmarkMailbox_EnqueueDispatchLatency`,
      `BenchmarkTCP_RoundTrip`) closed the roadmap's stated gaps.
      Diagnostics: unknown-wireID warning rate-limited to once per
      wireID per minute; `Send`'s local-only error semantics get an
      unmissable doc callout in `protocol.go` and the README. Diátaxis
      docs set: `docs/README.md` index, `docs/tutorial.md`, `docs/
      how-to-custom-codec.md`, `docs/how-to-custom-transport.md`,
      `docs/concurrency-model.md`. README repositioned around
      "protocol composition runtime — Babel for Go" with an early
      actor-framework comparison table. See `docs/roadmap.md`'s Phase
      6 write-up for deviations from the original sketch.
- [x] Phase 5 (v0.7.0): protocol library under `/pkg/protocols` (all
      zero-dependency, in the core module). `pkg/protocols/membership`:
      types-only IPC contract (`GetView`/`View{Active}`, `NeighborUp`/
      `NeighborDown`) — the interchangeability seam, no codecs/WireName
      (IPC is local-only). `pkg/protocols/hyparview`: faithful HyParView
      (active/passive views, JOIN, ForwardJoin ARWL/PRWL walks, periodic
      Shuffle routed over active links with a path-retracing reply — no
      transient sessions, resolving the roadmap open question — Neighbor
      promotion with the empty-view priority rule, session-layer failure
      detection). `pkg/protocols/plumtree`: faithful Plumtree over the
      contract (eager/lazy sets, batched IHAVE, GRAFT/PRUNE tree repair,
      sender+seq ids, bounded GRAFT cache). Every wire message has
      `WireName()` + a `SelfMarshaler` codec. Sim-based suites
      (convergence/churn/shuffle, broadcast exactly-once/duplicate-bound/
      partition-heal) + pure-logic unit tests, `-race -count=5` stable.
      `cmd/gossip` rewritten onto the contract (static membership = the
      simplest contract impl); new `cmd/broadcast` flagship (Plumtree/
      HyParView/TCP, stdin→cluster, real-TCP integration test).
- [x] Phase 4 (v0.6.0): prototest deterministic simulation —
      `prototest.Sim` runs a full protocol stack under a seeded
      scheduler on the mesh's shared `FakeClock` (`NewSim`/`Node`/
      `Run`/`RunUntil`/`Step`); mesh fault injection
      (`Cut`/`Heal`/`Isolate`/`SetLoss`/`SetDelay`) off one seeded RNG;
      `NewMesh(t, WithSeed/WithRealClock)` logging its seed; `NewRuntime`
      on virtual time by default. Quiescence via `Runtime.Quiescent()`
      (per-protocol in-flight counter) plus a `SyncDeliverer`/
      `InboundSink` synchronous-delivery seam; `ctx.Every` reimplemented
      on `AfterFunc` and `Clock.NewTicker` removed so periodic timers are
      goroutine-free and deterministic. Proof tests: byte-identical
      same-seed trace under `-race -count=20`, partition/heal in
      sub-second wall time, loss/delay ordering, virtual-time timers.
      Guide in `docs/simulation.md`.
- [x] Phase 3 (v0.5.0): transport — `transport.Layer` addressed by
      `transport.Address` (`Message.Peer`/`Event.Peer()`), SessionLayer
      the sole `Address`→logical-`Host` translation point (`Sessions`
      seam + retry table unchanged, Hello unchanged on the wire);
      `NewTCPLayer` dial/listen hooks (`WithDialFunc`/`WithListenFunc`)
      and `WithTLS` sugar forwarded through `WithTCPTransport`;
      `pkg/transport/quic` nested module (`quic.NewLayer` on `quic-go`, one
      conn + one bidi stream per peer, same framing, `protorun` ALPN,
      `quic.DevTLS`); TLS/mTLS + hooks + QUIC layer/session/two-runtime
      tests; `docs/how-to-tls.md`.
- [x] Phase 2 (v0.4.0): codec ergonomics — `WireCodec[M]` reflective
      default codec (cached per-type plan; strings/`[]byte`/slices/maps/
      arrays/nested-structs/pointers; deterministic sorted-key maps;
      normative format in `docs/wire-format.md`; round-trip + fuzz
      tests; benches vs `BinaryCodec`), `Handle` one-line registration
      (picks `WireCodec` or `SelfCodec` via `SelfMarshaler`),
      `JSONCodec` (core), `pkg/codec/protobuf` nested module
      (`ProtoCodec[M proto.Message]`, own go.mod + `replace`, tracked
      `go.work`), strict-mode WireName nudge. `cmd/gossip` and
      `cmd/pingpong` migrated to `Handle`.
- [x] Phase 1 (v0.3.0): supervision — `RegisterFactory` +
      `WithSupervision`, panic directives
      (Resume/Restart/Stop/Escalate), per-protocol supervisor with
      quarantine + mailbox drain, pending-request auto-fail
      (`ErrProtocolRestarting`), owner-indexed deregistration,
      cancellable `ExpBackoff`, session replay, `RestartHandler`,
      restart budget → `OnGiveUp`, `ProtocolFailed` notification +
      `protorun.protocol.restart` metric, `ErrProtocolFailed` from
      `Run`/`Shutdown` on escalate.
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

- HyParView and Plumtree were "out of scope" pre-launch but shipped
  in Phase 5 as `pkg/protocols/hyparview` and `pkg/protocols/plumtree`. SWIM
  stays out of scope (memberlist owns that niche); consensus
  (Paxos/Raft over protorun) is a v1.x showcase, not a launch battery.
- Wire-level TLS / authentication is not baked into the *protocol*
  layer, but Phase 3 added the seam: `transport.WithTLS` (and the
  QUIC backend, where TLS is mandatory) make "layer it on" a
  one-liner rather than a fork.
- Connection-pool / multiplexing. One connection per peer pair (TCP
  or QUIC) is fine for protorun's scope.

## Post-launch

Everything the roadmap explicitly deferred past v1.0, gathered in one
place now that every numbered phase is done:

- **SWIM.** Out of scope by design — `memberlist` already owns that
  niche in the Go ecosystem; protorun's membership battery is
  HyParView (see `pkg/protocols/hyparview`).
- **Consensus showcase.** Proving the composition model at its hardest
  (a broadcast/dissemination protocol is comparatively forgiving;
  consensus is not).
  - **Raft — done.** `pkg/protocols/raft` ships a faithful Raft (leader
    election, log replication, current-term-only commitment, follower log
    repair, higher-term stepdown) with a `Storage` persistence seam and a
    Sim suite asserting Election/State-Machine/Leader-Completeness safety,
    minority-partition safety, dueling-candidate resolution, and
    byte-identical determinism (`-race -count=3`). Membership change (§6)
    and snapshots (§7) remain out of scope.
  - **Paxos — done.** `pkg/protocols/paxos` ships a faithful single-decree
    Paxos (Lamport's synod: disjoint ballots, promise/accept quorums, the
    Phase-2a value-adoption rule, randomized-backoff liveness) with the same
    `Storage` seam and a Sim suite asserting Agreement/Integrity, a
    multi-seed dueling gauntlet, value adoption, minority-cannot-decide,
    chosen-is-forever, and byte-identical determinism (`-race -count=3`).
    Single-decree only; Multi-Paxos / log replication is Raft's role in this
    tree, so a distinct Multi-Paxos remains an optional future data point.
- **Sim event-trace recorder.** `prototest.Sim`'s Phase 4 open question
  ("expose the step hook as a full event-trace recorder, for
  visualization, or keep it minimal") resolved to minimal for launch —
  `Step()` reports only whether progress was made. A dedicated
  recorder (for visualizing a simulated run's schedule) is deferred
  until there's a concrete visualization consumer to design it against.
- **A "supervision tuning" how-to.** Sketched for Phase 6's docs set but
  not written — the README's Supervision section and `docs/roadmap.md`'s
  Phase 1 write-up already cover `MaxRestarts`/`Window`/`Backoff`/
  `OnGiveUp` in enough depth that a fourth how-to would mostly repeat
  them. Revisit if real usage surfaces tuning questions those two
  sources don't answer.
