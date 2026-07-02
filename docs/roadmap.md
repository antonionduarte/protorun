# Roadmap to launch

Design-level plan for everything between v0.1.0 and a public launch.
The framework is unlaunched, so every phase here is allowed to break
API and wire format. Phases are ordered by dependency, not priority:
the deep breaking changes come first while they are cheap, the
protocol library comes last because it consumes everything else.

Positioning this plan serves: **protorun is a protocol-composition
runtime, not an actor framework.** The addressable unit is a protocol
layer on a node, not an actor. Nothing in Go occupies that niche
(Babel is Java-only); the actor frameworks (Proto.Actor, Ergo, GoAkt,
Hollywood) compete on actors, clustering, and supervision. We compete
on: layered protocols with typed IPC contracts, session lifecycle as
a first-class protocol event surface, and deterministic simulation of
full protocol stacks.

Each phase ends in a tag (v0.2.0 ... v0.7.0). v1.0.0 is the launch
gate: everything below done, wire format frozen.

---

## Phase 0 — breaking-changes window (v0.2.0)

The four changes that touch everything. Done first, together, so the
rest of the plan builds on the final shapes.

### 0.1 Module rename — done

Module path is now `github.com/antonionduarte/protorun` (see
CHANGELOG.md for the previous path). The core package is promoted to
the module root, and `transport`, `wire`, `prototest` live at the
repo root too. Import:

```go
import "github.com/antonionduarte/protorun"
import "github.com/antonionduarte/protorun/transport"
```

The core module stays **zero-dependency** (stdlib only) — that is a
marketing line Ergo uses and we can too. Everything that needs a
third-party dep (protobuf codec, QUIC transport, OTel adapter, YAML
config) becomes a nested module with its own go.mod (`codec/protobuf`,
`transport/quic`, `otel`, `config`).

The GitHub repository is renamed to `antonionduarte/protorun`; the
old name redirects.

### 0.2 Unified mailbox + backpressure policy — done

Delivered: one ordered mailbox per protocol carrying a tagged
`protoEvent` union; `Register` grows `WithMailbox(Mailbox{Capacity,
Overflow})` with `OverflowBlock` (default) / `DropOldest` /
`DropNewest` / `Unbounded`; `WithDeadLetter` hook; `protorun.mailbox.
depth` + `protorun.mailbox.dropped` metrics; strict-mode 80%-occupancy
warning. Design record below.

Today each protocol has six buffered channels (messages, timers,
session events, requests, replies, notifications) drained by one
`select`. Two problems:

- **No ordering across kinds.** A message from peer P can be handled
  after the `SessionDisconnected` for P that was emitted before it,
  because they ride different channels and `select` picks randomly.
- **Head-of-line blocking.** `processMessage` and
  `fanoutSessionEvent` block the single dispatcher goroutine when a
  mailbox is full, so one slow protocol stalls delivery to every
  protocol.

Replace the six channels with **one ordered mailbox per protocol**
carrying a tagged `protoEvent` union. Arrival order is preserved
across event kinds, which makes session-event/message interleaving
deterministic and makes Phase 4 simulation possible.

Overflow policy is chosen at registration:

```go
rt.Register(p, protorun.WithMailbox(protorun.Mailbox{
    Capacity: 1024,                  // default 1024
    Overflow: protorun.OverflowBlock, // Block | DropOldest | DropNewest | Unbounded
}))
```

- `OverflowBlock` (default): dispatcher blocks — today's behavior,
  but now documented and per-protocol.
- `DropOldest` / `DropNewest`: dropped events go to a runtime-level
  dead-letter hook `WithDeadLetter(func(DeadLetter))` and a
  `protorun.mailbox.dropped` counter. Replies are safe to drop
  because `SendRequest` timeouts already cover the lost-reply case.
- `Unbounded`: linked-list mailbox for protocols that must never
  block the dispatcher (accepting the memory risk explicitly).

New metrics: `protorun.mailbox.depth` (histogram, sampled on
enqueue), `protorun.mailbox.dropped`. Strict mode warns on sustained
>80% occupancy.

### 0.3 Timer API redesign — done

Delivered: `ctx.After` / `ctx.Every` returning a `TimerHandle` with an
idempotent, fire-safe, same-loop-safe `Cancel`; monotonic-`uint64`
keys; per-protocol handle tracking with cancel-all on shutdown. The
old `Timer`/`TimerID`/`SetupTimer`/`SetupPeriodicTimer`/`CancelTimer`/
`RegisterTimerHandler` surface is removed. Design record below.

The current surface (`Timer` interface, user-managed `TimerID() int`
uniqueness, `RegisterTimerHandler`, silent overwrite on ID reuse) is
the weakest API in the framework. Delete it entirely. New surface:

```go
h := ctx.After(500*time.Millisecond, func() { ... })  // one-shot
h := ctx.Every(time.Second, func() { ... })           // periodic
h.Cancel()                                            // idempotent
```

- Payload travels by closure capture — no `Timer` struct, no ID.
- Handles are internally keyed by a monotonic `uint64`; no user
  uniqueness contract, no silent replacement.
- Callbacks fire on the owning protocol's event loop (through the
  unified mailbox), same as today.
- All handles owned by a protocol are cancelled automatically on
  protocol stop/restart (needed by Phase 1) and runtime shutdown.
- `Cancel` after fire, double-`Cancel`, and `Cancel` from inside the
  callback are all no-ops.

`Timing`, `Timer`, `SetupTimer`, `SetupPeriodicTimer`, `CancelTimer`,
`RegisterTimerHandler` are removed, not deprecated.

### 0.4 Clock seam — done

Delivered: a `Clock` interface (`Now` / `AfterFunc` / `NewTicker`)
consumed by the timer table, retry backoff, request timeouts, the
strict watchdog, and IPC latency; `WithClock` option over a zero-
allocation real-time default; `prototest.FakeClock` with `Advance`.
`prototest.NewRuntime` still uses the real clock — the default switch
lands in Phase 4. Design record below.

Introduce a `Clock` interface consumed by the timer table, retry
backoff, request timeouts, and the strict-mode watchdog:

```go
type Clock interface {
    Now() time.Time
    AfterFunc(d time.Duration, fn func()) ClockTimer
    NewTicker(d time.Duration) ClockTicker
}
```

`WithClock(c)` runtime option; default is the real clock with zero
overhead change. `prototest` ships a `FakeClock` with
`Advance(d)`. This is the foundation for deterministic simulation
(Phase 4): with a fake clock, timer order is fully controlled by the
test.

---

## Phase 1 — supervision and restart (v0.3.0) — done

Delivered: `RegisterFactory` + `WithSupervision(Supervision{...})`
with a `Directive` (`Resume`/`Restart`/`Stop`/`Escalate`), sliding-
window restart budget (`MaxRestarts`/`Window`), `ExpBackoff` (exported
`BackoffFunc`), and `OnGiveUp` (`Stop`/`Escalate`). A per-protocol
supervisor goroutine executes the numbered restart contract off the
event loop: quarantine (enqueue dead-letters, never blocks) + old-
mailbox drain, timer cancellation, pending-request auto-fail with
`ErrProtocolRestarting`, owner-indexed deregistration of codecs/routes/
subscriptions, clock-driven cancellable backoff, fresh-instance
rebuild (Start → Init), established-peer session replay, and an
optional `RestartHandler.OnRestart`. `Escalate` records a fatal error
and `Run`/`Shutdown` surface `ErrProtocolFailed`. Observability:
`protorun.protocol.restart` counter (outcome attr) and a runtime-
published `ProtocolFailed` notification (restarted/stopped/escalated).
Design record below.

Deviations from the sketch below: the observability notification is a
single `ProtocolFailed` type with an `Outcome` field (resolving the
open question) rather than `ProtocolRestarted`; the restart counter's
second attribute is `outcome`, not `where`. `Restart` on a singleton
`Register` panics in strict mode / warn-downgrades to `Resume`
otherwise.

The loudest gap versus every actor framework. Today a panicking
handler is recovered and logged, but the protocol keeps running with
whatever half-mutated state it had. That is `Resume` semantics with
no alternative.

### Registration grows a factory form

```go
rt.Register(p)                          // singleton, policy defaults to Resume
rt.RegisterFactory(newGossip,           // func() protorun.Protocol
    protorun.WithSupervision(protorun.Supervision{
        OnPanic:     protorun.Restart,  // Resume | Restart | Stop | Escalate
        MaxRestarts: 5,
        Window:      time.Minute,
        Backoff:     protorun.ExpBackoff(100*time.Millisecond, 5*time.Second),
        OnGiveUp:    protorun.Escalate, // Stop | Escalate
    }))
```

`Restart` requires a factory — fresh state is the whole point, and
reusing a singleton instance after a panic re-creates the Erlang
mistake of "restart with corrupted state". Strict mode panics at
registration if `Restart` is configured on a singleton; non-strict
logs and downgrades to `Resume`.

### Restart mechanics

On a panic with `OnPanic: Restart`:

1. Quarantine: stop draining the mailbox; discard queued events,
   routing droppable ones to the dead-letter hook.
2. Cancel all timer handles owned by the protocol.
3. Auto-fail all pending `SendRequest` callbacks with a new
   `ErrProtocolRestarting` sentinel.
4. Deregister everything the protocol owns: codecs/wire routes,
   request-handler routes, notification subscriptions. This needs an
   owner→keys reverse index on `codecRegistry` and `ipcRouter`
   (today they only map wireID→proto).
5. Construct a fresh instance from the factory, run `Start` (strict
   phase checks apply as at boot), then `Init`.
6. **Session replay:** the runtime snapshots its established-peers
   set and delivers synthetic `OnSessionConnected` for each, so the
   new instance can rebuild peer state exactly the way it built it
   the first time. Sessions are runtime-owned and survive the
   restart — no reconnect storm.

`Stop` performs steps 1–4 and removes the protocol. `Escalate`
shuts the runtime down; `Run()` returns `ErrProtocolFailed` wrapping
the panic value.

Restart budget: more than `MaxRestarts` within `Window` triggers
`OnGiveUp`.

### Observability

- `protorun.protocol.restart` counter (attrs: protocol, where).
- Runtime publishes a `protorun.ProtocolRestarted{Protocol string,
  Attempt int}` notification so sibling protocols can react.
- Optional `RestartHandler` interface: `OnRestart(attempt int)`
  called on the *new* instance after session replay.

---

## Phase 2 — codec ergonomics (v0.4.0) — done

Delivered: `WireCodec[M]`, a reflection-based default codec with a
cached per-type plan covering strings, `[]byte`, slices, maps
(sorted-key deterministic encode), arrays, nested structs, and pointers
to structs on top of every fixed-size type — normative format in
`docs/wire-format.md`, round-trip + arbitrary-bytes fuzz tests, benches
against `BinaryCodec`. `Handle(ctx, fn)` infers `M` and registers
`WireCodec[M]` (or `SelfCodec[M]` when the type implements the new
`SelfMarshaler`) plus the handler in one call. Interop: `JSONCodec[M]`
in core and a nested `codec/protobuf` module (`ProtoCodec[M
proto.Message]`, own go.mod + `replace`, tracked `go.work`, Makefile/CI
covering both modules). Strict mode gains a once-per-type WireName nudge.
`cmd/gossip` (hand-rolled codec) and `cmd/pingpong` (BinaryCodec)
migrated to `Handle`; README quick start rewritten around it.

Deviations from the sketch below: `WireCodec` itself does not check
`SelfMarshaler` — `Handle` picks `SelfCodec` for such types instead, so
the two codecs stay single-purpose; the reflective codec is pure
reflection.

### 2.1 `WireCodec[M]` — reflective default codec

A reflection-based codec that handles what `BinaryCodec` cannot:
`string`, `[]byte`, slices, maps (sorted-key encode for determinism),
nested structs, and pointers to structs, plus all fixed-size types.
Encoding is field-declaration-order, length-prefixed for var-size
values — the format gets a normative section in
`docs/wire-format.md`. Per-type encode/decode plans are compiled
once via reflection and cached (same trick as `wireIDCache`), so the
steady-state cost is a plan lookup, not per-field reflection.

Round-trip fuzz tests (`go test -fuzz`) over generated field shapes.

### 2.2 `Handle` — one-line message registration

```go
protorun.Handle(ctx, p.onPing) // func(*Ping, transport.Host)
```

Infers `M` from the handler signature and registers `WireCodec[M]` +
the handler in one call. `RegisterCodec` + `RegisterHandler` remain
for custom codecs. A `SelfMarshaler` interface
(`MarshalWire() ([]byte, error)` / `UnmarshalWire([]byte) error`)
lets a message type take over its own encoding while still using
`Handle`; `WireCodec` defers to it when implemented.

The README quick start shrinks by a third.

### 2.3 Interop codecs

- `codec/protobuf` (nested module): `ProtoCodec[M proto.Message]`
  for shops that already have .proto definitions.
- `JSONCodec[M]` in core (stdlib): debugging and wire-inspection.

Strict mode gains a nudge: registering a message without `WireName()`
logs a warning once per type (rename-safety, already documented as a
production requirement).

---

## Phase 3 — transport: TLS hook, Address migration, QUIC (v0.5.0)

### 3.1 Finish the Address migration

`transport.Layer` methods currently take `Host` even though
`Address` exists. Migrate `Layer`, `SessionLayer` internals, and the
retry table to `Address`, keeping `Host` as the TCP implementation.
This was already pending in TODO.md; the QUIC backend below is the
"real second backend" that was the trigger condition.

### 3.2 Dial/listen hooks + TLS

`NewTCPLayer` grows functional options:

```go
transport.WithDialFunc(func(ctx context.Context, addr string) (net.Conn, error))
transport.WithListenFunc(func(addr string) (net.Listener, error))
transport.WithTLS(cfg *tls.Config) // sugar: sets both to tls.Dialer/tls.Listen
```

Wire-level TLS stays out of the framework's *protocol* scope — the
handshake and framing are unchanged — but the seam makes "layer it
on top" a one-line reality instead of a fork. Ships with an mTLS
how-to (self-signed CA, both directions verified).

### 3.3 `transport/quic` (nested module)

QUIC backend on `quic-go`: one bidirectional stream per peer, same
4-byte length framing and `LayerIdentifier` byte on top. Session
handshake runs unchanged above it. Purpose: validate that
`transport.Layer` + `Address` is a real abstraction, and give
latency-sensitive users 0-RTT reconnects. Not the default; TCP
remains the reference backend.

---

## Phase 4 — prototest → deterministic simulation (v0.6.0)

The Sessions seam plus the Clock seam make protorun the only Go
framework where a *full protocol stack* can run under seeded,
virtual-time simulation. Lean into it — this is the headline
differentiator for the launch.

### Mesh fault injection

```go
mesh.Cut(a, b)            // drop the link both ways (SessionDisconnected)
mesh.Heal(a, b)
mesh.Isolate(h)           // cut all links of h
mesh.SetLoss(a, b, 0.1)   // seeded probabilistic drop
mesh.SetDelay(a, b, 5*time.Millisecond, jitter)
```

All randomness (loss, delivery interleaving) flows from one seed:
`prototest.NewMesh(t, prototest.WithSeed(42))`. A failing test
prints its seed; re-running with the seed reproduces the exact
schedule.

### Virtual time

`prototest.NewRuntime` wires `FakeClock` by default. The mesh gains
a scheduler pump:

```go
sim := prototest.NewSim(t, prototest.WithSeed(seed))
sim.AddNode(...)                      // runtime + protocols
sim.Run(30*time.Second, func(step prototest.Step) {
    // invariant checks per step; virtual time, finishes in ms real time
})
```

`Run` alternates: drain all deliverable events (in seeded order),
then advance the fake clock to the next timer deadline. Timers,
retries, and request timeouts all become deterministic. A 30-second
partition/heal convergence test runs in milliseconds and never
flakes.

This is not Antithesis — user goroutines outside handlers are not
controlled — but for protocols that follow the authoring contract
(all state inside handlers) the schedule is fully deterministic.

---

## Phase 5 — protocol library (v0.7.0)

Babel's pull was never just the runtime; it was the protocol
implementations around it. Ship batteries under `/protocols`,
dogfooding every phase above.

### 5.1 `protocols/membership` — the IPC contract

A tiny package of shared wire/IPC types, no implementation:

```go
type GetView struct{ ... }                 // Request
type View struct{ Active []transport.Host } // Reply
type NeighborUp struct{ Peer transport.Host }   // Notification
type NeighborDown struct{ Peer transport.Host } // Notification
```

This is the composition story made concrete: any broadcast protocol
written against these types works over any membership protocol that
publishes them. Interchangeability via typed IPC contracts instead
of Go interfaces — the thing actor frameworks structurally can't
express.

### 5.2 `protocols/hyparview`

Full HyParView: join protocol, active/passive views, periodic
shuffle, priority neighbor promotion on active-view failure.
Publishes the membership contract. Uses `OnSessionDisconnected` /
`OnSessionGivenUp` for failure detection (the session layer is the
monitor — no extra heartbeats).

### 5.3 `protocols/plumtree`

Plumtree over the membership contract: eager push on tree links,
lazy push (IHAVE) on the rest, graft/prune tree repair. Public
surface mirrors the gossip example's pattern: a `Broadcast` trigger
request plus a `Delivered` notification for the app layer.

Both protocols get Phase-4 sim tests as their primary suites:
50-node convergence, churn (nodes joining/leaving mid-broadcast),
partition/heal with delivery-invariant checks, all seeded. The
existing `cmd/gossip` example is rewritten to consume the membership
contract, and a new `cmd/broadcast` example stacks
plumtree/hyparview as the flagship demo.

Explicitly still out of scope: SWIM (memberlist owns that niche),
consensus (a Paxos/Raft over protorun is a v1.x showcase, not a
launch battery).

---

## Phase 6 — launch plumbing (v0.8.0 → v1.0.0)

- **`config` (nested module):** YAML runtime config with
  per-protocol sections. `config.Section[T](cfg, "hyparview")`
  decodes a typed struct; protocols receive config through their
  constructors (no framework magic). Replaces the TODO item.
- **`otel` (nested module):** `Metrics` → OpenTelemetry meter
  adapter, attrs mapped to otel attributes. slog already bridges to
  otel via existing community handlers; document it.
- **Benchmarks:** publish `docs/benchmarks.md` — methodology plus
  numbers for in-process IPC round-trip, mailbox throughput, and
  TCP-path end-to-end latency. Include Hollywood/Ergo in-process
  numbers with honest framing (different model, different unit of
  work), because readers will ask.
- **Diagnostics polish (folds in remaining TODO items):** rate-
  limited warn on unknown wireID, loud startup error path for
  missing network layer, and a prominent doc callout that `Send`
  errors are split: synchronous return covers local failures
  (`ErrNoCodec`), delivery failures surface as session events.
- **Docs set (Diátaxis):** tutorial ("your first protocol", built on
  prototest), how-tos (mTLS, custom codec, custom transport,
  supervision tuning), reference (wire format — already
  authoritative, plus WireCodec format), explanation (concurrency
  model, why protocol composition ≠ actors).
- **README repositioning:** lead with the niche ("protocol
  composition runtime — Babel for Go"), add the comparison table
  against actor frameworks, embed the seeded-simulation pitch, link
  the protocol library.
- **v1.0.0 gate:** wire format frozen (version byte already
  negotiated in Hello), API frozen, CHANGELOG discipline from v0.2.0
  onward.

---

## Sequencing summary

| Phase | Tag | Contents | Size |
|---|---|---|---|
| 0 | v0.2.0 | rename, unified mailbox, timers, Clock | L |
| 1 | v0.3.0 | supervision/restart, session replay | L |
| 2 | v0.4.0 | WireCodec, Handle, protobuf/JSON codecs | M |
| 3 | v0.5.0 | Address migration, TLS hooks, QUIC | M |
| 4 | v0.6.0 | mesh faults, virtual time, Sim harness | L |
| 5 | v0.7.0 | membership contract, HyParView, Plumtree | L |
| 6 | v0.8.0+ | config, otel, benchmarks, docs, README | M |

Dependencies that force this order: supervision needs the unified
mailbox (quarantine) and owner-indexed registries; the Sim harness
needs the Clock seam and ordered mailboxes; the protocol library
needs new timers, `Handle`, and the Sim harness for its test suites;
QUIC needs the Address migration.

## Open questions (decide when the phase starts)

- Phase 0: does `Unbounded` mailbox justify existing at launch, or
  is it an attractive nuisance? (Lean: ship it, strict-mode warn.)
- Phase 4: expose the Sim step hook as a full event-trace recorder
  (for visualization) or keep it minimal? (Lean: minimal at launch.)
- Phase 5: HyParView shuffle over TCP sessions opens short-lived
  connections to passive peers — allow transient sessions in the
  session layer, or route shuffles through active view only?
  (Needs a spike; Babel allows transient channels.)
