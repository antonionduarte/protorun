# protorun

**A protocol-composition runtime for Go — Babel for Go.** Distributed
protocols (membership, gossip, consensus, replication...) compose
naturally as layers: a gossip protocol asks a membership protocol "who
are my neighbors?" and broadcasts to the answer; a consensus protocol
asks "is this node still alive?" and routes around it if not. protorun
is the substrate for that composition, heavily inspired by
[Babel](https://github.com/pfouto/babel-core) (Java) — nothing else in
Go occupies this niche. Protocols are Go types implementing
`Start(ctx)` and `Init(ctx)`; the runtime handles per-protocol
event-loop concurrency, session establishment (TCP or QUIC), type-safe
message dispatch, retries, panic recovery/supervision, and
inter-protocol coordination via typed Request/Reply and Notifications.

> **Status:** pre-v1. The public API is settling but breaking changes are
> still on the table. See [`TODO.md`](TODO.md) for what's planned next.

## Quick start

```go
package main

import (
    "context"
    "log/slog"

    "github.com/antonionduarte/protorun/pkg/protorun"
    "github.com/antonionduarte/protorun/pkg/transport"
)

type PingMessage struct {
    protorun.BaseMessage
    Seq uint64
}

type Pinger struct {
    peer transport.Host
    ctx  protorun.ProtocolContext
}

func (p *Pinger) Start(ctx protorun.ProtocolContext) {
    p.ctx = ctx
    protorun.Handle(ctx, p.handle) // registers codec + handler in one call
}

func (p *Pinger) Init(ctx protorun.ProtocolContext) {
    _ = ctx.ConnectWithRetry(p.peer)
}

func (p *Pinger) OnSessionConnected(_ transport.Host) {
    _ = p.ctx.Send(&PingMessage{Seq: 1}, p.peer)
}

func (p *Pinger) handle(msg *PingMessage, from transport.Host) {
    p.ctx.Logger().Info("got ping", "from", from, "seq", msg.Seq)
    _ = p.ctx.Send(&PingMessage{Seq: msg.Seq + 1}, p.peer)
}

func main() {
    self := transport.NewHost(5001, "127.0.0.1")
    peer := transport.NewHost(5002, "127.0.0.1")

    rt := protorun.New(self,
        protorun.WithLogger(slog.Default()),
        protorun.WithTCPTransport(context.Background()),
    )
    rt.Register(&Pinger{peer: peer})
    _ = rt.Run()
}
```

Run two instances with their `-self-port` and `-peer-port` flipped and they
start exchanging messages.

A complete two-binary version of this example lives at
[`cmd/pingpong/`](cmd/pingpong/). For a multi-layer example exercising IPC,
session events, and a 10-node integration test, see
[`cmd/gossip/`](cmd/gossip/): a membership protocol stacked under an
eager-push gossip protocol.

## How protorun compares

protorun is not an actor framework, and doesn't compete with one on
actors, clustering, or supervision trees — see
[`docs/concurrency-model.md`](docs/concurrency-model.md) for the full
argument. The table below is factual, not a leaderboard (per each
project's own public docs at the time of writing — check upstream for
the current state):

| | **protorun** | Proto.Actor (Go) | Ergo | GoAkt | Hollywood |
|---|---|---|---|---|---|
| Unit of composition | a protocol layer on a node — a small fixed set wired at startup | an actor, dynamically spawned, PID-addressed | an actor/process, PID- or name-addressed | an actor or "grain", location-transparent | an actor, PID-addressed |
| Coordination model | typed IPC contracts (Request/Reply, Notifications) between layers | direct PID references | direct PID/registered-name references | direct PID/name references | direct PID references |
| Membership/broadcast batteries | HyParView + Plumtree shipped in-tree (`pkg/protocols/`) | none shipped; clusters via Consul.IO | pub/sub + service discovery via external registrars (etcd, Saturn) | pluggable discovery (Consul/etcd/Kubernetes/NATS/mDNS); no shipped gossip algorithm | none shipped |
| Deterministic full-stack simulation | `prototest.Sim`: seeded scheduler, virtual clock, real runtimes | not documented | not documented | not documented | not documented |
| Zero-dependency core | yes — the library module is stdlib-only (one test-only dep, goleak); examples and interop (protobuf, QUIC, OTel, YAML) live in nested modules | no — protobuf + gRPC | yes, advertised for the core; external registrars are opt-in | no — protobuf/CBOR + Consul/etcd/NATS clients | not documented; protobuf + dRPC transport suggests dependencies |

Where they win decisively: actor population scale (thousands of
short-lived, individually addressable actors), remote actor
references, clustering providers, and (Ergo, GoAkt) OTP-style
supervision trees. protorun optimizes for a different job: a handful
of protocol layers that need typed contracts, session-lifecycle
awareness, and a way to prove the whole stack converges before it ever
touches a socket.

## Test a whole protocol stack before you open a socket

The headline capability: `prototest.Sim` runs **real `Runtime`s, real
protocols, real IPC** — not a model of them — under a seeded scheduler
and a virtual clock. A 30-second partition/heal convergence test
finishes in milliseconds and reproduces byte-for-byte given the same
seed:

```go
sim := prototest.NewSim(t, prototest.WithSeed(42))
a := sim.Node(hostA, newMyProto(hostA, hostB))
b := sim.Node(hostB, newMyProto(hostB, hostA))

sim.Run(1 * time.Second)             // let sessions establish
sim.Mesh().Cut(hostA, hostB)         // partition mid-run
sim.Run(5 * time.Second)             // ... assert divergence ...
sim.Mesh().Heal(hostA, hostB)        // reachable again (no auto-reconnect)

ok := sim.RunUntil(func() bool { return converged(a) && converged(b) },
    30*time.Second)
```

See [Testing](#testing) below and [`docs/simulation.md`](docs/simulation.md)
for the mechanism, the determinism contract, and fault injection.

## Architecture

```
+------------------------------------------------------+
|             Your protocols (Protocol)                |
|  - Start(ctx) registers handlers                     |
|  - Init(ctx) bootstraps connections, timers          |
|  - Each gets its own goroutine event loop            |
+-----------------+-------------+----------------------+
                  |             |
                  | messages    | IPC
                  |             |
+-----------------v-------------v----------------------+
| Runtime                                              |
|  - Codec registry (wireID -> owning protocol)        |
|  - IPC router (request handlers, notif fanout)       |
|  - Timer table, retry table                          |
|  - Per-component slog logger                         |
+-----------------+------------------------------------+
                  |
+-----------------v------------------------------------+
| SessionLayer                                         |
|  - Hello/Ack handshake binds connections to Hosts    |
|  - Emits SessionConnected / Disconnected / Failed    |
+-----------------+------------------------------------+
                  |
+-----------------v------------------------------------+
| TransportLayer (transport.Layer, addressed by        |
| transport.Address)                                   |
|  - length-prefixed framing                           |
|  - TCP (reference) with optional TLS/mTLS, or QUIC    |
|    (pkg/transport/quic module) — SessionLayer runs    |
|    unchanged over either                             |
+------------------------------------------------------+
```

`transport.Layer` addresses peers by the abstract `transport.Address`;
`Host` (ip:port) is the endpoint type both TCP and QUIC use. The
SessionLayer is the single translation point between transport
`Address`es and the stable logical `Host`s protocols see.

TLS on the TCP layer is a one-liner — no fork:

```go
rt := protorun.New(self,
    protorun.WithTCPTransport(ctx, transport.WithTLS(cfg)))
```

`transport.WithDialFunc` / `transport.WithListenFunc` expose the raw
dial/listen seams for anything TLS sugar doesn't cover. The QUIC backend
lives in the nested [`pkg/transport/quic`](pkg/transport/quic/) module (it pulls
in `quic-go`; the core module stays zero-dependency) and is wired via
`protorun.WithTransport(quicLayer, sessionLayer)`.

## Concepts

### Protocols

A `Protocol` is any Go type with `Start(ProtocolContext)` and
`Init(ProtocolContext)`. The runtime calls every protocol's `Start` first
(registration phase), then every protocol's `Init` (activation phase). When
one protocol's `Init` fires off a request, the target's handler is already
registered.

Optional interfaces a protocol can also implement:

- `SessionConnectedHandler` / `SessionDisconnectedHandler` /
  `SessionFailedHandler` / `SessionGivenUpHandler`
  to react to peer lifecycle events.
- `PanicHandler` to observe when one of your handlers panicked.

### Messages

Embed `protorun.BaseMessage` and you have a wire-ready type. The wire ID is
derived from the Go type name (FNV-1a hash). For long-lived deployments that
might rename types, implement `WireName() string` on the type to freeze the
ID (strict mode warns once per type when you don't).

`protorun.Handle(ctx, fn)` is the default registration path: it infers the
message type from your `func(*M, transport.Host)` handler, picks a codec, and
registers both the codec and the handler in one call.

```go
protorun.Handle(ctx, p.onPing) // func(*Ping, transport.Host)
```

Which codec `Handle` picks:

- **`WireCodec[*M]`** — the reflective default. Handles strings, `[]byte`,
  slices, maps (deterministic sorted-key encoding), arrays, nested structs,
  and pointers to structs, on top of every fixed-size type. Per-type
  encode/decode plans are compiled once and cached. Its byte layout is
  normative in [`docs/wire-format.md`](docs/wire-format.md). Used for any type
  that doesn't implement `SelfMarshaler`.
- **`SelfCodec[*M]`** — used automatically when `*M` implements
  `SelfMarshaler` (`MarshalWire() ([]byte, error)` / `UnmarshalWire([]byte)
  error`), letting a message own its encoding while still registering via
  `Handle`.

For a custom codec, keep the explicit two-call form —
`protorun.RegisterCodec(ctx, myCodec)` then
`protorun.RegisterHandler(ctx, fn)`. Other codecs the framework ships:

- **`BinaryCodec[*M]`** — `encoding/binary` for fixed-size structs; the
  lowest-overhead option when your message has no variable-length fields.
- **`JSONCodec[*M]`** — `encoding/json`, for development and wire
  inspection. Not a stable wire format.
- **`pkg/codec/protobuf` (nested module)** — `ProtoCodec[M proto.Message]` for
  shops with existing `.proto` definitions. Lives in its own module so the
  core stays zero-dependency.

### Send's error is local-only — read this before you check it

> **`ctx.Send` returning `nil` does not mean the peer received anything.**
> The error return and actual delivery are two different things:
>
> - The return value is **synchronous and local**: it reports failures that
>   never left this process — `ErrNoCodec` (no codec registered for the
>   message type), or no session layer configured. Nothing about the
>   network or the peer is checked before `Send` returns.
> - Whether the message reached the peer is **asynchronous and reported
>   through session events**, not through this call. A dead connection, a
>   mid-flight drop, or a peer that never completed its handshake all
>   still return `nil` from `Send` — the failure shows up later as
>   `SessionDisconnected` / `SessionFailed` / `SessionGivenUp` on whoever
>   implements `SessionDisconnectedHandler` / `SessionGivenUpHandler` for
>   that peer.
>
> If a protocol needs proof of delivery, build it at the application
> layer (an ack message, or `SendRequest`/`Responder` for a real
> round-trip) — `Send` only ever promises the local half.

One special case: `Send` to your **own** Host loops back through the
normal inbound path — the handler receives a freshly decoded instance
(never an alias of the sent value), queued FIFO behind the mailbox.
Quorum protocols can broadcast to "all members including self" without
special-casing the local vote.

### Inter-protocol coordination (IPC)

Two patterns, both same-runtime only (cross-node still goes through the
peer-message path):

```go
// Request/Reply: one handler per type, runtime-wide
protorun.RegisterRequestHandler(ctx, func(req *GetView, r protorun.Responder[*View]) {
    r.Reply(&View{Peers: snapshotOfPeers()})
})

protorun.SendRequest(ctx, &GetView{}, func(rep *View, err error) {
    // runs on the requester's event loop
})
```

```go
// Notifications: pub/sub fanout, many subscribers per type
protorun.SubscribeNotification(ctx, func(ev ViewChanged) { ... })
protorun.PublishNotification(ctx, ViewChanged{Added: peer})
```

### Timers

Schedule work on the protocol's own event loop with `After` (one-shot)
and `Every` (periodic). Both return a `TimerHandle`; `Cancel` is
idempotent, safe after the timer fired, and safe to call from inside a
handler.

```go
h := ctx.After(500*time.Millisecond, func() { /* runs on the loop */ })
ctx.Every(time.Second, p.tick)
h.Cancel()
```

The payload rides along by closure capture — no timer struct, no
user-managed IDs. All of a protocol's timers are cancelled
automatically on shutdown.

### Concurrency model

Each protocol gets one goroutine that pulls events off its single
ordered mailbox and dispatches them sequentially. Messages, timers,
session events, and IPC all share that one queue, so arrival order is
delivery order across kinds. Handlers can mutate protocol state without
locking, as long as access stays inside the handlers.

The mailbox capacity and overflow behaviour are set per protocol at
registration:

```go
rt.Register(p, protorun.WithMailbox(protorun.Mailbox{
    Capacity: 1024,                   // default
    Overflow: protorun.OverflowBlock, // Block | DropOldest | DropNewest | Unbounded
}))
```

`OverflowBlock` (default) backpressures the producer; the drop policies
route evicted events to a `WithDeadLetter` hook; `OverflowUnbounded`
never blocks but can grow without limit.

Public methods you expose on your protocol (for example an `enqueue(...)`
method called from another protocol's goroutine or the application's main
loop) are *not* on the event loop. For those, route work back onto the
event loop via IPC; a self-targeted `SendRequest` is the idiomatic pattern.
The gossip example does this: `gossip.TriggerBroadcast` is the public way
to ask the gossip protocol to broadcast.

### Supervision

By default a panicking handler is recovered, logged, and the protocol
keeps running (`Resume`). For state that can't survive a half-mutation,
register a factory and a supervision policy so the runtime rebuilds the
protocol from scratch instead:

```go
rt.RegisterFactory(newGossip, protorun.WithSupervision(protorun.Supervision{
    OnPanic:  protorun.Restart, // Resume | Restart | Stop | Escalate
    Backoff:  protorun.ExpBackoff(100*time.Millisecond, 5*time.Second),
    OnGiveUp: protorun.Escalate, // when MaxRestarts within Window is exceeded
}))
```

On `Restart` the supervisor quarantines the mailbox (further events
dead-letter, producers never block), cancels the protocol's timers,
fails its pending `SendRequest`s with `ErrProtocolRestarting`,
deregisters its codecs and IPC routes, waits out the backoff, then
builds a fresh instance (`Start` → `Init`) and replays a synthetic
`SessionConnected` for every established peer so it rebuilds peer state
the way it did at boot. Sessions stay up across the restart. Implement
`RestartHandler.OnRestart(attempt)` to observe it. `Stop` removes the
protocol; `Escalate` cancels the runtime and `Run` returns an
`ErrProtocolFailed`-wrapped error. Every outcome publishes a
`ProtocolFailed` notification siblings can subscribe to.

### Configuration

The core module reads no config files — `protorun.New` takes plain Go
values and `Option`s. For YAML-driven deployments, the nested
[`pkg/config`](pkg/config/) module loads a document with a reserved `runtime:`
block (logging level/components/format) plus arbitrary named sections
your own code decodes with `config.Section[T]`. Protocols receive their
config the same way they receive everything else: as a constructor
argument, no framework magic.

```go
cfg, _ := config.Load("node.yaml")
hv, _ := config.Section[hyparview.Config](cfg, "hyparview")
rt := protorun.New(self, cfg.Runtime().Options()...)
rt.Register(hyparview.New(self, hv))
```

See [`cmd/pingpong`](cmd/pingpong/) for a complete example.

### Metrics

`protorun.WithMetrics(m)` plugs a `Metrics` implementation (Counter +
Histogram, structured `Attr`s) into the runtime's instrumented paths
(dispatch, IPC, mailbox depth/drops, sessions, panics, restarts — see
`metrics.go`). The default is a no-op. For OpenTelemetry, the nested
[`pkg/otel`](pkg/otel/) module adapts a `metric.Meter`:

```go
rt := protorun.New(self, protorun.WithMetrics(otelmetrics.Metrics(meter)))
```

Instruments are created once per metric name and cached; a failed
instrument creation is reported once via OTel's own error handler and
that name becomes a permanent no-op — metrics never panic the hot path.

## Testing

Test protocols against `prototest`, not TCP. An in-memory mesh stands in
for the whole transport + handshake stack at the Sessions seam, so full
runtimes talk in-process with no wire, no ports, and no flakiness.

The headline is **deterministic simulation**: a whole protocol stack runs
under a seeded scheduler on a virtual clock. A 30-second partition/heal
convergence test finishes in milliseconds of real time and, for a given
seed, produces the exact same schedule every run.

```go
func TestConverges(t *testing.T) {
    sim := prototest.NewSim(t, prototest.WithSeed(42))
    a := sim.Node(hostA, newMyProto(hostA, hostB))
    b := sim.Node(hostB, newMyProto(hostB, hostA))

    sim.Run(1 * time.Second)              // let sessions establish
    sim.Mesh().Cut(hostA, hostB)          // partition mid-run
    sim.Run(5 * time.Second)              // ... assert divergence ...
    sim.Mesh().Heal(hostA, hostB)         // reachable again (no auto-reconnect)

    ok := sim.RunUntil(func() bool { return converged(a) && converged(b) },
        30*time.Second)
    if !ok { t.Fatal("did not converge") }
}
```

The scheduler delivers network events in seeded order, settles every
runtime to quiescence, then advances the shared clock to the next
timer/delivery deadline — so timers, request timeouts, and retry backoff
are all deterministic. Inject faults with `Cut` / `Heal` / `Isolate` /
`SetLoss` / `SetDelay`. Every run logs its seed; drop it into
`prototest.WithSeed(n)` to replay a failure exactly.

Determinism holds for protocols that follow the authoring contract (all
state and sends inside handlers, no goroutines of their own, no wall-clock
reads). See [`docs/simulation.md`](docs/simulation.md) for how it works
and the full contract.

## Protocol library

protorun ships batteries — real, paper-faithful distributed protocols you
can stack and swap, all under [`pkg/protocols/`](pkg/protocols/) and all in the
core module (no third-party dependencies):

- **`pkg/protocols/membership`** — the interchangeability seam. A types-only
  IPC contract: a membership protocol answers `GetView` (returning its
  active view) and publishes `NeighborUp` / `NeighborDown`; a
  dissemination protocol consumes exactly those. Interchangeability comes
  from typed IPC contracts, not Go interfaces — the thing actor
  frameworks structurally can't express. Being local IPC, the contract
  carries no codecs and no `WireName`.
- **`pkg/protocols/hyparview`** — a faithful HyParView (Leitão et al., 2007):
  a small symmetric session-backed active view plus a larger passive
  view, JOIN + ForwardJoin random walks, periodic shuffle, and
  priority-based promotion on failure. Failure detection rides the
  session layer — no extra heartbeats. Publishes the membership contract.
- **`pkg/protocols/plumtree`** — a faithful Plumtree ("Epidemic Broadcast
  Trees", 2007) over the contract: an eager-push spanning tree with
  lazy-push `IHAVE` announcements, self-optimising via GRAFT/PRUNE.
  Originate with a `Broadcast` request; receive a `Delivered` notification
  per unique message.
- **`pkg/protocols/raft`** — a faithful Raft (Ongaro & Ousterhout, 2014):
  randomized-timeout leader election, log replication with `nextIndex`
  repair, current-term-only commitment (§5.4.2), and the up-to-date vote
  restriction (§5.4.1), over a **static** peer set with a `Storage`
  persistence seam. `Propose` a command (get `{Index, Term}` or a
  `NotLeaderError`), receive `Applied` notifications in log order. Static
  membership is deliberate — consensus needs total, stable membership,
  which is the opposite of what a partial-view gossip protocol provides.
  Membership change (§6) and snapshots (§7) are out of scope.
- **`pkg/protocols/paxos`** — a faithful single-decree Paxos (Lamport,
  "Paxos Made Simple", 2001): the synod protocol with proposer, acceptor,
  and learner in one type, per-node **disjoint** ballots, the Phase-2a
  **value-adoption** rule that makes it safe, and randomized-backoff
  liveness against dueling proposers, over the same **static** peer set and
  `Storage` seam as Raft. `Propose` a value (get an ack or an
  `AlreadyDecidedError`), receive a `Decided` notification exactly once.
  Single-decree only — log replication is Raft's job; Paxos is the second,
  independent consensus data point.

The point is composition: **Plumtree runs over HyParView without either
knowing about the other** — they meet only at the contract. Swap HyParView
for the gossip example's static membership (or a future SWIM) without
touching the layer above:

```go
rt.Register(hyparview.New(self, hyparview.Config{Contacts: contacts}))
rt.Register(plumtree.New(self, plumtree.Config{}))
// the app originates a broadcast and hears deliveries, both over IPC:
protorun.SendRequest(ctx, &plumtree.Broadcast{Payload: line}, func(*plumtree.BroadcastAck, error) {})
protorun.SubscribeNotification(ctx, func(ev plumtree.Delivered) { /* ... */ })
```

Every protocol's primary test suite runs on the seeded simulation
(`prototest.Sim`): 20-node convergence, churn, shuffle rotation,
exactly-once broadcast, spanning-tree duplicate bounds, partition/heal,
for Raft Election Safety / State Machine Safety / Leader Completeness /
minority-partition safety, and for Paxos Agreement/Integrity, the
multi-seed dueling gauntlet, value adoption, and chosen-is-forever — all in
milliseconds of real time. See
[`docs/protocols.md`](docs/protocols.md) for the full story, and
[`cmd/broadcast/`](cmd/broadcast/) for the flagship Plumtree-over-
HyParView-over-TCP demo.

## Documentation

The full docs set lives in [`docs/README.md`](docs/README.md), organized
as a tutorial, how-to guides, reference, and explanation
([Diátaxis](https://diataxis.fr)):

- **Tutorial:** [your first protocol](docs/tutorial.md), zero to a
  passing deterministic test.
- **How-tos:** [TLS/mTLS](docs/how-to-tls.md),
  [a custom codec](docs/how-to-custom-codec.md),
  [a custom transport backend](docs/how-to-custom-transport.md).
- **Reference:** [wire format](docs/wire-format.md),
  [benchmarks](docs/benchmarks.md).
- **Explanation:** [concurrency model](docs/concurrency-model.md) (and
  why protocol composition isn't actors),
  [deterministic simulation](docs/simulation.md),
  [the protocol library](docs/protocols.md),
  [the protoviz visual debugger](docs/visualizer-design.md).

### Visual debugger (protoviz)

`prototest`'s `WithTrace` recorder writes a `protoviz/1` JSONL trace of a
simulated run — every delivery, drop, session event, fault, and periodic
protocol-state snapshot. [`viz/`](viz/) is a static web viewer for those
traces: drag a `.jsonl` in (or pick one of the four bundled samples) and
scrub the run step by step — an overlay graph, a Lamport sequence diagram,
protocol-specific lenses (membership, broadcast tree, raft/paxos
consensus), and a state Inspector with per-key diffing. Backward scrubbing
is free thanks to the simulator's determinism.

```bash
cd viz && npm install && npm run dev   # then open a sample trace
```

See [`viz/README.md`](viz/README.md) for the lens catalogue and dev notes,
and [`docs/visualizer-design.md`](docs/visualizer-design.md) for the design.

Plus:

- Full API reference: `go doc github.com/antonionduarte/protorun/pkg/protorun`
- Pingpong example: [`cmd/pingpong/`](cmd/pingpong/)
- Gossip example (membership + eager-push gossip + 10-node integration
  test): [`cmd/gossip/`](cmd/gossip/)
- Broadcast example (Plumtree over HyParView over TCP):
  [`cmd/broadcast/`](cmd/broadcast/)
- QUIC transport backend: [`pkg/transport/quic/`](pkg/transport/quic/)
- YAML config module: [`pkg/config/`](pkg/config/)
- OpenTelemetry metrics adapter: [`pkg/otel/`](pkg/otel/)

## Build, test, lint

```bash
make build          # go build ./...
make test           # go test ./...
make test-race      # go test -race ./...
make lint           # golangci-lint run ./...
make coverage       # go test ... -coverprofile + summary
```

Pre-commit hooks (run lint + tests on staged Go files):

```bash
make hooks-install
```

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md). The short version: protocols
only interact with the runtime via `ProtocolContext`; cross-protocol
coordination is IPC, never direct method calls; new tests use goleak +
`-race`; lint must pass with zero issues.

## License

MIT. See [`LICENSE`](LICENSE).
