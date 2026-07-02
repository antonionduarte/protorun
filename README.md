# protorun

A small framework for building distributed protocols in Go, inspired by
[Babel](https://github.com/pfouto/babel-core), the Java protocol-composition
framework. Protocols are written as Go types implementing `Start(ctx)` and
`Init(ctx)`. The runtime handles event-loop concurrency, session establishment
over TCP, type-safe message dispatch, retries, panic recovery, and
inter-protocol coordination via Request/Reply and Notifications.

> **Status:** pre-v1. The public API is settling but breaking changes are
> still on the table. See [`TODO.md`](TODO.md) for what's planned next.

## Why

Distributed protocols (membership, gossip, consensus, replication...) compose
naturally as layers. A gossip protocol asks a membership protocol "who are
my neighbors?" and uses the answer to broadcast. A consensus protocol asks
a membership protocol "is this node still alive?" and routes around if not.

protorun gives you the substrate for that composition without the
boilerplate: message routing by Go-type wire identifier, per-protocol event
loops that serialize handler execution (so you can mutate state without
locking), and IPC primitives (Request/Reply, fan-out Notifications) so
protocols on the same runtime can coordinate without going through the
network.

## Quick start

```go
package main

import (
    "context"
    "log/slog"

    "github.com/antonionduarte/protorun"
    "github.com/antonionduarte/protorun/transport"
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
| TransportLayer (TCP today; pluggable interface)      |
|  - length-prefixed framing                           |
+------------------------------------------------------+
```

## Concepts

### Protocols

A `Protocol` is any Go type with `Start(ProtocolContext)` and
`Init(ProtocolContext)`. The runtime calls every protocol's `Start` first
(registration phase), then every protocol's `Init` (activation phase). When
one protocol's `Init` fires off a request, the target's handler is already
registered.

Optional interfaces a protocol can also implement:

- `SessionConnectedHandler` / `SessionDisconnectedHandler` / `SessionGivenUpHandler`
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
- **`codec/protobuf` (nested module)** — `ProtoCodec[M proto.Message]` for
  shops with existing `.proto` definitions. Lives in its own module so the
  core stays zero-dependency.

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

## Documentation

- Full API reference: `go doc github.com/antonionduarte/protorun`
- Pingpong example: [`cmd/pingpong/`](cmd/pingpong/)
- Gossip example (membership + eager-push gossip + 10-node integration
  test): [`cmd/gossip/`](cmd/gossip/)
- Wire format details: see the package doc on `transport` and
  `wire`.

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
