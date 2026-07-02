# Tutorial: your first protocol

This walks through writing a protocol from nothing to a passing,
deterministic test — no TCP, no ports, no `main` function. Every
snippet below is real, compiled code (verified against this version of
the module); copy it into a scratch module and it runs as shown.

By the end you will have a two-message protocol, `counter`, and a test
that proves two instances of it converge, using
[`prototest.Sim`](simulation.md) — the same harness the framework's own
protocol library ([`docs/protocols.md`](protocols.md)) is tested with.

## What we're building

`counter` connects to one peer. The moment the session comes up, it
sends the peer an `Increment`. Whoever receives an `Increment` adds it
to a running total and echoes a `Count` back with the new value. Two
nodes pointed at each other will each end up having incremented once
and having been told about it — small enough to read in one sitting,
big enough to exercise messages, session events, and `Send` all at
once.

## Step 1: the wire messages

Every message type embeds `protorun.BaseMessage` and should implement
`WireName()` so its wire identifier survives a rename (see
[Messages](../README.md#messages) in the README, and
[`docs/wire-format.md`](wire-format.md) for the byte-level detail):

```go
package counter

import (
	"github.com/antonionduarte/protorun/pkg/protorun"
	"github.com/antonionduarte/protorun/pkg/transport"
)

// Increment asks the receiver to add By to its running total.
type Increment struct {
	protorun.BaseMessage
	By uint64
}

func (Increment) WireName() string { return "tutorial.increment" }

// Count reports the sender's total after applying an Increment.
type Count struct {
	protorun.BaseMessage
	Value uint64
}

func (Count) WireName() string { return "tutorial.count" }
```

Both fields are fixed-size (`uint64`), so `protorun.Handle` will pick
the reflective `WireCodec` automatically — nothing else to configure
here.

## Step 2: the protocol type

A `Protocol` is any type with `Start(ProtocolContext)` and
`Init(ProtocolContext)`. `Start` is where you register message
handlers; `Init` is where you open connections. State lives as plain
struct fields — no locking needed, because every handler on a given
protocol instance runs on that protocol's own single event-loop
goroutine (see [Concurrency model](concurrency-model.md)):

```go
// Counter is the whole protocol: it connects to one peer, sends an
// Increment once the session is up, and echoes every Increment it
// receives back as a Count carrying its running total.
type Counter struct {
	ctx  protorun.ProtocolContext
	peer transport.Host

	total    uint64
	lastEcho uint64
}

// New builds a Counter that will connect to peer on Init.
func New(peer transport.Host) *Counter {
	return &Counter{peer: peer}
}

func (c *Counter) Start(ctx protorun.ProtocolContext) {
	c.ctx = ctx
	protorun.Handle(ctx, c.onIncrement)
	protorun.Handle(ctx, c.onCount)
}

func (c *Counter) Init(ctx protorun.ProtocolContext) {
	if err := ctx.Connect(c.peer); err != nil {
		ctx.Logger().Error("connect failed", "peer", c.peer, "err", err)
	}
}

// OnSessionConnected is an optional handler (SessionConnectedHandler):
// the runtime calls it whenever a session reaches this peer, including
// the one we just asked for in Init.
func (c *Counter) OnSessionConnected(h transport.Host) {
	if h == c.peer {
		_ = c.ctx.Send(&Increment{By: 1}, c.peer)
	}
}

// Total is this node's running total, mutated only inside onIncrement.
func (c *Counter) Total() uint64 { return c.total }

// LastEcho is the last Count value this node received back.
func (c *Counter) LastEcho() uint64 { return c.lastEcho }

func (c *Counter) onIncrement(msg *Increment, from transport.Host) {
	c.total += msg.By
	_ = c.ctx.Send(&Count{Value: c.total}, from)
}

func (c *Counter) onCount(msg *Count, _ transport.Host) {
	c.lastEcho = msg.Value
}
```

Notice what's absent: no mutex, no channel plumbing, no explicit event
loop. `ctx.Connect`, `protorun.Handle`, and `ctx.Send` are the entire
surface a protocol needs; the runtime owns everything else (see
[Wiring order](../README.md#quick-start) for how this plugs into a real
`main`).

`ctx.Send`'s returned error is worth internalizing early: it is
**local-only** (`ErrNoCodec`, no session layer, ...). A `nil` return
does not mean the peer received the `Increment` — delivery failure
surfaces later as a session event, not as this call's return value.
See the callout in the README's [Concepts](../README.md#concepts)
section.

## Step 3: test it with `prototest.Sim`

Real TCP sockets, ports, and `time.Sleep`-based polling would all work
here, but they're unnecessary and slow. `prototest.Sim` runs real
`Runtime`s — same `Counter`, same handlers — over an in-memory mesh on
a virtual clock, so the whole test finishes in milliseconds:

```go
package counter_test

import (
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/prototest"
	"github.com/antonionduarte/protorun/pkg/transport"

	"yourmodule/counter"
)

func TestCounterConverges(t *testing.T) {
	sim := prototest.NewSim(t, prototest.WithSeed(1))

	hostA := transport.NewHost(7001, "127.0.0.1")
	hostB := transport.NewHost(7002, "127.0.0.1")

	protoA := counter.New(hostB)
	protoB := counter.New(hostA)

	sim.Node(hostA, protoA)
	sim.Node(hostB, protoB)

	ok := sim.RunUntil(func() bool {
		return protoA.Total() == 1 && protoA.LastEcho() == 1 &&
			protoB.Total() == 1 && protoB.LastEcho() == 1
	}, 5*time.Second)
	if !ok {
		t.Fatal("counters did not converge")
	}
}
```

Run it:

```
$ go test ./counter/... -v
=== RUN   TestCounterConverges
    mesh.go:144: prototest: seed 1 — re-run with prototest.WithSeed(1) to reproduce
--- PASS: TestCounterConverges (0.00s)
PASS
```

Real time: effectively zero, because nothing here waits on a wall
clock or a socket. `sim.Node` builds a full `Runtime` on the mesh (the
`ports` above are just `Host` identities — the mesh never binds a real
socket); `RunUntil` drains every deliverable event in seeded order,
settles both runtimes to quiescence, and repeats, up to a 5-second
*virtual* horizon. The predicate closure reads `protoA`/`protoB`
fields directly — safe here because `RunUntil` only evaluates it at a
quiescent point, when no handler is running (see
[`Sim.RunUntil`](../pkg/prototest/sim.go)). A protocol with richer internal
state usually exposes it through IPC instead (`GetView`-style
request/reply, as `pkg/protocols/hyparview` and `pkg/protocols/plumtree` do —
see [`docs/protocols.md`](protocols.md)); for a two-field tutorial
protocol, direct reads keep the example honest about what's actually
happening.

## What's next

- Swap the hand-rolled fields for two peers behaving asymmetrically,
  or add a third node, to see how `Sim` scales.
- Read [`docs/simulation.md`](simulation.md) for the full determinism
  contract (what you can and can't rely on) and fault injection
  (`Cut`/`Heal`/`SetLoss`/`SetDelay`).
- Read [`docs/concurrency-model.md`](concurrency-model.md) for why
  protocols compose this way instead of as actors.
- When `Counter` needs a peer to answer a question (not just react to
  a message), reach for `RegisterRequestHandler` + `SendRequest`
  instead of another message pair — see Inter-protocol coordination in
  the [README](../README.md#inter-protocol-coordination-ipc).
- To run this over real TCP instead of the Sim, wire it exactly like
  [`cmd/pingpong`](../cmd/pingpong/): `protorun.New(self,
  protorun.WithTCPTransport(ctx))`, then `rt.Register(counter.New(peer))`,
  then `rt.Run()`.
