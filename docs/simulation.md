# Deterministic simulation

protorun can run a *full protocol stack* — real `Runtime`s, real
protocols, real IPC — under a seeded, virtual-time simulation. A
30-second partition/heal convergence test finishes in milliseconds of
real time and, for a given seed, produces the exact same schedule every
run. This is the framework's headline testing capability, and it is what
the Sessions seam (Phase 0/4) and the Clock seam (Phase 0.4) were built
for.

This document explains what the simulation gives you, how it stays
deterministic, and how to reproduce a failing run.

## The three levels of prototest

`prototest` offers three levels of testing, smallest first:

1. **Bare mesh** — `prototest.NewMesh(t)` gives an in-memory stand-in for
   the TCP + handshake stack at the Sessions seam. Drive nodes directly
   to exercise the session contract (Connect/Disconnect/Send/events).
2. **Runtimes over a mesh** — `prototest.NewRuntime(t, mesh, host,
   protocols)` stands up a full `Runtime` on a mesh node. Nodes on one
   mesh share a virtual clock by default. Good for two- or three-node
   protocol behavior without the scheduler.
3. **Simulation** — `prototest.NewSim(t)` runs the whole stack under the
   seeded scheduler with virtual time and injectable network faults.
   This is the level convergence, churn, and partition tests live at.

## A first simulation

```go
func TestConverges(t *testing.T) {
    sim := prototest.NewSim(t, prototest.WithSeed(42))

    a := sim.Node(hostA, newMyProto(hostA, hostB))
    b := sim.Node(hostB, newMyProto(hostB, hostA))
    _ = a
    _ = b

    // Let sessions establish, then run to a predicate or a horizon.
    sim.Run(1 * time.Second)
    ok := sim.RunUntil(func() bool {
        return converged(a) && converged(b)
    }, 30*time.Second)
    if !ok {
        t.Fatal("did not converge within 30 virtual seconds")
    }
}
```

`sim.Node(host, protocols...)` returns the real `*protorun.Runtime`, so
you can subscribe to notifications, read metrics, or register extra
probe protocols exactly as in production.

## The scheduler

The scheduler owns the mesh's shared virtual clock and a set of pending
inbound deliveries. Its loop is:

1. **Drain.** Deliver every event that is due at the current virtual time
   — application messages and session events alike — one at a time, in an
   order chosen by the seeded RNG. After each delivery, wait for
   **quiescence** (below). A handler's own `Send` lands back in the
   scheduler's pending set as a new (possibly same-instant) delivery, so
   cascades are captured, not lost.
2. **Advance.** With nothing more due now, step the clock to the next
   deadline — the earliest of a pending delivery time and a scheduled
   timer (`ctx.After` / `ctx.Every`, request timeouts, retry backoff).
   Firing a timer enqueues synchronously; wait for quiescence again.
3. Repeat until the virtual horizon (`Run(d)`), until a predicate holds
   (`RunUntil`), or — for `Step()` — after a single unit of progress.
   `StepUntil(pred, maxSteps)` is `Step` with eyes: each unit of
   progress is described by a `DeliveryInfo` (kind, from, to, and the
   message's wire id), so a test can freeze the schedule on a
   condition like "the second Accept has been delivered but no
   learner has heard yet" — the dangerous intermediate states that
   `Run` would settle straight through — instead of reverse-
   engineering delay schedules.

Because timers and network deliveries share one timeline, ordering across
them is exact: a message delayed to `t+5s` is delivered *after* a timer
armed for `t+2s`, every run.

## Quiescence: the crux

Between scheduling decisions the scheduler must know every runtime has
fully settled — no protocol mailbox holds an event and no handler is
mid-dispatch. It asks each runtime `Runtime.Quiescent()`.

`Quiescent` is backed by a per-protocol `inFlight` counter that a producer
increments **before** pushing an event onto the mailbox and the event loop
decrements **after** the handler returns. Because the increment precedes
the push, there is no instant where an event is live but uncounted;
because the decrement follows dispatch, the count stays positive until the
handler is truly done. `sync/atomic` operations are sequentially
consistent in Go, so a scheduler that reads zero has observed every
increment matched by a decrement — the runtime really is idle.

This is sound only because the simulation is the *sole* source of new
work while it waits:

- Inbound delivery is **synchronous**. Under a Sim the mesh implements
  `protorun.SyncDeliverer`; the runtime installs the mesh's sink and does
  **not** start its async message/event pump goroutines. The scheduler
  calls the sink directly, so by the time `deliver` returns the event is
  already counted.
- Timer fires are **synchronous**. `ctx.Every` is built on the same
  one-shot `AfterFunc` seam as `ctx.After` (no background ticker
  goroutine), so advancing the virtual clock fires and enqueues on the
  scheduler's own goroutine.
- Cross-node sends go to the **scheduler's pending set**, not straight
  into a peer's mailbox, so a handler on node A can never silently
  resurrect an already-settled node B.

The scheduler polls `Quiescent()` with `runtime.Gosched()` between reads —
a yield, not a sleep — to let the event-loop goroutines run.

## The determinism contract

The schedule is byte-for-byte reproducible for a given seed **provided the
protocols follow the authoring contract**:

- all state mutation and all sends happen inside handlers (message /
  timer / session / IPC), through the `ProtocolContext`;
- no goroutines of the protocol's own;
- no wall-clock reads — use `ctx.After` / `ctx.Every`, which run on the
  virtual clock, never `time.Now` / `time.Sleep`.

Under those rules the only concurrency the Sim does not control is absent,
so the seed fixes loss, jitter, and delivery interleaving.

One in-protocol gotcha worth calling out: **Go randomizes map iteration.**
If a handler forwards to peers by ranging a `map`, the send order — and
therefore which delivery the seeded RNG picks first — varies run to run.
Sort before iterating (the flood protocol in the prototest suite does).
This is a property of Go, not of the harness; the simulation is only as
deterministic as the code it runs.

Out-of-contract protocols (background goroutines, `time.Now`,
`time.Sleep`) still run, but get best-effort scheduling, not determinism.

## Network fault injection

Faults are set on the mesh (`sim.Mesh()`), consulted on every send:

```go
sim.Mesh().Cut(a, b)                 // drop the link both ways; tears down any live
                                     // session (SessionDisconnected both sides), loses
                                     // in-flight messages, fails Connect across it
sim.Mesh().Heal(a, b)                // link reachable again — NO auto-reconnect; the
                                     // protocol re-establishes via its own timers
sim.Mesh().Isolate(h)               // Cut(h, x) for every other node x
sim.Mesh().SetLoss(a, b, 0.1)       // seeded per-message drop, both directions
sim.Mesh().SetDelay(a, b, 5*time.Millisecond, jitter) // per-message delay d ± seeded jitter
```

`Heal` deliberately does not reconnect anything: a real partition heal
only makes the network reachable again; protocols must reconnect
themselves (typically from a reconnect timer or a session-event handler).
This mirrors production, where the framework never silently reopens a
session a fault tore down.

## Reproducing a failing run

Every mesh and Sim logs its seed at construction:

```
prototest: seed 6021397... — re-run with prototest.WithSeed(6021397...) to reproduce
```

A bare `NewSim(t)` derives its seed deterministically from the test name,
so a failure is already stable run-to-run. To pin an exact schedule (for
example, to bisect), copy the logged number into `prototest.WithSeed(n)`
and the run replays identically — same loss decisions, same jitter, same
interleaving.

## Timers in the simulation

`ctx.After` and `ctx.Every` fire on the shared virtual clock. A test that
would sleep for seconds of wall time instead advances virtual time in
`Run` / `Step`, so periodic shuffles, heartbeats, request timeouts, and
retry backoff are all exercised deterministically and instantly. See
`prototest.TestSim_TimerFiresInVirtualTime` for the smallest example.
