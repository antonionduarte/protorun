# Concurrency model, and why protocol composition is not actors

This explains how a running protorun node actually executes — one
event loop per protocol, one ordered mailbox, IPC that never leaves
the process — and why that model is described as **protocol
composition**, deliberately not as an actor system, even though the
two look superficially similar (isolated units of state, message-
passing, no shared-memory locking).

## One protocol, one goroutine, one mailbox

Every registered protocol gets exactly one goroutine, running an event
loop that pulls events off exactly one ordered mailbox
(`Mailbox`/`WithMailbox`, `mailbox.go`) and dispatches them one at a
time. The mailbox carries a single tagged union — `protoEvent` — with
six kinds:

- **message** — an inbound peer message, decoded and routed by `WireID`
- **timer** — an `After`/`Every` fire
- **session** — `SessionConnected` / `SessionDisconnected` / `SessionGivenUp`
- **request** — an inbound `SendRequest` this protocol handles
- **reply** — the answer to a request *this* protocol sent
- **notification** — a published event this protocol subscribed to

Because all six kinds share one queue, **arrival order is dispatch
order across kinds, not just within one kind**. A message from peer P
is never handled before the `SessionDisconnected` for P that was
enqueued before it, even though they're structurally different event
types. Earlier versions of the runtime used six separate channels
drained by one `select`, and Go's `select` picks among ready cases
pseudo-randomly — so a message and the disconnect that preceded it
could reorder. The unified mailbox exists specifically to remove that
class of bug, and as a side effect it is what makes a full protocol
stack replayable under `prototest.Sim` (see
[`docs/simulation.md`](simulation.md)): a scheduler that controls
delivery order into one queue per protocol has actually fixed the
schedule; a scheduler racing six queues per protocol has not.

Because dispatch is strictly serial per protocol, **handlers can
mutate protocol state with no locking**, as long as every read and
write happens inside a handler (message handler, timer callback,
session-event handler, IPC handler). This is the same deal actors make
— but where the guarantee comes from, and what it's used for, differs;
see below.

## IPC is same-process, and it still goes through the mailbox

`RegisterRequestHandler`/`SendRequest` (ask one handler, get one
reply) and `SubscribeNotification`/`PublishNotification` (fan out to
every subscriber) are protorun's only cross-protocol coordination
primitives, and both are **strictly local** — same runtime, same
process. A request, a reply, and a published notification are each
just another `protoEvent` kind, enqueued onto the target protocol's
mailbox exactly like an inbound message would be. That means a
protocol's request handler runs on *its own* event loop, serialized
with everything else that protocol does — never on the caller's
goroutine, never concurrently with the protocol's other handlers.

Crossing a node boundary is a completely different, and more
restrictive, path: only `Send` (a peer message with a registered
codec) puts bytes on a wire. There is no "remote IPC" — a request or
notification never implicitly serializes itself and hops to another
node. If two protocols need to coordinate across nodes, that
coordination is built out of ordinary peer messages, explicitly, the
same way any protocol talks to its peers.

## Why this isn't actors

Actor frameworks (Proto.Actor, Ergo, GoAkt, Hollywood — the Go/BEAM
peers closest to this space) and protorun share surface-level DNA:
isolated state, message-passing instead of locks, one mailbox per
unit of execution. The differences are what each model optimizes for:

- **Unit of composition.** An actor system's addressable unit is the
  actor: potentially thousands of them, spawned and torn down
  dynamically, addressed by a reference (PID/PID-like) that can be
  handed to anyone who needs to talk to it. protorun's addressable
  unit is a **protocol layer on a node** — a small, fixed set (typically
  single digits) wired once at startup (`rt.Register(...)`), each one a
  layer in a stack (a membership protocol under a broadcast protocol
  under an application), not a population of short-lived workers.
- **Coordination model.** An actor holds another actor's address and
  sends it messages directly; swapping the receiving actor for a
  different implementation means changing who holds that address.
  protorun's layers coordinate through **typed IPC contracts** instead:
  `pkg/protocols/membership` defines `GetView`/`NeighborUp`/`NeighborDown`
  as plain types with no implementation, and any dissemination
  protocol written against those types runs over *any* membership
  protocol that answers them — `pkg/protocols/plumtree` runs unmodified over
  `pkg/protocols/hyparview` or over `cmd/gossip`'s static contact list,
  because both publish the same contract (see
  [`docs/protocols.md`](protocols.md)). The interchangeability comes
  from the shared type vocabulary, not from holding a reference to a
  specific instance.
- **Session lifecycle as a protocol event.** protorun surfaces peer
  connectivity itself — `SessionConnected`/`SessionDisconnected`/
  `SessionGivenUp` — as first-class events any protocol can handle,
  because "is this peer still there" is usually load-bearing
  information for a distributed protocol, not incidental plumbing.
  Actor frameworks generally treat remote-actor connectivity as
  clustering-layer infrastructure beneath the actor model, not as a
  message every actor can subscribe to.
- **What each ships batteries for.** Actor frameworks generally ship
  clustering, remote actor references, and supervision trees — because
  those are what an actor population needs to survive process and
  machine boundaries. protorun ships **distributed protocols** —
  HyParView (partial-view membership) and Plumtree (epidemic broadcast
  trees) under [`pkg/protocols/`](../pkg/protocols/) — because a protocol
  composition runtime's batteries are algorithms protocols are built
  from, not actor infrastructure. protorun does have supervision
  (`RegisterFactory` + `WithSupervision`, see the README's Supervision
  section) but it is scoped to *restarting a misbehaving protocol
  layer from fresh state*, not a per-actor/per-message supervision
  hierarchy.
- **Deterministic full-stack simulation.** Because dispatch order is
  fixed per protocol and the Clock is a seam (`WithClock`,
  `prototest.FakeClock`), `prototest.Sim` can run **real Runtimes,
  real protocols, real layered IPC** — not a model of them — under a
  seeded scheduler and virtual time, and reproduce the exact same
  schedule for the same seed. This falls out of the mailbox-ordering
  guarantee above: a scheduler that owns delivery order into a fixed
  set of per-protocol queues can actually control the whole system.
  See [`docs/simulation.md`](simulation.md) for the mechanism and its
  honest limits.

None of this makes protorun "better" than an actor framework — they
solve different problems. If what you need is a dynamic population of
lightweight, individually-addressable, remotely-supervisable
processes, that is the actor model's home turf. If what you need is a
small number of protocol layers — membership, dissemination,
consensus, replication — that stack cleanly, hand typed messages to
each other, and can be simulated as a whole system before you ever
open a socket, that is what protorun is for. This is the "protocol
composition runtime, not an actor framework" positioning stated in
[`docs/roadmap.md`](roadmap.md), spelled out end to end.
