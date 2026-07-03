# The protocol library (`pkg/protocols/`)

protorun ships batteries: real, paper-faithful distributed protocols you
can stack, run, and swap. They live under `pkg/protocols/`, all in the core
module (zero third-party dependencies), and they dogfood every framework
capability — sessions, timers, IPC, and the seeded simulation.

There are five packages:

| Package | What it is |
|---|---|
| `pkg/protocols/membership` | The interchangeability seam: a types-only IPC contract. |
| `pkg/protocols/hyparview` | A partial-view membership protocol (HyParView). |
| `pkg/protocols/plumtree` | An epidemic broadcast-tree protocol (Plumtree). |
| `pkg/protocols/raft` | The Raft consensus algorithm — leader election + replicated log. |
| `pkg/protocols/paxos` | Single-decree Paxos — the synod protocol, one immutable value. |

The headline is composition: **Plumtree runs over HyParView without either
knowing about the other.** They meet only at the membership contract.

## The contract: `pkg/protocols/membership`

`membership` is a package of types with no implementation. A membership
protocol answers `GetView` with its active view and publishes `NeighborUp`
/ `NeighborDown` as that view changes; a dissemination protocol consumes
exactly those:

```go
type GetView struct{ /* Request */ }
type View struct{ Active []transport.Host /* Reply */ }
type NeighborUp   struct{ Peer transport.Host /* Notification */ }
type NeighborDown struct{ Peer transport.Host /* Notification */ }
```

This is interchangeability via **typed IPC contracts, not Go interfaces**.
In protorun, cross-protocol coordination is always IPC — never a direct
method call — so the "interface" two layers share is a set of
request/reply/notification types the runtime routes between them. Any
dissemination protocol written against these four types works over any
membership protocol that honours them. `cmd/gossip` proves it from the
cheap end (a static contact list implements the contract);
`pkg/protocols/hyparview` proves it from the real end (a self-healing overlay
implements the same contract), and Plumtree runs over either unchanged.

**Why no codecs or `WireName`?** IPC in protorun is strictly local —
same-runtime, same-process — so contract values never touch the network.
A codec is only for bytes on a transport; `WireName` only freezes a
*network* wire id across renames. The contrast is sharp inside a
membership protocol: HyParView's own peer-to-peer messages (JOIN,
ForwardJoin, Shuffle, ...) do cross nodes, so they carry codecs and
`WireName`; the `NeighborUp` it publishes to a local Plumtree does not.

## `pkg/protocols/hyparview`

A faithful implementation of HyParView (Leitão, Pereira, Rodrigues,
2007). It maintains two views:

- a small, symmetric, **session-backed active view** (target size
  `ActiveSize`, default 5) — these are the peers you hold a live protorun
  session with;
- a larger, unconnected **passive view** (`PassiveSize`, default 30) — a
  random sample of the system kept fresh by the shuffle.

Mechanisms: JOIN via a contact; ForwardJoin random walks (ARWL/PRWL);
periodic Shuffle; graceful Disconnect; and passive-view promotion on
active-view failure, using the paper's priority rule (high priority when
the promoting node's active view is empty, so a node with no neighbours is
always admitted). Everything is configured through a `Config` with sane
zero-value defaults.

**Failure detection is the session layer** — there are no extra
heartbeats. A dropped active-view session surfaces as
`OnSessionDisconnected` (or `OnSessionGivenUp` for an exhausted retryable
dial); the peer leaves the active view, `NeighborDown` is published, and
the passive view is promoted to refill.

### Shuffle transport (the resolved open question)

The periodic shuffle never opens a transient session to a passive peer.
The Shuffle **request** is a TTL random walk over active views, exactly as
in the paper. The twist is the **reply**: the walk records its path (each
forwarder appends itself), and the accepting node returns its
`ShuffleReply` by retracing that path hop-by-hop back to the origin. Every
hop — request and reply — is an existing active-view session, so no
change to the session layer is needed. A passive peer is contacted only
during promotion, which legitimately opens a session because the peer is
*becoming* an active-view member. (This is option (a) from the roadmap;
it was preferred because it needs no framework changes.)

## `pkg/protocols/plumtree`

A faithful implementation of Plumtree ("Epidemic Broadcast Trees", Leitão
et al., 2007) over the membership contract. It splits its neighbours into:

- an **eager** set (tree links) — full messages are pushed here;
- a **lazy** set — only cheap `IHAVE` announcements go here.

New neighbours start eager (per the paper). A duplicate message receipt
means a redundant tree edge, so the receiver **PRUNEs** the sender to
lazy; a missing-message timer fired by an `IHAVE` **GRAFTs** the announcer
back to eager and pulls the message. The eager-link graph thus
self-optimises toward a spanning tree and self-heals after churn. `IHAVE`s
are batched per lazy peer and flushed on a short timer.

- **Message ids are sender+seq** — `(origin host, per-origin sequence)`.
  Chosen over content-addressing because it is O(1) to mint (no payload
  hash), collision-free by construction, compact as a map/cache key, and
  names the origin (useful for `Delivered.From` and debugging). Plumtree's
  job is to deliver each *broadcast* once, not to coalesce equal bytes.

- **Public surface** mirrors the gossip example's trigger pattern: a
  `Broadcast` request (fire-and-forget `BroadcastAck`) originates a
  broadcast, and a `Delivered{ID, Payload, From}` notification hands each
  unique delivery to the app layer.

### Anti-entropy caveat

Plumtree recovers a missed message only by GRAFTing a peer that still
holds it in its bounded cache (`Config.CacheSize`). It is **not** a full
anti-entropy protocol: a node partitioned for longer than the cache
retains a message — or fallen further behind than `CacheSize` messages —
loses that message permanently. Subsequent broadcasts are unaffected (the
tree repairs and delivers them everywhere); only messages that aged out of
every reachable cache during the outage are lost. Bridging longer
partitions is the application's responsibility (e.g. a periodic snapshot),
by design — this keeps Plumtree honest and cheap.

## `pkg/protocols/raft`

A faithful implementation of Raft (Ongaro & Ousterhout, "In Search of an
Understandable Consensus Algorithm", 2014) — leader election, log
replication, and a linearizable replicated log over a fixed set of
servers. Unlike HyParView/Plumtree it does **not** sit on the membership
contract, and that is the point: consensus needs the opposite of gossip
(see below).

- **What's faithful.** Randomized-timeout leader election (§5.2); the
  AppendEntries log-consistency check with `nextIndex` backoff for
  follower log repair (§5.3); commitment by majority counting restricted
  to **current-term** entries — the Figure-8 rule (§5.4.2); the
  "up-to-date" vote restriction (§5.4.1); higher-term stepdown on every
  RPC and reply (§5.1); and persistence of `currentTerm`/`votedFor`/log
  through a `Storage` seam, written before the triggering reply and
  reloaded at `Init`.

- **Why not HyParView underneath.** A consensus group has fixed, total
  membership: every server must know the whole roster to compute "have I
  heard from a majority of *all* servers". A partial-view membership
  protocol is engineered to give each node a small, churning *sample* and
  never a global view — exactly what Raft cannot use. Consensus wants
  total and stable membership; gossip wants partial and churning. They do
  not compose, so Raft takes its peers by `Config`, statically.

- **Storage caveat (loud).** The default `MemoryStorage` is **not
  durable**: on restart it forgets term, vote, and log. That violates
  Raft's crash-recovery model and can break safety across restarts (a
  server could vote twice in a term, or a committed entry could be lost if
  enough servers restart). It exists so tests and demos need no disk; it
  makes Raft's guarantees hold only for the lifetime of the process.
  **Production deployments must supply a crash-durable `Storage`** (fsync'd
  WAL or embedded KV).

- **Out of scope (documented, not stubbed).** Cluster membership change
  (§6 joint consensus), log compaction/snapshots (§7), client session
  dedup / exactly-once client semantics, and read-index/lease reads. The
  log grows without bound and `Propose` is at-least-once from the client's
  view.

- **Public surface.** A `Propose{Command}` request replies with the
  assigned `{Index, Term}` on the leader or fails with a `*NotLeaderError`
  naming the believed leader (for client redirect) elsewhere — note this
  is *acceptance*, not a commit ack. Commit-and-apply surfaces as an
  `Applied{Index, Term, Command}` notification published in strict log
  order, and leadership transitions as `LeaderChanged{Leader, Term}`. A
  `DebugState` request exposes role/term/commit/log summary for tests.

- **Example wiring** (a 3-node group; each node lists the other two):

  ```go
  peers := []transport.Host{n1, n2} // the OTHER members
  rt.Register(raft.New(self, raft.Config{Peers: peers}))
  // drive it:
  protorun.SendRequest(ctx, &raft.Propose{Command: cmd},
      func(rep *raft.ProposeReply, err error) { /* index+term, or NotLeaderError */ })
  protorun.SubscribeNotification(ctx, func(ev raft.Applied) { apply(ev.Command) })
  ```

## `pkg/protocols/paxos`

A faithful implementation of classic **single-decree Paxos** — Lamport's
synod protocol from "Paxos Made Simple" (2001). A fixed set of nodes agree
on **one** immutable value. All three roles (proposer, acceptor, learner)
live in a single `Protocol` type, and every node plays all three at once.
Like Raft it does **not** sit on the membership contract — a synod needs
total, stable membership to compute majority sizes (same rationale as
Raft's `Config`).

- **What's faithful.** Phase 1 (prepare/promise): a proposer picks a ballot
  from its own **disjoint** sequence `round*N + nodeIndex`, so no two nodes
  ever mint the same ballot number — a ballot uniquely identifies its
  proposer and needs no tie-break; an acceptor promises iff the ballot is
  strictly above every ballot it has promised. Phase 2 (accept/accepted):
  on a majority of promises the proposer **must** adopt the highest-ballot
  accepted value among them (its own only if none was accepted) — this
  value-adoption rule is the crux of Paxos safety. Learning: an acceptor
  that accepts announces `Accepted` to every learner, and a value is chosen
  once a learner sees a majority accept the same ballot. Acceptor state
  (promised ballot + accepted ballot/value) is persisted through a
  `Storage` seam before the promise or accepted announcement leaves the
  process.

- **Safety, informally.** Once a value `v` is chosen at ballot `b` (a
  majority accepted `b`), every higher ballot's promise quorum intersects
  that majority in at least one acceptor, which reports `(b, v)`; the
  adoption rule then forces every ballot above `b` to carry `v` too. So the
  decree, once chosen, can never change — Agreement holds across all nodes
  and all future rounds. This is exactly the property a naive proposer that
  ignores its promises' accepted values would violate.

- **Liveness.** A proposer whose round stalls retries with a higher ballot
  after a **randomized backoff** from the per-node seeded RNG. That
  desynchronizes dueling proposers so one eventually completes a round
  uninterrupted. Lamport's distinguished-proposer (leader) optimization is
  deliberately **not** implemented — backoff alone drives progress under
  the Sim.

- **Partition-heal catch-up (documented design).** `Heal` does not
  reconnect anything, and the framework never replays lost messages. So a
  node stranded in a minority that could never learn the decision catches
  up on reconnect: when a session (re)establishes, an acceptor holding an
  accepted value re-announces it (`OnSessionConnected` in `protocol.go`).
  After a heal each majority acceptor re-sends its `Accepted` for the chosen
  ballot to the reconnecting minority node, which tallies a majority and
  decides. No polling, no special catch-up RPC — the ordinary Phase-2b
  announcement, replayed on reconnect.

- **Single-decree only (scope).** This package decides **one** value. It is
  **not** Multi-Paxos: there is no log, no sequence of instances, no
  leader-lease read path. Log replication is what `pkg/protocols/raft`
  provides in this tree; Paxos is here as the second, independent consensus
  data point (the synod, distilled). Storage caveat is identical to Raft's:
  the default `MemoryStorage` is **not durable** and voids the
  crash-recovery guarantees — production must supply a crash-durable
  `Storage`.

- **Public surface.** A `Propose{Value}` request replies with an empty ack
  (a proposal is underway — NOT a decision) or fails with an
  `*AlreadyDecidedError` carrying the chosen value if the decree is already
  settled. The decision surfaces, exactly once per node, as a
  `Decided{Value, Ballot}` notification. A `DebugState` request exposes
  promised/accepted/decided state for tests.

- **Example wiring** (a 3-node synod; each node lists the other two):

  ```go
  peers := []transport.Host{n1, n2} // the OTHER members
  rt.Register(paxos.New(self, paxos.Config{Peers: peers}))
  // drive it:
  protorun.SendRequest(ctx, &paxos.Propose{Value: v},
      func(_ *paxos.ProposeReply, err error) { /* ack, or AlreadyDecidedError */ })
  protorun.SubscribeNotification(ctx, func(ev paxos.Decided) { use(ev.Value) })
  ```

## Testing them: the Sim is the primary vehicle

All of these protocols follow the authoring contract (all state and sends
inside handlers, no goroutines, no wall-clock reads — each seeds a per-node
RNG from its own `Host` and sorts before every random pick), so they run
under `prototest.Sim` deterministically. Their primary suites are Sim-based
and finish in milliseconds of real time:

- HyParView: 20-node convergence from a contact chain (non-empty,
  self-free, symmetric, bounded active views), churn (kill a quarter via
  `Isolate`; survivors reconverge and exclude the dead), and shuffle
  rotation (passive views populate and change over virtual time).
- Plumtree over HyParView: 20-node broadcast (delivered exactly once,
  duplicate rate bounded after the tree converges — proving PRUNE works),
  and partition/heal (mid-partition broadcasts stay on their side; after
  heal, GRAFT repairs the tree for subsequent broadcasts).
- Raft: 5-node election (one leader per term + no spurious re-election
  over a quiet period), replication (identical applied sequences on every
  node), leader-crash (new leader keeps all committed entries; the deposed
  leader rejoins and converges), partition (a minority commits nothing and
  the heal produces zero applied-sequence divergence), dueling candidates
  (split votes resolve via randomized timeouts), and a determinism check
  (same seed ⇒ byte-identical applied trace). Safety is *asserted*, not
  eyeballed — each test file names the invariant it guards.
- Paxos: 5-node happy path (all decide the same value exactly once —
  Agreement + Integrity), a **multi-seed dueling gauntlet** (two proposers,
  different values, adversarial delays, ~10 seeds — exactly one value chosen
  on every seed), value adoption (a majority accepts A's value but nobody
  learns it, then B drives a higher-ballot round and the synod still decides
  A — engineered via precise delay timing), quorum (a minority commits
  nothing; heal lets it catch up to the same value), chosen-is-forever (late
  proposals of new values still decide the original), and a determinism
  check (same seed ⇒ byte-identical decision trace). The ballot arithmetic,
  predicates, and adoption rule are also pinned by ordinary unit tests.

Pure logic (view sets, seeded sampling, the payload cache, wire
round-trips) is covered by ordinary unit tests, so the Sim does not carry
everything.

## Examples

- [`cmd/gossip`](../cmd/gossip/) — eager-push gossip over the contract,
  with a **static** membership implementation as the baseline. 10-node
  integration test + 100/1000-node scale probes.
- [`cmd/broadcast`](../cmd/broadcast/) — the flagship: Plumtree over
  HyParView over TCP. Type a line, it broadcasts to the cluster; every
  node prints what it delivers. Real-TCP integration test included.
