# The protocol library (`protocols/`)

protorun ships batteries: real, paper-faithful distributed protocols you
can stack, run, and swap. They live under `protocols/`, all in the core
module (zero third-party dependencies), and they dogfood every framework
capability — sessions, timers, IPC, and the seeded simulation.

There are three packages:

| Package | What it is |
|---|---|
| `protocols/membership` | The interchangeability seam: a types-only IPC contract. |
| `protocols/hyparview` | A partial-view membership protocol (HyParView). |
| `protocols/plumtree` | An epidemic broadcast-tree protocol (Plumtree). |

The headline is composition: **Plumtree runs over HyParView without either
knowing about the other.** They meet only at the membership contract.

## The contract: `protocols/membership`

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
`protocols/hyparview` proves it from the real end (a self-healing overlay
implements the same contract), and Plumtree runs over either unchanged.

**Why no codecs or `WireName`?** IPC in protorun is strictly local —
same-runtime, same-process — so contract values never touch the network.
A codec is only for bytes on a transport; `WireName` only freezes a
*network* wire id across renames. The contrast is sharp inside a
membership protocol: HyParView's own peer-to-peer messages (JOIN,
ForwardJoin, Shuffle, ...) do cross nodes, so they carry codecs and
`WireName`; the `NeighborUp` it publishes to a local Plumtree does not.

## `protocols/hyparview`

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

## `protocols/plumtree`

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

## Testing them: the Sim is the primary vehicle

Both protocols follow the authoring contract (all state and sends inside
handlers, no goroutines, no wall-clock reads — each seeds a per-node RNG
from its own `Host` and sorts before every random pick), so they run under
`prototest.Sim` deterministically. Their primary suites are Sim-based and
finish in milliseconds of real time:

- HyParView: 20-node convergence from a contact chain (non-empty,
  self-free, symmetric, bounded active views), churn (kill a quarter via
  `Isolate`; survivors reconverge and exclude the dead), and shuffle
  rotation (passive views populate and change over virtual time).
- Plumtree over HyParView: 20-node broadcast (delivered exactly once,
  duplicate rate bounded after the tree converges — proving PRUNE works),
  and partition/heal (mid-partition broadcasts stay on their side; after
  heal, GRAFT repairs the tree for subsequent broadcasts).

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
