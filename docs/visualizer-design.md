# protoviz — a visual debugger for protocol runs

Design exploration for a UI layer over protorun: watch a protocol
stack evolve step by step — topology, messages, per-node state —
during a run and after it. Reworks the ideas from the old
`overlay-graph-visualizer` project (2022-era, vanilla JS + D3) into
a first-class part of this repo.

## What the old project got right (and what it lacked)

The old visualizer had three ideas worth keeping:

1. **The trace is an event log, and a view is a fold.** Its log
   format was a sequence of state-changing operations (spawned /
   connected / disconnected) with timestamps; the picture at time T
   is a fold over events ≤ T. That is exactly the right model, and
   it maps one-to-one onto protorun's deterministic simulator.
2. **The overlay graph is the primary view.** Nodes and links,
   force-directed, with membership visible at a glance.
3. **Click a node, see its internal state.** State inspection
   belongs in the same picture as topology.

What it lacked: the logs were hand-written (no producer), scrubbing
backward was on the TODO list ("scary but..."), there was one view
for every protocol type, and the code was a 300-line D3 sketch
served by `http-server`. All four gaps have natural answers in
protorun.

## Why protorun is unusually well positioned for this

The hard part of a protocol visualizer is not the UI — it is
getting a **faithful, complete, ordered trace**. protorun already
has every ingredient:

- **A deterministic, delivery-granular scheduler.** `prototest.Sim`
  advances one delivery at a time and describes each unit of
  progress as a `DeliveryInfo` (kind, from, to, wire id). A trace is
  just those steps written down. Same seed → byte-identical trace →
  **scrubbing backward and replaying are free**, because any state
  at step N is a re-fold (or a snapshot) of a reproducible sequence.
- **Session lifecycle is first-class.** SessionConnected /
  Disconnected / Failed / GivenUp events *are* the topology edges;
  no log parsing, no inference.
- **Wire ids name message types.** `DeliveryInfo.WireID` plus the
  codec registry's id→type mapping labels every arrow.
- **Every in-tree protocol already answers `DebugState` over IPC**
  (hyparview, plumtree, raft, paxos) — the node-inspection panel's
  data source already exists, reachable without breaking the
  all-state-in-handlers contract.
- **Quiescent points make state capture sound.** The Sim settles to
  quiescence between deliveries; sampling node state there is
  race-free and deterministic. For live runs, the `evCallback`
  mailbox event (used by RestartHandler today) can run a snapshot
  closure ON the protocol's own loop — same safety argument as any
  handler.

## The standardization problem, and the answer: one trace, many lenses

Different protocol types want different pictures — a membership
protocol is a graph, consensus is a sequence diagram, a broadcast
tree is a tree. The mistake would be one "standard visualization."
The design instead standardizes the **trace**, and treats each
visualization as a **lens**: a pure function from
`(trace prefix, lens config) → picture`.

### Layer 1 — the universal trace (protoviz trace format v1)

JSONL, one event per line, all emitted by the RUNTIME with zero
protocol cooperation:

```jsonc
{"step":184,"t":"12.450s","kind":"deliver","from":"n1:5001","to":"n2:5002","wire":"raft.AppendEntries","bytes":74}
{"step":185,"t":"12.450s","kind":"session","event":"disconnected","a":"n2:5002","b":"n4:5004"}
{"step":186,"t":"12.700s","kind":"clock","fires":"n3:5003/election"}
{"step":187,"t":"12.700s","kind":"drop","reason":"cut","from":"n1:5001","to":"n4:5004","wire":"raft.AppendEntries"}
```

Event kinds: node up/down, session connected/disconnected/failed/
given-up, message sent/delivered/dropped (with wire-type name, size,
and the fault that dropped it), clock advance/timer fire, IPC
request/reply/notification (local, per node), protocol restart /
stop / escalate, dead letters. `step` is the Sim's global step
counter — a total order. Wall-clock traces (live mode) use `t` and a
per-node sequence instead; the format is the same.

### Layer 2 — state snapshots (the lens fuel)

Interleaved with envelope events, the recorder samples each node's
protocol state at quiescent points:

```jsonc
{"step":185,"kind":"state","node":"n2:5002","protocol":"raft","state":{"role":"follower","term":7,"commit":141,"leader":"n1:5001"}}
```

Source: the existing `DebugState` IPC surfaces, driven by the
recorder through a harness probe (exactly how the sim tests poll
today). Protocols need NO new API — a lens simply knows the shape
of its protocol's `DebugStateReply`. An optional future nicety is a
`Snapshotter` interface for cheaper capture, but it is not needed
to ship.

Snapshot cadence is configurable: every step (small sims, perfect
scrubbing), every N steps, or on-demand at scrub time by re-running
the fold (determinism makes lazy snapshotting valid).

### Layer 3 — lenses

Each lens declares which trace kinds and which state shapes it
consumes. Shipping set:

| Lens | Consumes | Picture |
|---|---|---|
| **Topology** (default, universal) | session events, deliveries | Force-directed graph; edges = live sessions; messages animate along edges; node color by protocol-declared class. The old visualizer, reborn. |
| **Sequence** (universal) | deliveries, filtered by wire type / node set | Classic Lamport diagram: nodes as lanes, messages as arrows, drops as scissored arrows. The consensus debugging view. |
| **Membership** (hyparview) | topology + hyparview state | Active view = solid edges, passive view = ghost edges; shuffle walks highlighted; symmetric-view violations flagged red. |
| **Broadcast tree** (plumtree) | topology + plumtree state | Eager = solid, lazy = dashed; replaying one broadcast shows the tree light up hop by hop; GRAFT/PRUNE recolor live. |
| **Consensus** (raft / paxos) | sequence + state | Role/term badges per lane; per-node log bar (commitIndex / lastApplied) for Raft; ballot/promise state for Paxos; leader changes as lane markers. |
| **Inspector** (universal chrome) | state snapshots | Click any node at any step → its DebugState, pretty-printed, with diff-vs-previous-step highlighting. |

Lenses are per-protocol *plugins* in the UI (a TypeScript module
that registers: name, wire-type patterns it claims, a state-shape
decoder, and a React component). A run of an unknown user protocol
still gets Topology + Sequence + raw-JSON Inspector — the universal
floor. That is the answer to "different protocols need different
views": the floor is universal, the ceiling is pluggable.

### The chrome (universal, ShadCN default components)

- **Step scrubber**: Slider bound to the step counter; play /
  pause / speed; keyboard ←/→ for single-step. Backward is free.
- **Fault ribbon**: Cut/Heal/Isolate/loss/delay changes shown as
  bands on the scrubber (the trace records fault mutations too).
- **Filter bar**: Command palette (⌘K) to filter by wire type, node,
  or lens; Tabs to switch lenses; Resizable split panes.
- **Seed banner**: every Sim trace carries its seed; the UI shows
  "reproduce: prototest.WithSeed(N)" — a failing CI run's trace
  artifact opens in the visualizer and replays exactly.

## Architecture

```
+---------------------------------------------------------------+
| viz/  (Vite + React + TS + Tailwind + ShadCN, default theme)  |
|   lenses/{topology,sequence,membership,tree,consensus}        |
|   loads: trace file (drag-drop)  |  live: EventSource /events |
+-------------------------------▲-------------------------------+
                                |  JSONL lines / SSE data frames
+-------------------------------+-------------------------------+
| pkg/prototest: TraceRecorder  |  cmd/protoviz (live server)   |
|  wraps a Sim: writes each     |  serves built UI + fans a     |
|  stepInfo + session event +   |  cluster's Tracer streams out |
|  periodic DebugState samples  |  over SSE (/events, /ingest)  |
+---------------------------------------------------------------+
```

- **Post-run (Phase A)**: `prototest.NewSim(t, WithTrace(w))` — the
  recorder hooks the scheduler it already owns; tests opt in with
  one option; failing tests can dump the trace as an artifact. The
  UI is a static bundle; opening a trace is drag-and-drop. No
  server, no runtime changes.
- **Live (Phase C, as built)**: a `Tracer` seam on the runtime
  (sibling of `Metrics`: `WithTracer(t)`, disabled-by-default, same
  guard-before-alloc discipline the metrics fast path uses), and
  `cmd/protoviz` to serve the UI + a Server-Sent-Events stream. Live
  mode is lossy-by-design (bounded ring, drop-oldest) at every hop so
  tracing can never backpressure a real cluster. See the live-mode
  section below for the as-built shape.

The UI lives in `viz/` at the repo root — not a Go module, excluded
from the Go workspace; its build artifact can be embedded into
`cmd/protoviz` via `go:embed` so the live server is a single binary.

## Phasing

| Phase | Deliverable | Effort |
|---|---|---|
| A | Trace format + Sim recorder (`WithTrace`) + viewer app with Topology, Sequence, Inspector lenses + scrubber | the core; UI-heavy |
| B | Protocol lenses: membership, broadcast-tree, consensus; trace-artifact-on-test-failure helper | per-lens, incremental |
| C | Live mode: `Tracer` runtime seam, `cmd/protoviz`, SSE streaming, follow-mode UI | done — see below |

Phase A alone already delivers the original dream — scrub through a
run, watch the overlay form, click nodes — with protorun's
determinism turning the old project's two hardest TODOs (backward
scrubbing, log production) into non-features.

## Live mode (Phase C, as built)

The live path streams a running cluster's events into the same viewer
that opens a file, format-compatible with `protoviz/1`.

**The `Tracer` seam** (`pkg/protorun/tracer.go`). `WithTracer(t)` is
the sibling of `WithMetrics`: disabled by default, and every emit site
(`sendMessage`, `processMessage`, `dispatchSessionEvent`, the
supervisor outcomes, the dead-letter emit) guards on
`Runtime.tracerEnabled` before building the event — no `TraceEvent` is
allocated and no method is called when no tracer is installed
(benchmarked: allocs/op unchanged). `TraceEvent` is flat and small
(`Kind, Peer, Wire, Bytes, Detail, At`) and always from the local
node's point of view; the server tags each stream by node. Kinds:
`send`, `deliver`, `session-connected`/`-disconnected`/`-failed`/
`-givenup`, `restart`/`stop`/`escalate`, `dead-letter`. `Trace` is
called synchronously and MUST be fast and non-blocking (drop, never
stall) — the runtime does not shield itself from a slow tracer beyond
that contract.

**The HTTP tracer** (`cmd/internal/viztrace`). `NewHTTPTracer(server,
node)` honors that contract: `Trace` marshals into a bounded,
drop-oldest ring and returns; a background goroutine POSTs batches to
`/ingest?node=<host>` every 250 ms. A failed POST drops its batch.
`cmd/broadcast -viz http://host:port` wires it in, so the flagship
demo streams itself.

**The server** (`cmd/protoviz`). One stdlib-only binary:

- `GET /events` — SSE (`text/event-stream`). A bounded replay ring
  (default 50k, `-ring`) means a late-joining viewer gets the ring
  replayed then the live tail, with no gap or duplicate (a client is
  registered and the ring snapshotted under one lock). Heartbeat
  comment every 15 s.
- `POST /ingest?node=<host>` — line-delimited JSONL from cluster HTTP
  tracers. The server annotates each line with the `node` param and a
  server-side monotonic `step` (so interleaved node streams have one
  total order for the fold) and converts runtime kinds to the
  `protoviz/1` kinds the viewer parses.
- `-replay file -pace d` — stream an existing trace file over
  `/events` at a fixed pace, so live-mode UX is demoable with zero
  cluster setup.
- Static UI from `-ui` (default `viz/dist`); a missing dir serves a
  "`cd viz && npm run build`" page.

**Ingest normalization — the deliver decision.** A message crossing
the wire is reported twice: the sender emits `send`, the receiver
emits `deliver`. Folding both into the topology/sequence lenses would
double-count and, under loss, count messages that never arrived. So
the server treats the **receiver's `deliver` as the authoritative
record**: a receiver's `deliver` (whose `Peer` is the sender) becomes
`{"kind":"deliver","from":Peer,"to":node}`. Sender-side `send` events
are **dropped from that record** but still **forwarded as `kind:"send"`**
for future lenses — nothing is thrown away on the wire. Session events
map onto the existing session fold (`session-givenup` → a terminal
`failed`, an edge removal); the lifecycle kinds pass through for future
use.

**The viewer.** The loader gains a "Connect live" URL box; connecting
opens an `EventSource` on `/events`. Events append to an in-memory
trace and the fold engine grows incrementally (append-only, keyframes
extended in place, `indexForStep` a binary search — no rebuild per
event). A follow toggle (default on) pins the scrubber to the newest
step; scrubbing back disengages it; a Live badge + "Live" button
re-engage. Connection state (connecting / live / reconnecting) shows in
the header. Live runs have no seed banner and no virtual time — wall
time (`t`) is displayed when present. The parser is tolerant of both
the server's normalized `session` kind and the runtime's raw
`session-connected`-style kinds.

## Open questions

- Trace size: a 50-node, 30-virtual-second sim can be ~10^5 events;
  JSONL is fine (a few MB), but per-step state snapshots multiply
  it. Lean: snapshot every N steps + lazy re-fold for exact scrubs.
- Lens plugin API stability: keep it internal (in-tree lenses only)
  until v1; third-party lenses are a post-launch idea.
- Live-mode clock: wall-clock traces lack the Sim's total order.
  Resolved for launch by having the **server** assign a monotonic
  `step` as it ingests, giving the fold one total order across
  interleaved node streams; wall time (`t`) is carried for display.
  Per-node send/receive matching by message id (for a true live
  sequence lens) is still future work; format v1 reserves the fields.
- Does `cmd/broadcast` grow a trace flag so the flagship demo doubles
  as the visualizer's demo data source? **Yes** — `-viz <server>`
  installs the live HTTP tracer, so the demo streams itself to a
  running `protoviz`.
```
