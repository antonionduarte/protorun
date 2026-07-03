# protoviz — the protocol visual debugger

A viewer for `protoviz/1` JSONL traces produced by `pkg/prototest`'s
`WithTrace` recorder. Open a trace, scrub through a run step by step, watch the
overlay graph form, and click nodes to inspect their protocol state. It also
connects **live** to a running cluster and tails events as they happen.

This is the viewer for the design in
[`docs/visualizer-design.md`](../docs/visualizer-design.md). It is a static
Vite + React app — no server, no Go dependency; opening a trace is
drag-and-drop, and live mode is one URL box.

## Run it

```bash
cd viz
npm install
npm run dev      # open the printed localhost URL, pick a sample trace
```

Other scripts:

```bash
npm test         # vitest: trace parser + fold-engine invariants on the real samples
npm run build    # tsc + vite production build into dist/
npm run preview  # serve the production build
npm run typecheck
```

The four committed sample traces in [`sample-traces/`](./sample-traces) are the
Vite `publicDir`, so they are fetchable at `/raft-partition.jsonl` etc. and are
copied into `dist/` on build.

## Live mode

Build the viewer and serve it (plus a live event stream) with `cmd/protoviz`:

```bash
cd viz && npm run build           # produce viz/dist
go run ./cmd/protoviz -addr :7777 # from the repo root; serves viz/dist + /events

# Demo it with zero cluster setup — replay a sample over the live stream:
go run ./cmd/protoviz -replay viz/sample-traces/raft-partition.jsonl -pace 50ms

# The real thing: a cluster that streams itself (each node -> /ingest):
go run ./cmd/broadcast -self-port 8801 -viz http://localhost:7777
go run ./cmd/broadcast -self-port 8802 -contact-port 8801 -viz http://localhost:7777
```

Open the served page, hit **Connect live** (default `http://localhost:7777`),
and watch the overlay form in real time. The scrubber follows the newest event
by default; scrub back to pause on a moment, then hit **Live** to catch up. A
badge in the header shows the connection state (connecting / live /
reconnecting). Live runs have no seed banner and display wall time.

## What's in the box

**Stack.** Vite + React + TypeScript + Tailwind + a default (neutral) shadcn/ui
theme, `d3-force` for graph layout, `vitest` for unit tests. No custom design
system — restrained, default shadcn components throughout.

**Core model** (`src/lib/`):

- `trace.ts` — a tolerant parser for the `protoviz/1` schema (all kinds:
  meta / node / session / deliver / drop / clock / fault / state, plus the
  live-mode kinds send / restart / stop / escalate / dead-letter). It never
  throws on a bad line; malformed or unknown lines are collected as warnings
  and skipped. `parseLiveLine` decodes one streamed SSE line and tolerates the
  runtime's raw `session-connected`-style kinds. The authoritative schema is
  `pkg/prototest/trace.go`.
- `fold.ts` — the world-state engine. A view at step *S* is a fold over all
  events with step ≤ *S*: alive nodes, the live session set (unordered pairs),
  the latest state snapshot per `(node, protocol)`, fault status (cut pairs,
  isolated nodes, lossy/delayed links), and the events at the current step for
  animation. Full keyframe snapshots every 500 events + forward-fold make
  scrubbing (including backward) instant even on the 6.4k-line hyparview trace.
  `append` grows the trace incrementally for live mode (keyframes extended in
  place, `indexForStep` a binary search) — no rebuild per streamed event.
- `lenses.ts` — the lens registry. Universal lenses always apply; protocol
  lenses register against a predicate over the trace's declared protocol names.

**Chrome.** Left rail: drag-and-drop loader + sample picker + run summary with a
copyable `prototest.WithSeed(N)` badge. Bottom: the scrubber (slider, play/pause,
0.5×/1×/4×/16× speed, `←/→` single-step, `Home`/`End`, `space` to play), with a
fault ribbon marking cut/heal/isolate/loss events on the track. Top: lens tabs
(only lenses whose `canRender` matched). `⌘K` command palette: jump-to-step,
filter by wire type, filter by node. Main: the active lens beside a resizable
Inspector panel. Dark / light / system via the default shadcn class mechanism.

**Lenses** (`src/lenses/`):

| Lens | Applies to | Renders |
|---|---|---|
| Topology | all traces | force graph; edges = live sessions; the current delivery animates as a moving dot, drops flash a red X; cut pairs = no edge; isolated nodes get a muted ring. Node positions stay stable across scrubbing. |
| Sequence | all traces | Lamport diagram: node lanes, deliver arrows labelled by wire type, drops as X-terminated arrows, session tick marks. Windowed to ±200 events around the current step. |
| Inspector | all traces | current-step event detail + selected node's latest state per protocol, pretty JSON with per-key diff highlighting vs the previous snapshot. |
| Membership | hyparview | topology driven by HyParView Active/Passive views: active = solid, passive = dashed ghost, asymmetric active = amber half-edge. |
| Broadcast tree | plumtree | eager links solid, lazy dashed; a broadcast visibly floods. |
| Consensus | raft / paxos | sequence base + per-lane role/term/commit (raft) or ballot (paxos) header badges, and leader-change markers. |

## Dev notes / deviations from the design

- **Plumtree tree edges are derived from the message stream, not state.** The
  sample trace's `plumtree.DebugStatsReply` carries only counters (Delivered,
  Duplicates, and Eager/Lazy *counts*), not per-peer eager/lazy lists. The tree
  lens therefore reconstructs eager/lazy links from `plumtree.Gossip` /
  `Graft` / `Prune` / `IHave` deliveries up to the current step — which is
  exactly what those links mean. See `src/lenses/tree.tsx`.
- The traces are ground truth: the raft/paxos/hyparview lenses decode the
  exact Go `DebugState*Reply` shapes (`src/lib/protocols.ts`).
- The graph layout keeps a single persistent `d3-force` simulation and recycles
  existing node coordinates on every membership recompute, re-heating alpha only
  when the node set actually changes — so scrubbing never scrambles positions
  (`src/lib/useGraphLayout.ts`).
