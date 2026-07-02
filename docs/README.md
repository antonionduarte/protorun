# Documentation index

protorun's docs follow [Diátaxis](https://diataxis.fr): tutorials teach
by doing, how-tos solve one specific problem, reference is the
lookup-table for exact behavior, and explanation builds understanding
of why things are shaped the way they are. Start with the tutorial if
you're new; jump straight to a how-to if you already know what you're
trying to do.

## Tutorial

Learning-oriented: follow along, end with something working.

- [**Your first protocol**](tutorial.md) — write a tiny two-message
  protocol from nothing to a passing, deterministic test, using
  `prototest.Sim`. No TCP, no ports, no `main` function.

## How-to guides

Problem-oriented: you know what you need, here's how to get it.

- [**TLS and mutual TLS**](how-to-tls.md) — encrypt the TCP transport,
  one option at construction time; mTLS with a shared CA.
- [**A custom codec**](how-to-custom-codec.md) — `SelfMarshaler` for a
  message that owns its own encoding, or a full `Codec[M]` for types
  you don't control (protobuf, foreign formats).
- [**A custom transport backend**](how-to-custom-transport.md) — what
  `transport.Layer` requires, walked through the QUIC backend as a
  real second implementation.

## Reference

Information-oriented: precise, exhaustive, consult as needed.

- [**Wire format**](wire-format.md) — the authoritative byte-level spec
  for every layer of the envelope (TCP framing, session handshake,
  `WireCodec`'s payload format).
- [**Benchmarks**](benchmarks.md) — methodology and measured numbers
  for codec cost, mailbox latency, in-process IPC, and real-TCP round
  trips, all measured in this repository.

## Explanation

Understanding-oriented: the reasoning and trade-offs behind the design.

- [**Concurrency model**](concurrency-model.md) — the per-protocol
  event loop, the unified mailbox, why IPC never leaves the process,
  and why protocol composition is a different model from actors (not
  a better or worse one — a different one).
- [**Deterministic simulation**](simulation.md) — how `prototest.Sim`
  runs a full protocol stack under a seeded scheduler and virtual
  time, what the determinism contract requires of a protocol, and how
  to reproduce a failing run.
- [**The protocol library**](protocols.md) — the membership contract,
  HyParView, and Plumtree: what each does, and how the contract makes
  them interchangeable.

## Also in this repository

- [`../README.md`](../README.md) — project overview, quick start,
  concepts, and the actor-framework comparison.
- [`roadmap.md`](roadmap.md) — the pre-v1.0 design roadmap: what
  shipped in each phase and why, kept for the historical record and
  the design rationale behind decisions above.
- [`../TODO.md`](../TODO.md) — the current state of what's done vs.
  planned.
- [`../CONTRIBUTING.md`](../CONTRIBUTING.md) — how to work on protorun
  itself.
