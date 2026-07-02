# Wire format

The authoritative spec for every byte the framework puts on the wire.
Each layer owns one slice of the envelope; the code comment at each
encode/decode site describes only the bytes that site owns and points
back here for the whole picture.

## Envelope

Top down, a full application frame:

```
[Length(uint32 BE) || LayerID(1 byte) || WireID(uint64 LE) || Payload...]
 └── TCPLayer ────┘ └─ SessionLayer ─┘ └────────── Runtime ───────────┘
```

| Field   | Size | Owner                          | Meaning                                    |
| ------- | ---- | ------------------------------ | ------------------------------------------ |
| Length  | 4    | `transport/tcp.go`             | Byte length of everything after the prefix |
| LayerID | 1    | `transport/session.go`         | `0` Application, `1` Session (handshake)   |
| WireID  | 8    | `protorun` (`message.go`)      | FNV-1a hash of the message type name (or `WireName()`) |
| Payload | n    | the message's registered codec | Codec-marshaled message bytes              |

The `Payload` bytes are whatever the registered `Codec[M]` produces.
`BinaryCodec` (fixed-size structs) and `WireCodec` (the reflective
default) are framework-owned; their formats are specified below.
`SelfCodec`, `JSONCodec`, and the nested `codec/protobuf` module defer
to the message type or an external library and are not framework wire
formats.

`Length` counts `LayerID + Body`; `maxFrameSize` (16 MiB) bounds it.

## Session (handshake) bodies

When `LayerID = 1`, the body is a handshake message:

```
Hello:  [Type=1 || Version(1 byte) || WriteHost(sender's logical Host)]
Ack:    [Type=2]
Reject: [Type=3 || Version(1 byte, the refuser's version)]
```

`WriteHost` is `[Port(uint32 LE) || len(IP)(uint32 LE) || IP bytes]`
(see `transport/host.go`).

## Handshake sequence

```
dialer                                listener
  │ ──── TCP connect ───────────────────► │
  │ ──── Hello(version, self) ──────────► │  version ok?
  │                                       │  ├─ yes: record mapping
  │ ◄──────────────────────────── Ack ─── │  │       emit SessionConnected
  │  emit SessionConnected                │  │
  │                                       │  └─ no:  emit SessionVersionMismatch(inbound)
  │ ◄─────────────────────────emit Reject(version) ── │         close connection
  │  emit SessionVersionMismatch          │
  │  (runtime: terminal given-up)         │
```

The dialer is Established only when the Ack arrives; until then no
`SessionConnected` is emitted and a handshake timeout (default 5s,
`WithHandshakeTimeout`) bounds the wait. A `Reject` is terminal: the
runtime stops any retry schedule immediately and fans out given-up.

## Versioning

`transport.ProtocolVersion` is the single version byte advertised in
Hello and echoed in Reject. Bump it whenever the framing or handshake
structure changes in a way prior builds can't parse. Old builds treat
an unknown handshake type (like Reject) as a parse error and close the
connection — the same net effect they had before Reject existed.

## WireCodec payload format

`WireCodec[M]` is the reflective default codec (registered
automatically by `protorun.Handle`). It marshals the `Payload` bytes of
any message whose fields are made of the supported kinds. This section
is the authoritative spec for those bytes.

**No schema evolution.** Fields carry no tags or numbers on the wire —
the `WireID` already pins the concrete type, and there is exactly one
schema per type. Renaming or reordering a struct's fields, changing a
field's type, or inserting a field is a **wire break**: it changes the
byte layout with no version negotiation. This is the same stance as
`WireName` takes for the type identifier itself; freeze the struct
layout the same way you freeze the wire id.

### Field ordering and selection

Fields are encoded in **struct declaration order**. Two fields are
skipped and never appear on the wire:

- **unexported fields** (they are invisible to reflection-based access);
- fields tagged **`wire:"-"`**.

A skipped field decodes back to its zero value.

### Kind encodings

| Kind                          | Encoding                                                             |
| ----------------------------- | ------------------------------------------------------------------- |
| `bool`                        | 1 byte, `0` or `1`                                                   |
| `int8` / `uint8`              | 1 byte                                                              |
| `int16` / `uint16`            | 2 bytes, little-endian                                              |
| `int32` / `uint32`            | 4 bytes, little-endian                                              |
| `int64` / `uint64`            | 8 bytes, little-endian                                              |
| `float32`                     | 4 bytes, little-endian IEEE-754 bits                               |
| `float64`                     | 8 bytes, little-endian IEEE-754 bits                               |
| `string`, `[]byte`            | `uvarint` length prefix, then the raw bytes                        |
| slice `[]T`                   | `uvarint` element count, then each element encoded in order        |
| array `[N]T` (fixed-size `T`) | `N` elements in order, **no** length prefix (count is in the type) |
| `map[K]V`                     | `uvarint` entry count, then key/value pairs (see determinism)      |
| nested `struct` (by value)    | its fields in declaration order, no framing                        |
| `*struct`                     | 1 presence byte (`0` = nil, `1` = present), then the struct body   |

Fixed-size scalars are little-endian, consistent with the rest of the
application body (the `WireID` header is little-endian too). Variable-
length values use `binary.AppendUvarint` / `binary.Uvarint` for their
length prefix.

### Map determinism

Map entries are written with keys sorted by their **encoded byte order**
(bytewise, ascending). Marshal is therefore deterministic: the same map
value always produces the same bytes regardless of Go's randomised map
iteration order. This matters because retransmission and dedup key off a
stable payload hash — a non-deterministic encoding would make the same
logical message hash differently on each send.

### Decoding rules

- Trailing bytes after the last field are an **error** (a decode must
  consume the whole payload).
- A truncated payload — a length prefix that overruns the remaining
  bytes, a scalar cut short, a missing pointer presence byte — is an
  **error**, never a panic. Arbitrary/fuzzed input is rejected the same
  way.
- Length prefixes are bounded against the remaining input, so a
  hostile length can't drive an unbounded allocation.
- Empty and nil `[]byte`/slices/maps are wire-indistinguishable (both
  encode as a zero count) and decode to a nil value.

### Rejected kinds

The following are rejected when the per-type plan is compiled (surfaced
as an error from `Marshal`/`Unmarshal`, and eagerly as a panic from
`Handle`), because they are not portably representable:

- **platform-sized `int` / `uint` / `uintptr`** — width varies by
  architecture; pick a sized type (`int32`/`int64`/`uint32`/`uint64`);
- **`interface`, `chan`, `func`, `complex`, `unsafe.Pointer`**.

Pointers are supported only as pointer-to-struct. A message needing an
encoding WireCodec can't express implements `SelfMarshaler` and is
registered with `SelfCodec` (both still work through `Handle`).
