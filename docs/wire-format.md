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
