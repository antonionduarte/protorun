# How to: a custom codec

Most messages need nothing beyond `protorun.Handle`, which picks the
reflective `WireCodec[*M]` default (see
[Messages](../README.md#messages) and the byte-level spec in
[`docs/wire-format.md`](wire-format.md)). This page is for the two
cases where you want something else:

1. Your message type can own its encoding (a compact custom layout, a
   format some other system already expects) — implement
   `SelfMarshaler` and keep using `Handle`.
2. You need full control over the `Codec[M]` — a type you don't
   control the definition of, or a codec shared by more than one
   message type — use `RegisterCodec` + `RegisterHandler` directly.

## Option 1: `SelfMarshaler`

Implement two methods and `Handle` picks `SelfCodec[*M]` for you
automatically — no separate registration call:

```go
type SelfMarshaler interface {
	MarshalWire() ([]byte, error)
	UnmarshalWire(data []byte) error
}
```

Reach for this when a message has variable-length fields laid out in a
format `WireCodec` doesn't produce (it always length-prefixes strings
and byte slices with a `uvarint`; see the "WireCodec payload format"
section of [`docs/wire-format.md`](wire-format.md) for what it *does*
support) — for example, matching an existing on-disk or cross-language
format, or hand-tuning the byte layout for a very hot message type.

The core module's own `wire` package (`wire.WriteString` /
`wire.ReadString`, `wire.WriteUint64` / `wire.ReadUint64`, ...) is a
small helper kit for exactly this — length-prefixed strings/bytes and
fixed-width integers over an `io.Writer`/`io.Reader`, with no
allocation surprises:

```go
package chat

import (
	"bytes"

	"github.com/antonionduarte/protorun"
	"github.com/antonionduarte/protorun/wire"
)

// ChatMessage owns its own encoding instead of using the reflective
// WireCodec default.
type ChatMessage struct {
	protorun.BaseMessage
	From string
	Body string
}

func (ChatMessage) WireName() string { return "chat.message" }

func (m *ChatMessage) MarshalWire() ([]byte, error) {
	var buf bytes.Buffer
	if err := wire.WriteString(&buf, m.From); err != nil {
		return nil, err
	}
	if err := wire.WriteString(&buf, m.Body); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (m *ChatMessage) UnmarshalWire(data []byte) error {
	r := bytes.NewReader(data)
	from, err := wire.ReadString(r)
	if err != nil {
		return err
	}
	body, err := wire.ReadString(r)
	if err != nil {
		return err
	}
	m.From, m.Body = from, body
	return nil
}
```

Registration is unchanged — `Handle` detects the `SelfMarshaler` and
wires `SelfCodec[*ChatMessage]` for you:

```go
protorun.Handle(ctx, p.onChatMessage) // func(*ChatMessage, transport.Host)
```

`SelfCodec.Unmarshal` allocates the destination with `reflect.New`
(the same way `BinaryCodec` does), then calls `UnmarshalWire` on it —
so `*ChatMessage`'s method set, not a value receiver, must implement
`SelfMarshaler` (note the pointer receivers above).

## Option 2: a full `Codec[M]`

For anything `SelfMarshaler` doesn't fit — you don't control the
message type's definition, or one codec needs to serve several message
types (protobuf-generated messages, for instance) — implement
`Codec[M]` directly and register both halves explicitly:

```go
type Codec[M Message] interface {
	Marshal(m M) ([]byte, error)
	Unmarshal(data []byte) (M, error)
}
```

```go
protorun.RegisterCodec(ctx, myCodec)             // Codec[*MyMessage]
protorun.RegisterHandler(ctx, p.onMyMessage)     // func(*MyMessage, transport.Host)
```

This is exactly the shape the framework's own shipped codecs use —
`BinaryCodec[M]` (`codec.go`), `JSONCodec[M]` (`jsoncodec.go`), and the
nested [`codec/protobuf`](../codec/protobuf/) module's
`ProtoCodec[M proto.Message]`, which wraps a message you didn't define
(a generated protobuf type) the same way you'd wrap any external type:

```go
protorun.RegisterCodec[*pb.MyProtoMessage](ctx, protobuf.ProtoCodec[*pb.MyProtoMessage]{})
protorun.RegisterHandler(ctx, p.onProtoMessage)
```

`Marshal` must not mutate the message it receives, and `Unmarshal`
must allocate and return a fresh instance — the same two rules
`Codec[M]`'s doc comment states and every codec in this repo follows.

## Which one?

| Situation | Use |
|---|---|
| Fixed-size fields, no custom layout needed | `Handle` — picks `WireCodec` |
| You own the type, want a custom byte layout | `SelfMarshaler` + `Handle` |
| You don't own the type, or one codec serves many types | `Codec[M]` + `RegisterCodec`/`RegisterHandler` |
