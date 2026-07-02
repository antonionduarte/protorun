package protorun

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"reflect"
	"sync"
)

// WireNamer lets a message type freeze its wire identifier across Go-type
// renames or package moves. Optional, but production protocols SHOULD
// implement it on every message type; see WireID for why.
type WireNamer interface {
	WireName() string
}

// WireID returns the deterministic 64-bit wire identifier for type T.
// Use this when you need the id without an instance, e.g. inside a
// generic registration helper. The id is the FNV-1a 64-bit hash of
// either T.WireName() (if T implements WireNamer) or
// reflect.TypeOf(*new(T)).String() (the Go type name, fully qualified).
//
// Used internally for Message, Request, Reply, and Notification types.
//
// IMPORTANT: the default (hashing the Go type name) silently changes
// when the type is renamed, moved between packages, or when the import
// path changes. Such a change breaks wire compatibility with already-
// deployed peers without any compile-time signal. Production protocols
// should implement WireName() on every message type to freeze the wire
// identifier independently of Go type names.
func WireID[T any]() uint64 {
	var zero T
	t := reflect.TypeOf(zero)
	if t == nil {
		// T is an interface or otherwise non-instantiable; reject explicitly.
		panic("protorun: WireID called with non-instantiable type parameter")
	}
	// Allocate a non-nil instance to call any optional WireName() method.
	// For pointer types, `var zero T` is typed nil and would panic on a
	// method call. reflect.New(t.Elem()) produces a fresh *T.
	var probe any
	if t.Kind() == reflect.Pointer {
		probe = reflect.New(t.Elem()).Interface()
	} else {
		probe = zero
	}
	if wn, ok := probe.(WireNamer); ok {
		return hashString(wn.WireName())
	}
	return hashString(t.String())
}

// wireIDOf returns the wire id for a runtime-typed Message. Used on the
// send path. Cached per reflect.Type so the cost is a single sync.Map
// load after the first hash.
func wireIDOf(m Message) uint64 {
	t := reflect.TypeOf(m)
	if cached, ok := wireIDCache.Load(t); ok {
		return cached.(uint64)
	}
	if wn, ok := m.(WireNamer); ok {
		id := hashString(wn.WireName())
		wireIDCache.Store(t, id)
		return id
	}
	id := hashString(t.String())
	wireIDCache.Store(t, id)
	return id
}

var wireIDCache sync.Map // map[reflect.Type]uint64

func hashString(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

// Codec is the public typed codec interface a protocol author
// implements (or supplies via a generic helper like BinaryCodec[M]) to
// encode and decode a concrete Message type.
//
// Implementations must obey the following contract:
//   - Marshal MUST NOT mutate the Message instance it receives.
//   - Unmarshal MUST return a new Message instance of the correct
//     concrete type for the given wire identifier.
type Codec[M Message] interface {
	Marshal(m M) ([]byte, error)
	Unmarshal(data []byte) (M, error)
}

// codec is the type-erased internal form. RegisterCodec[M] adapts a
// Codec[M] into a codec for storage in proto.codecs.
type codec interface {
	marshal(Message) ([]byte, error)
	unmarshal([]byte) (Message, error)
}

// codecAdapter[M] bridges a typed Codec[M] to the runtime's untyped
// internal codec interface.
type codecAdapter[M Message] struct {
	c Codec[M]
}

func (a codecAdapter[M]) marshal(m Message) ([]byte, error) {
	typed, ok := m.(M)
	if !ok {
		return nil, fmt.Errorf("protorun: codec mismatch: expected %T, got %T", *new(M), m)
	}
	return a.c.Marshal(typed)
}

func (a codecAdapter[M]) unmarshal(data []byte) (Message, error) {
	return a.c.Unmarshal(data)
}

// (RegisterCodec[M] and RegisterHandler[M] live in protocol.go alongside
// ProtocolContext: they're free-function workarounds for the lack
// of generic methods on Go interfaces, not codec primitives.)

// BinaryCodec is a Codec[M] that encodes M with encoding/binary using
// little-endian byte order. It works for any M whose fields are
// fixed-size types (boolN/intN/uintN/floatN/complex/fixed arrays/structs
// of same). Variable-length fields (string, slice, int/uint, map,
// interface, pointer) are not supported by encoding/binary and require a
// custom Codec built on the wire helpers.
//
// M must be a pointer to a struct: encoding/binary needs a non-nil
// pointer for both Read and Write. BinaryCodec.Unmarshal allocates a
// fresh value via reflect.New(t.Elem()).
type BinaryCodec[M Message] struct{}

func (BinaryCodec[M]) Marshal(m M) ([]byte, error) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, m); err != nil {
		return nil, fmt.Errorf("BinaryCodec.Marshal: %w", err)
	}
	return buf.Bytes(), nil
}

func (BinaryCodec[M]) Unmarshal(data []byte) (M, error) {
	var zero M
	t := reflect.TypeOf(zero)
	if t == nil || t.Kind() != reflect.Pointer {
		return zero, fmt.Errorf("BinaryCodec[M]: M must be a pointer type, got %T", zero)
	}
	ptr := reflect.New(t.Elem()).Interface().(M)
	if err := binary.Read(bytes.NewReader(data), binary.LittleEndian, ptr); err != nil {
		return zero, fmt.Errorf("BinaryCodec.Unmarshal: %w", err)
	}
	return ptr, nil
}

// SelfMarshaler lets a message type own its wire encoding. A type that
// implements it can be registered with SelfCodec[M] (directly, or
// automatically by Handle) instead of a reflective or hand-written
// codec. The two halves must round-trip: UnmarshalWire(MarshalWire())
// must reconstruct the value.
//
// Implement this when a message needs an encoding WireCodec can't
// express (a compact varint layout, a foreign format, a checksum) but
// you still want the one-call Handle registration.
type SelfMarshaler interface {
	MarshalWire() ([]byte, error)
	UnmarshalWire(data []byte) error
}

// SelfCodec is the Codec[M] that defers to M's own SelfMarshaler. Marshal
// calls the value's MarshalWire; Unmarshal allocates a fresh M (like
// BinaryCodec, via reflect.New) and calls UnmarshalWire on it. M must be
// a pointer to a struct that implements SelfMarshaler.
type SelfCodec[M Message] struct{}

func (SelfCodec[M]) Marshal(m M) ([]byte, error) {
	sm, ok := any(m).(SelfMarshaler)
	if !ok {
		return nil, fmt.Errorf("SelfCodec[M]: %T does not implement SelfMarshaler", m)
	}
	return sm.MarshalWire()
}

func (SelfCodec[M]) Unmarshal(data []byte) (M, error) {
	var zero M
	t := reflect.TypeOf(zero)
	if t == nil || t.Kind() != reflect.Pointer {
		return zero, fmt.Errorf("SelfCodec[M]: M must be a pointer type, got %T", zero)
	}
	ptr := reflect.New(t.Elem()).Interface().(M)
	sm, ok := any(ptr).(SelfMarshaler)
	if !ok {
		return zero, fmt.Errorf("SelfCodec[M]: %T does not implement SelfMarshaler", ptr)
	}
	if err := sm.UnmarshalWire(data); err != nil {
		return zero, fmt.Errorf("SelfCodec.Unmarshal: %w", err)
	}
	return ptr, nil
}
