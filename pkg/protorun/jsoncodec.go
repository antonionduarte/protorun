package protorun

import (
	"encoding/json"
	"fmt"
	"reflect"
)

// JSONCodec is a Codec[M] that marshals M with encoding/json. It exists
// for development and wire inspection: a JSON payload is trivially
// dumpable and diffable when you are debugging a protocol.
//
// It is NOT a stable wire format. JSON is self-describing and permissive
// (field names on the wire, no fixed field order guarantees across Go
// versions, floats stringified), the opposite of what the framework's
// byte-exact WireID contract wants. Ship BinaryCodec / WireCodec /
// SelfCodec in production; reach for JSONCodec only while iterating.
//
// M must be a pointer to a struct; Unmarshal allocates a fresh value via
// reflect.New like BinaryCodec.
type JSONCodec[M Message] struct{}

func (JSONCodec[M]) Marshal(m M) ([]byte, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("JSONCodec.Marshal: %w", err)
	}
	return b, nil
}

func (JSONCodec[M]) Unmarshal(data []byte) (M, error) {
	var zero M
	t := reflect.TypeOf(zero)
	if t == nil || t.Kind() != reflect.Pointer {
		return zero, fmt.Errorf("JSONCodec[M]: M must be a pointer type, got %T", zero)
	}
	ptr := reflect.New(t.Elem()).Interface().(M)
	if err := json.Unmarshal(data, ptr); err != nil {
		return zero, fmt.Errorf("JSONCodec.Unmarshal: %w", err)
	}
	return ptr, nil
}
