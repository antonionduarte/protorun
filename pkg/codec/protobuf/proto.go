// Package protobuf provides a protorun Codec backed by Protocol Buffers,
// for shops that already have .proto definitions and generated Go types.
//
// It is a nested module (its own go.mod) so the core protorun module can
// stay zero-dependency: only programs that opt into protobuf pull in
// google.golang.org/protobuf.
//
// ProtoCodec is generic over proto.Message. To register it with the
// runtime, the message type must also satisfy protorun.Message (embed
// protorun.BaseMessage) so it is a legal Codec[M]:
//
//	protorun.RegisterCodec[*MyProtoMsg](ctx, protobuf.ProtoCodec[*MyProtoMsg]{})
//
// where *MyProtoMsg is both a generated proto.Message and a
// protorun.Message.
package protobuf

import (
	"fmt"
	"reflect"

	"google.golang.org/protobuf/proto"
)

// ProtoCodec marshals M with proto.Marshal / proto.Unmarshal. M must be a
// pointer to a generated protobuf message; Unmarshal allocates a fresh
// value via reflect.New, the same way protorun's BinaryCodec does.
type ProtoCodec[M proto.Message] struct{}

func (ProtoCodec[M]) Marshal(m M) ([]byte, error) {
	b, err := proto.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("ProtoCodec.Marshal: %w", err)
	}
	return b, nil
}

func (ProtoCodec[M]) Unmarshal(data []byte) (M, error) {
	var zero M
	t := reflect.TypeOf(zero)
	if t == nil || t.Kind() != reflect.Pointer {
		return zero, fmt.Errorf("ProtoCodec[M]: M must be a pointer type, got %T", zero)
	}
	msg := reflect.New(t.Elem()).Interface().(M)
	if err := proto.Unmarshal(data, msg); err != nil {
		return zero, fmt.Errorf("ProtoCodec.Unmarshal: %w", err)
	}
	return msg, nil
}
