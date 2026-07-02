// Package gossip implements an eager-push gossip protocol on top of
// the membership protocol.
package gossip

import (
	"bytes"
	"fmt"

	"github.com/antonionduarte/protorun"
	"github.com/antonionduarte/protorun/wire"
)

// Message is a single gossip envelope: an ID for deduplication, plus
// an opaque payload. Forwarded verbatim through the network until
// every node has seen it once.
type Message struct {
	protorun.BaseMessage
	ID      uint64
	Payload []byte
}

// Codec serializes Message as [ID(uint64 LE) || PayloadLen(uint32 LE) || Payload].
type Codec struct{}

func (Codec) Marshal(m *Message) ([]byte, error) {
	var buf bytes.Buffer
	if err := wire.WriteUint64(&buf, m.ID); err != nil {
		return nil, fmt.Errorf("gossip codec: write id: %w", err)
	}
	if err := wire.WriteBytes(&buf, m.Payload); err != nil {
		return nil, fmt.Errorf("gossip codec: write payload: %w", err)
	}
	return buf.Bytes(), nil
}

func (Codec) Unmarshal(data []byte) (*Message, error) {
	r := bytes.NewReader(data)
	id, err := wire.ReadUint64(r)
	if err != nil {
		return nil, fmt.Errorf("gossip codec: read id: %w", err)
	}
	payload, err := wire.ReadBytes(r)
	if err != nil {
		return nil, fmt.Errorf("gossip codec: read payload: %w", err)
	}
	return &Message{ID: id, Payload: payload}, nil
}
