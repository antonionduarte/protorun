package protorun

import (
	"bytes"
	"context"
	"encoding/binary"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/transport"
)

// countingHandler counts how many records match substr in their
// message, guarded by a mutex since processMessage may be exercised
// from more than one goroutine in principle.
type countingHandler struct {
	mu     sync.Mutex
	substr string
	count  int
}

func (h *countingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *countingHandler) Handle(_ context.Context, r slog.Record) error {
	if strings.Contains(r.Message, h.substr) {
		h.mu.Lock()
		h.count++
		h.mu.Unlock()
	}
	return nil
}
func (h *countingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *countingHandler) WithGroup(string) slog.Handler      { return h }

func (h *countingHandler) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.count
}

// processFrame builds the on-wire bytes for a single application-level
// message: [WireID(uint64 LE)][payload]. Used by the processMessage tests.
func processFrame(t *testing.T, wireID uint64, payload []byte) bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, wireID); err != nil {
		t.Fatalf("write wireID: %v", err)
	}
	buf.Write(payload)
	return buf
}

// TestProcessMessage_DispatchesToHandler verifies that a well-formed frame
// is decoded and pushed into the owning protocol's messageChannel.
func TestProcessMessage_DispatchesToHandler(t *testing.T) {
	rt := New(transport.NewHost(9001, "127.0.0.1"))

	proto := newProtoProtocol(&MockProtocol{}, 0)
	rt.registerProtocol(proto)
	proto.ensureContext()
	RegisterCodec(proto.ctx, localCodec{})

	frame := processFrame(t, WireID[*localMessage](), nil)
	rt.processMessage(frame, transport.NewHost(9999, "127.0.0.1"))

	ev, ok := recvEvent(proto, time.Second)
	if !ok {
		t.Fatalf("expected a message to be dispatched to proto's mailbox")
	}
	if ev.kind != evMessage {
		t.Fatalf("expected a message event, got kind=%v", ev.kind)
	}
	if _, ok := ev.payload.(*localMessage); !ok {
		t.Fatalf("expected *localMessage, got %T", ev.payload)
	}
}

// TestProcessMessage_UnknownWireID ensures that an unknown wire id is
// dropped without panic and without dispatch.
func TestProcessMessage_UnknownWireID(t *testing.T) {
	rt := New(transport.NewHost(0, "127.0.0.1"))
	frame := processFrame(t, 0xdeadbeef, nil)
	rt.processMessage(frame, transport.NewHost(8888, "127.0.0.1"))
}

// TestProcessMessage_UnknownWireIDWarnIsRateLimited proves that
// repeated messages for the same unknown wireID inside
// unknownWireIDWarnWindow log only once, and that a distinct wireID
// (or the same one after the window elapses) logs again.
func TestProcessMessage_UnknownWireIDWarnIsRateLimited(t *testing.T) {
	handler := &countingHandler{substr: "unknown wireID"}
	clock := newManualClock()
	rt := New(transport.NewHost(0, "127.0.0.1"),
		WithClock(clock),
		WithLogger(slog.New(handler)),
	)

	from := transport.NewHost(8888, "127.0.0.1")
	send := func(wireID uint64) {
		frame := processFrame(t, wireID, nil)
		rt.processMessage(frame, from)
	}

	send(0xdeadbeef)
	send(0xdeadbeef)
	send(0xdeadbeef)
	if got := handler.Count(); got != 1 {
		t.Fatalf("warn count after 3 sends of the same wireID within the window = %d, want 1", got)
	}

	// A different wireID is not covered by the first one's rate limit.
	send(0xfeedface)
	if got := handler.Count(); got != 2 {
		t.Fatalf("warn count after a second distinct wireID = %d, want 2", got)
	}

	// Advancing past the window re-arms the first wireID.
	clock.Advance(unknownWireIDWarnWindow + time.Second)
	send(0xdeadbeef)
	if got := handler.Count(); got != 3 {
		t.Fatalf("warn count after the window elapsed = %d, want 3", got)
	}
}

// TestProcessMessage_DecodeError ensures that if the codec returns an error
// the message is not dispatched.
func TestProcessMessage_DecodeError(t *testing.T) {
	rt := New(transport.NewHost(9201, "127.0.0.1"))

	proto := newProtoProtocol(&MockProtocol{}, 0)
	rt.registerProtocol(proto)
	proto.ensureContext()
	RegisterCodec(proto.ctx, failingCodec{})

	frame := processFrame(t, WireID[*failingMessageBM](), nil)
	rt.processMessage(frame, transport.NewHost(9999, "127.0.0.1"))

	if ev, ok := recvEvent(proto, 50*time.Millisecond); ok {
		t.Fatalf("did not expect a message to be dispatched when Decode fails, got %+v", ev)
	}
}

// TestProcessMessage_TruncatedHeader ensures that a buffer that doesn't
// contain enough bytes for the uint64 wire id is handled gracefully.
func TestProcessMessage_TruncatedHeader(t *testing.T) {
	rt := New(transport.NewHost(0, "127.0.0.1"))
	var buf bytes.Buffer
	buf.WriteByte(0x01)
	rt.processMessage(buf, transport.NewHost(9999, "127.0.0.1"))
}
