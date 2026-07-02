package protorun

import (
	"context"
	"encoding/binary"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/antonionduarte/protorun/transport"
)

// selfMsg owns its wire encoding via SelfMarshaler, so Handle should
// pick SelfCodec for it.
type selfMsg struct {
	BaseMessage
	N uint32
}

func (m *selfMsg) MarshalWire() ([]byte, error) {
	return binary.LittleEndian.AppendUint32(nil, m.N), nil
}

func (m *selfMsg) UnmarshalWire(data []byte) error {
	if len(data) != 4 {
		return errShortSelf
	}
	m.N = binary.LittleEndian.Uint32(data)
	return nil
}

var errShortSelf = errStr("selfMsg: want 4 bytes")

type errStr string

func (e errStr) Error() string { return string(e) }

func TestSelfCodec_RoundTrip(t *testing.T) {
	c := SelfCodec[*selfMsg]{}
	payload, err := c.Marshal(&selfMsg{N: 0xCAFEBABE})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := c.Unmarshal(payload)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.N != 0xCAFEBABE {
		t.Fatalf("round-trip: got %#x, want 0xCAFEBABE", got.N)
	}
}

// varMsg is a variable-length message with no SelfMarshaler, so Handle
// should route it through WireCodec.
type varMsg struct {
	BaseMessage
	Name string
	Tags []string
}

func TestJSONCodec_RoundTrip(t *testing.T) {
	c := JSONCodec[*varMsg]{}
	payload, err := c.Marshal(&varMsg{Name: "x", Tags: []string{"a", "b"}})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := c.Unmarshal(payload)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Name != "x" || len(got.Tags) != 2 {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

// newTestContext stands up a runtime + protocol far enough to obtain a
// usable ProtocolContext for registration tests.
func newTestContext(t *testing.T, opts ...Option) (*Runtime, ProtocolContext) {
	t.Helper()
	self := transport.NewHost(0, "127.0.0.1")
	rt := New(self, opts...)
	_ = registerMockStack(rt, self)
	t.Cleanup(rt.Cancel)
	proto := newProtoProtocol(&MockProtocol{}, 8)
	rt.registerProtocol(proto)
	proto.ensureContext()
	proto.setPhase(phaseRegistering)
	return rt, proto.ctx
}

// TestHandle_PicksWireCodec verifies Handle registers a working codec +
// handler for a variable-length type (WireCodec path) in one call.
func TestHandle_PicksWireCodec(t *testing.T) {
	rt, ctx := newTestContext(t)
	Handle(ctx, func(_ *varMsg, _ transport.Host) {})

	entry, ok := rt.codecs.Get(WireID[*varMsg]())
	if !ok {
		t.Fatalf("Handle did not register a codec")
	}
	payload, err := entry.codec.marshal(&varMsg{Name: "hi", Tags: []string{"t"}})
	if err != nil {
		t.Fatalf("marshal via registered codec: %v", err)
	}
	if _, err := entry.codec.unmarshal(payload); err != nil {
		t.Fatalf("unmarshal via registered codec: %v", err)
	}
}

// TestHandle_PicksSelfCodec verifies Handle uses SelfCodec when the
// message implements SelfMarshaler.
func TestHandle_PicksSelfCodec(t *testing.T) {
	rt, ctx := newTestContext(t)
	Handle(ctx, func(_ *selfMsg, _ transport.Host) {})

	entry, ok := rt.codecs.Get(WireID[*selfMsg]())
	if !ok {
		t.Fatalf("Handle did not register a codec")
	}
	payload, err := entry.codec.marshal(&selfMsg{N: 5})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// SelfCodec's MarshalWire writes exactly 4 bytes.
	if len(payload) != 4 {
		t.Fatalf("expected SelfCodec (4-byte) payload, got %d bytes", len(payload))
	}
}

// TestHandle_UnsupportedTypePanics verifies Handle fails loudly at
// registration for a WireCodec-incompatible field.
func TestHandle_UnsupportedTypePanics(t *testing.T) {
	_, ctx := newTestContext(t)
	defer func() {
		rec := recover()
		if rec == nil {
			t.Fatalf("expected panic for unsupported field type")
		}
		if s, _ := rec.(string); !strings.Contains(s, "WireCodec cannot encode") {
			t.Fatalf("unexpected panic: %v", rec)
		}
	}()
	Handle(ctx, func(_ *wireBadChan, _ transport.Host) {})
}

// warnRecorder captures Warn-level log records for assertions.
type warnRecorder struct {
	mu   sync.Mutex
	msgs []string
}

func (w *warnRecorder) Enabled(_ context.Context, l slog.Level) bool { return l >= slog.LevelWarn }
func (w *warnRecorder) Handle(_ context.Context, r slog.Record) error {
	w.mu.Lock()
	w.msgs = append(w.msgs, r.Message)
	w.mu.Unlock()
	return nil
}
func (w *warnRecorder) WithAttrs(_ []slog.Attr) slog.Handler { return w }
func (w *warnRecorder) WithGroup(_ string) slog.Handler      { return w }

func (w *warnRecorder) count(substr string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, m := range w.msgs {
		if strings.Contains(m, substr) {
			n++
		}
	}
	return n
}

// noWireName has no WireName(), so strict mode should nudge on codec
// registration.
type noWireName struct {
	BaseMessage
	Seq uint64
}

// hasWireName freezes its wire id, so strict mode must not nudge.
type hasWireName struct {
	BaseMessage
	Seq uint64
}

func (*hasWireName) WireName() string { return "protorun.test/hasWireName" }

// TestStrictWireNameNudge_WarnsForMissingName verifies the strict-mode
// nudge fires for a type without WireName and stays silent for a type
// that has one.
func TestStrictWireNameNudge_WarnsForMissingName(t *testing.T) {
	rec := &warnRecorder{}
	_, ctx := newTestContext(t, WithStrict(true), WithLogger(slog.New(rec)))

	RegisterCodec(ctx, WireCodec[*noWireName]{})
	RegisterCodec(ctx, WireCodec[*hasWireName]{})

	if n := rec.count("no WireName"); n != 1 {
		t.Fatalf("expected one WireName nudge, got %d", n)
	}
}

// TestStrictWireNameNudge_DedupsPerType verifies the nudge is emitted at
// most once per wire id even if the same type is registered repeatedly
// (the double-registration guard normally prevents that, so the dedup is
// exercised directly through the binding).
func TestStrictWireNameNudge_DedupsPerType(t *testing.T) {
	rec := &warnRecorder{}
	_, ctx := newTestContext(t, WithStrict(true), WithLogger(slog.New(rec)))
	b := ctx.binding()

	id := WireID[*noWireName]()
	b.strictWireNameNudge(id, "noWireName", false)
	b.strictWireNameNudge(id, "noWireName", false)
	b.strictWireNameNudge(id, "noWireName", false)

	if n := rec.count("no WireName"); n != 1 {
		t.Fatalf("expected exactly one nudge after repeats, got %d", n)
	}
}

// TestStrictWireNameNudge_OffByDefault verifies no nudge without strict.
func TestStrictWireNameNudge_OffByDefault(t *testing.T) {
	rec := &warnRecorder{}
	_, ctx := newTestContext(t, WithLogger(slog.New(rec)))
	RegisterCodec(ctx, WireCodec[*noWireName]{})
	if n := rec.count("no WireName"); n != 0 {
		t.Fatalf("expected no nudge with strict off, got %d", n)
	}
}
