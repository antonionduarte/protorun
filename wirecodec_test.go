package protorun

import (
	"reflect"
	"testing"
)

// wireNested is a struct used both by value and behind a pointer to
// exercise nested-struct and pointer-to-struct encoding.
type wireNested struct {
	X uint64
	Y string
}

// wireAll exercises every supported kind in one message, plus the two
// skip rules (unexported field, wire:"-" tag). The two skipped fields
// are platform-sized ints — kinds WireCodec rejects — proving the skip
// happens before the plan compiler ever sees them.
type wireAll struct {
	BaseMessage
	B    bool
	I8   int8
	I16  int16
	I32  int32
	I64  int64
	U8   uint8
	U16  uint16
	U32  uint32
	U64  uint64
	F32  float32
	F64  float64
	S    string
	Raw  []byte
	Ints []int32
	Strs []string
	M    map[string]uint32
	Arr  [3]uint16
	Nest wireNested
	Ptr  *wireNested

	skipUnexported int
	Skipped        int `wire:"-"`
}

func fullWireAll() *wireAll {
	return &wireAll{
		B: true, I8: -8, I16: -1600, I32: -320000, I64: -6400000000,
		U8: 8, U16: 1600, U32: 320000, U64: 6400000000,
		F32: 3.5, F64: -2.25,
		S:    "héllo",
		Raw:  []byte{1, 2, 3, 4},
		Ints: []int32{-1, 0, 1, 2},
		Strs: []string{"a", "bb", "ccc"},
		M:    map[string]uint32{"one": 1, "two": 2, "three": 3},
		Arr:  [3]uint16{10, 20, 30},
		Nest: wireNested{X: 99, Y: "nested"},
		Ptr:  &wireNested{X: 7, Y: "ptr"},

		skipUnexported: 111,
		Skipped:        222,
	}
}

func TestWireCodec_RoundTrip_AllKinds(t *testing.T) {
	c := WireCodec[*wireAll]{}
	original := fullWireAll()

	payload, err := c.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := c.Unmarshal(payload)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	// The skipped fields are not on the wire, so they decode to zero.
	// Compare against a copy with those cleared.
	want := *original
	want.skipUnexported = 0
	want.Skipped = 0
	if !reflect.DeepEqual(*got, want) {
		t.Fatalf("round-trip mismatch:\n got=%+v\nwant=%+v", *got, want)
	}
}

func TestWireCodec_MarshalDoesNotMutate(t *testing.T) {
	c := WireCodec[*wireAll]{}
	original := fullWireAll()
	before := *original
	if _, err := c.Marshal(original); err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !reflect.DeepEqual(*original, before) {
		t.Fatalf("Marshal mutated its argument")
	}
}

func TestWireCodec_MapEncodingDeterministic(t *testing.T) {
	c := WireCodec[*wireAll]{}
	m := fullWireAll()
	first, err := c.Marshal(m)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	// Re-marshalling the same value (map iteration order differs run to
	// run) must yield byte-identical output.
	for range 50 {
		again, err := c.Marshal(m)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		if !reflect.DeepEqual(first, again) {
			t.Fatalf("map encoding is not deterministic")
		}
	}
}

func TestWireCodec_NilPointerRoundTrip(t *testing.T) {
	c := WireCodec[*wireAll]{}
	m := fullWireAll()
	m.Ptr = nil
	payload, err := c.Marshal(m)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := c.Unmarshal(payload)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Ptr != nil {
		t.Fatalf("nil pointer round-trip: got %+v, want nil", got.Ptr)
	}
}

func TestWireCodec_RejectsTrailingBytes(t *testing.T) {
	c := WireCodec[*wireAll]{}
	payload, err := c.Marshal(fullWireAll())
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if _, err := c.Unmarshal(append(payload, 0xFF)); err == nil {
		t.Fatalf("expected error on trailing bytes")
	}
}

func TestWireCodec_RejectsTruncatedInput(t *testing.T) {
	c := WireCodec[*wireAll]{}
	payload, err := c.Marshal(fullWireAll())
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for n := range len(payload) {
		if _, err := c.Unmarshal(payload[:n]); err == nil {
			t.Fatalf("expected error on truncated input of len %d", n)
		}
	}
}

// wireBadInt has a platform-sized int field, which the compiler must
// reject with a message steering the user to a sized type.
type wireBadInt struct {
	BaseMessage
	N int
}

func TestWireCodec_RejectsPlatformInt(t *testing.T) {
	_, err := WireCodec[*wireBadInt]{}.Marshal(&wireBadInt{N: 1})
	if err == nil {
		t.Fatalf("expected error for platform-sized int field")
	}
}

// wireBadChan has an unsupported chan field.
type wireBadChan struct {
	BaseMessage
	C chan int
}

func TestWireCodec_RejectsChan(t *testing.T) {
	if _, err := (WireCodec[*wireBadChan]{}).Marshal(&wireBadChan{}); err == nil {
		t.Fatalf("expected error for chan field")
	}
}

// wireRecursive is a self-referential type: the compiler must terminate
// via the pointer back-reference rather than recursing forever.
type wireRecursive struct {
	BaseMessage
	Label string
	Next  *wireRecursive
}

func TestWireCodec_RecursiveType(t *testing.T) {
	c := WireCodec[*wireRecursive]{}
	original := &wireRecursive{Label: "a", Next: &wireRecursive{Label: "b"}}
	payload, err := c.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := c.Unmarshal(payload)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Label != "a" || got.Next == nil || got.Next.Label != "b" || got.Next.Next != nil {
		t.Fatalf("recursive round-trip mismatch: %+v", got)
	}
}

// buildFuzzMessage packs fuzzed primitives into a wireAll so every kind
// is driven by the corpus. Slices and the map are derived from the byte
// input so the fuzzer can vary their length and contents.
func buildFuzzMessage(b bool, i32 int32, u64 uint64, f64 float64, s string, raw []byte) *wireAll {
	m := &wireAll{
		B: b, I32: i32, U64: u64, F64: f64, S: s, Raw: raw,
		Ints: make([]int32, 0, len(raw)),
		Strs: []string{s},
		M:    map[string]uint32{},
		Arr:  [3]uint16{uint16(u64), uint16(i32), uint16(len(raw))},
		Nest: wireNested{X: u64, Y: s},
	}
	for i, bb := range raw {
		m.Ints = append(m.Ints, int32(bb))
		m.M[s+string(rune('a'+i%26))] = uint32(bb)
	}
	if b {
		m.Ptr = &wireNested{X: u64, Y: s}
	}
	return m
}

// FuzzWireCodec_RoundTrip asserts Marshal is canonical: decoding a
// payload and re-encoding it reproduces the same bytes. Byte-identity
// (rather than value equality) is the stable invariant because empty and
// nil slices are wire-indistinguishable and map order is normalised by
// sorting — Marshal is the canonical form.
func FuzzWireCodec_RoundTrip(f *testing.F) {
	f.Add(true, int32(-5), uint64(42), 3.14, "seed", []byte{1, 2, 3})
	f.Add(false, int32(0), uint64(0), 0.0, "", []byte{})
	f.Add(true, int32(1<<30), ^uint64(0), -1.5, "λ", []byte{0, 255, 128})

	c := WireCodec[*wireAll]{}
	f.Fuzz(func(t *testing.T, b bool, i32 int32, u64 uint64, f64 float64, s string, raw []byte) {
		m := buildFuzzMessage(b, i32, u64, f64, s, raw)
		payload, err := c.Marshal(m)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		decoded, err := c.Unmarshal(payload)
		if err != nil {
			t.Fatalf("Unmarshal of self-produced payload failed: %v", err)
		}
		again, err := c.Marshal(decoded)
		if err != nil {
			t.Fatalf("re-Marshal: %v", err)
		}
		if !reflect.DeepEqual(payload, again) {
			t.Fatalf("Marshal not canonical:\n first=%v\n again=%v", payload, again)
		}
	})
}

// FuzzWireCodec_Unmarshal feeds arbitrary bytes to Unmarshal: it must
// return an error or a value, never panic, on any input.
func FuzzWireCodec_Unmarshal(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	if seed, err := (WireCodec[*wireAll]{}).Marshal(fullWireAll()); err == nil {
		f.Add(seed)
	}

	c := WireCodec[*wireAll]{}
	f.Fuzz(func(_ *testing.T, data []byte) {
		_, _ = c.Unmarshal(data)
	})
}
