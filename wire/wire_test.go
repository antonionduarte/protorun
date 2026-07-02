package wire

import (
	"bytes"
	"strings"
	"testing"
)

func TestWire_StringRoundTrip(t *testing.T) {
	cases := []string{"", "hello", strings.Repeat("x", 4096)}
	for _, want := range cases {
		var buf bytes.Buffer
		if err := WriteString(&buf, want); err != nil {
			t.Fatalf("WriteString(%q): %v", want, err)
		}
		got, err := ReadString(&buf)
		if err != nil {
			t.Fatalf("ReadString: %v", err)
		}
		if got != want {
			t.Fatalf("round-trip: got %q, want %q", got, want)
		}
	}
}

func TestWire_BytesRoundTrip_Empty(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteBytes(&buf, nil); err != nil {
		t.Fatalf("WriteBytes(nil): %v", err)
	}
	got, err := ReadBytes(&buf)
	if err != nil {
		t.Fatalf("ReadBytes: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty, got %v", got)
	}
}

func TestWire_UintRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteUint64(&buf, 0xdeadbeefcafebabe); err != nil {
		t.Fatal(err)
	}
	got, err := ReadUint64(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got != 0xdeadbeefcafebabe {
		t.Fatalf("uint64 round-trip: got %#x", got)
	}
}
