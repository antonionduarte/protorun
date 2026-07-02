package protorun

import "testing"

// TestWireID_Stable verifies that WireID[*localMessage]() returns the same
// uint64 across calls and matches wireIDOf for a runtime-typed instance.
func TestWireID_Stable(t *testing.T) {
	a := WireID[*localMessage]()
	b := WireID[*localMessage]()
	if a != b {
		t.Fatalf("WireID not stable: %d vs %d", a, b)
	}
	c := wireIDOf(&localMessage{})
	if a != c {
		t.Fatalf("WireID and wireIDOf disagree: %d vs %d", a, c)
	}
}

// namedMessage implements WireNamer; its wire id should hash the override
// string instead of the Go type name.
type namedMessage struct {
	BaseMessage
}

func (namedMessage) WireName() string { return "test.namedMessage.v1" }

func TestWireID_OverrideViaWireNamer(t *testing.T) {
	got := WireID[*namedMessage]()
	want := hashString("test.namedMessage.v1")
	if got != want {
		t.Fatalf("WireNamer override not honored: got %#x, want %#x", got, want)
	}
	if got2 := wireIDOf(&namedMessage{}); got2 != want {
		t.Fatalf("wireIDOf override not honored: got %#x, want %#x", got2, want)
	}
}

// Sanity check: distinct types hash to distinct ids.
func TestWireID_DistinctTypes(t *testing.T) {
	if WireID[*localMessage]() == WireID[*failingMessageBM]() {
		t.Fatalf("expected distinct WireIDs for distinct types")
	}
}

var _ Message = (*namedMessage)(nil)
