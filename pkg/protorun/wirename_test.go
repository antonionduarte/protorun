package protorun

import "testing"

// wnPlain has no WireName, so its wire name is the Go type name.
type wnPlain struct{ BaseMessage }

// wnNamed freezes its wire name via WireNamer.
type wnNamed struct{ BaseMessage }

func (*wnNamed) WireName() string { return "test.FrozenName" }

func TestWireNameOfType(t *testing.T) {
	if got := wireNameOfType[*wnPlain](); got != "*protorun.wnPlain" {
		t.Errorf("plain type name = %q, want *protorun.wnPlain", got)
	}
	if got := wireNameOfType[*wnNamed](); got != "test.FrozenName" {
		t.Errorf("named type name = %q, want test.FrozenName", got)
	}
}

func TestWireNameOf_ReverseMapping(t *testing.T) {
	// Unknown ids report false so callers can fall back to hex.
	if name, ok := WireNameOf(0xDEADBEEFCAFEF00D); ok {
		t.Errorf("unregistered id resolved to %q, want not-found", name)
	}

	// Recording the id->name pair (as RegisterCodec does) makes it
	// resolvable, and the resolved name is the string that was hashed.
	id := WireID[*wnNamed]()
	recordWireName(id, wireNameOfType[*wnNamed]())
	name, ok := WireNameOf(id)
	if !ok {
		t.Fatalf("recorded id %#x did not resolve", id)
	}
	if name != "test.FrozenName" {
		t.Errorf("WireNameOf = %q, want test.FrozenName", name)
	}

	// First writer wins: a duplicate registration cannot flip the label.
	recordWireName(id, "test.Different")
	if name, _ := WireNameOf(id); name != "test.FrozenName" {
		t.Errorf("duplicate registration changed label to %q", name)
	}
}
