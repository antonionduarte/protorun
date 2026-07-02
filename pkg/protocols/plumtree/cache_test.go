package plumtree

import (
	"testing"

	"github.com/antonionduarte/protorun/pkg/transport"
)

func mid(seq uint64) MessageID {
	return MessageID{Origin: transport.NewHost(1, "10.0.0.1"), Seq: seq}
}

func TestPayloadCache_PutGet(t *testing.T) {
	c := newPayloadCache(3)
	c.put(mid(1), []byte("a"))
	c.put(mid(2), []byte("b"))
	if got, ok := c.get(mid(1)); !ok || string(got) != "a" {
		t.Fatalf("get(1) = %q ok=%v", got, ok)
	}
	if _, ok := c.get(mid(99)); ok {
		t.Fatalf("get of absent id should miss")
	}
}

func TestPayloadCache_FIFOEviction(t *testing.T) {
	c := newPayloadCache(2)
	c.put(mid(1), []byte("a"))
	c.put(mid(2), []byte("b"))
	c.put(mid(3), []byte("c")) // evicts oldest (1)
	if _, ok := c.get(mid(1)); ok {
		t.Fatalf("id 1 should have been evicted")
	}
	if _, ok := c.get(mid(2)); !ok {
		t.Fatalf("id 2 should still be cached")
	}
	if _, ok := c.get(mid(3)); !ok {
		t.Fatalf("id 3 should be cached")
	}
	if c.len() != 2 {
		t.Fatalf("cache len = %d, want 2", c.len())
	}
}

func TestPayloadCache_DuplicatePutKeepsFirst(t *testing.T) {
	c := newPayloadCache(4)
	c.put(mid(1), []byte("first"))
	c.put(mid(1), []byte("second")) // ignored
	if got, _ := c.get(mid(1)); string(got) != "first" {
		t.Fatalf("duplicate put should keep first copy, got %q", got)
	}
	if c.len() != 1 {
		t.Fatalf("duplicate put should not grow the cache: len=%d", c.len())
	}
}

func TestHostSet_EagerLazyBasics(t *testing.T) {
	s := newHostSet()
	a := transport.NewHost(1, "h")
	b := transport.NewHost(2, "h")
	s.add(a)
	s.add(a)
	if s.len() != 1 || !s.contains(a) {
		t.Fatalf("add/contains broken")
	}
	s.add(b)
	got := s.sorted()
	if len(got) != 2 || got[0].String() > got[1].String() {
		t.Fatalf("sorted broken: %v", got)
	}
	s.remove(a)
	if s.contains(a) || s.len() != 1 {
		t.Fatalf("remove broken")
	}
}
