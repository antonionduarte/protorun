package hyparview

import (
	"math/rand/v2"
	"testing"

	"github.com/antonionduarte/protorun/transport"
)

func h(i int) transport.Host { return transport.NewHost(i, "10.0.0.1") }

func TestHostSet_AddRemoveContains(t *testing.T) {
	s := newHostSet()
	if s.len() != 0 {
		t.Fatalf("new set should be empty")
	}
	s.add(h(1))
	s.add(h(1)) // idempotent
	s.add(h(2))
	if s.len() != 2 {
		t.Fatalf("len = %d, want 2", s.len())
	}
	if !s.contains(h(1)) || !s.contains(h(2)) {
		t.Fatalf("missing added members")
	}
	s.remove(h(1))
	if s.contains(h(1)) {
		t.Fatalf("remove did not delete")
	}
	if s.len() != 1 {
		t.Fatalf("len = %d, want 1", s.len())
	}
}

func TestHostSet_SortedIsStable(t *testing.T) {
	s := newHostSet()
	for _, i := range []int{5, 1, 9, 3} {
		s.add(h(i))
	}
	got := s.sorted()
	if len(got) != 4 {
		t.Fatalf("sorted len = %d", len(got))
	}
	// Host string is "10.0.0.1:<port>"; sorted() orders by that string.
	for i := 1; i < len(got); i++ {
		if got[i-1].String() > got[i].String() {
			t.Fatalf("sorted() not ordered: %v", got)
		}
	}
}

func TestHostSet_RandomExceptExcludes(t *testing.T) {
	s := newHostSet()
	s.add(h(1))
	s.add(h(2))
	s.add(h(3))
	rng := rand.New(rand.NewPCG(1, 2))
	for range 50 {
		got, ok := s.randomExcept(rng, h(1), h(2))
		if !ok || got != h(3) {
			t.Fatalf("randomExcept should only ever return h(3), got %v ok=%v", got, ok)
		}
	}
	if _, ok := s.randomExcept(rng, h(1), h(2), h(3)); ok {
		t.Fatalf("randomExcept should report no candidate when all excluded")
	}
}

func TestHostSet_RandomExceptIsSeedDeterministic(t *testing.T) {
	build := func() *hostSet {
		s := newHostSet()
		for i := range 10 {
			s.add(h(i))
		}
		return s
	}
	seq := func(seed uint64) []transport.Host {
		s := build()
		rng := rand.New(rand.NewPCG(seed, seed+1))
		var out []transport.Host
		for range 20 {
			g, _ := s.randomExcept(rng)
			out = append(out, g)
		}
		return out
	}
	a, b := seq(42), seq(42)
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("same seed diverged at %d: %v vs %v", i, a[i], b[i])
		}
	}
}

func TestHostSet_RandomSampleBounds(t *testing.T) {
	s := newHostSet()
	for i := range 5 {
		s.add(h(i))
	}
	rng := rand.New(rand.NewPCG(7, 8))
	got := s.randomSample(rng, 3, h(0))
	if len(got) != 3 {
		t.Fatalf("sample len = %d, want 3", len(got))
	}
	for _, g := range got {
		if g == h(0) {
			t.Fatalf("sample must exclude h(0)")
		}
	}
	// Requesting more than available returns all candidates, no dups.
	all := s.randomSample(rng, 100, h(0))
	if len(all) != 4 {
		t.Fatalf("oversized sample len = %d, want 4", len(all))
	}
	seen := make(map[transport.Host]bool)
	for _, g := range all {
		if seen[g] {
			t.Fatalf("sample contains a duplicate: %v", g)
		}
		seen[g] = true
	}
}
