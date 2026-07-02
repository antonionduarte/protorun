package hyparview

import (
	"math/rand/v2"
	"slices"
	"sort"

	"github.com/antonionduarte/protorun/transport"
)

// hostSet is a set of hosts with deterministic iteration. HyParView
// makes many "pick a random member" and "range over members" decisions,
// and the sim's determinism contract requires those to be reproducible;
// Go randomises map iteration, so every ordered operation here sorts by
// Host string first and only then consults the (seeded) RNG. The set is
// kept small (active ~5, passive ~30), so the per-operation sort is
// negligible.
type hostSet struct {
	m map[transport.Host]struct{}
}

func newHostSet() *hostSet { return &hostSet{m: make(map[transport.Host]struct{})} }

func (s *hostSet) contains(h transport.Host) bool { _, ok := s.m[h]; return ok }
func (s *hostSet) len() int                        { return len(s.m) }
func (s *hostSet) add(h transport.Host)            { s.m[h] = struct{}{} }
func (s *hostSet) remove(h transport.Host)         { delete(s.m, h) }

// sorted returns the members in a stable (Host-string) order — the basis
// for every deterministic random pick and fan-out.
func (s *hostSet) sorted() []transport.Host {
	out := make([]transport.Host, 0, len(s.m))
	for h := range s.m {
		out = append(out, h)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

// randomExcept returns a uniformly-random member other than any host in
// exclude, and whether one existed. Selection is over the sorted members
// so it is reproducible under a seeded RNG.
func (s *hostSet) randomExcept(rng *rand.Rand, exclude ...transport.Host) (transport.Host, bool) {
	cand := filterExcept(s.sorted(), exclude...)
	if len(cand) == 0 {
		return transport.Host{}, false
	}
	return cand[rng.IntN(len(cand))], true
}

// randomSample returns up to n distinct members other than exclude,
// chosen by a seeded partial Fisher-Yates over the sorted members.
func (s *hostSet) randomSample(rng *rand.Rand, n int, exclude ...transport.Host) []transport.Host {
	cand := filterExcept(s.sorted(), exclude...)
	rng.Shuffle(len(cand), func(i, j int) { cand[i], cand[j] = cand[j], cand[i] })
	if n < len(cand) {
		cand = cand[:n]
	}
	return cand
}

// filterExcept returns the hosts not present in exclude, preserving
// order.
func filterExcept(hosts []transport.Host, exclude ...transport.Host) []transport.Host {
	if len(exclude) == 0 {
		return hosts
	}
	out := hosts[:0:0]
	for _, h := range hosts {
		if !slices.Contains(exclude, h) {
			out = append(out, h)
		}
	}
	return out
}
