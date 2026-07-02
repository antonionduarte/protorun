package plumtree

import (
	"sort"

	"github.com/antonionduarte/protorun/pkg/transport"
)

// hostSet is a set of hosts with deterministic (Host-string sorted)
// iteration. Plumtree fans out to its eager and lazy sets by ranging
// them, and the sim's determinism contract requires a stable order (Go
// randomises map iteration), so every fan-out goes through sorted.
type hostSet struct {
	m map[transport.Host]struct{}
}

func newHostSet() *hostSet { return &hostSet{m: make(map[transport.Host]struct{})} }

func (s *hostSet) contains(h transport.Host) bool { _, ok := s.m[h]; return ok }
func (s *hostSet) len() int                       { return len(s.m) }
func (s *hostSet) add(h transport.Host)           { s.m[h] = struct{}{} }
func (s *hostSet) remove(h transport.Host)        { delete(s.m, h) }

func (s *hostSet) sorted() []transport.Host {
	out := make([]transport.Host, 0, len(s.m))
	for h := range s.m {
		out = append(out, h)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}
