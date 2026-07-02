package hyparview

import (
	"testing"

	"github.com/antonionduarte/protorun/pkg/protocols/membership"
	"github.com/antonionduarte/protorun/pkg/transport"
)

// selfMarshaler is the interface every HyParView wire message satisfies.
type selfMarshaler interface {
	MarshalWire() ([]byte, error)
	UnmarshalWire([]byte) error
}

func roundTrip[M selfMarshaler](t *testing.T, in M, out M) {
	t.Helper()
	b, err := in.MarshalWire()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := out.UnmarshalWire(b); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
}

func TestMessages_RoundTrip(t *testing.T) {
	a := transport.NewHost(1, "10.0.0.1")
	b := transport.NewHost(2, "2001:db8::1") // IPv6, to exercise host encoding
	c := transport.NewHost(3, "node-c.example")

	t.Run("ForwardJoin", func(t *testing.T) {
		in := &ForwardJoin{NewNode: a, TTL: 6}
		out := &ForwardJoin{}
		roundTrip(t, in, out)
		if out.NewNode != a || out.TTL != 6 {
			t.Fatalf("got %+v", out)
		}
	})

	t.Run("Neighbor", func(t *testing.T) {
		for _, prio := range []bool{true, false} {
			in := &Neighbor{Priority: prio}
			out := &Neighbor{}
			roundTrip(t, in, out)
			if out.Priority != prio {
				t.Fatalf("priority %v -> %v", prio, out.Priority)
			}
		}
	})

	t.Run("NeighborReply", func(t *testing.T) {
		in := &NeighborReply{Accepted: true}
		out := &NeighborReply{}
		roundTrip(t, in, out)
		if !out.Accepted {
			t.Fatalf("accepted lost")
		}
	})

	t.Run("Shuffle", func(t *testing.T) {
		in := &Shuffle{
			Origin:  a,
			TTL:     5,
			Active:  []transport.Host{b, c},
			Passive: []transport.Host{c},
			Path:    []transport.Host{a, b},
		}
		out := &Shuffle{}
		roundTrip(t, in, out)
		if out.Origin != a || out.TTL != 5 {
			t.Fatalf("scalar fields wrong: %+v", out)
		}
		if len(out.Active) != 2 || out.Active[0] != b || out.Active[1] != c {
			t.Fatalf("active list wrong: %v", out.Active)
		}
		if len(out.Passive) != 1 || out.Passive[0] != c {
			t.Fatalf("passive list wrong: %v", out.Passive)
		}
		if len(out.Path) != 2 || out.Path[0] != a || out.Path[1] != b {
			t.Fatalf("path wrong: %v", out.Path)
		}
	})

	t.Run("ShuffleReply", func(t *testing.T) {
		in := &ShuffleReply{Nodes: []transport.Host{a, c}, Route: []transport.Host{a, b, c}}
		out := &ShuffleReply{}
		roundTrip(t, in, out)
		if len(out.Nodes) != 2 || len(out.Route) != 3 {
			t.Fatalf("lists wrong: %+v", out)
		}
	})

	t.Run("empty lists", func(t *testing.T) {
		in := &Shuffle{Origin: a, TTL: 0}
		out := &Shuffle{}
		roundTrip(t, in, out)
		if len(out.Active) != 0 || len(out.Passive) != 0 || len(out.Path) != 0 {
			t.Fatalf("empty lists should stay empty: %+v", out)
		}
	})
}

// TestContractTypesAreLocalIPC documents (and guards) that the membership
// contract types carry no wire encoding: they are IPC-only. If someone
// later makes them SelfMarshaler/codec-bearing, this is the tripwire.
func TestContractTypesAreLocalIPC(t *testing.T) {
	var up any = membership.NeighborUp{}
	var down any = membership.NeighborDown{}
	var view any = membership.View{}
	for _, v := range []any{up, down, view} {
		if _, ok := v.(selfMarshaler); ok {
			t.Fatalf("%T should be IPC-only and must not implement wire marshalling", v)
		}
	}
}
