package plumtree

import (
	"testing"

	"github.com/antonionduarte/protorun/transport"
)

type selfMarshaler interface {
	MarshalWire() ([]byte, error)
	UnmarshalWire([]byte) error
}

func rt[M selfMarshaler](t *testing.T, in, out M) {
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
	origin := transport.NewHost(7, "2001:db8::2")
	id := MessageID{Origin: origin, Seq: 42}

	t.Run("Gossip", func(t *testing.T) {
		in := &Gossip{ID: id, Round: 3, Payload: []byte("payload bytes")}
		out := &Gossip{}
		rt(t, in, out)
		if out.ID != id || out.Round != 3 || string(out.Payload) != "payload bytes" {
			t.Fatalf("got %+v", out)
		}
	})

	t.Run("Gossip empty payload", func(t *testing.T) {
		in := &Gossip{ID: id, Round: 0, Payload: nil}
		out := &Gossip{}
		rt(t, in, out)
		if out.ID != id || len(out.Payload) != 0 {
			t.Fatalf("got %+v", out)
		}
	})

	t.Run("IHave", func(t *testing.T) {
		in := &IHave{Announcements: []announce{
			{ID: id, Round: 1},
			{ID: MessageID{Origin: origin, Seq: 43}, Round: 2},
		}}
		out := &IHave{}
		rt(t, in, out)
		if len(out.Announcements) != 2 {
			t.Fatalf("announcement count = %d", len(out.Announcements))
		}
		if out.Announcements[0].ID != id || out.Announcements[0].Round != 1 {
			t.Fatalf("ann[0] = %+v", out.Announcements[0])
		}
		if out.Announcements[1].ID.Seq != 43 || out.Announcements[1].Round != 2 {
			t.Fatalf("ann[1] = %+v", out.Announcements[1])
		}
	})

	t.Run("IHave empty", func(t *testing.T) {
		in := &IHave{}
		out := &IHave{}
		rt(t, in, out)
		if len(out.Announcements) != 0 {
			t.Fatalf("empty IHave should stay empty: %v", out.Announcements)
		}
	})

	t.Run("Graft", func(t *testing.T) {
		in := &Graft{ID: id, Round: 9}
		out := &Graft{}
		rt(t, in, out)
		if out.ID != id || out.Round != 9 {
			t.Fatalf("got %+v", out)
		}
	})
}

// TestPublicIPCTypesAreLocal guards that the public app-facing surface
// (Broadcast/BroadcastAck/Delivered) stays IPC-only — no wire marshalling.
func TestPublicIPCTypesAreLocal(t *testing.T) {
	for _, v := range []any{Broadcast{}, BroadcastAck{}, Delivered{}} {
		if _, ok := v.(selfMarshaler); ok {
			t.Fatalf("%T is app IPC and must not implement wire marshalling", v)
		}
	}
}
