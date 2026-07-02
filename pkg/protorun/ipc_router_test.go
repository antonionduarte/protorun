package protorun

import (
	"sync"
	"testing"
)

// TestCodecRegistry_SetGet verifies the basic happy path: store +
// retrieve a codec route by wireID.
func TestCodecRegistry_SetGet(t *testing.T) {
	r := newCodecRegistry()
	proto := &protoProtocol{}

	if _, ok := r.Get(42); ok {
		t.Fatalf("Get on empty registry should miss")
	}

	r.Set(42, proto, codecAdapter[*localMessage]{c: localCodec{}})
	got, ok := r.Get(42)
	if !ok || got.proto != proto {
		t.Fatalf("Get after Set: ok=%v got=%v want=%v", ok, got.proto, proto)
	}
	if got.codec == nil {
		t.Fatalf("expected the codec to be registered alongside the protocol")
	}
}

// TestCodecRegistry_OverwriteWins verifies a second Set replaces the
// first. Last-writer-wins per the documented contract.
func TestCodecRegistry_OverwriteWins(t *testing.T) {
	r := newCodecRegistry()
	first, second := &protoProtocol{}, &protoProtocol{}
	r.Set(1, first, codecAdapter[*localMessage]{c: localCodec{}})
	r.Set(1, second, codecAdapter[*localMessage]{c: localCodec{}})
	if got, _ := r.Get(1); got.proto != second {
		t.Fatalf("expected second writer to win, got %v", got.proto)
	}
}

// TestIPCRouter_RegisterRouteAndQuery verifies the request-side happy
// path: register a route, look it up, get the same proto back.
func TestIPCRouter_RegisterRouteAndQuery(t *testing.T) {
	r := newIPCRouter()
	proto := &protoProtocol{}
	handler := func(_ Request, _ replyToken) {}

	prev, hadPrev := r.RegisterRequestRoute(7, proto, handler)
	if hadPrev {
		t.Fatalf("expected no previous route, got %+v", prev)
	}

	got, ok := r.Route(7)
	if !ok || got.proto != proto {
		t.Fatalf("Route after register: ok=%v proto=%v", ok, got.proto)
	}
}

// TestIPCRouter_ReregistrationReportsPrevious verifies the second
// RegisterRequestRoute returns the previous owner so the caller can
// log / panic on conflict.
func TestIPCRouter_ReregistrationReportsPrevious(t *testing.T) {
	r := newIPCRouter()
	a, b := &protoProtocol{}, &protoProtocol{}
	r.RegisterRequestRoute(1, a, func(_ Request, _ replyToken) {})
	prev, hadPrev := r.RegisterRequestRoute(1, b, func(_ Request, _ replyToken) {})
	if !hadPrev {
		t.Fatalf("expected hadPrev=true on re-registration")
	}
	if prev.proto != a {
		t.Fatalf("expected prev.proto=a (%v), got %v", a, prev.proto)
	}
}

// TestIPCRouter_SubscribeAndSnapshot verifies Subscribe accumulates
// fan-out subscribers and SnapshotSubscribers returns all of them.
func TestIPCRouter_SubscribeAndSnapshot(t *testing.T) {
	r := newIPCRouter()
	a, b, c := &protoProtocol{}, &protoProtocol{}, &protoProtocol{}
	noop := func(_ Notification) {}

	r.Subscribe(99, a, noop)
	r.Subscribe(99, b, noop)
	r.Subscribe(99, c, noop)

	subs := r.SnapshotSubscribers(99)
	if len(subs) != 3 {
		t.Fatalf("expected 3 subscribers, got %d", len(subs))
	}
}

// TestIPCRouter_Unsubscribe verifies Unsubscribe drops the protocol
// from the fan-out and cleans up the empty bucket.
func TestIPCRouter_Unsubscribe(t *testing.T) {
	r := newIPCRouter()
	a, b := &protoProtocol{}, &protoProtocol{}
	noop := func(_ Notification) {}

	r.Subscribe(5, a, noop)
	r.Subscribe(5, b, noop)
	r.Unsubscribe(5, a)

	subs := r.SnapshotSubscribers(5)
	if len(subs) != 1 || subs[0].proto != b {
		t.Fatalf("expected only b after unsubscribe, got %+v", subs)
	}

	r.Unsubscribe(5, b)
	if len(r.SnapshotSubscribers(5)) != 0 {
		t.Fatalf("expected empty fanout after final unsubscribe")
	}
}

// TestIPCRouter_ConcurrentSubscribePublish stresses the read/write
// mutex with concurrent subscribers and publishers. Failure mode the
// race detector catches: any data race in the fanout map.
func TestIPCRouter_ConcurrentSubscribePublish(t *testing.T) {
	r := newIPCRouter()
	const goroutines = 100
	const ops = 100
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for range goroutines {
		go func() {
			defer wg.Done()
			p := &protoProtocol{}
			noop := func(_ Notification) {}
			for range ops {
				r.Subscribe(1, p, noop)
				r.Unsubscribe(1, p)
			}
		}()
		go func() {
			defer wg.Done()
			for range ops {
				_ = r.SnapshotSubscribers(1)
			}
		}()
	}
	wg.Wait()
}
