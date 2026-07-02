package transport

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// TestTCPLayer_DialAndListenHooks verifies the WithDialFunc / WithListenFunc
// seams: a layer built with custom dial and listen functions must route its
// connect and accept through them (proving the hooks are wired) while the
// framing and events behave exactly as with the defaults.
func TestTCPLayer_DialAndListenHooks(t *testing.T) {
	var dialed, listened atomic.Bool

	dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
		dialed.Store(true)
		var d net.Dialer
		return d.DialContext(ctx, network, addr)
	}
	listen := func(network, addr string) (net.Listener, error) {
		listened.Store(true)
		return net.Listen(network, addr)
	}

	server := NewHost(7285, "127.0.0.1")
	client := NewHost(7286, "127.0.0.1")

	srv := NewTCPLayer(server, t.Context(), 0, WithListenFunc(listen))
	defer srv.Cancel()
	cli := NewTCPLayer(client, t.Context(), 0, WithDialFunc(dial))
	defer cli.Cancel()

	cli.Connect(server)

	connects := 0
	deadline := time.After(5 * time.Second)
	for connects < 2 {
		select {
		case <-srv.OutEvents():
			connects++
		case <-cli.OutEvents():
			connects++
		case <-deadline:
			t.Fatal("timed out waiting for both sides to connect through the hooks")
		}
	}

	if !dialed.Load() {
		t.Error("WithDialFunc hook was never invoked")
	}
	if !listened.Load() {
		t.Error("WithListenFunc hook was never invoked")
	}
}

// TestEventConstructors covers the exported event constructors used by
// out-of-tree Layer backends (they cannot set the unexported peer field
// through a struct literal).
func TestEventConstructors(t *testing.T) {
	h := NewHost(9001, "127.0.0.1")
	if got := NewConnected(h).Peer(); !got.Equal(h) {
		t.Errorf("NewConnected peer = %v, want %v", got, h)
	}
	if got := NewDisconnected(h).Peer(); !got.Equal(h) {
		t.Errorf("NewDisconnected peer = %v, want %v", got, h)
	}
	if got := NewFailed(h).Peer(); !got.Equal(h) {
		t.Errorf("NewFailed peer = %v, want %v", got, h)
	}
}
