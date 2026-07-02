package protorun

import (
	"context"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/transport"
)

// TestRuntime_TwoRuntimes_PingPong stands up two independent runtimes on
// localhost ports and asserts that they complete a session handshake and
// exchange at least one application message in each direction.
func TestRuntime_TwoRuntimes_PingPong(t *testing.T) {
	hostA := transport.NewHost(7301, "127.0.0.1")
	hostB := transport.NewHost(7302, "127.0.0.1")

	protoA := &twoSidedProtocol{Peer: hostB}
	protoB := &twoSidedProtocol{Peer: hostA}

	rtA := buildRuntime(t, hostA, protoA)
	rtB := buildRuntime(t, hostB, protoB)

	if err := rtA.start(); err != nil {
		t.Fatalf("rtA.Start: %v", err)
	}
	defer rtA.Cancel()
	if err := rtB.start(); err != nil {
		t.Fatalf("rtB.Start: %v", err)
	}
	defer rtB.Cancel()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if protoA.received.Load() && protoB.received.Load() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for ping-pong message exchange between rtA/rtB")
}

// buildRuntime wires a fresh Runtime + TCPLayer + SessionLayer + the given
// protocol implementation.
func buildRuntime(t *testing.T, self transport.Host, p Protocol) *Runtime {
	t.Helper()
	rt := New(self)
	ctx := context.Background()
	tcp := transport.NewTCPLayer(self, ctx, 0)
	session := transport.NewSessionLayer(tcp, self, ctx, 0, 0)
	rt.registerNetworkLayer(tcp)
	rt.registerSessionLayer(session)
	rt.registerProtocol(newProtoProtocol(p, 0))
	return rt
}

// TestRuntime_LifecycleShutdown starts a runtime, lets it run briefly, then
// calls Cancel and ensures shutdown completes without further events being
// emitted on the local session layer.
func TestRuntime_LifecycleShutdown(t *testing.T) {
	hostA := transport.NewHost(7303, "127.0.0.1")
	hostB := transport.NewHost(7304, "127.0.0.1")

	protoA := &twoSidedProtocol{Peer: hostB}
	protoB := &twoSidedProtocol{Peer: hostA}

	rtA := New(hostA)
	ctxA := context.Background()
	tcpA := transport.NewTCPLayer(hostA, ctxA, 0)
	sessionA := transport.NewSessionLayer(tcpA, hostA, ctxA, 0, 0)
	rtA.registerNetworkLayer(tcpA)
	rtA.registerSessionLayer(sessionA)
	rtA.registerProtocol(newProtoProtocol(protoA, 0))

	rtB := New(hostB)
	ctxB := context.Background()
	tcpB := transport.NewTCPLayer(hostB, ctxB, 0)
	sessionB := transport.NewSessionLayer(tcpB, hostB, ctxB, 0, 0)
	rtB.registerNetworkLayer(tcpB)
	rtB.registerSessionLayer(sessionB)
	rtB.registerProtocol(newProtoProtocol(protoB, 0))

	if err := rtA.start(); err != nil {
		t.Fatalf("rtA.Start: %v", err)
	}
	if err := rtB.start(); err != nil {
		t.Fatalf("rtB.Start: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	rtA.Cancel()
	rtB.Cancel()

	time.Sleep(20 * time.Millisecond)

	select {
	case ev := <-sessionA.OutChannelEvents():
		t.Fatalf("did not expect session event on A after Cancel, got %T", ev)
	case <-time.After(20 * time.Millisecond):
	}
}

// TestRuntime_RapidStartCancel runs many tight Start->short-sleep->Cancel
// cycles with a 1ms periodic timer arming/disarming each iteration. Run
// under -race to catch send-on-closed-channel or goroutine-leak regressions.
func TestRuntime_RapidStartCancel(t *testing.T) {
	const iterations = 50
	for i := range iterations {
		self := transport.NewHost(0, "127.0.0.1")
		rt := New(self)

		mock := NewMockNetworkLayer()
		rt.registerNetworkLayer(mock)
		sess := transport.NewSessionLayer(mock, self, context.Background(), 0, 0)
		rt.registerSessionLayer(sess)

		proto := newProtoProtocol(&MockProtocol{}, 0)
		rt.registerProtocol(proto)

		if err := rt.start(); err != nil {
			t.Fatalf("iteration %d: Start failed: %v", i, err)
		}
		rt.every(proto, time.Millisecond, func() {})
		time.Sleep(5 * time.Millisecond)

		done := make(chan struct{})
		go func() {
			rt.Cancel()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d: Cancel did not return within 2s", i)
		}
	}
}
