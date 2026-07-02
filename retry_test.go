package protorun

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/transport"
)

// retryWatcher captures session lifecycle events for an integration test.
type retryWatcher struct {
	mu        sync.Mutex
	connected []transport.Host
	givenUp   []retryGiveUp
}

type retryGiveUp struct {
	host     transport.Host
	attempts int
}

func (w *retryWatcher) Start(_ ProtocolContext)  {}
func (w *retryWatcher) Init(ctx ProtocolContext) {}
func (w *retryWatcher) OnSessionConnected(h transport.Host) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.connected = append(w.connected, h)
}
func (w *retryWatcher) OnSessionDisconnected(_ transport.Host) {}
func (w *retryWatcher) OnSessionGivenUp(h transport.Host, attempts int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.givenUp = append(w.givenUp, retryGiveUp{host: h, attempts: attempts})
}

// retryClient is a tiny protocol that calls ConnectWithRetry on its target
// during Init. Tests then observe the runtime via the retryWatcher.
type retryClient struct {
	target transport.Host
}

func (p *retryClient) Start(_ ProtocolContext) {}
func (p *retryClient) Init(ctx ProtocolContext) {
	ctx.ConnectWithRetry(p.target)
}

// buildRetryRuntime stands up a real runtime (with TCP+session) and
// registers the retryClient for `target`, plus a retryWatcher to observe
// events.
func buildRetryRuntime(t *testing.T, self, target transport.Host, policy RetryPolicy) (*Runtime, *retryWatcher) {
	t.Helper()
	w := &retryWatcher{}
	rt := New(self, WithRetryPolicy(policy))
	ctx := context.Background()
	tcp := transport.NewTCPLayer(self, ctx, 0)
	session := transport.NewSessionLayer(tcp, self, ctx, 0, 0)
	rt.registerNetworkLayer(tcp)
	rt.registerSessionLayer(session)
	rt.Register(&retryClient{target: target})
	rt.Register(w)
	return rt, w
}

// TestRuntime_ConnectRetry_LatePeer brings up a client first (which fails
// to connect immediately), then brings up the peer ~150ms later. The
// retry schedule (Initial=50ms, Multiplier=2, Max=200ms) should land at
// least one attempt during the window when the peer is up, and the
// watcher should observe a SessionConnected.
func TestRuntime_ConnectRetry_LatePeer(t *testing.T) {
	clientHost := transport.NewHost(6310, "127.0.0.1")
	peerHost := transport.NewHost(6311, "127.0.0.1")

	policy := RetryPolicy{Initial: 50 * time.Millisecond, Max: 200 * time.Millisecond, Multiplier: 2}
	rtA, watcher := buildRetryRuntime(t, clientHost, peerHost, policy)
	if err := rtA.start(); err != nil {
		t.Fatalf("rtA.Start: %v", err)
	}
	defer rtA.Cancel()

	// Bring up the peer after a short delay so the first connect attempt
	// fails and a retry has to land successfully.
	time.Sleep(120 * time.Millisecond)

	rtB := New(peerHost)
	ctx := context.Background()
	tcpB := transport.NewTCPLayer(peerHost, ctx, 0)
	sessB := transport.NewSessionLayer(tcpB, peerHost, ctx, 0, 0)
	rtB.registerNetworkLayer(tcpB)
	rtB.registerSessionLayer(sessB)
	rtB.Register(&MockProtocol{})
	if err := rtB.start(); err != nil {
		t.Fatalf("rtB.Start: %v", err)
	}
	defer rtB.Cancel()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		watcher.mu.Lock()
		got := len(watcher.connected)
		watcher.mu.Unlock()
		if got > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("retry never landed a successful SessionConnected")
}

// TestRuntime_ConnectRetry_MaxAttempts retries against a peer that never
// comes online and expects exactly N SessionGivenUp events after MaxAttempts.
func TestRuntime_ConnectRetry_MaxAttempts(t *testing.T) {
	clientHost := transport.NewHost(6320, "127.0.0.1")
	deadHost := transport.NewHost(6321, "127.0.0.1")

	policy := RetryPolicy{
		Initial:     20 * time.Millisecond,
		Max:         50 * time.Millisecond,
		Multiplier:  2,
		MaxAttempts: 3,
	}
	rt, watcher := buildRetryRuntime(t, clientHost, deadHost, policy)
	if err := rt.start(); err != nil {
		t.Fatalf("rt.Start: %v", err)
	}
	defer rt.Cancel()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		watcher.mu.Lock()
		gu := len(watcher.givenUp)
		watcher.mu.Unlock()
		if gu >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	watcher.mu.Lock()
	defer watcher.mu.Unlock()
	if len(watcher.givenUp) != 1 {
		t.Fatalf("expected exactly one SessionGivenUp, got %d (%+v)", len(watcher.givenUp), watcher.givenUp)
	}
	if watcher.givenUp[0].attempts != policy.MaxAttempts {
		t.Errorf("expected give-up at attempt %d, got %d", policy.MaxAttempts, watcher.givenUp[0].attempts)
	}
}

// TestRuntime_ConnectRetry_DisconnectCancels schedules a retry, calls
// Disconnect mid-cycle, and asserts no further attempts (no SessionGivenUp
// despite a finite MaxAttempts) and that the runtime's retry bookkeeping
// is cleared.
func TestRuntime_ConnectRetry_DisconnectCancels(t *testing.T) {
	clientHost := transport.NewHost(6330, "127.0.0.1")
	deadHost := transport.NewHost(6331, "127.0.0.1")

	policy := RetryPolicy{
		Initial:     30 * time.Millisecond,
		Max:         60 * time.Millisecond,
		Multiplier:  2,
		MaxAttempts: 50, // would terminate eventually if retries weren't cancelled
	}

	rt, watcher := buildRetryRuntime(t, clientHost, deadHost, policy)
	if err := rt.start(); err != nil {
		t.Fatalf("rt.Start: %v", err)
	}
	defer rt.Cancel()

	// Let a couple of retries fire.
	time.Sleep(120 * time.Millisecond)

	rt.disconnect(deadHost)

	// connectionRetries entry should be gone after Disconnect.
	rt.retryMu.Lock()
	stillTracked := len(rt.connectionRetries)
	rt.retryMu.Unlock()
	if stillTracked != 0 {
		t.Fatalf("retry state not cleared after Disconnect: %d entries", stillTracked)
	}

	// Wait long enough that the original schedule would have fired
	// MaxAttempts times if it hadn't been cancelled.
	time.Sleep(500 * time.Millisecond)

	watcher.mu.Lock()
	defer watcher.mu.Unlock()
	if len(watcher.givenUp) != 0 {
		t.Fatalf("expected no SessionGivenUp after Disconnect, got %+v", watcher.givenUp)
	}
}
