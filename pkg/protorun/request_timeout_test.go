package protorun

import (
	"errors"
	"testing"
	"time"

	"github.com/antonionduarte/protorun/pkg/transport"
)

// slowReq / slowRep are an IPC pair whose handler never replies, so the
// requester's SendRequest can only complete by timing out.
type slowReq struct{ BaseRequest }
type slowRep struct{ BaseReply }

// timeoutProtocol registers a request handler that never answers, then
// on Init sends itself a request with no per-call timeout so the
// runtime default applies. The terminal error is reported on result.
type timeoutProtocol struct {
	result chan error
}

func (p *timeoutProtocol) Start(ctx ProtocolContext) {
	RegisterRequestHandler(ctx, func(_ *slowReq, _ Responder[*slowRep]) {
		// Intentionally never replies.
	})
}

func (p *timeoutProtocol) Init(ctx ProtocolContext) {
	SendRequest(ctx, &slowReq{}, func(_ *slowRep, err error) {
		p.result <- err
	})
}

// TestSendRequest_DefaultTimeout verifies that a SendRequest with no
// per-call timeout falls back to the runtime default set via
// WithDefaultRequestTimeout, and that an unanswered request surfaces
// ErrRequestTimeout.
func TestSendRequest_DefaultTimeout(t *testing.T) {
	self := transport.NewHost(0, "127.0.0.1")
	p := &timeoutProtocol{result: make(chan error, 1)}

	rt := New(self, WithDefaultRequestTimeout(50*time.Millisecond))
	_ = registerMockStack(rt, self)
	rt.Register(p)
	if err := rt.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer rt.Cancel()

	select {
	case err := <-p.result:
		if !errors.Is(err, ErrRequestTimeout) {
			t.Fatalf("expected ErrRequestTimeout, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("request did not time out via the configured default")
	}
}
