package protorun

import (
	"context"
	"testing"

	"github.com/antonionduarte/protorun/pkg/transport"
)

func TestRegisterProtocol(t *testing.T) {
	rt := New(transport.NewHost(8080, "127.0.0.1"))

	protoProtocol := newProtoProtocol(&MockProtocol{}, 0)
	rt.registerProtocol(protoProtocol)

	if len(rt.protocols) != 1 || rt.protocols[0] != protoProtocol {
		t.Errorf("Protocol was not registered correctly: got %v", rt.protocols)
	}
}

func TestStartAndCancel(t *testing.T) {
	self := transport.NewHost(0, "127.0.0.1")
	rt := New(self)

	mockNetworkLayer := NewMockNetworkLayer()
	rt.registerNetworkLayer(mockNetworkLayer)
	session := transport.NewSessionLayer(mockNetworkLayer, self, context.Background(), 0, 0)
	rt.registerSessionLayer(session)

	if err := rt.start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	rt.Cancel()

	if !mockNetworkLayer.CancelCalled {
		t.Errorf("Expected Cancel to be called on network layer, but it wasn't")
	}
}

func TestStart_FailsWithoutLayers(t *testing.T) {
	rt := New(transport.NewHost(0, "127.0.0.1"))
	if err := rt.start(); err == nil {
		t.Fatalf("expected Start to fail without network/session layer registered")
	}

	rt.registerNetworkLayer(NewMockNetworkLayer())
	if err := rt.start(); err == nil {
		t.Fatalf("expected Start to fail without session layer registered")
	}
}
