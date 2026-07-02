package transport

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestTCPLayerConnection(t *testing.T) {
	first := NewHost(6503, "127.0.0.1")
	second := NewHost(6504, "127.0.0.1")

	firstCtx := context.Background()
	secondCtx := context.Background()

	firstCtx, firstCancel := context.WithCancel(firstCtx)
	secondCtx, secondCancel := context.WithCancel(secondCtx)

	defer firstCancel()
	defer secondCancel()

	firstNode := NewTCPLayer(first, firstCtx, 0)
	secondNode := NewTCPLayer(second, secondCtx, 0)

	firstNode.Connect(second)

	connectCount := 0
	for connectCount < 2 {
		select {
		case <-secondNode.OutEvents():
			connectCount++
		case <-firstNode.OutEvents():
			connectCount++
		}
	}

	if secondNode.activeConnectionCount() != 1 {
		t.Errorf("TCPConnection on secondNode should've been established.")
	}

	if firstNode.activeConnectionCount() != 1 {
		t.Errorf("TCPConnection on firstNode should've been established.")
	}
}

func TestTCPLayerSendMessage(t *testing.T) {
	first := NewHost(6601, "127.0.0.1")
	second := NewHost(6602, "127.0.0.1")

	firstCtx := context.Background()
	secondCtx := context.Background()

	firstCtx, firstCancel := context.WithCancel(firstCtx)
	secondCtx, secondCancel := context.WithCancel(secondCtx)

	defer firstCancel()
	defer secondCancel()

	firstNode := NewTCPLayer(first, firstCtx, 0)
	secondNode := NewTCPLayer(second, secondCtx, 0)

	firstNode.Connect(second)

	connectCount := 0
	for connectCount < 2 {
		select {
		case <-secondNode.OutEvents():
			connectCount++
		case <-firstNode.OutEvents():
			connectCount++
		}
	}

	msg := NewMessage(*bytes.NewBuffer([]byte("Test message")), firstNode.self)
	firstNode.send(msg, secondNode.self)

	receivedMsg := <-secondNode.OutChannel()

	if receivedMsg.Msg.String() != "Test message" {
		t.Errorf("SecondNode received incorrect message.")
	}

	var clientHost Host
	for key := range secondNode.activeConnections {
		clientHost = key
		break
	}

	secondNode.send(msg, clientHost)

	receivedMsg = <-firstNode.OutChannel()
	if receivedMsg.Msg.String() != "Test message" {
		t.Errorf("FirstNode received incorrect message.")
	}
}

// TestTCPLayerFramingMultipleMessages ensures that multiple messages sent back-to-back
// are correctly framed and delivered as distinct Messages on the receiver.
func TestTCPLayerFramingMultipleMessages(t *testing.T) {
	first := NewHost(6603, "127.0.0.1")
	second := NewHost(6604, "127.0.0.1")

	firstCtx, firstCancel := context.WithCancel(context.Background())
	secondCtx, secondCancel := context.WithCancel(context.Background())
	defer firstCancel()
	defer secondCancel()

	firstNode := NewTCPLayer(first, firstCtx, 0)
	secondNode := NewTCPLayer(second, secondCtx, 0)

	firstNode.Connect(second)

	connectCount := 0
	for connectCount < 2 {
		select {
		case <-secondNode.OutEvents():
			connectCount++
		case <-firstNode.OutEvents():
			connectCount++
		}
	}

	msg1 := NewMessage(*bytes.NewBuffer([]byte("msg1")), firstNode.self)
	msg2 := NewMessage(*bytes.NewBuffer([]byte("msg2")), firstNode.self)

	firstNode.send(msg1, secondNode.self)
	firstNode.send(msg2, secondNode.self)

	rcv1 := <-secondNode.OutChannel()
	rcv2 := <-secondNode.OutChannel()

	if got := rcv1.Msg.String(); got != "msg1" {
		t.Fatalf("first received message mismatch: got %q, want %q", got, "msg1")
	}
	if got := rcv2.Msg.String(); got != "msg2" {
		t.Fatalf("second received message mismatch: got %q, want %q", got, "msg2")
	}
}

func TestDisconnect(t *testing.T) {
	first := NewHost(6913, "127.0.0.1")
	second := NewHost(6912, "127.0.0.1")

	firstCtx := context.Background()
	secondCtx := context.Background()

	firstCtx, firstCancel := context.WithCancel(firstCtx)
	secondCtx, secondCancel := context.WithCancel(secondCtx)

	defer firstCancel()
	defer secondCancel()

	firstNode := NewTCPLayer(first, firstCtx, 0)
	secondNode := NewTCPLayer(second, secondCtx, 0)

	firstNode.Connect(second)

	connectCount := 0
	for connectCount < 2 {
		select {
		case <-secondNode.OutEvents():
			connectCount++
		case <-firstNode.OutEvents():
			connectCount++
		}
	}

	firstNode.Disconnect(second)

	disconnectCount := 0
	for disconnectCount < 2 {
		select {
		case <-secondNode.OutEvents():
			disconnectCount++
		case <-firstNode.OutEvents():
			disconnectCount++
		}
	}

	if firstNode.activeConnectionCount() != 0 {
		t.Errorf("TCPConnection on firstNode should've been deleted.")
	}

	if secondNode.activeConnectionCount() != 0 {
		t.Errorf("TCPConnection on secondNode should've been deleted.")
	}
}

// TestTCPLayerCancelClosesConnections verifies that Cancel() on TCPLayer
// closes all active connections and leaves no connections tracked.
func TestTCPLayerCancelClosesConnections(t *testing.T) {
	first := NewHost(6605, "127.0.0.1")
	second := NewHost(6606, "127.0.0.1")

	ctx1 := t.Context()
	ctx2 := t.Context()

	firstNode := NewTCPLayer(first, ctx1, 0)
	secondNode := NewTCPLayer(second, ctx2, 0)

	firstNode.Connect(second)

	connectCount := 0
	for connectCount < 2 {
		select {
		case <-secondNode.OutEvents():
			connectCount++
		case <-firstNode.OutEvents():
			connectCount++
		}
	}

	// Now cancel both layers and ensure there are no active connections.
	firstNode.Cancel()
	secondNode.Cancel()

	// Give some time for handlers to observe ctx cancellation.
	time.Sleep(10 * time.Millisecond)

	if got := firstNode.activeConnectionCount(); got != 0 {
		t.Fatalf("expected firstNode to have 0 active connections after Cancel, got %d", got)
	}
	if got := secondNode.activeConnectionCount(); got != 0 {
		t.Fatalf("expected secondNode to have 0 active connections after Cancel, got %d", got)
	}
}

// TestTCPLayer_SendAfterCancel ensures that Send/Connect/Disconnect calls
// arriving after Cancel do not panic or block indefinitely. Pre-fix, Cancel
// closed the request channels and any in-flight Send would either panic on
// send-to-closed-channel or deadlock if the goroutine already exited.
func TestTCPLayer_SendAfterCancel(t *testing.T) {
	self := NewHost(6607, "127.0.0.1")
	node := NewTCPLayer(self, context.Background(), 0)
	node.Cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		msg := NewMessage(*bytes.NewBuffer([]byte("x")), self)
		node.Send(msg, NewHost(1, "127.0.0.1"))
		node.Connect(NewHost(2, "127.0.0.1"))
		node.Disconnect(NewHost(3, "127.0.0.1"))
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("Send/Connect/Disconnect after Cancel blocked")
	}
}
