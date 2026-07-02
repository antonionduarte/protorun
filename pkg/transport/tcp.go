package transport

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"
)

type (
	TCPLayer struct {
		sendChan          chan TCPSendMessage
		connectChan       chan Host
		disconnectChan    chan Host
		outChannel        chan Message
		outEvents         chan Event
		activeConnections map[Host]net.Conn
		mutex             sync.Mutex
		self              Host

		listener   net.Listener
		cancelFunc context.CancelFunc
		ctx        context.Context

		logger *slog.Logger
	}

	TCPSendMessage struct {
		host Host
		msg  Message
	}
)

// maxFrameSize is a safety limit for a single TCP frame payload.
// Frames with a declared length larger than this are treated as protocol errors.
const maxFrameSize uint32 = 16 * 1024 * 1024 // 16MiB

// encodeFrame wraps a payload with a 4-byte big-endian length prefix:
//
//	[Length(uint32 BE) || payload...]
//
// The payload's internal structure belongs to the layers above; see
// docs/wire-format.md for the full envelope.
func encodeFrame(payload []byte) ([]byte, error) {
	// Empty payloads are allowed and emitted as a 0-length frame.
	if len(payload) > int(^uint32(0)) {
		return nil, fmt.Errorf("payload too large: %d bytes", len(payload))
	}

	buf := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(payload)))
	copy(buf[4:], payload)
	return buf, nil
}

// decodeFrames consumes as many complete frames as possible from buf and returns
// a slice of payload byte slices. The consumed bytes are removed from buf.
// If an invalid length is found (e.g. exceeds maxFrameSize), an error is returned.
func decodeFrames(buf *bytes.Buffer) ([][]byte, error) {
	var frames [][]byte
	for {
		// Need at least 4 bytes for the length prefix.
		if buf.Len() < 4 {
			return frames, nil
		}

		header := buf.Bytes()
		length := binary.BigEndian.Uint32(header[:4])
		if length > maxFrameSize {
			return nil, fmt.Errorf("frame length %d exceeds maxFrameSize %d", length, maxFrameSize)
		}

		// Wait until the full frame is available.
		if buf.Len() < int(4+length) {
			return frames, nil
		}

		// Consume header.
		_ = buf.Next(4)

		// Consume payload.
		payload := buf.Next(int(length))
		// Make a copy to decouple from the underlying buffer.
		cpy := make([]byte, len(payload))
		copy(cpy, payload)
		frames = append(frames, cpy)
	}
}

func NewTCPLayer(self Host, ctx context.Context, outBuf int) *TCPLayer {
	ctx, cancel := context.WithCancel(ctx)
	logger := slog.Default().With("component", "transport", "transport", "tcp")
	if outBuf <= 0 {
		outBuf = defaultTransportOutBuffer
	}

	tcpLayer := &TCPLayer{
		outChannel:        make(chan Message, outBuf),
		outEvents:         make(chan Event, outBuf),
		activeConnections: make(map[Host]net.Conn),
		sendChan:          make(chan TCPSendMessage),
		connectChan:       make(chan Host),
		disconnectChan:    make(chan Host),
		self:              self,
		ctx:               ctx,
		cancelFunc:        cancel,
		logger:            logger,
	}

	logger.Info("tcp layer starting", "self", self.String())
	tcpLayer.listen()            // start listening
	go tcpLayer.handler()        // start main event loop
	go tcpLayer.closeOnCtxDone() // ensure resources release on ctx cancel
	return tcpLayer
}

func (t *TCPLayer) Send(msg Message, sendTo Host) {
	t.logger.Debug("tcp send requested", "to", sendTo.String(), "bytes", msg.Msg.Len())
	select {
	case t.sendChan <- TCPSendMessage{sendTo, msg}:
	case <-t.ctx.Done():
	}
}

func (t *TCPLayer) Connect(host Host) {
	t.logger.Debug("tcp connect requested", "to", host.String())
	select {
	case t.connectChan <- host:
	case <-t.ctx.Done():
	}
}

func (t *TCPLayer) Disconnect(host Host) {
	t.logger.Debug("tcp disconnect requested", "host", host.String())
	select {
	case t.disconnectChan <- host:
	case <-t.ctx.Done():
	}
}

func (t *TCPLayer) OutChannel() chan Message {
	return t.outChannel
}

func (t *TCPLayer) OutEvents() chan Event {
	return t.outEvents
}

func (t *TCPLayer) Cancel() {
	t.cancelFunc()
}

// closeOnCtxDone is started by NewTCPLayer to ensure the listener and any
// active connections are closed as soon as the layer's context is cancelled
// (whether via Cancel() or because the parent context was cancelled).
// This makes ctx the single source of truth for liveness.
func (t *TCPLayer) closeOnCtxDone() {
	<-t.ctx.Done()
	if t.listener != nil {
		_ = t.listener.Close()
	}
	t.mutex.Lock()
	for host, conn := range t.activeConnections {
		if conn != nil {
			_ = conn.Close()
		}
		delete(t.activeConnections, host)
	}
	t.mutex.Unlock()
}

func (t *TCPLayer) send(networkMessage Message, sendTo Host) {
	conn, ok := t.getActiveConn(sendTo)
	if !ok || conn == nil {
		// no active connection
		return
	}
	frame, err := encodeFrame(networkMessage.Msg.Bytes())
	if err != nil {
		t.logger.Error("tcp encode frame failed", "host", sendTo.String(), "err", err)
		t.outEvents <- &Failed{host: sendTo}
		// Call the internal disconnect directly: invoking the public
		// Disconnect would try to push onto disconnectChan, which is
		// drained by the handler goroutine that is currently inside this
		// very send() call. That deadlocks until ctx cancellation. The
		// lowercase form takes the mutex and emits the event
		// synchronously.
		t.disconnect(sendTo)
		return
	}

	if _, err := conn.Write(frame); err != nil {
		t.outEvents <- &Failed{host: sendTo}
		t.disconnect(sendTo)
	}
}

func (t *TCPLayer) disconnect(host Host) {
	conn, ok := t.removeActiveConn(host)
	if !ok {
		return
	}
	if conn != nil {
		_ = conn.Close()
	}
	t.outEvents <- &Disconnected{host: host}
}

func (t *TCPLayer) connect(host Host) {
	conn, err := net.Dial("tcp", host.String())
	if err != nil {
		t.logger.Error("tcp connect failed", "host", host.String(), "err", err)
		t.outEvents <- &Failed{host: host}
		return
	}
	t.logger.Info("tcp connect established", "host", host.String())
	t.addActiveConn(conn, host)
	t.outEvents <- &Connected{host: host}

	go t.connectionHandler(conn, host)
}

func (t *TCPLayer) handler() {
	for {
		select {
		case <-t.ctx.Done():
			return
		case host := <-t.disconnectChan:
			t.disconnect(host)
		case host := <-t.connectChan:
			t.connect(host)
		case send := <-t.sendChan:
			t.send(send.msg, send.host)
		}
	}
}

func (t *TCPLayer) listen() {
	ln, err := net.Listen("tcp", t.self.String())
	if err != nil {
		t.logger.Error("tcp listen failed", "self", t.self.String(), "err", err)
		return
	}
	t.listener = ln
	t.logger.Info("tcp listening", "self", t.self.String())
	go t.listenerHandler(ln)
}

func (t *TCPLayer) listenerHandler(listener net.Listener) {
	defer func() { _ = listener.Close() }()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-t.ctx.Done():
				return
			default:
				// log error or continue
				continue
			}
		}
		if conn == nil {
			// Defensive: stdlib's net.Listener.Accept doesn't document
			// (nil, nil) as a possible return, but nothing in the contract
			// forbids it either.
			continue
		}
		remote := conn.RemoteAddr().(*net.TCPAddr)
		host := NewHost(remote.Port, remote.IP.String())

		t.logger.Info("tcp inbound connection accepted", "remote", host.String())

		t.addActiveConn(conn, host)
		t.outEvents <- &Connected{host: host}

		go t.connectionHandler(conn, host)
	}
}

func (t *TCPLayer) connectionHandler(conn net.Conn, host Host) {
	readBuf := make([]byte, 4096)
	recvBuf := bytes.NewBuffer(nil)
	for {
		select {
		case <-t.ctx.Done():
			_ = conn.Close()
			return
		default:
			frames, err := t.readFrames(conn, readBuf, recvBuf, host)
			if err != nil {
				t.disconnect(host)
				return
			}
			for _, frame := range frames {
				// Each frame is the payload upper layers already
				// understand (see docs/wire-format.md).
				data := make([]byte, len(frame))
				copy(data, frame)
				t.outChannel <- Message{
					Host: host,
					Msg:  *bytes.NewBuffer(data),
				}
			}
		}
	}
}

// readFrames performs one round of read+frame-decode against conn. A read
// timeout returns (nil, nil) so the caller can loop and re-check ctx. Any
// other error is returned after logging; the caller should disconnect.
func (t *TCPLayer) readFrames(conn net.Conn, readBuf []byte, recvBuf *bytes.Buffer, host Host) ([][]byte, error) {
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))

	n, err := conn.Read(readBuf)
	if err != nil {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return nil, nil
		}
		t.logger.Error("tcp read error, disconnecting", "host", host.String(), "err", err)
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}

	if _, werr := recvBuf.Write(readBuf[:n]); werr != nil {
		t.logger.Error("tcp recv buffer write failed", "host", host.String(), "err", werr)
		return nil, werr
	}

	frames, derr := decodeFrames(recvBuf)
	if derr != nil {
		t.logger.Error("tcp frame decode failed, disconnecting", "host", host.String(), "err", derr)
		t.outEvents <- &Failed{host: host}
		return nil, derr
	}
	return frames, nil
}

// concurrency-safe
func (t *TCPLayer) addActiveConn(conn net.Conn, host Host) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.activeConnections[host] = conn
}

func (t *TCPLayer) removeActiveConn(host Host) (net.Conn, bool) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	conn, ok := t.activeConnections[host]
	if ok {
		delete(t.activeConnections, host)
	}
	return conn, ok
}

func (t *TCPLayer) getActiveConn(host Host) (net.Conn, bool) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	conn, ok := t.activeConnections[host]
	return conn, ok
}

// activeConnectionCount returns the number of active connections in a
// concurrency-safe way. Intended for use in tests.
func (t *TCPLayer) activeConnectionCount() int {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return len(t.activeConnections)
}
