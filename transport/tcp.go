package transport

import (
	"bytes"
	"context"
	"crypto/tls"
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

		// dial/listen are the pluggable endpoints. They default to plain
		// TCP; WithDialFunc / WithListenFunc / WithTLS replace them so a
		// caller can layer TLS (or any net.Conn/net.Listener) underneath
		// the unchanged framing and handshake above.
		dial   DialFunc
		listen ListenFunc

		listener   net.Listener
		cancelFunc context.CancelFunc
		ctx        context.Context

		logger *slog.Logger
	}

	TCPSendMessage struct {
		host Host
		msg  Message
	}

	// DialFunc opens a connection to addr on the given network. It
	// receives the layer's context so a slow dial is cancelled on
	// shutdown. The default is a net.Dialer; WithTLS swaps in a
	// tls.Dialer.
	DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

	// ListenFunc opens a listener on addr for the given network. The
	// default is net.Listen; WithTLS swaps in tls.Listen.
	ListenFunc func(network, addr string) (net.Listener, error)

	// TCPOption customizes a TCPLayer at construction time.
	TCPOption func(*TCPLayer)
)

// WithDialFunc overrides how outbound connections are opened. Use it to
// wrap connections (TLS, proxying, instrumentation) without forking the
// layer. The framing and session handshake run unchanged on top.
func WithDialFunc(f DialFunc) TCPOption {
	return func(t *TCPLayer) {
		if f != nil {
			t.dial = f
		}
	}
}

// WithListenFunc overrides how the inbound listener is opened, mirroring
// WithDialFunc for the accept side.
func WithListenFunc(f ListenFunc) TCPOption {
	return func(t *TCPLayer) {
		if f != nil {
			t.listen = f
		}
	}
}

// WithTLS is sugar that sets both the dial and listen functions to their
// TLS equivalents (crypto/tls.Dialer and tls.Listen) using cfg. Framing
// and the session handshake are untouched — TLS terminates below them.
//
// What cfg must carry depends on the side:
//
//   - Server (accepts connections): cfg.Certificates (or GetCertificate)
//     must provide the server's certificate + key.
//   - Client (dials out): cfg.RootCAs must trust the server's
//     certificate chain, or cfg.InsecureSkipVerify = true for
//     development against self-signed certs. cfg.ServerName should match
//     the certificate when verification is on.
//   - Mutual TLS: the server additionally sets cfg.ClientAuth (typically
//     tls.RequireAndVerifyClientCert) and cfg.ClientCAs; the client
//     additionally sets cfg.Certificates with its own client cert.
//
// A node that both dials and accepts (the usual peer-to-peer case)
// supplies a cfg covering both roles. See docs/how-to-tls.md.
func WithTLS(cfg *tls.Config) TCPOption {
	return func(t *TCPLayer) {
		if cfg == nil {
			return
		}
		dialer := &tls.Dialer{Config: cfg}
		t.dial = dialer.DialContext
		t.listen = func(network, addr string) (net.Listener, error) {
			return tls.Listen(network, addr, cfg)
		}
	}
}

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

func NewTCPLayer(self Host, ctx context.Context, outBuf int, opts ...TCPOption) *TCPLayer {
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
		dial:              defaultDial,
		listen:            net.Listen,
		ctx:               ctx,
		cancelFunc:        cancel,
		logger:            logger,
	}
	for _, opt := range opts {
		opt(tcpLayer)
	}

	logger.Info("tcp layer starting", "self", self.String())
	tcpLayer.startListen()       // start listening
	go tcpLayer.handler()        // start main event loop
	go tcpLayer.closeOnCtxDone() // ensure resources release on ctx cancel
	return tcpLayer
}

// defaultDial is the plain-TCP dialer used unless WithDialFunc / WithTLS
// override it.
func defaultDial(ctx context.Context, network, addr string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, network, addr)
}

// hostFromAddress narrows an Address to the concrete Host this backend
// speaks. TCP only understands ip:port endpoints; an Address of any
// other kind is a wiring error (a SessionLayer pointed at the wrong
// backend), reported as a Failed event by the caller.
func hostFromAddress(a Address) (Host, bool) {
	switch v := a.(type) {
	case Host:
		return v, true
	case *Host:
		if v != nil {
			return *v, true
		}
	}
	return Host{}, false
}

func (t *TCPLayer) Send(msg Message, sendTo Address) {
	host, ok := hostFromAddress(sendTo)
	if !ok {
		t.logger.Error("tcp send to non-Host address", "addr", sendTo.String())
		t.emitFailed(sendTo)
		return
	}
	t.logger.Debug("tcp send requested", "to", host.String(), "bytes", msg.Msg.Len())
	select {
	case t.sendChan <- TCPSendMessage{host, msg}:
	case <-t.ctx.Done():
	}
}

func (t *TCPLayer) Connect(peer Address) {
	host, ok := hostFromAddress(peer)
	if !ok {
		t.logger.Error("tcp connect to non-Host address", "addr", peer.String())
		t.emitFailed(peer)
		return
	}
	t.logger.Debug("tcp connect requested", "to", host.String())
	select {
	case t.connectChan <- host:
	case <-t.ctx.Done():
	}
}

func (t *TCPLayer) Disconnect(peer Address) {
	host, ok := hostFromAddress(peer)
	if !ok {
		t.logger.Error("tcp disconnect of non-Host address", "addr", peer.String())
		return
	}
	t.logger.Debug("tcp disconnect requested", "host", host.String())
	select {
	case t.disconnectChan <- host:
	case <-t.ctx.Done():
	}
}

// emitFailed pushes a Failed event, bailing out on shutdown so a caller
// can't block on a drained channel.
func (t *TCPLayer) emitFailed(peer Address) {
	select {
	case t.outEvents <- &Failed{peer: peer}:
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
		t.outEvents <- &Failed{peer: sendTo}
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
		t.outEvents <- &Failed{peer: sendTo}
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
	t.outEvents <- &Disconnected{peer: host}
}

func (t *TCPLayer) connect(host Host) {
	conn, err := t.dial(t.ctx, "tcp", host.String())
	if err != nil {
		t.logger.Error("tcp connect failed", "host", host.String(), "err", err)
		t.outEvents <- &Failed{peer: host}
		return
	}
	t.logger.Info("tcp connect established", "host", host.String())
	t.addActiveConn(conn, host)
	t.outEvents <- &Connected{peer: host}

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

func (t *TCPLayer) startListen() {
	ln, err := t.listen("tcp", t.self.String())
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
		host, ok := remoteHost(conn)
		if !ok {
			t.logger.Error("tcp inbound connection with unexpected remote address type", "remote", conn.RemoteAddr())
			_ = conn.Close()
			continue
		}

		t.logger.Info("tcp inbound connection accepted", "remote", host.String())

		t.addActiveConn(conn, host)
		t.outEvents <- &Connected{peer: host}

		go t.connectionHandler(conn, host)
	}
}

// remoteHost extracts the peer Host from a connection's remote address.
// With TLS in play conn is a *tls.Conn whose RemoteAddr still returns the
// underlying *net.TCPAddr, so this works for both plain and TLS conns.
func remoteHost(conn net.Conn) (Host, bool) {
	addr, ok := conn.RemoteAddr().(*net.TCPAddr)
	if !ok {
		return Host{}, false
	}
	return NewHost(addr.Port, addr.IP.String()), true
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
					Peer: host,
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
		t.outEvents <- &Failed{peer: host}
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
