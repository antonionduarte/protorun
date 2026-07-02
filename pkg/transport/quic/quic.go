// Package quic is a QUIC transport backend for protorun's transport.Layer
// seam. It mirrors the reference TCP backend one-for-one — same 4-byte
// big-endian length framing, same LayerIdentifier byte, same OutChannel /
// OutEvents / Cancel contract — so the SessionLayer (Hello/Ack handshake)
// and the runtime above it run completely unchanged. The only difference
// is what carries the bytes: one QUIC connection per peer pair with a
// single bidirectional stream, instead of a TCP socket.
//
// QUIC mandates TLS, so NewLayer requires a *tls.Config. For tests and
// local development, DevTLS generates a throwaway self-signed config; it
// is explicitly not for production.
//
// This lives in its own module (github.com/antonionduarte/protorun/
// pkg/transport/quic) because it depends on github.com/quic-go/quic-go; the
// core protorun module stays zero-dependency.
package quic

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

	"github.com/antonionduarte/protorun/pkg/transport"
	quicgo "github.com/quic-go/quic-go"
)

// alpn is the ALPN protocol identifier advertised on every QUIC
// connection. Both peers must agree on it; NewLayer forces it onto the
// supplied tls.Config so a caller can't accidentally negotiate something
// else.
const alpn = "protorun"

// maxFrameSize bounds a single frame payload, matching the TCP backend.
const maxFrameSize uint32 = 16 * 1024 * 1024 // 16MiB

const defaultOutBuffer = 16

// closeCodeShutdown is the QUIC application error code used when tearing a
// connection down on Disconnect or layer shutdown. The value is arbitrary
// (there is no peer contract on it); 0 reads as a clean close.
const closeCodeShutdown quicgo.ApplicationErrorCode = 0

type (
	// Layer is the QUIC implementation of transport.Layer.
	Layer struct {
		self transport.Host

		sendChan       chan sendReq
		connectChan    chan transport.Host
		disconnectChan chan transport.Host
		outChannel     chan transport.Message
		outEvents      chan transport.Event

		mu    sync.Mutex
		conns map[transport.Host]*peerConn

		listener *quicgo.Listener
		tlsConf  *tls.Config
		quicConf *quicgo.Config

		ctx    context.Context
		cancel context.CancelFunc

		logger *slog.Logger
	}

	peerConn struct {
		conn   *quicgo.Conn
		stream *quicgo.Stream
	}

	sendReq struct {
		host transport.Host
		msg  transport.Message
	}

	// Option customizes a Layer at construction time.
	Option func(*Layer)
)

// WithQUICConfig overrides the quic-go Config used for dialing and
// listening (keepalives, idle timeout, etc.). By default a nil Config is
// passed, letting quic-go apply its own defaults.
func WithQUICConfig(c *quicgo.Config) Option {
	return func(l *Layer) { l.quicConf = c }
}

// WithOutBuffer sets the capacity of the OutChannel / OutEvents channels.
func WithOutBuffer(n int) Option {
	return func(l *Layer) {
		if n > 0 {
			l.outChannel = make(chan transport.Message, n)
			l.outEvents = make(chan transport.Event, n)
		}
	}
}

// NewLayer builds a QUIC transport.Layer listening on self and ready to
// dial peers. tlsConf is mandatory (QUIC has no un-encrypted mode); a nil
// config is an error. The config is cloned and its ALPN forced to
// "protorun", so the caller only needs to supply certificates/trust:
//
//   - as a server (accepts dials): Certificates (or GetCertificate);
//   - as a client (dials out): RootCAs trusting the peer, or
//     InsecureSkipVerify for dev;
//   - a peer that does both supplies a config covering both roles.
//
// See DevTLS for a ready-made throwaway config.
func NewLayer(self transport.Host, ctx context.Context, tlsConf *tls.Config, opts ...Option) (*Layer, error) {
	if tlsConf == nil {
		return nil, errors.New("quic.NewLayer: tlsConf is required (QUIC mandates TLS)")
	}
	ctx, cancel := context.WithCancel(ctx)

	tc := tlsConf.Clone()
	tc.NextProtos = []string{alpn}

	l := &Layer{
		self:           self,
		sendChan:       make(chan sendReq),
		connectChan:    make(chan transport.Host),
		disconnectChan: make(chan transport.Host),
		outChannel:     make(chan transport.Message, defaultOutBuffer),
		outEvents:      make(chan transport.Event, defaultOutBuffer),
		conns:          make(map[transport.Host]*peerConn),
		tlsConf:        tc,
		ctx:            ctx,
		cancel:         cancel,
		logger:         slog.Default().With("component", "transport", "transport", "quic"),
	}
	for _, opt := range opts {
		opt(l)
	}

	ln, err := quicgo.ListenAddr(self.String(), l.tlsConf, l.quicConf)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("quic.NewLayer: listen on %s: %w", self.String(), err)
	}
	l.listener = ln
	l.logger.Info("quic layer starting", "self", self.String())

	go l.handler()
	go l.acceptLoop()
	go l.closeOnCtxDone()
	return l, nil
}

// --- transport.Layer surface (Address at the boundary, Host within) ---

func (l *Layer) Connect(peer transport.Address) {
	host, ok := hostFromAddress(peer)
	if !ok {
		l.logger.Error("quic connect to non-Host address", "addr", peer.String())
		l.emitFailed(peer)
		return
	}
	select {
	case l.connectChan <- host:
	case <-l.ctx.Done():
	}
}

func (l *Layer) Disconnect(peer transport.Address) {
	host, ok := hostFromAddress(peer)
	if !ok {
		l.logger.Error("quic disconnect of non-Host address", "addr", peer.String())
		return
	}
	select {
	case l.disconnectChan <- host:
	case <-l.ctx.Done():
	}
}

func (l *Layer) Send(msg transport.Message, sendTo transport.Address) {
	host, ok := hostFromAddress(sendTo)
	if !ok {
		l.logger.Error("quic send to non-Host address", "addr", sendTo.String())
		l.emitFailed(sendTo)
		return
	}
	select {
	case l.sendChan <- sendReq{host: host, msg: msg}:
	case <-l.ctx.Done():
	}
}

func (l *Layer) OutChannel() chan transport.Message { return l.outChannel }
func (l *Layer) OutEvents() chan transport.Event    { return l.outEvents }
func (l *Layer) Cancel()                            { l.cancel() }

// --- internals ---

// hostFromAddress narrows an Address to the Host this backend speaks.
// QUIC, like TCP, addresses peers by ip:port.
func hostFromAddress(a transport.Address) (transport.Host, bool) {
	switch v := a.(type) {
	case transport.Host:
		return v, true
	case *transport.Host:
		if v != nil {
			return *v, true
		}
	}
	return transport.Host{}, false
}

func remoteHost(conn *quicgo.Conn) (transport.Host, bool) {
	addr, ok := conn.RemoteAddr().(*net.UDPAddr)
	if !ok {
		return transport.Host{}, false
	}
	return transport.NewHost(addr.Port, addr.IP.String()), true
}

func (l *Layer) emitFailed(peer transport.Address) {
	select {
	case l.outEvents <- transport.NewFailed(peer):
	case <-l.ctx.Done():
	}
}

// handler serializes connect/disconnect/send requests onto one goroutine,
// mirroring the TCP backend's event loop.
func (l *Layer) handler() {
	for {
		select {
		case <-l.ctx.Done():
			return
		case host := <-l.connectChan:
			l.connect(host)
		case host := <-l.disconnectChan:
			l.disconnect(host)
		case req := <-l.sendChan:
			l.send(req.host, req.msg)
		}
	}
}

func (l *Layer) connect(host transport.Host) {
	conn, err := quicgo.DialAddr(l.ctx, host.String(), l.tlsConf, l.quicConf)
	if err != nil {
		l.logger.Error("quic connect failed", "host", host.String(), "err", err)
		l.emitPeerFailed(host)
		return
	}
	// One bidirectional stream per connection carries every frame. It is
	// opened eagerly; the peer's AcceptStream unblocks once we send our
	// first frame (the SessionLayer Hello).
	stream, err := conn.OpenStreamSync(l.ctx)
	if err != nil {
		l.logger.Error("quic open stream failed", "host", host.String(), "err", err)
		_ = conn.CloseWithError(closeCodeShutdown, "open stream failed")
		l.emitPeerFailed(host)
		return
	}
	l.addConn(host, conn, stream)
	l.logger.Info("quic connect established", "host", host.String())
	l.emitConnected(host)
	go l.readPump(conn, stream, host)
}

func (l *Layer) acceptLoop() {
	for {
		conn, err := l.listener.Accept(l.ctx)
		if err != nil {
			select {
			case <-l.ctx.Done():
				return
			default:
				continue
			}
		}
		go l.handleInbound(conn)
	}
}

func (l *Layer) handleInbound(conn *quicgo.Conn) {
	// Block until the dialer opens its bidi stream (i.e. sends the Hello).
	stream, err := conn.AcceptStream(l.ctx)
	if err != nil {
		_ = conn.CloseWithError(closeCodeShutdown, "accept stream failed")
		return
	}
	host, ok := remoteHost(conn)
	if !ok {
		l.logger.Error("quic inbound connection with unexpected remote address", "remote", conn.RemoteAddr())
		_ = conn.CloseWithError(closeCodeShutdown, "bad remote addr")
		return
	}
	l.addConn(host, conn, stream)
	l.logger.Info("quic inbound connection accepted", "remote", host.String())
	l.emitConnected(host)
	l.readPump(conn, stream, host)
}

// emitConnected pushes a Connected carrying host, bailing on shutdown.
func (l *Layer) emitConnected(host transport.Host) {
	select {
	case l.outEvents <- transport.NewConnected(host):
	case <-l.ctx.Done():
	}
}

func (l *Layer) send(host transport.Host, msg transport.Message) {
	pc, ok := l.getConn(host)
	if !ok || pc.stream == nil {
		return
	}
	frame, err := encodeFrame(msg.Msg.Bytes())
	if err != nil {
		l.logger.Error("quic encode frame failed", "host", host.String(), "err", err)
		l.emitPeerFailed(host)
		l.disconnect(host)
		return
	}
	if _, err := pc.stream.Write(frame); err != nil {
		l.logger.Error("quic stream write failed", "host", host.String(), "err", err)
		l.emitPeerFailed(host)
		l.disconnect(host)
	}
}

func (l *Layer) disconnect(host transport.Host) {
	pc, ok := l.removeConn(host)
	if !ok {
		return
	}
	if pc.stream != nil {
		_ = pc.stream.Close()
	}
	if pc.conn != nil {
		_ = pc.conn.CloseWithError(closeCodeShutdown, "disconnect")
	}
	l.emitDisconnected(host)
}

func (l *Layer) readPump(conn *quicgo.Conn, stream *quicgo.Stream, host transport.Host) {
	readBuf := make([]byte, 4096)
	recvBuf := bytes.NewBuffer(nil)
	for {
		select {
		case <-l.ctx.Done():
			_ = stream.Close()
			_ = conn.CloseWithError(closeCodeShutdown, "shutdown")
			return
		default:
			frames, err := l.readFrames(stream, readBuf, recvBuf, host)
			if err != nil {
				l.disconnect(host)
				return
			}
			for _, frame := range frames {
				data := make([]byte, len(frame))
				copy(data, frame)
				select {
				case l.outChannel <- transport.Message{Peer: host, Msg: *bytes.NewBuffer(data)}:
				case <-l.ctx.Done():
					return
				}
			}
		}
	}
}

// readFrames does one read+decode round against the stream. A read
// timeout returns (nil, nil) so the caller re-checks ctx; any other error
// is returned for the caller to disconnect on.
func (l *Layer) readFrames(
	stream *quicgo.Stream, readBuf []byte, recvBuf *bytes.Buffer, host transport.Host,
) ([][]byte, error) {
	_ = stream.SetReadDeadline(time.Now().Add(time.Second))

	n, err := stream.Read(readBuf)
	if n > 0 {
		if _, werr := recvBuf.Write(readBuf[:n]); werr != nil {
			return nil, werr
		}
		frames, derr := decodeFrames(recvBuf)
		if derr != nil {
			l.logger.Error("quic frame decode failed", "host", host.String(), "err", derr)
			l.emitPeerFailed(host)
			return nil, derr
		}
		if len(frames) > 0 {
			return frames, nil
		}
	}
	if err != nil {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return nil, nil
		}
		return nil, err
	}
	return nil, nil
}

func (l *Layer) closeOnCtxDone() {
	<-l.ctx.Done()
	if l.listener != nil {
		_ = l.listener.Close()
	}
	l.mu.Lock()
	for host, pc := range l.conns {
		if pc.stream != nil {
			_ = pc.stream.Close()
		}
		if pc.conn != nil {
			_ = pc.conn.CloseWithError(closeCodeShutdown, "shutdown")
		}
		delete(l.conns, host)
	}
	l.mu.Unlock()
}

func (l *Layer) emitPeerFailed(host transport.Host) {
	select {
	case l.outEvents <- transport.NewFailed(host):
	case <-l.ctx.Done():
	}
}

func (l *Layer) emitDisconnected(host transport.Host) {
	select {
	case l.outEvents <- transport.NewDisconnected(host):
	case <-l.ctx.Done():
	}
}

// --- connection map (concurrency-safe) ---

func (l *Layer) addConn(host transport.Host, conn *quicgo.Conn, stream *quicgo.Stream) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.conns[host] = &peerConn{conn: conn, stream: stream}
}

func (l *Layer) removeConn(host transport.Host) (*peerConn, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	pc, ok := l.conns[host]
	if ok {
		delete(l.conns, host)
	}
	return pc, ok
}

func (l *Layer) getConn(host transport.Host) (*peerConn, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	pc, ok := l.conns[host]
	return pc, ok
}

// --- framing (identical to the TCP backend, reimplemented here to keep
// the core package's copy unexported) ---

func encodeFrame(payload []byte) ([]byte, error) {
	if len(payload) > int(^uint32(0)) {
		return nil, fmt.Errorf("payload too large: %d bytes", len(payload))
	}
	buf := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(payload)))
	copy(buf[4:], payload)
	return buf, nil
}

func decodeFrames(buf *bytes.Buffer) ([][]byte, error) {
	var frames [][]byte
	for {
		if buf.Len() < 4 {
			return frames, nil
		}
		length := binary.BigEndian.Uint32(buf.Bytes()[:4])
		if length > maxFrameSize {
			return nil, fmt.Errorf("frame length %d exceeds maxFrameSize %d", length, maxFrameSize)
		}
		if buf.Len() < int(4+length) {
			return frames, nil
		}
		_ = buf.Next(4)
		payload := buf.Next(int(length))
		cpy := make([]byte, len(payload))
		copy(cpy, payload)
		frames = append(frames, cpy)
	}
}
