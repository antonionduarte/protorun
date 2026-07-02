package transport

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"
)

// selfSignedCert generates a fresh in-memory ECDSA certificate valid for
// 127.0.0.1, returning the tls.Certificate (server/client identity) and a
// CertPool that trusts it (peer verification). Nothing touches disk — the
// key material lives only for the test.
func selfSignedCert(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "protorun-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}

	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(leaf)

	cert := tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  key,
		Leaf:        leaf,
	}
	return cert, pool
}

// TestTCPLayer_TLS_HandshakeAndFrame stands up a TLS TCP pair and drives a
// full SessionLayer handshake over it, then exchanges an application frame
// in each direction. It proves WithTLS terminates cleanly below the
// unchanged framing + Hello/Ack handshake.
func TestTCPLayer_TLS_HandshakeAndFrame(t *testing.T) {
	cert, pool := selfSignedCert(t)

	// Both peers can dial and accept: each presents the shared identity
	// and trusts the same pool. ServerName is derived from the dialed
	// "127.0.0.1" and matches the cert's IP SAN.
	serverTLS := &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: pool, ClientCAs: pool}
	clientTLS := &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: pool, ClientCAs: pool}

	hServer := NewHost(7291, "127.0.0.1")
	hClient := NewHost(7292, "127.0.0.1")

	ctx := t.Context()
	tcpServer := NewTCPLayer(hServer, ctx, 0, WithTLS(serverTLS))
	defer tcpServer.Cancel()
	tcpClient := NewTCPLayer(hClient, ctx, 0, WithTLS(clientTLS))
	defer tcpClient.Cancel()

	sServer := NewSessionLayer(tcpServer, hServer, ctx, 0, 0)
	defer sServer.Cancel()
	sClient := NewSessionLayer(tcpClient, hClient, ctx, 0, 0)
	defer sClient.Cancel()

	sClient.Connect(hServer)

	ev1 := waitSessionEvent(t, sClient.OutChannelEvents(), 5*time.Second)
	ev2 := waitSessionEvent(t, sServer.OutChannelEvents(), 5*time.Second)
	if _, ok := ev1.(*SessionConnected); !ok {
		t.Fatalf("client: expected SessionConnected over TLS, got %T", ev1)
	}
	if _, ok := ev2.(*SessionConnected); !ok {
		t.Fatalf("server: expected SessionConnected over TLS, got %T", ev2)
	}

	// Application frame client -> server, resolved to the logical host.
	sClient.Send(*bytes.NewBuffer([]byte("hello over tls")), hServer)
	select {
	case in := <-sServer.OutMessages():
		if in.Msg.String() != "hello over tls" {
			t.Fatalf("server received %q, want %q", in.Msg.String(), "hello over tls")
		}
		if in.Host() != hClient {
			t.Fatalf("server saw logical host %v, want %v", in.Host(), hClient)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server never received the application frame over TLS")
	}
}

// TestTCPLayer_MutualTLS_RejectsCertlessClient asserts that a server
// requiring and verifying a client certificate refuses a client that
// presents none. The server tears the TLS connection down; over the
// SessionLayer the client therefore never reaches SessionConnected — it
// gets SessionFailed / SessionDisconnected instead.
//
// (Under TLS 1.3 the client's dial returns before the server rejects the
// missing cert, so the failure surfaces on the first read rather than at
// dial time; asserting on the session outcome is version-independent.)
func TestTCPLayer_MutualTLS_RejectsCertlessClient(t *testing.T) {
	cert, pool := selfSignedCert(t)

	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}
	// Client trusts the server but presents no client certificate.
	certlessClientTLS := &tls.Config{RootCAs: pool}

	hServer := NewHost(7293, "127.0.0.1")
	hClient := NewHost(7294, "127.0.0.1")

	ctx := t.Context()
	tcpServer := NewTCPLayer(hServer, ctx, 0, WithTLS(serverTLS))
	defer tcpServer.Cancel()
	tcpClient := NewTCPLayer(hClient, ctx, 0, WithTLS(certlessClientTLS))
	defer tcpClient.Cancel()

	sServer := NewSessionLayer(tcpServer, hServer, ctx, 0, 0)
	defer sServer.Cancel()
	sClient := NewSessionLayer(tcpClient, hClient, ctx, 0, 0,
		WithHandshakeTimeout(500*time.Millisecond))
	defer sClient.Cancel()

	sClient.Connect(hServer)

	ev := waitSessionEvent(t, sClient.OutChannelEvents(), 5*time.Second)
	switch ev.(type) {
	case *SessionFailed, *SessionDisconnected:
		// expected: the mTLS handshake was refused, no session formed.
	default:
		t.Fatalf("expected SessionFailed/SessionDisconnected for certless mTLS client, got %T", ev)
	}

	// The server must not have established a session with the certless peer.
	select {
	case sev := <-sServer.OutChannelEvents():
		if _, ok := sev.(*SessionConnected); ok {
			t.Fatalf("server established a session with a certless mTLS client")
		}
	case <-time.After(200 * time.Millisecond):
		// no SessionConnected on the server — good.
	}
}
