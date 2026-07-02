package quic

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"time"
)

// DevTLS generates a throwaway, in-memory *tls.Config suitable for local
// development and tests: a fresh self-signed ECDSA certificate valid for
// 127.0.0.1 and ::1, wired so a node can both dial and accept (it presents
// the cert and trusts it as both a server and a client CA).
//
// NOT FOR PRODUCTION. The certificate is self-signed, its key never leaves
// memory, and it trusts only itself — there is no real identity. Supply
// your own *tls.Config (real certificates, real trust roots) for anything
// that matters. NewLayer forces the "protorun" ALPN regardless of what
// this sets.
func DevTLS() (*tls.Config, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("quic.DevTLS: generate key: %w", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "protorun-dev"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("quic.DevTLS: create certificate: %w", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("quic.DevTLS: parse certificate: %w", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(leaf)

	return &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{der},
			PrivateKey:  key,
			Leaf:        leaf,
		}},
		RootCAs:    pool,
		ClientCAs:  pool,
		MinVersion: tls.VersionTLS13,
		NextProtos: []string{alpn},
	}, nil
}
