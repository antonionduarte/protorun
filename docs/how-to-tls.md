# How to: TLS and mutual TLS on the TCP transport

protorun's TCP backend takes a `*tls.Config` through a functional option,
so encrypting the wire is a one-liner — no fork, no wrapper protocol. TLS
terminates *below* the framing and the Hello/Ack session handshake: the
wire format and the handshake are byte-for-byte identical whether or not
TLS is on. This page shows server TLS and mutual TLS (mTLS) with runnable
snippets.

The relevant option is `transport.WithTLS(cfg)`, forwarded through
`protorun.WithTCPTransport`:

```go
rt := protorun.New(self,
    protorun.WithTCPTransport(ctx, transport.WithTLS(cfg)))
```

`WithTLS` sets both the dial side (`tls.Dialer`) and the listen side
(`tls.Listen`) from the same config. For finer control — a custom
`net.Conn`, a proxy, connection instrumentation — use the lower-level
`transport.WithDialFunc` / `transport.WithListenFunc` seams instead.

## What the config must contain

A protorun node is usually a peer: it both **dials** other peers and
**accepts** dials. A single `*tls.Config` therefore has to satisfy both
roles.

| Role | Fields |
| --- | --- |
| Server (accepts) | `Certificates` (or `GetCertificate`) — the node's cert + key |
| Client (dials) | `RootCAs` trusting the peer's chain, or `InsecureSkipVerify` for dev only |
| Mutual TLS, server side | additionally `ClientAuth` (e.g. `tls.RequireAndVerifyClientCert`) + `ClientCAs` |
| Mutual TLS, client side | additionally `Certificates` — the node's own client cert |

`ServerName` is derived from the dialed address when you leave it empty;
make sure the server certificate covers that host (a SAN entry for the IP
or DNS name).

## Generating a certificate

Production certs come from your CA or a tool like `cfssl` / `mkcert`. For
a quick self-signed dev cert:

```bash
# A self-signed cert valid for localhost, key + cert as PEM files.
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
    -keyout key.pem -out cert.pem -days 365 -nodes \
    -subj "/CN=protorun-dev" \
    -addext "subjectAltName=IP:127.0.0.1,DNS:localhost"
```

Load it with `tls.LoadX509KeyPair("cert.pem", "key.pem")`. To generate one
entirely in memory (no files on disk), mint an ECDSA key and a self-signed
`x509.Certificate` with `x509.CreateCertificate` — the QUIC module's
`quic.DevTLS` and the transport TLS tests do exactly this; copy that
pattern, and clearly mark it not-for-production.

## Server TLS

The server presents a certificate; the client verifies it against a trust
root. Only the transport wiring changes — protocols are untouched.

```go
// --- server node ---
cert, _ := tls.LoadX509KeyPair("server-cert.pem", "server-key.pem")
serverTLS := &tls.Config{
    Certificates: []tls.Certificate{cert},
}
rt := protorun.New(serverHost,
    protorun.WithTCPTransport(ctx, transport.WithTLS(serverTLS)))

// --- client node ---
roots := x509.NewCertPool()
pem, _ := os.ReadFile("server-cert.pem") // or your CA cert
roots.AppendCertsFromPEM(pem)
clientTLS := &tls.Config{
    RootCAs: roots, // trust the server; ServerName auto-derived from the dial addr
}
rt := protorun.New(clientHost,
    protorun.WithTCPTransport(ctx, transport.WithTLS(clientTLS)))
```

For development against a self-signed server you can skip verification with
`&tls.Config{InsecureSkipVerify: true}` — never in production.

## Mutual TLS

Both sides authenticate. The server additionally *requires and verifies* a
client certificate; the client additionally *presents* one. Because
protorun peers are symmetric, give every node a config that covers both
roles and trusts the shared CA:

```go
cert, _ := tls.LoadX509KeyPair("node-cert.pem", "node-key.pem")

roots := x509.NewCertPool()
caPEM, _ := os.ReadFile("ca-cert.pem")
roots.AppendCertsFromPEM(caPEM)

mtls := &tls.Config{
    Certificates: []tls.Certificate{cert}, // presented when dialing AND accepting
    RootCAs:      roots,                    // verify peers we dial
    ClientCAs:    roots,                    // verify peers that dial us
    ClientAuth:   tls.RequireAndVerifyClientCert,
}

rt := protorun.New(self,
    protorun.WithTCPTransport(ctx, transport.WithTLS(mtls)))
```

A peer that dials without a valid client certificate is rejected during
the TLS handshake. Under TLS 1.3 the rejection surfaces after the dial
returns (on the first read), so at the protocol level it manifests as the
session never reaching `SessionConnected` — you get a `SessionFailed` /
`SessionDisconnected` instead. The runtime's retry policy still applies:
tune it if a misconfigured peer should stop retrying.

## QUIC

QUIC mandates TLS, so the `pkg/transport/quic` module always encrypts. Pass
your `*tls.Config` to `quic.NewLayer`; the same role guidance above
applies. For tests and local development, `quic.DevTLS()` returns a
throwaway in-memory config (self-signed, not for production). See the
[`pkg/transport/quic`](../pkg/transport/quic/) module.
