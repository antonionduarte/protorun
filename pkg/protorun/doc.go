// Package protorun is a framework for the modular development of
// distributed protocols, heavily inspired by the Babel framework
// (https://github.com/pfouto/babel-core). Protocols are written as Go
// types implementing Start(ctx) and Init(ctx); the framework handles
// session establishment, message routing by Go-type wire identifier,
// per-protocol event-loop concurrency, optional reconnect with
// configurable backoff, and clean shutdown with signal handling.
//
// # Quick start
//
//	rt := protorun.New(self,
//	    protorun.WithLogger(logger),
//	    protorun.WithTCPTransport(ctx),
//	)
//	rt.Register(myprotocol.New(...))
//	if err := rt.Run(); err != nil { /* ... */ }
//
// # Defining a protocol
//
// Implement two methods, Start (register handlers/codecs) and Init
// (perform initial work, e.g. dial peers). Optional interfaces let
// the protocol observe session lifecycle:
//
//	type MyProto struct{ peer transport.Host }
//
//	func (p *MyProto) Start(ctx protorun.ProtocolContext) {
//	    protorun.RegisterCodec(ctx, protorun.BinaryCodec[*MyMsg]{})
//	    protorun.RegisterHandler(ctx, p.handle)
//	}
//	func (p *MyProto) Init(ctx protorun.ProtocolContext) {
//	    _ = ctx.Connect(p.peer)
//	}
//	func (p *MyProto) handle(m *MyMsg, from transport.Host) { /* ... */ }
//
// # Wire identifiers and rename safety
//
// The framework derives each message's wire identifier by hashing the
// Go type name (or M.WireName() if the type implements WireNamer).
// Production protocols SHOULD implement WireName on every message
// type. Relying on the Go type name silently breaks wire
// compatibility when the type is renamed or moves between packages,
// with no compile-time signal. See WireID for details.
//
// # See also
//
// The cmd/pingpong example shows a complete two-node program. The
// Example functions in this package illustrate individual API entry
// points.
package protorun
