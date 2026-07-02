module github.com/antonionduarte/protorun/pkg/transport/quic

go 1.26

require (
	github.com/antonionduarte/protorun v0.0.0-00010101000000-000000000000
	github.com/quic-go/quic-go v0.60.0
	go.uber.org/goleak v1.3.0
)

require (
	golang.org/x/crypto v0.51.0 // indirect
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
)

// The core module is developed in lockstep in this repo; resolve it
// locally rather than through a published version.
replace github.com/antonionduarte/protorun => ../../..
