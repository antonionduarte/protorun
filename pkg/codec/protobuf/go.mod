module github.com/antonionduarte/protorun/pkg/codec/protobuf

go 1.26

require (
	github.com/antonionduarte/protorun v0.0.0-00010101000000-000000000000
	go.uber.org/goleak v1.3.0
	google.golang.org/protobuf v1.36.11
)

// The core module is developed in lockstep in this repo; resolve it
// locally rather than through a published version.
replace github.com/antonionduarte/protorun => ../../..
