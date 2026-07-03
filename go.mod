module github.com/antonionduarte/protorun

go 1.26

toolchain go1.26.4

// Test-only dependency (goroutine-leak detection in every test
// package); the runtime itself is stdlib-only.
require go.uber.org/goleak v1.3.0
