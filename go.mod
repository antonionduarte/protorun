module github.com/antonionduarte/protorun

go 1.26

toolchain go1.26.4

require github.com/antonionduarte/protorun/pkg/config v0.0.0-00010101000000-000000000000

require (
	github.com/kr/pretty v0.3.1 // indirect
	github.com/rogpeppe/go-internal v1.10.0 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

require go.uber.org/goleak v1.3.0

// cmd/pingpong (in this module) uses the config nested module; resolve
// it locally rather than through a published version.
replace github.com/antonionduarte/protorun/pkg/config => ./pkg/config
