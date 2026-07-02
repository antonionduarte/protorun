module github.com/antonionduarte/protorun/config

go 1.26

require (
	github.com/antonionduarte/protorun v0.0.0-00010101000000-000000000000
	gopkg.in/yaml.v3 v3.0.1
)

require github.com/kr/text v0.2.0 // indirect

// The core module is developed in lockstep in this repo; resolve it
// locally rather than through a published version.
replace github.com/antonionduarte/protorun => ..
