// The examples module. Lives apart from the root module so example
// binaries can consume the nested modules (pkg/config pulls YAML)
// without the core library module growing any dependency: the root
// module stays stdlib-only (plus test-only goleak).
module github.com/antonionduarte/protorun/cmd

go 1.26

require (
	github.com/antonionduarte/protorun v0.0.0
	github.com/antonionduarte/protorun/pkg/config v0.0.0
	go.uber.org/goleak v1.3.0
)

require gopkg.in/yaml.v3 v3.0.1 // indirect

replace github.com/antonionduarte/protorun => ..

replace github.com/antonionduarte/protorun/pkg/config => ../pkg/config
