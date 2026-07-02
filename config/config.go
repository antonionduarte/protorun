// Package config is a YAML loader for protorun deployments. It is a
// nested module (its own go.mod) so the core protorun module keeps its
// zero-dependency guarantee: only programs that opt into YAML config
// pull in gopkg.in/yaml.v3.
//
// # Philosophy: no framework magic
//
// This package does not scan struct tags on your Protocol types, does
// not reflect over rt.Register calls, and does not hand config to
// protocols behind their back. A parsed File is just a reserved
// "runtime:" block plus a bag of named sections; a protocol receives
// its configuration the same way it receives everything else it
// needs — as an argument to its constructor:
//
//	cfg, err := config.Load("node.yaml")
//	hv, err := config.Section[hyparview.Config](cfg, "hyparview")
//	rt := protorun.New(self, cfg.Runtime().Options()...)
//	rt.Register(hyparview.New(self, hv))
//
// The wiring in main is explicit and greppable: nothing decides which
// protocol gets which section except the line of code that calls
// Section. This mirrors the rest of protorun's authoring contract —
// protocols only talk to the framework through ProtocolContext, never
// through ambient global state.
//
// # Shape
//
// A config document is a single YAML mapping. One key is reserved:
//
//	runtime:
//	  logging:
//	    level: debug                      # debug | info | warn | error
//	    components: [protocol, session]   # allow-list; empty = everything
//	    format: text                      # text | json
//
// Every other top-level key is a "named section" — an arbitrary,
// caller-defined block decoded on demand via Section[T]. protorun
// itself never looks inside a named section; only your own
// config.Section[T](cfg, "hyparview") call does.
//
// # Strictness
//
// Both the runtime block and every section are decoded with yaml.v3's
// KnownFields(true): a YAML key that has no matching Go struct field
// is a decode error, not a silently-dropped typo. This applies
// recursively to nested struct fields (e.g. runtime.logging.*) and to
// whatever shape you pass to Section[T]. It does not apply to
// top-level section *names* — "runtime" is the only reserved key, so
// an unrecognized top-level key is, by design, just an unread named
// section rather than an error (Section returns ErrSectionNotFound
// only when nothing decodes it — a typo'd section name reads back as
// "not found", not as a parse error).
package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"

	"github.com/antonionduarte/protorun"
)

// ErrSectionNotFound is returned by Section when the file has no
// top-level key matching the requested name.
var ErrSectionNotFound = errors.New("config: section not found")

// LoggingConfig is the YAML shape of runtime.logging. It mirrors
// protorun.LoggingConfig field-for-field; Runtime.Options builds a
// protorun.LoggingConfig from it when constructing the logger option.
type LoggingConfig struct {
	// Level is one of "debug", "info", "warn", "error".
	Level string `yaml:"level"`
	// Components is an allow-list of component tags ("runtime",
	// "session", "transport", "protocol"). Empty means unfiltered.
	Components []string `yaml:"components"`
	// Format is "text" or "json"; empty defaults to "text".
	Format string `yaml:"format"`
}

// Runtime is the parsed "runtime:" block.
type Runtime struct {
	Logging LoggingConfig `yaml:"logging"`
}

// Options builds the []protorun.Option implied by this Runtime block.
// Today that is a single WithLogger built from Logging via
// protorun.NewLoggerFromConfig; callers that need more (WithMetrics,
// WithStrict, ...) append their own options after these:
//
//	rt := protorun.New(self, append(cfg.Runtime().Options(), protorun.WithStrict(true))...)
func (r Runtime) Options() []protorun.Option {
	logger := protorun.NewLoggerFromConfig(protorun.LoggingConfig{
		Level:      r.Logging.Level,
		Components: r.Logging.Components,
		Format:     r.Logging.Format,
	})
	return []protorun.Option{protorun.WithLogger(logger)}
}

// File is a parsed configuration document: the reserved Runtime block
// plus every other top-level key, held as raw YAML for on-demand
// decoding via Section.
type File struct {
	runtime  Runtime
	sections map[string]yaml.Node
}

// document is the strict top-level shape Load decodes into. Sections
// is an inline catch-all map: yaml.v3 routes every key that isn't
// "runtime" there regardless of KnownFields, which is what lets
// top-level section names stay open-ended while runtime's own fields
// stay strict.
type document struct {
	Runtime  Runtime               `yaml:"runtime"`
	Sections map[string]yaml.Node `yaml:",inline"`
}

// Load reads and parses the YAML file at path. Decoding is strict
// (yaml.v3 KnownFields): an unrecognized field inside the runtime
// block is an error, not a silent no-op.
func Load(path string) (*File, error) {
	// #nosec G304 -- path is the caller-supplied config path, the same
	// trust boundary every config-file loader in this repo uses.
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("config: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)

	var doc document
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}

	return &File{runtime: doc.Runtime, sections: doc.Sections}, nil
}

// Runtime returns the parsed "runtime:" block.
func (f *File) Runtime() Runtime { return f.runtime }

// Section decodes the named top-level section into T. Returns
// ErrSectionNotFound (wrapped; test with errors.Is) if the file has no
// such key. Decoding is strict like Load: an unrecognized field in the
// section's YAML is an error.
//
// T is typically a small config struct a protocol constructor takes
// directly, e.g. hyparview.Config or a caller-defined type — Section
// has no relationship to any specific protocol package.
func Section[T any](f *File, name string) (T, error) {
	var zero T

	node, ok := f.sections[name]
	if !ok {
		return zero, fmt.Errorf("config: section %q: %w", name, ErrSectionNotFound)
	}

	// yaml.Node.Decode has no KnownFields option, so re-marshal the
	// captured node and decode it through a fresh strict Decoder to get
	// the same unknown-field strictness Load applies to the runtime
	// block.
	raw, err := yaml.Marshal(node)
	if err != nil {
		return zero, fmt.Errorf("config: section %q: %w", name, err)
	}

	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)

	var v T
	if err := dec.Decode(&v); err != nil {
		return zero, fmt.Errorf("config: section %q: %w", name, err)
	}
	return v, nil
}
