package config

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/antonionduarte/protorun/pkg/protorun"
	"github.com/antonionduarte/protorun/pkg/transport"
)

func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "node.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

type hyparviewLikeConfig struct {
	ActiveSize  int `yaml:"activeSize"`
	PassiveSize int `yaml:"passiveSize"`
}

func TestLoad_HappyPath(t *testing.T) {
	path := writeConfig(t, `
runtime:
  logging:
    level: debug
    components: [protocol, session]
    format: text

hyparview:
  activeSize: 5
  passiveSize: 30
`)

	f, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	rt := f.Runtime()
	if rt.Logging.Level != "debug" {
		t.Errorf("Logging.Level = %q, want debug", rt.Logging.Level)
	}
	if len(rt.Logging.Components) != 2 || rt.Logging.Components[0] != "protocol" {
		t.Errorf("Logging.Components = %v", rt.Logging.Components)
	}
	if rt.Logging.Format != "text" {
		t.Errorf("Logging.Format = %q, want text", rt.Logging.Format)
	}

	hv, err := Section[hyparviewLikeConfig](f, "hyparview")
	if err != nil {
		t.Fatalf("Section: %v", err)
	}
	if hv.ActiveSize != 5 || hv.PassiveSize != 30 {
		t.Errorf("hv = %+v, want {5 30}", hv)
	}
}

func TestSection_MissingSection(t *testing.T) {
	path := writeConfig(t, `
runtime:
  logging:
    level: info
`)
	f, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	_, err = Section[hyparviewLikeConfig](f, "hyparview")
	if !errors.Is(err, ErrSectionNotFound) {
		t.Fatalf("Section error = %v, want ErrSectionNotFound", err)
	}
}

func TestSection_TypeMismatch(t *testing.T) {
	path := writeConfig(t, `
runtime:
  logging:
    level: info

hyparview:
  activeSize: "not a number"
  passiveSize: 30
`)
	f, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	_, err = Section[hyparviewLikeConfig](f, "hyparview")
	if err == nil {
		t.Fatal("Section: want type-mismatch error, got nil")
	}
}

func TestSection_UnknownFieldStrictness(t *testing.T) {
	path := writeConfig(t, `
runtime:
  logging:
    level: info

hyparview:
  activeSize: 5
  passiveSyze: 30
`)
	f, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	_, err = Section[hyparviewLikeConfig](f, "hyparview")
	if err == nil {
		t.Fatal("Section: want unknown-field error for passiveSyze typo, got nil")
	}
}

func TestLoad_UnknownFieldInRuntimeBlock(t *testing.T) {
	path := writeConfig(t, `
runtime:
  logging:
    verbosity: info
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("Load: want unknown-field error for verbosity (not a LoggingConfig field), got nil")
	}
}

func TestLoad_UnknownTopLevelKeyIsASection(t *testing.T) {
	// A typo'd "runtime" key (e.g. "runtme") is not a reserved key, so
	// it is treated like any other named section — Load succeeds and
	// Runtime() comes back zero-valued. This is a documented tradeoff:
	// only "runtime" is recognized at the top level.
	path := writeConfig(t, `
runtme:
  logging:
    level: debug
`)
	f, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if f.Runtime().Logging.Level != "" {
		t.Errorf("Runtime().Logging.Level = %q, want empty (typo key not recognized)", f.Runtime().Logging.Level)
	}
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	if err == nil {
		t.Fatal("Load: want error for missing file, got nil")
	}
}

func TestRuntime_Options(t *testing.T) {
	path := writeConfig(t, `
runtime:
  logging:
    level: warn
    format: json
`)
	f, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	opts := f.Runtime().Options()
	if len(opts) != 1 {
		t.Fatalf("Options() returned %d options, want 1", len(opts))
	}

	self := transport.NewHost(0, "127.0.0.1")
	rt := protorun.New(self, opts...)
	if rt.Logger() == nil {
		t.Fatal("rt.Logger() is nil after applying Runtime().Options()")
	}
}
