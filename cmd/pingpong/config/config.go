// Package config holds the YAML schema and loader used by the pingpong
// example. It is intentionally outside the framework: the runtime
// itself reads no config files. Users wire their own configuration
// loaders to whatever knobs the framework exposes (see
// protorun.LoggingConfig, protorun.WithLogger, protorun.WithRetryPolicy,
// protorun.Default*Buffer).
package config

import (
	"io"
	"os"

	"gopkg.in/yaml.v3"

	"github.com/antonionduarte/protorun"
)

// HostConfig is a simple host (ip:port) specification used in pingpong's
// YAML config.
type HostConfig struct {
	IP   string `yaml:"ip"`
	Port int    `yaml:"port"`
}

// RuntimeConfig holds the example's per-run knobs. ChannelBuffer applies
// to the transport + session layer buffers when the example wires them.
type RuntimeConfig struct {
	Self          HostConfig `yaml:"self"`
	Peer          HostConfig `yaml:"peer"`
	ChannelBuffer int        `yaml:"channelBuffer"`
}

// loggingFile mirrors protorun.LoggingConfig's wire shape with yaml tags.
type loggingFile struct {
	Level      string   `yaml:"level"`
	Components []string `yaml:"components"`
	Format     string   `yaml:"format"`
}

// Config is the top-level YAML configuration structure.
type Config struct {
	Logging protorun.LoggingConfig
	Runtime RuntimeConfig
}

type configFile struct {
	Logging loggingFile   `yaml:"logging"`
	Runtime RuntimeConfig `yaml:"runtime"`
}

// LoadConfig reads a YAML file from path and returns the populated
// Config. Empty fields keep their zero values; the runtime's defaults
// kick in at the appropriate constructor.
func LoadConfig(path string) (*Config, error) {
	// #nosec G304 -- path is the user-supplied YAML config path passed via the -config flag.
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}

	var raw configFile
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	return &Config{
		Logging: protorun.LoggingConfig{
			Level:      raw.Logging.Level,
			Components: raw.Logging.Components,
			Format:     raw.Logging.Format,
		},
		Runtime: raw.Runtime,
	}, nil
}
