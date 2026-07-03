package protorun

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestParseLogLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"debug":    slog.LevelDebug,
		"info":     slog.LevelInfo,
		"warn":     slog.LevelWarn,
		"error":    slog.LevelError,
		"":         slog.LevelInfo, // default
		"nonsense": slog.LevelInfo, // default
	}
	for in, want := range cases {
		if got := ParseLogLevel(in); got != want {
			t.Errorf("ParseLogLevel(%q) = %v, want %v", in, got, want)
		}
	}
}

// captureHandler collects the buffer a filter chain writes into so
// tests can assert which records survived the component filter.
func captureLogger(components []string) (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	var h slog.Handler = slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	if len(components) > 0 {
		h = NewComponentFilterHandler(h, components)
	}
	return slog.New(h), &buf
}

func TestComponentFilterHandler_RecordAttr(t *testing.T) {
	logger, buf := captureLogger([]string{"session"})

	logger.Info("kept", "component", "session")
	logger.Info("dropped", "component", "transport")
	logger.Info("no-component passes through")

	out := buf.String()
	if !strings.Contains(out, "kept") {
		t.Errorf("allowed component was filtered out:\n%s", out)
	}
	if strings.Contains(out, "dropped") {
		t.Errorf("disallowed component passed the filter:\n%s", out)
	}
	if !strings.Contains(out, "no-component passes through") {
		t.Errorf("record without a component attribute must pass:\n%s", out)
	}
}

func TestComponentFilterHandler_WithAttrsComponent(t *testing.T) {
	logger, buf := captureLogger([]string{"runtime"})

	// Logger-level component captured via WithAttrs (the way the
	// runtime scopes its per-component loggers).
	kept := logger.With("component", "runtime")
	dropped := logger.With("component", "protocol")

	kept.Info("runtime line")
	dropped.Info("protocol line")

	// WithGroup must preserve the captured component.
	kept.WithGroup("g").Info("grouped runtime line")

	out := buf.String()
	if !strings.Contains(out, "runtime line") {
		t.Errorf("allowed logger-level component was filtered:\n%s", out)
	}
	if strings.Contains(out, "protocol line") {
		t.Errorf("disallowed logger-level component passed:\n%s", out)
	}
	if !strings.Contains(out, "grouped runtime line") {
		t.Errorf("WithGroup lost the captured component:\n%s", out)
	}
}

func TestComponentFilterHandler_EnabledDelegates(t *testing.T) {
	var buf bytes.Buffer
	next := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})
	h := NewComponentFilterHandler(next, []string{"x"})

	if h.Enabled(t.Context(), slog.LevelDebug) {
		t.Error("Enabled(debug) should delegate to the warn-level inner handler")
	}
	if !h.Enabled(t.Context(), slog.LevelError) {
		t.Error("Enabled(error) should delegate to the warn-level inner handler")
	}
}

func TestNewLoggerFromConfig(t *testing.T) {
	// Smoke coverage over the format/level/filter wiring; output goes
	// to stderr by design, so only construction and non-panicking use
	// are asserted here (the filter mechanics are covered above).
	for _, cfg := range []LoggingConfig{
		{},
		{Level: "debug", Format: "json"},
		{Level: "error", Format: "text", Components: []string{"runtime"}},
	} {
		logger := NewLoggerFromConfig(cfg)
		if logger == nil {
			t.Fatalf("NewLoggerFromConfig(%+v) returned nil", cfg)
		}
		logger.Debug("smoke", "component", "runtime")
	}
}
