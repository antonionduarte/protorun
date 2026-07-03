package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// TestScenarios_ProduceValidTrace runs every scenario into a buffer and
// asserts the output is a well-formed protoviz/1 trace: the first line is
// the meta header, every line parses as JSON, and the expected event kinds
// appear. This doubles as the JSON-validity check for the committed sample
// traces (same producer, same code path).
func TestScenarios_ProduceValidTrace(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, sc := range scenarios() {
		t.Run(sc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := runScenario(logger, sc, 42, &buf); err != nil {
				t.Fatalf("scenario %s: %v", sc.name, err)
			}

			kinds := map[string]int{}
			var first map[string]any
			scan := bufio.NewScanner(&buf)
			scan.Buffer(make([]byte, 0, 1<<20), 1<<20)
			n := 0
			for scan.Scan() {
				line := scan.Bytes()
				if len(bytes.TrimSpace(line)) == 0 {
					continue
				}
				var ev map[string]any
				if err := json.Unmarshal(line, &ev); err != nil {
					t.Fatalf("line %d is not valid JSON: %v\n%s", n+1, err, line)
				}
				if n == 0 {
					first = ev
				}
				if k, ok := ev["kind"].(string); ok {
					kinds[k]++
				}
				n++
			}
			if err := scan.Err(); err != nil {
				t.Fatalf("scan: %v", err)
			}

			if n == 0 {
				t.Fatal("empty trace")
			}
			if first["kind"] != "meta" {
				t.Errorf("first line kind = %v, want meta", first["kind"])
			}
			if first["format"] != "protoviz/1" {
				t.Errorf("meta format = %v, want protoviz/1", first["format"])
			}
			// Every scenario delivers messages, advances the clock, forms
			// sessions, and samples state.
			for _, want := range []string{"node", "deliver", "clock", "session", "state"} {
				if kinds[want] == 0 {
					t.Errorf("no %q events in %s trace", want, sc.name)
				}
			}
		})
	}
}
