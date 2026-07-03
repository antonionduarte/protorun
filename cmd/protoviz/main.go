// Command protoviz is the live half of the protocol visualizer (Stage 3 of
// docs/visualizer-design.md). One binary, three jobs:
//
//   - Serve the built viewer (viz/dist) plus a live Server-Sent-Events
//     stream at /events. Cluster processes push their trace streams to
//     /ingest; the server annotates and fans them out to every viewer.
//
//     protoviz -addr :7777 -ui viz/dist
//
//   - Replay an existing protoviz/1 trace file over /events at a fixed
//     pace, so live-mode UX is demoable with zero cluster setup.
//
//     protoviz -replay viz/sample-traces/raft-partition.jsonl -pace 50ms
//
// The companion NewHTTPTracer (httptracer.go) is what a cluster process
// wires into protorun.WithTracer to stream itself here; cmd/broadcast does
// exactly that behind its -viz flag.
package main

import (
	"bufio"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"
)

func main() {
	addr := flag.String("addr", ":7777", "listen address")
	ui := flag.String("ui", "viz/dist", "directory of the built viewer (run: cd viz && npm run build)")
	ring := flag.Int("ring", 50000, "replay ring capacity (events retained for late-joining viewers)")
	replay := flag.String("replay", "", "replay this protoviz/1 trace file over /events instead of ingesting")
	pace := flag.Duration("pace", 50*time.Millisecond, "replay: delay between events")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	h := newHub(*ring)

	mux := http.NewServeMux()
	mux.HandleFunc("/events", h.handleEvents)
	mux.HandleFunc("/ingest", h.handleIngest)
	mux.Handle("/", uiHandler(*ui))

	if *replay != "" {
		lines, err := readTraceLines(*replay)
		if err != nil {
			logger.Error("cannot read replay file", "path", *replay, "err", err)
			os.Exit(1)
		}
		logger.Info("replay mode", "path", *replay, "events", len(lines), "pace", pace.String())
		go replayLines(h, lines, *pace)
	} else {
		logger.Info("live ingest mode", "ingest", "/ingest?node=<host>")
	}

	logger.Info("protoviz serving", "addr", *addr, "ui", *ui)
	srv := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil {
		logger.Error("server exited", "err", err)
		os.Exit(1)
	}
}

// readTraceLines loads a trace file into its non-empty lines.
func readTraceLines(path string) ([][]byte, error) {
	// path is the operator-supplied -replay flag: a local trace file, not
	// attacker-influenced input.
	f, err := os.Open(path) //nolint:gosec // operator-chosen -replay path.
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var lines [][]byte
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		cp := make([]byte, len(line))
		copy(cp, line)
		lines = append(lines, cp)
	}
	return lines, scanner.Err()
}

// replayLines publishes the file's lines one per pace tick. The lines are
// already protoviz/1, so they are streamed verbatim (their own step counter
// is already a total order); no server-side re-stamping is needed.
func replayLines(h *hub, lines [][]byte, pace time.Duration) {
	for _, line := range lines {
		h.publish(line)
		if pace > 0 {
			time.Sleep(pace)
		}
	}
}

// uiHandler serves the built viewer from dir, falling back to index.html so
// client-side routes resolve. When dir is missing it serves a plain page
// telling the operator to build the viewer. File lookups go through
// http.Dir, which rejects path-traversal, so a request path is never
// concatenated into a filesystem path directly.
func uiHandler(dir string) http.Handler {
	root := http.Dir(dir)
	fs := http.FileServer(root)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := os.Stat(dir); err != nil {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write(buildHint)
			return
		}
		// SPA fallback: serve index.html for a path with no matching file.
		// root.Open sanitizes the request path (no traversal escapes dir).
		if r.URL.Path != "/" {
			f, err := root.Open(r.URL.Path)
			if err != nil {
				r.URL.Path = "/"
				fs.ServeHTTP(w, r)
				return
			}
			_ = f.Close()
		}
		fs.ServeHTTP(w, r)
	})
}

// buildHint is shown when -ui points at a directory that does not exist yet.
var buildHint = []byte(`<!doctype html><html><head><meta charset="utf-8">` +
	`<title>protoviz</title></head><body style="font-family:system-ui;max-width:40rem;margin:4rem auto;padding:0 1rem">` +
	`<h1>protoviz</h1><p>The viewer bundle was not found. Build it first:</p>` +
	`<pre style="background:#f4f4f5;padding:1rem;border-radius:.5rem">cd viz &amp;&amp; npm run build</pre>` +
	`<p>Then restart protoviz (or point <code>-ui</code> at the build output).</p>` +
	`<p>The live event stream is available at <code>/events</code> regardless.</p>` +
	`</body></html>`)
