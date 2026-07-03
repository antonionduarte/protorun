// Command tracegen runs canned protorun simulations and writes protoviz
// trace files (JSONL, format protoviz/1). It is two things at once: the
// producer of the visualizer's demo data, and living documentation of how
// to wire a prototest trace recorder — the WithTrace / WithTraceStateEvery
// / WithTraceSampler options plus a scenario's own DebugState probes.
//
// Each scenario builds a seeded Sim, adds a cluster of real protocols,
// runs a scripted schedule (faults included), samples per-node state
// through probe protocols, and lets the recorder stream the events out.
//
//	go run ./cmd/tracegen -scenario raft-partition -seed 42 -out raft.jsonl
//
// Scenarios: broadcast, hyparview-churn, raft-partition, paxos-duel.
//
// tracegen drives a Sim outside `go test`, which testing.TB cannot express
// (it has an unexported method). prototest therefore accepts the narrower
// prototest.TB, and cliTB below is a minimal implementation over slog.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
)

// scenario names one runnable trace producer.
type scenario struct {
	name string
	run  func(t *cliTB, out io.Writer, seed int64)
}

func scenarios() []scenario {
	return []scenario{
		{"broadcast", runBroadcast},
		{"hyparview-churn", runHyParViewChurn},
		{"raft-partition", runRaftPartition},
		{"paxos-duel", runPaxosDuel},
	}
}

func main() {
	name := flag.String("scenario", "", "scenario to run: broadcast|hyparview-churn|raft-partition|paxos-duel")
	seed := flag.Int64("seed", 42, "seed pinning the simulation schedule")
	out := flag.String("out", "", "output trace file (JSONL); default stdout")
	all := flag.Bool("all", false, "run every scenario, writing <name>.jsonl next to -out's directory")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if *all {
		runAll(logger, *seed, *out)
		return
	}
	if *name == "" {
		fmt.Fprintln(os.Stderr, "scenario is required (use -scenario NAME or -all)")
		flag.Usage()
		os.Exit(2)
	}
	sc, ok := lookup(*name)
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown scenario %q\n", *name)
		os.Exit(2)
	}
	if err := generate(logger, sc, *seed, *out); err != nil {
		fmt.Fprintf(os.Stderr, "tracegen: %v\n", err)
		os.Exit(1)
	}
}

// runAll generates every scenario into the directory of -out (or the
// current directory), each as <name>.jsonl.
func runAll(logger *slog.Logger, seed int64, outDir string) {
	if outDir == "" {
		outDir = "."
	}
	if err := os.MkdirAll(outDir, 0o750); err != nil {
		fmt.Fprintf(os.Stderr, "tracegen: %v\n", err)
		os.Exit(1)
	}
	for _, sc := range scenarios() {
		path := fmt.Sprintf("%s/%s.jsonl", outDir, sc.name)
		if err := generate(logger, sc, seed, path); err != nil {
			fmt.Fprintf(os.Stderr, "tracegen: %s: %v\n", sc.name, err)
			os.Exit(1)
		}
	}
}

func lookup(name string) (scenario, bool) {
	for _, sc := range scenarios() {
		if sc.name == name {
			return sc, true
		}
	}
	return scenario{}, false
}

// generate opens the output sink, runs one scenario, and reports the file
// size on success. Cleanups (runtime shutdowns and the recorder flush) run
// before the file is closed, so the trace is complete on return.
func generate(logger *slog.Logger, sc scenario, seed int64, outPath string) (err error) {
	var f *os.File
	if outPath == "" || outPath == "-" {
		f = os.Stdout
	} else {
		f, err = os.Create(outPath) //nolint:gosec // outPath is an operator-supplied CLI flag; this is a trace generator.
		if err != nil {
			return err
		}
		defer func() {
			if cerr := f.Close(); cerr != nil && err == nil {
				err = cerr
			}
		}()
	}

	logger.Info("generating trace", "scenario", sc.name, "seed", seed, "out", outPath)
	return runScenario(logger, sc, seed, f)
}

// runScenario runs one scenario to out, running its cleanups (which flush
// the recorder and shut the runtimes down) before returning, and reporting
// a failure the scenario signalled via the harness. Separated from file
// handling so tests can drive a scenario into a buffer.
func runScenario(logger *slog.Logger, sc scenario, seed int64, out io.Writer) (err error) {
	tb := &cliTB{name: sc.name, logger: logger}
	defer func() {
		tb.runCleanups()
		if r := recover(); r != nil && !errors.Is(asError(r), errFatal) {
			panic(r) // a genuine panic, not a scenario Fatalf
		}
		if tb.Failed() && err == nil {
			err = fmt.Errorf("scenario %s failed", sc.name)
		}
	}()
	sc.run(tb, out, seed)
	return err
}

func asError(v any) error {
	if e, ok := v.(error); ok {
		return e
	}
	return nil
}

// errFatal is the sentinel a cliTB.Fatalf panics with, so generate can
// unwind the scenario, run cleanups, and report failure without a stack
// dump.
var errFatal = errors.New("tracegen: fatal")

// cliTB is a minimal prototest.TB backed by slog, letting a plain binary
// drive a Sim. Cleanups run in LIFO order at the end of a scenario, which
// is what flushes the recorder and shuts the runtimes down.
type cliTB struct {
	name     string
	logger   *slog.Logger
	failed   bool
	cleanups []func()
}

func (t *cliTB) Helper()      {}
func (t *cliTB) Name() string { return t.name }

func (t *cliTB) Logf(format string, args ...any) {
	t.logger.Info(fmt.Sprintf(format, args...))
}

func (t *cliTB) Errorf(format string, args ...any) {
	t.failed = true
	t.logger.Error(fmt.Sprintf(format, args...))
}

func (t *cliTB) Fatalf(format string, args ...any) {
	t.failed = true
	t.logger.Error(fmt.Sprintf(format, args...))
	panic(errFatal)
}

func (t *cliTB) Failed() bool { return t.failed }

func (t *cliTB) Cleanup(fn func()) {
	t.cleanups = append(t.cleanups, fn)
}

func (t *cliTB) runCleanups() {
	for _, fn := range slices.Backward(t.cleanups) {
		fn()
	}
	t.cleanups = nil
}
