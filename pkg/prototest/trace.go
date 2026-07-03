package prototest

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/antonionduarte/protorun/pkg/protorun"
	"github.com/antonionduarte/protorun/pkg/transport"
)

// traceFormat is the version tag every trace's meta line carries, so a
// reader can reject an incompatible stream. Bump it on a breaking change
// to the event schema below.
const traceFormat = "protoviz/1"

// TB is the slice of testing.TB that prototest's harness actually uses.
// *testing.T and *testing.B satisfy it structurally, so existing tests
// pass unchanged; a non-test driver (cmd/tracegen, generating sample
// traces) can supply its own tiny implementation to run a Sim outside
// `go test`, which is otherwise impossible because testing.TB has an
// unexported method and cannot be implemented.
type TB interface {
	Helper()
	Name() string
	Logf(format string, args ...any)
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
	Failed() bool
	Cleanup(func())
}

// traceEvent is the single JSONL envelope for every protoviz trace event
// (protoviz trace format v1). One struct with omitempty on every
// kind-specific field keeps the writer trivial and the stream self-
// describing: Kind selects which fields are meaningful, and a reader
// decodes by Kind. Step is the simulator's running count of progress units
// (deliveries and clock advances) — a total order — and VirtualTime is the
// elapsed virtual time since the sim epoch.
type traceEvent struct {
	Step        uint64 `json:"step,omitempty"`
	VirtualTime string `json:"t,omitempty"`
	Kind        string `json:"kind"`

	// meta (first line): format version, seed, node roster, start instant.
	Format string `json:"format,omitempty"`
	Seed   int64  `json:"seed,omitempty"`
	Start  string `json:"start,omitempty"`

	// node: a node joined. Host is its address, Protocols its stack by %T.
	// Nodes is reused by meta (the roster) and fault (the hosts involved).
	Host      string   `json:"host,omitempty"`
	Protocols []string `json:"protocols,omitempty"`
	Nodes     []string `json:"nodes,omitempty"`

	// session: a session event as delivered to Node about Peer.
	Event string `json:"event,omitempty"`
	Node  string `json:"node,omitempty"` // observing node (also the "state" subject)
	Peer  string `json:"peer,omitempty"`

	// deliver / drop: a message from From to To of wire type Wire. Bytes is
	// the wire-body size; Reason names the fault that dropped it.
	From   string `json:"from,omitempty"`
	To     string `json:"to,omitempty"`
	Wire   string `json:"wire,omitempty"`
	Bytes  int    `json:"bytes,omitempty"`
	Reason string `json:"reason,omitempty"`

	// fault: a mesh mutation. Mutation is cut|heal|isolate|loss|delay,
	// Nodes the hosts involved, Params the mutation's numeric arguments.
	Mutation string         `json:"mutation,omitempty"`
	Params   map[string]any `json:"params,omitempty"`

	// state: a node's protocol state snapshot, decoded shape-specifically
	// by a lens. Protocol is the label the sampler chose.
	Protocol string          `json:"protocol,omitempty"`
	State    json.RawMessage `json:"state,omitempty"`
}

// traceNode is a pending node registration held until meta is emitted (so
// the meta line's roster is complete for the common case where every node
// joins before the first step).
type traceNode struct {
	host      transport.Host
	protocols []string
}

// recorder writes a protoviz trace as the mesh and its Sim scheduler run.
// It is attached by the WithTrace* options and driven entirely from the
// simulator's existing choke points (deliver, clock advance, fault
// methods, node registration); it only observes and never enqueues work,
// so a traced run is schedule-identical to an untraced one. All methods
// are safe on a nil receiver, so call sites need no guard.
//
// mu serializes writes across the two goroutines that can emit: the
// scheduler/test goroutine (deliveries, clock, faults, samples) and a
// node's event-loop goroutine (fault drops during a handler's send). Under
// the Sim's one-delivery-at-a-time discipline these never actually
// overlap, but the lock keeps the writer correct regardless.
type recorder struct {
	mu   sync.Mutex
	mesh *Mesh
	seed int64
	sink *bufio.Writer
	enc  *json.Encoder

	metaWritten  bool
	pendingNodes []traceNode
	step         uint64

	// State sampling. sampler is invoked every stateEvery steps at a
	// quiescent point; sim is the value it receives (set by NewSim).
	sim        *Sim
	sampler    func(sim *Sim, emit func(node transport.Host, protocol string, state any))
	stateEvery uint64
	lastSample uint64

	// On-failure artifact: when buf is non-nil the trace is buffered in
	// memory and dumped to a file only if the test failed. tb is the
	// harness used for the failure check and the artifact-path log.
	tb        TB
	buf       *bytes.Buffer
	onFailure bool
}

// newRecorder builds the recorder for a mesh, or returns nil when no trace
// option was set (the common case, so tracing costs nothing when off). It
// registers a Cleanup that flushes the buffer and, for the on-failure
// mode, writes the artifact.
func newRecorder(t TB, m *Mesh, cfg meshConfig) *recorder {
	if cfg.traceWriter == nil && !cfg.traceOnFailure {
		return nil
	}
	r := &recorder{
		mesh:       m,
		seed:       cfg.seed,
		tb:         t,
		sampler:    cfg.traceSampler,
		stateEvery: uint64(max(cfg.traceStateEvery, 0)),
	}
	switch {
	case cfg.traceWriter != nil:
		// Explicit writer streams; on-failure buffering is redundant here.
		r.sink = bufio.NewWriter(cfg.traceWriter)
	default:
		r.buf = &bytes.Buffer{}
		r.onFailure = true
		r.sink = bufio.NewWriter(r.buf)
	}
	r.enc = json.NewEncoder(r.sink)
	r.enc.SetEscapeHTML(false)
	t.Cleanup(r.finish)
	return r
}

// vtime renders the elapsed virtual time since the sim epoch, or "" for a
// mesh with no virtual clock (tracing is really a Sim feature).
func (r *recorder) vtime() string {
	if r.mesh.clock == nil {
		return ""
	}
	return r.mesh.clock.Now().Sub(simEpoch).String()
}

// emitLocked writes one event, filling in step and virtual time. Caller
// holds r.mu. ensureMetaLocked runs first so meta is always the first line.
func (r *recorder) emitLocked(ev *traceEvent) {
	r.ensureMetaLocked()
	ev.Step = r.step
	if ev.VirtualTime == "" {
		ev.VirtualTime = r.vtime()
	}
	_ = r.enc.Encode(ev)
}

// ensureMetaLocked writes the meta line and the node events for every node
// registered so far, exactly once. Caller holds r.mu.
func (r *recorder) ensureMetaLocked() {
	if r.metaWritten {
		return
	}
	r.metaWritten = true
	roster := make([]string, len(r.pendingNodes))
	for i, n := range r.pendingNodes {
		roster[i] = n.host.String()
	}
	_ = r.enc.Encode(&traceEvent{
		Kind:   "meta",
		Format: traceFormat,
		Seed:   r.seed,
		Nodes:  roster,
		Start:  simEpoch.Format(time.RFC3339),
	})
	for _, n := range r.pendingNodes {
		_ = r.enc.Encode(&traceEvent{Kind: "node", Host: n.host.String(), Protocols: n.protocols})
	}
	r.pendingNodes = nil
}

// node records a node joining the mesh with its protocol stack (by %T). If
// meta has not been written yet the node is held for the roster; a late
// join (after the first event) is written immediately.
func (r *recorder) node(host transport.Host, protocols []protorun.Protocol) {
	if r == nil {
		return
	}
	names := make([]string, len(protocols))
	for i, p := range protocols {
		names[i] = fmt.Sprintf("%T", p)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.metaWritten {
		r.pendingNodes = append(r.pendingNodes, traceNode{host: host, protocols: names})
		return
	}
	_ = r.enc.Encode(&traceEvent{Kind: "node", Host: host.String(), Protocols: names, Step: r.step})
}

// deliverMsg records an application message delivery. It counts as one
// unit of progress, so it advances the step counter.
func (r *recorder) deliverMsg(from, to transport.Host, wireID uint64, nbytes int) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.step++
	r.emitLocked(&traceEvent{
		Kind:  "deliver",
		From:  from.String(),
		To:    to.String(),
		Wire:  wireLabel(wireID),
		Bytes: nbytes,
	})
	r.mu.Unlock()
}

// session records a session event as delivered to observer about its peer.
// Also a unit of progress.
func (r *recorder) session(observer transport.Host, ev transport.SessionEvent) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.step++
	r.emitLocked(&traceEvent{
		Kind:  "session",
		Event: sessionEventName(ev),
		Node:  observer.String(),
		Peer:  ev.Host().String(),
	})
	r.mu.Unlock()
}

// drop records a message the fault policy discarded before delivery. Not a
// unit of progress (nothing was delivered), so the step counter is left
// alone; the drop is stamped with the step currently being processed.
func (r *recorder) drop(from, to transport.Host, wireID uint64, reason string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.emitLocked(&traceEvent{
		Kind:   "drop",
		From:   from.String(),
		To:     to.String(),
		Wire:   wireLabel(wireID),
		Reason: reason,
	})
	r.mu.Unlock()
}

// clockAdvance records the virtual clock stepping forward, one unit of
// progress. The new time is captured by emitLocked.
func (r *recorder) clockAdvance() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.step++
	r.emitLocked(&traceEvent{Kind: "clock"})
	r.mu.Unlock()
}

// fault records a mesh mutation (cut/heal/isolate/loss/delay) with the
// hosts and parameters involved.
func (r *recorder) fault(mutation string, nodes []transport.Host, params map[string]any) {
	if r == nil {
		return
	}
	hosts := make([]string, len(nodes))
	for i, h := range nodes {
		hosts[i] = h.String()
	}
	r.mu.Lock()
	r.emitLocked(&traceEvent{Kind: "fault", Mutation: mutation, Nodes: hosts, Params: params})
	r.mu.Unlock()
}

// recordState writes a protocol-state snapshot. state is marshaled with
// encoding/json; a lens decodes the shape. Marshal failures degrade to a
// JSON null rather than dropping the event.
func (r *recorder) recordState(node transport.Host, protocol string, state any) {
	if r == nil {
		return
	}
	raw, err := json.Marshal(state)
	if err != nil {
		raw = []byte("null")
	}
	r.mu.Lock()
	r.emitLocked(&traceEvent{
		Kind:     "state",
		Node:     node.String(),
		Protocol: protocol,
		State:    json.RawMessage(raw),
	})
	r.mu.Unlock()
}

// maybeSample invokes the state sampler if the cadence has come due. Called
// by the scheduler at quiescent points. The sampler runs without r.mu held
// so its emit callback (recordState) can take the lock.
func (r *recorder) maybeSample() {
	if r == nil || r.sampler == nil || r.stateEvery == 0 {
		return
	}
	r.mu.Lock()
	due := r.step-r.lastSample >= r.stateEvery
	if due {
		r.lastSample = r.step
	}
	r.mu.Unlock()
	if due {
		r.sampler(r.sim, r.recordState)
	}
}

// finish flushes the buffered writer and, in on-failure mode, writes the
// artifact when the test failed. Registered on the harness Cleanup.
func (r *recorder) finish() {
	r.mu.Lock()
	r.ensureMetaLocked() // guarantee at least meta + roster even with no steps
	_ = r.sink.Flush()
	r.mu.Unlock()

	if !r.onFailure || !r.tb.Failed() {
		return
	}
	dir := os.Getenv("PROTOTEST_TRACE_DIR")
	if dir == "" {
		dir = os.TempDir()
	}
	name := fmt.Sprintf("prototest-%s-%d.jsonl", sanitizeName(r.tb.Name()), r.seed)
	path := filepath.Join(dir, name)
	// name is sanitizeName'd to [A-Za-z0-9_-] and dir is operator-chosen: a
	// diagnostic artifact, not attacker-influenced I/O.
	//nolint:gosec // sanitized filename + operator-chosen dir; diagnostic artifact only.
	if err := os.WriteFile(path, r.buf.Bytes(), 0o600); err != nil {
		r.tb.Logf("prototest: failed to write trace artifact: %v", err)
		return
	}
	r.tb.Logf("prototest: trace artifact written to %s (open it in protoviz)", path)
}

// sanitizeName turns a test name into a filesystem-safe token.
func sanitizeName(name string) string {
	return strings.Map(func(rr rune) rune {
		switch {
		case rr >= 'a' && rr <= 'z', rr >= 'A' && rr <= 'Z', rr >= '0' && rr <= '9', rr == '-', rr == '_':
			return rr
		default:
			return '_'
		}
	}, name)
}

// wireLabel resolves a wire id to its registered human-readable type name,
// falling back to a hex rendering for an id no codec has registered.
func wireLabel(id uint64) string {
	if name, ok := protorun.WireNameOf(id); ok {
		return name
	}
	return fmt.Sprintf("0x%016x", id)
}

// sessionEventName maps a transport.SessionEvent to its trace label.
func sessionEventName(ev transport.SessionEvent) string {
	switch ev.(type) {
	case *transport.SessionConnected:
		return "connected"
	case *transport.SessionDisconnected:
		return "disconnected"
	case *transport.SessionFailed:
		return "failed"
	default:
		return "unknown"
	}
}
