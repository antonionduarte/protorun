package main

import (
	"encoding/json"

	"github.com/antonionduarte/protorun/cmd/internal/viztrace"
)

// outEvent is a protoviz/1 line the server streams to the viewer. It is a
// superset of the fields the file-trace parser already understands, plus
// "step"/"node" the server stamps and the "send"/lifecycle kinds the
// viewer tolerates.
type outEvent struct {
	Step   uint64 `json:"step"`
	Kind   string `json:"kind"`
	Node   string `json:"node,omitempty"`
	From   string `json:"from,omitempty"`
	To     string `json:"to,omitempty"`
	Peer   string `json:"peer,omitempty"`
	Event  string `json:"event,omitempty"`
	Wire   string `json:"wire,omitempty"`
	Bytes  int    `json:"bytes,omitempty"`
	Detail string `json:"detail,omitempty"`
	T      string `json:"t,omitempty"`
}

// normalizeIngest turns one raw HTTP-tracer line into a protoviz/1 line,
// stamped with the source node and a total-order step. It returns ok=false
// for a line that fails to parse or carries a kind the viewer can't use.
//
// The topology and sequence lenses build their "who delivered what from
// whom" record from DELIVER events only: a runtime reports "deliver" from
// the RECEIVING side, so a receiver's deliver (peer = the sender) becomes
// the authoritative {kind:"deliver", from:peer, to:node}. Sender-side
// "send" events are NOT folded into that record (they would double-count
// and, under loss, count messages that never arrived) — but they are still
// forwarded as kind "send" for future lenses, so nothing is thrown away on
// the wire. See docs/visualizer-design.md's live-mode section.
func normalizeIngest(raw []byte, node string, step uint64) ([]byte, bool) {
	var t viztrace.TraceLine
	if err := json.Unmarshal(raw, &t); err != nil {
		return nil, false
	}
	out := outEvent{Step: step, Node: node, T: t.At}
	switch t.Kind {
	case "deliver":
		// Receiver-authoritative: from = the reported peer (sender),
		// to = this node.
		out.Kind, out.From, out.To, out.Wire, out.Bytes = "deliver", t.Peer, node, t.Wire, t.Bytes
	case "send":
		// Forwarded for future use; the topology/sequence lenses ignore it.
		out.Kind, out.From, out.To, out.Wire, out.Bytes = "send", node, t.Peer, t.Wire, t.Bytes
	case "restart", "stop", "escalate":
		out.Kind, out.Detail = t.Kind, t.Detail
	case "dead-letter":
		out.Kind, out.Peer, out.Detail = "dead-letter", t.Peer, t.Detail
	default:
		event, ok := sessionEventName(t.Kind)
		if !ok {
			return nil, false
		}
		out.Kind, out.Event, out.Peer = "session", event, t.Peer
	}
	line, err := json.Marshal(&out)
	if err != nil {
		return nil, false
	}
	return line, true
}

// sessionEventName maps a runtime session-lifecycle kind onto the viewer's
// session-event name, or ok=false for a non-session kind. Given-up is a
// terminal disconnect, so it folds to "failed" (an edge removal the viewer
// already understands).
func sessionEventName(kind string) (string, bool) {
	switch kind {
	case "session-connected":
		return "connected", true
	case "session-disconnected":
		return "disconnected", true
	case "session-failed", "session-givenup":
		return "failed", true
	default:
		return "", false
	}
}
