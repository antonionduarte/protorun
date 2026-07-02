# Protocol Runtime

A runtime for hosting distributed protocols: each protocol runs in its
own event loop, exchanges typed messages with peers over sessions, and
coordinates with sibling protocols through IPC. One context — the
vocabulary below is shared by the runtime, transport, and examples.

## Language

### Identity

**Host**:
The stable logical identity of a node — the address it listens on and
is known by, independent of any live connection.
_Avoid_: peer ID, node ID, address

**Transport host**:
The ephemeral endpoint of an underlying connection (e.g. a dialing
socket's local port). Meaningful only until the handshake binds it to
a Host.
_Avoid_: remote address, connection ID

### Sessions

**Session**:
The handshake-established binding of an underlying connection to a
Host. All application traffic to a Host flows over its session.
_Avoid_: connection (that's the transport-level thing a session binds)

**Established**:
A session state reached only when both sides have accepted the
handshake — the dialing side is not Established until it receives the
Ack. `SessionConnected` is the announcement of this and nothing less.
_Avoid_: connected (ambiguous with transport-level connect)

**Reject**:
A handshake refusal sent before disconnecting, carrying the refuser's
wire-format version, so the dialing side learns the incompatibility
instead of inferring a crash.
_Avoid_: NACK, error frame

**Sessions (seam)**:
The Runtime's view of session management: everything the Runtime
requires to reach peers and observe session events, independent of
what provides it (real handshake layer, in-memory mesh, ...).
_Avoid_: session layer (that's the production adapter, not the seam)

### Retry

**Given-up**:
The terminal outcome of dialing a Host: the runtime stops trying,
whether the retry budget ran out or the peer Rejected the handshake.
Protocols observe this, not the reason.
_Avoid_: failed (non-terminal), aborted

### Testing

**In-memory mesh**:
A set of runtimes exchanging messages entirely in-process —
no wire, no handshake, deterministic delivery. The second adapter
behind the Sessions seam; lives in `prototest` for framework users.
_Avoid_: mock network, fake transport (it sits at the Sessions seam,
not the transport one)

**prototest**:
The exported testing module for protocol authors: the in-memory mesh
plus the fixture that stands up a runnable runtime around it.
_Avoid_: test helpers, test utils
