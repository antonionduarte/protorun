package paxos

import (
	"time"

	"github.com/antonionduarte/protorun/pkg/transport"
)

// Config tunes a Paxos instance. The zero value is *almost* usable — New
// fills every unset timing field with a default — but Peers must be set:
// a Paxos synod has fixed, statically-configured membership so that every
// node can compute the majority size of the whole group. Peers lists the
// OTHER members of the group; self is passed to New separately.
//
// # Why a static peer set
//
// The safety of Paxos rests on quorum intersection: any two majorities of
// the SAME fixed membership share an acceptor. A node must therefore know
// the full roster to know how many acceptors form a majority. A partial-
// view membership protocol (HyParView and friends) deliberately gives each
// node a small, changing sample of the cluster and never a global view —
// exactly what a synod cannot use. Consensus wants total, stable
// membership; gossip wants partial, churning membership. They do not
// compose, which is why Paxos takes its peers by Config, not from a
// membership contract. (This is the same rationale as pkg/protocols/raft.)
type Config struct {
	// Peers are the other members of the consensus group (this node is
	// implicitly a member and must NOT appear here). Required and fixed for
	// the lifetime of the group; New sorts a copy for deterministic
	// iteration.
	Peers []transport.Host

	// RetryTimeoutMin / RetryTimeoutMax bound the randomized backoff a
	// proposer waits before abandoning a stalled round and retrying with a
	// higher ballot. Each retry picks a fresh delay uniformly in [min, max)
	// from the node's per-node RNG; randomizing it is what keeps two
	// dueling proposers from livelocking — they re-propose at desynchronized
	// times, so one eventually completes a round uninterrupted. Must be
	// comfortably above a message round-trip so a healthy round is not cut
	// short. Defaults 150ms / 300ms.
	RetryTimeoutMin time.Duration
	RetryTimeoutMax time.Duration

	// ReconnectInterval is how often a node re-dials any peer it does not
	// currently hold a session with. This is the only recovery path after a
	// partition heals (the framework never silently reopens a torn-down
	// session), and it is also what drives the catch-up re-announcement of
	// accepted values on reconnect. Default 100ms.
	ReconnectInterval time.Duration

	// Storage is the durable persistence seam for the acceptor's promised
	// ballot and accepted (ballot, value). Default MemoryStorage — see its
	// doc for the loud caveat that in-memory storage voids Paxos's
	// crash-recovery guarantees.
	Storage Storage
}

// fillDefaults fills unset timing fields and installs a MemoryStorage if
// none was supplied. Pointer receiver so the Config is not copied.
func (c *Config) fillDefaults() {
	c.RetryTimeoutMin = durOr(c.RetryTimeoutMin, 150*time.Millisecond)
	c.RetryTimeoutMax = durOr(c.RetryTimeoutMax, 300*time.Millisecond)
	c.ReconnectInterval = durOr(c.ReconnectInterval, 100*time.Millisecond)
	if c.RetryTimeoutMax <= c.RetryTimeoutMin {
		c.RetryTimeoutMax = c.RetryTimeoutMin * 2
	}
	if c.Storage == nil {
		c.Storage = NewMemoryStorage()
	}
}

func durOr(v, def time.Duration) time.Duration {
	if v <= 0 {
		return def
	}
	return v
}
