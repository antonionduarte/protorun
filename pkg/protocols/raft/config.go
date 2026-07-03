package raft

import (
	"time"

	"github.com/antonionduarte/protorun/pkg/transport"
)

// Config tunes a Raft instance. The zero value is *almost* usable — New
// fills every unset timing field with a default — but Peers must be set:
// a Raft consensus group has fixed, statically-configured membership
// (see the package doc for why HyParView is the wrong substrate). Peers
// lists the OTHER members of the group; self is passed to New separately.
//
// # Why a static peer set
//
// Raft's safety proofs assume a fixed configuration (§5) — every server
// knows the full membership and the majority size it must reach. Dynamic
// membership is a separate, carefully-staged sub-protocol (§6, joint
// consensus) that is explicitly out of scope here. A partial-view
// membership protocol like HyParView is the exact opposite of what Raft
// needs: it deliberately gives each node a small, changing sample of the
// cluster and never a global view, so a node could never compute "have I
// heard from a majority of ALL servers". Consensus wants total, stable
// membership; gossip wants partial, churning membership. They do not
// compose, which is why Raft takes its peers by Config, not from a
// membership contract.
type Config struct {
	// Peers are the other members of the consensus group (this node is
	// implicitly a member and must NOT appear here). Required and fixed
	// for the lifetime of the group; New sorts a copy for deterministic
	// iteration.
	Peers []transport.Host

	// HeartbeatInterval is how often a leader sends AppendEntries (empty
	// or carrying entries) to every follower. Must be comfortably below
	// ElectionTimeoutMin so a live leader keeps followers from timing
	// out. Default 50ms.
	HeartbeatInterval time.Duration

	// ElectionTimeoutMin / ElectionTimeoutMax bound the randomized
	// election timeout. Each reset picks a fresh timeout uniformly in
	// [min, max) from the node's per-node RNG, which is what breaks
	// symmetry between simultaneously-timing-out candidates (§5.2).
	// Defaults 150ms / 300ms.
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration

	// ReconnectInterval is how often a node re-dials any peer it does not
	// currently hold a session with. This is the only recovery path after
	// a partition heals (the framework never silently reopens a torn-down
	// session). Default 100ms.
	ReconnectInterval time.Duration

	// Storage is the durable persistence seam for currentTerm, votedFor,
	// and the log. Default MemoryStorage — see its doc for the loud
	// caveat that in-memory storage voids Raft's crash-recovery
	// guarantees.
	Storage Storage
}

// fillDefaults fills unset timing fields and installs a MemoryStorage if
// none was supplied. Pointer receiver so the Config is not copied.
func (c *Config) fillDefaults() {
	c.HeartbeatInterval = durOr(c.HeartbeatInterval, 50*time.Millisecond)
	c.ElectionTimeoutMin = durOr(c.ElectionTimeoutMin, 150*time.Millisecond)
	c.ElectionTimeoutMax = durOr(c.ElectionTimeoutMax, 300*time.Millisecond)
	c.ReconnectInterval = durOr(c.ReconnectInterval, 100*time.Millisecond)
	if c.ElectionTimeoutMax <= c.ElectionTimeoutMin {
		c.ElectionTimeoutMax = c.ElectionTimeoutMin * 2
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
