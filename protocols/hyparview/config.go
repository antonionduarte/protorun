package hyparview

import (
	"time"

	"github.com/antonionduarte/protorun/transport"
)

// Config tunes a HyParView instance. The zero value is usable: New fills
// every unset field with the paper's defaults (scaled for a small
// cluster). Sizes and walk lengths follow Leitão, Pereira and Rodrigues,
// "HyParView: A Membership Protocol for Reliable Gossip-Based Broadcast"
// (2007); the periods are protorun defaults, tuned so the sim tests
// converge quickly on the virtual clock.
type Config struct {
	// Contacts are the bootstrap nodes this node JOINs on Init. An empty
	// contact list makes this node a pure rendezvous point that others
	// join (the first node in a cluster has no contacts).
	Contacts []transport.Host

	// ActiveSize is the target active-view size (the paper's log(n)+c).
	// Active-view peers are session-backed and symmetric. Default 5.
	ActiveSize int

	// PassiveSize is the maximum passive-view size — the larger,
	// unconnected sample the shuffle keeps fresh. Default 30.
	PassiveSize int

	// ARWL (Active Random Walk Length) is the initial TTL of a
	// ForwardJoin and of a Shuffle walk. Default 6.
	ARWL int

	// PRWL (Passive Random Walk Length) is the TTL at which a ForwardJoin
	// also files the joining node into the passive view. Must be < ARWL.
	// Default 3.
	PRWL int

	// ShuffleActive (ka) is how many active-view peers a shuffle offers.
	// Default 3.
	ShuffleActive int

	// ShufflePassive (kp) is how many passive-view peers a shuffle
	// offers. Default 4.
	ShufflePassive int

	// ShuffleInterval is how often a node initiates a shuffle. Default
	// 10s.
	ShuffleInterval time.Duration

	// JoinInterval is how often a node with an empty active view
	// re-attempts JOIN to its contacts (bootstrap and full-isolation
	// recovery). Default 5s.
	JoinInterval time.Duration

	// NeighborTimeout bounds how long a pending Neighbor request waits
	// for acceptance before the candidate is abandoned (covers a silent
	// connect failure or a slow/rejecting peer). Default 3s.
	NeighborTimeout time.Duration
}

// fillDefaults fills unset fields in place. Pointer receiver so the heavy
// Config is not copied.
func (c *Config) fillDefaults() {
	c.ActiveSize = intOr(c.ActiveSize, 5)
	c.PassiveSize = intOr(c.PassiveSize, 30)
	c.ARWL = intOr(c.ARWL, 6)
	c.PRWL = intOr(c.PRWL, 3)
	c.ShuffleActive = intOr(c.ShuffleActive, 3)
	c.ShufflePassive = intOr(c.ShufflePassive, 4)
	c.ShuffleInterval = durOr(c.ShuffleInterval, 10*time.Second)
	c.JoinInterval = durOr(c.JoinInterval, 5*time.Second)
	c.NeighborTimeout = durOr(c.NeighborTimeout, 3*time.Second)
	if c.PRWL >= c.ARWL {
		c.PRWL = c.ARWL - 1
	}
}

func intOr(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}

func durOr(v, def time.Duration) time.Duration {
	if v <= 0 {
		return def
	}
	return v
}
