package plumtree

import "time"

// Config tunes a Plumtree instance. The zero value is usable: New fills
// every unset field with a default. Timings follow the two-timer scheme
// of Leitão, Pereira and Rodrigues, "Epidemic Broadcast Trees" (2007):
// the first, longer timeout gives the eager (tree) path a chance to
// deliver before a GRAFT is issued; the second, shorter one bounds a
// retry if that GRAFT also stalls.
type Config struct {
	// MissingTimeout is how long a node waits after the first IHAVE for a
	// message before GRAFTing to pull it (the paper's timeout1). Default
	// 1s.
	MissingTimeout time.Duration

	// GraftRetryTimeout is the shorter retry interval used after a GRAFT
	// has been sent, in case that GRAFT also fails to produce the message
	// (the paper's timeout2). Default 500ms.
	GraftRetryTimeout time.Duration

	// LazyInterval is how often queued IHAVE announcements are flushed to
	// lazy peers as batched messages. Must be well below MissingTimeout.
	// Default 100ms.
	LazyInterval time.Duration

	// CacheSize is how many recently-delivered payloads are retained to
	// serve GRAFTs. It bounds memory and, with it, how far behind a peer
	// may fall and still be repaired by GRAFT. See the anti-entropy
	// caveat on Protocol. Default 1024.
	CacheSize int
}

func (c Config) withDefaults() Config {
	if c.MissingTimeout <= 0 {
		c.MissingTimeout = time.Second
	}
	if c.GraftRetryTimeout <= 0 {
		c.GraftRetryTimeout = 500 * time.Millisecond
	}
	if c.LazyInterval <= 0 {
		c.LazyInterval = 100 * time.Millisecond
	}
	if c.CacheSize <= 0 {
		c.CacheSize = 1024
	}
	return c
}
