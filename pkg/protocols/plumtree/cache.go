package plumtree

// payloadCache is a bounded FIFO of delivered payloads kept so a GRAFT
// can be answered by replaying the message. It is deliberately finite:
// Plumtree is not a full anti-entropy protocol, so a peer that falls
// further behind than the cache retains cannot be repaired by GRAFT (see
// the anti-entropy caveat on Protocol). Eviction is oldest-first.
type payloadCache struct {
	max   int
	items map[MessageID][]byte
	order []MessageID // insertion order, for FIFO eviction
}

func newPayloadCache(max int) *payloadCache {
	return &payloadCache{max: max, items: make(map[MessageID][]byte)}
}

// put stores payload under id. A repeated id is ignored (the first,
// canonical copy is kept). Inserting past the cap evicts the oldest
// entry.
func (c *payloadCache) put(id MessageID, payload []byte) {
	if _, ok := c.items[id]; ok {
		return
	}
	c.items[id] = payload
	c.order = append(c.order, id)
	if len(c.order) > c.max {
		oldest := c.order[0]
		c.order = c.order[1:]
		delete(c.items, oldest)
	}
}

// get returns the payload for id and whether it is still cached.
func (c *payloadCache) get(id MessageID) ([]byte, bool) {
	p, ok := c.items[id]
	return p, ok
}

func (c *payloadCache) len() int { return len(c.items) }
