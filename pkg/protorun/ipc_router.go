package protorun

import "sync"

// ipcRouter owns the runtime-wide IPC routing tables: one request
// handler per type (the "ask the membership protocol" pattern) and a
// many-subscriber fan-out for notifications (the "broadcast the view
// changed" pattern).
//
// Read on every SendRequest and PublishNotification; write at
// registration / subscription time. The notification fan-out is kept
// as an immutable, subscription-ordered slice per wireID, replaced
// wholesale on every (cold) mutation — so the (hot) publish path can
// return it without copying, and fan-out order is deterministic
// (subscription order) rather than map-iteration order, which the
// prototest simulator's reproducibility relies on.
type ipcRouter struct {
	requestMu          sync.RWMutex
	requestRoutes      map[uint64]requestRoute
	notificationMu     sync.RWMutex
	notificationFanout map[uint64][]notificationSub
}

func newIPCRouter() *ipcRouter {
	return &ipcRouter{
		requestRoutes:      make(map[uint64]requestRoute),
		notificationFanout: make(map[uint64][]notificationSub),
	}
}

// RegisterRequestRoute installs proto as the handler for wireID.
// Returns the previous route (if any) so callers can detect
// re-registration. Idempotent for same proto.
func (r *ipcRouter) RegisterRequestRoute(
	wireID uint64,
	proto *protoProtocol,
	handler func(Request, replyToken),
) (prev requestRoute, hadPrev bool) {
	r.requestMu.Lock()
	defer r.requestMu.Unlock()
	prev, hadPrev = r.requestRoutes[wireID]
	r.requestRoutes[wireID] = requestRoute{proto: proto, handler: handler}
	return prev, hadPrev
}

// Route returns the registered route for wireID, or (zero, false).
func (r *ipcRouter) Route(wireID uint64) (requestRoute, bool) {
	r.requestMu.RLock()
	route, ok := r.requestRoutes[wireID]
	r.requestMu.RUnlock()
	return route, ok
}

// Subscribe adds proto as a subscriber for wireID's notifications.
// Replaces any prior subscription from the same proto for the same
// wireID (keeping the original position). Copy-on-write: the stored
// slice is never mutated in place, because publishers may still be
// ranging over the previous snapshot.
func (r *ipcRouter) Subscribe(wireID uint64, proto *protoProtocol, fn func(Notification)) {
	r.notificationMu.Lock()
	defer r.notificationMu.Unlock()
	old := r.notificationFanout[wireID]
	subs := make([]notificationSub, len(old), len(old)+1)
	copy(subs, old)
	for i := range subs {
		if subs[i].proto == proto {
			subs[i].fn = fn
			r.notificationFanout[wireID] = subs
			return
		}
	}
	r.notificationFanout[wireID] = append(subs, notificationSub{proto: proto, fn: fn})
}

// Unsubscribe removes proto from the fan-out for wireID. No-op if it
// wasn't subscribed; cleans up the empty bucket so the fanout map
// doesn't accumulate entries for unsubscribed types. Copy-on-write,
// same as Subscribe.
func (r *ipcRouter) Unsubscribe(wireID uint64, proto *protoProtocol) {
	r.notificationMu.Lock()
	defer r.notificationMu.Unlock()
	r.notificationFanout[wireID] = removeSub(r.notificationFanout[wireID], proto)
	if len(r.notificationFanout[wireID]) == 0 {
		delete(r.notificationFanout, wireID)
	}
}

// RemoveOwner deletes every routing-table entry owned by proto: its
// request-handler routes and all of its notification subscriptions.
// Called by the supervisor during restart/stop so the old instance
// stops receiving requests and notifications before the fresh
// instance re-registers. Both tables are scanned rather than
// reverse-indexed: registration/removal are cold paths.
func (r *ipcRouter) RemoveOwner(proto *protoProtocol) {
	r.requestMu.Lock()
	for id, route := range r.requestRoutes {
		if route.proto == proto {
			delete(r.requestRoutes, id)
		}
	}
	r.requestMu.Unlock()

	r.notificationMu.Lock()
	for id, subs := range r.notificationFanout {
		trimmed := removeSub(subs, proto)
		if len(trimmed) == 0 {
			delete(r.notificationFanout, id)
			continue
		}
		r.notificationFanout[id] = trimmed
	}
	r.notificationMu.Unlock()
}

// removeSub returns subs without proto's entry. When proto is present
// a fresh slice is built (copy-on-write); when absent, subs is
// returned unchanged with no allocation.
func removeSub(subs []notificationSub, proto *protoProtocol) []notificationSub {
	for i := range subs {
		if subs[i].proto != proto {
			continue
		}
		out := make([]notificationSub, 0, len(subs)-1)
		out = append(out, subs[:i]...)
		return append(out, subs[i+1:]...)
	}
	return subs
}

// SnapshotSubscribers returns the subscribers for wireID in
// subscription order. The returned slice is the router's immutable
// current version (mutations replace it, never edit it), so the hot
// publish path performs zero copies and the caller can fan out
// without holding the mutex (mailbox pushes can block on slow
// consumers). Callers MUST NOT modify the returned slice.
func (r *ipcRouter) SnapshotSubscribers(wireID uint64) []notificationSub {
	r.notificationMu.RLock()
	subs := r.notificationFanout[wireID]
	r.notificationMu.RUnlock()
	return subs
}

// notificationSub is a flat (proto, handler) pair held in the
// fan-out table.
type notificationSub struct {
	proto *protoProtocol
	fn    func(Notification)
}
