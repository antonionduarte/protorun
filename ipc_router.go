package protorun

import "sync"

// ipcRouter owns the runtime-wide IPC routing tables: one request
// handler per type (the "ask the membership protocol" pattern) and a
// many-subscriber fan-out for notifications (the "broadcast the view
// changed" pattern).
//
// Read on every SendRequest and PublishNotification; write at
// registration / subscription time. sync.RWMutex on the request
// side, sync.RWMutex on the notification side.
type ipcRouter struct {
	requestMu          sync.RWMutex
	requestRoutes      map[uint64]requestRoute
	notificationMu     sync.RWMutex
	notificationFanout map[uint64]map[*protoProtocol]func(Notification)
}

func newIPCRouter() *ipcRouter {
	return &ipcRouter{
		requestRoutes:      make(map[uint64]requestRoute),
		notificationFanout: make(map[uint64]map[*protoProtocol]func(Notification)),
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
// wireID.
func (r *ipcRouter) Subscribe(wireID uint64, proto *protoProtocol, fn func(Notification)) {
	r.notificationMu.Lock()
	defer r.notificationMu.Unlock()
	subs, ok := r.notificationFanout[wireID]
	if !ok {
		subs = make(map[*protoProtocol]func(Notification))
		r.notificationFanout[wireID] = subs
	}
	subs[proto] = fn
}

// Unsubscribe removes proto from the fan-out for wireID. No-op if it
// wasn't subscribed; cleans up the empty bucket so the fanout map
// doesn't accumulate entries for unsubscribed types.
func (r *ipcRouter) Unsubscribe(wireID uint64, proto *protoProtocol) {
	r.notificationMu.Lock()
	defer r.notificationMu.Unlock()
	subs, ok := r.notificationFanout[wireID]
	if !ok {
		return
	}
	delete(subs, proto)
	if len(subs) == 0 {
		delete(r.notificationFanout, wireID)
	}
}

// RemoveOwner deletes every routing-table entry owned by proto: its
// request-handler routes and all of its notification subscriptions.
// Called by the supervisor during restart/stop so the old instance
// stops receiving requests and notifications before the fresh
// instance re-registers. Both maps are scanned rather than
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
		delete(subs, proto)
		if len(subs) == 0 {
			delete(r.notificationFanout, id)
		}
	}
	r.notificationMu.Unlock()
}

// SnapshotSubscribers returns the (proto, handler) pairs subscribed
// to wireID. Snapshotted under lock so the caller can fan out without
// holding the mutex (channel sends can block on slow consumers).
func (r *ipcRouter) SnapshotSubscribers(wireID uint64) []notificationSub {
	r.notificationMu.RLock()
	subs := r.notificationFanout[wireID]
	out := make([]notificationSub, 0, len(subs))
	for proto, fn := range subs {
		out = append(out, notificationSub{proto: proto, fn: fn})
	}
	r.notificationMu.RUnlock()
	return out
}

// notificationSub is a flat (proto, handler) pair returned by
// SnapshotSubscribers. Kept tiny so the fanout snapshot doesn't
// allocate a map on every Publish.
type notificationSub struct {
	proto *protoProtocol
	fn    func(Notification)
}
