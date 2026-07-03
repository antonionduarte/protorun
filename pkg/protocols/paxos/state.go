package paxos

// This file holds the pure decision logic of single-decree Paxos —
// ballot arithmetic, the promise/accept predicates, majority sizing, and
// the value-adoption rule — with no dependency on the runtime or the
// event loop. Keeping it separate is what lets the safety-critical rules
// be unit-tested in isolation (state_test.go), independently of any
// simulation.

// promiseInfo is the accepted-value summary an acceptor returns in its
// Promise (Phase 1b): the highest (ballot, value) it has accepted, or
// HasAccepted == false when it has accepted nothing. A proposer collects
// these across a promise quorum and feeds them to chooseValue.
type promiseInfo struct {
	acceptedBallot uint64
	acceptedValue  []byte
	hasAccepted    bool
}

// majoritySize is the smallest number of acceptors that forms a strict
// majority of an n-member group: any two majorities of the same group
// intersect in at least one acceptor, which is the quorum-intersection
// property Paxos safety rests on.
func majoritySize(n int) int { return n/2 + 1 }

// nextBallot returns the smallest ballot in node index's disjoint ballot
// sequence that is strictly greater than maxSeen. Node i's sequence is
// {round*n + i : round >= 1}, so:
//
//   - Disjointness: for i != j in [0,n), round1*n+i == round2*n+j would
//     force i == j (mod n), impossible for distinct indices below n. No
//     two nodes ever pick the same ballot number, so a ballot uniquely
//     identifies its proposer — no tie-breaking needed anywhere.
//   - Monotonicity: round = maxSeen/n + 1 gives round*n > maxSeen, so the
//     result strictly exceeds maxSeen even after adding i.
//   - Non-zero: round >= 1 and i >= 0, so the result is >= n > 0. Ballot 0
//     is reserved as the null ballot ("nothing promised/accepted yet").
func nextBallot(maxSeen uint64, n, index int) uint64 {
	round := maxSeen/uint64(n) + 1
	return round*uint64(n) + uint64(index)
}

// canPromise reports whether an acceptor holding promised may promise a
// Prepare carrying ballot: it must be strictly greater than any ballot
// already promised (a promise is a commitment never to accept a lower
// ballot).
func canPromise(ballot, promised uint64) bool { return ballot > promised }

// canAccept reports whether an acceptor holding promised may accept an
// Accept carrying ballot: it must be at least the promised ballot. The
// asymmetry with canPromise (>= vs >) is deliberate and load-bearing: an
// acceptor that promised ballot n must still accept n itself — that is the
// whole point of having promised it.
func canAccept(ballot, promised uint64) bool { return ballot >= promised }

// chooseValue implements the Phase 2a value-selection rule: among the
// promises in a quorum, adopt the value of the highest ballot any acceptor
// reports having already accepted; if none has accepted anything, the
// proposer is free to propose its own value. Returns the chosen value and
// whether it was adopted (true) rather than the proposer's own (false).
//
// This is the crux of Paxos safety. If some value was already chosen (a
// majority accepted it at ballot b), then any later quorum intersects that
// majority in at least one acceptor, which reports (b, value); no ballot
// above b can carry a different value, so once chosen the value can never
// change. Ties on acceptedBallot cannot disagree on value — a ballot is
// owned by one proposer and carries one value — so iteration order does
// not affect the outcome.
func chooseValue(infos []promiseInfo, own []byte) ([]byte, bool) {
	var (
		best    []byte
		bestBal uint64
		adopted bool
	)
	for _, in := range infos {
		if in.hasAccepted && (!adopted || in.acceptedBallot > bestBal) {
			best, bestBal, adopted = in.acceptedValue, in.acceptedBallot, true
		}
	}
	if adopted {
		return best, true
	}
	return own, false
}
