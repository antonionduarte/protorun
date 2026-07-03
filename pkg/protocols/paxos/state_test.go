package paxos

import (
	"bytes"
	"testing"
)

// These unit tests pin the pure Paxos decision logic — ballot ordering and
// disjointness, the promise/accept predicates, the value-adoption rule, and
// majority arithmetic — independently of the event loop, so a regression in
// the algorithm is caught without a full simulation.

func TestMajoritySize(t *testing.T) {
	cases := map[int]int{1: 1, 2: 2, 3: 2, 4: 3, 5: 3, 6: 4, 7: 4}
	for n, want := range cases {
		if got := majoritySize(n); got != want {
			t.Fatalf("majoritySize(%d) = %d, want %d", n, got, want)
		}
	}
}

// TestNextBallot_OrderingAndDisjointness is the safety-critical ballot
// property: every node's ballots are strictly increasing above what it has
// seen, and no two distinct nodes ever mint the same ballot number.
func TestNextBallot_OrderingAndDisjointness(t *testing.T) {
	const n = 5

	// Strictly greater than maxSeen, and non-zero, for every node.
	for idx := range n {
		for _, maxSeen := range []uint64{0, 1, 4, 5, 6, 99, 1000} {
			b := nextBallot(maxSeen, n, idx)
			if b <= maxSeen {
				t.Fatalf("nextBallot(%d, %d, %d) = %d, not > maxSeen", maxSeen, n, idx, b)
			}
			if b == 0 {
				t.Fatalf("nextBallot produced the reserved null ballot 0 (idx=%d, maxSeen=%d)", idx, maxSeen)
			}
			if int(b%uint64(n)) != idx {
				t.Fatalf("nextBallot(%d,%d,%d)=%d is not in node %d's sequence (mod n = %d)",
					maxSeen, n, idx, b, idx, b%uint64(n))
			}
		}
	}

	// Disjointness: across the first several rounds, no ballot value repeats
	// across distinct nodes.
	seen := map[uint64]int{}
	for idx := range n {
		maxSeen := uint64(0)
		for range 20 {
			b := nextBallot(maxSeen, n, idx)
			if owner, dup := seen[b]; dup && owner != idx {
				t.Fatalf("ballot %d minted by both node %d and node %d", b, owner, idx)
			}
			seen[b] = idx
			maxSeen = b // simulate this node advancing its own sequence
		}
	}
}

// TestNextBallot_JumpsPastObserved confirms a proposer that observes a
// higher ballot (via a NACK) picks a ballot strictly above it, in its own
// sequence — so it does not waste a round re-proposing a losing ballot.
func TestNextBallot_JumpsPastObserved(t *testing.T) {
	const n = 5
	// Node 0 observes ballot 13 (owned by node 3: 13 = 2*5 + 3). Its next
	// ballot must exceed 13 and be in node 0's sequence.
	b := nextBallot(13, n, 0)
	if b <= 13 || b%n != 0 {
		t.Fatalf("nextBallot(13,5,0) = %d, want > 13 and in node 0's sequence", b)
	}
	if b != 15 { // round = 13/5+1 = 3, ballot = 3*5 + 0 = 15
		t.Fatalf("nextBallot(13,5,0) = %d, want 15", b)
	}
}

func TestPromiseAcceptPredicates(t *testing.T) {
	// canPromise is strict-greater; canAccept is greater-or-equal. The
	// asymmetry lets an acceptor that promised n still accept n.
	if canPromise(5, 5) {
		t.Fatal("canPromise(5,5) should be false: a promise must beat the promised ballot")
	}
	if !canPromise(6, 5) {
		t.Fatal("canPromise(6,5) should be true")
	}
	if canPromise(4, 5) {
		t.Fatal("canPromise(4,5) should be false")
	}
	if !canAccept(5, 5) {
		t.Fatal("canAccept(5,5) should be true: an acceptor must accept the ballot it promised")
	}
	if !canAccept(6, 5) {
		t.Fatal("canAccept(6,5) should be true")
	}
	if canAccept(4, 5) {
		t.Fatal("canAccept(4,5) should be false")
	}
}

func TestChooseValue_AdoptionRule(t *testing.T) {
	own := []byte("own")

	t.Run("no accepted value: proposer's own", func(t *testing.T) {
		infos := []promiseInfo{{hasAccepted: false}, {hasAccepted: false}}
		got, adopted := chooseValue(infos, own)
		if adopted || !bytes.Equal(got, own) {
			t.Fatalf("expected own value un-adopted, got %q adopted=%v", got, adopted)
		}
	})

	t.Run("single accepted value: adopt it", func(t *testing.T) {
		infos := []promiseInfo{
			{hasAccepted: false},
			{hasAccepted: true, acceptedBallot: 7, acceptedValue: []byte("A")},
		}
		got, adopted := chooseValue(infos, own)
		if !adopted || !bytes.Equal(got, []byte("A")) {
			t.Fatalf("expected to adopt %q, got %q adopted=%v", "A", got, adopted)
		}
	})

	t.Run("multiple accepted: adopt the highest ballot", func(t *testing.T) {
		// The §Phase-2a crux: with two different accepted values in the quorum,
		// the one at the HIGHER ballot must win, regardless of slice order.
		lowFirst := []promiseInfo{
			{hasAccepted: true, acceptedBallot: 3, acceptedValue: []byte("low")},
			{hasAccepted: true, acceptedBallot: 9, acceptedValue: []byte("high")},
		}
		got, adopted := chooseValue(lowFirst, own)
		if !adopted || !bytes.Equal(got, []byte("high")) {
			t.Fatalf("low-first: expected %q, got %q", "high", got)
		}
		highFirst := []promiseInfo{
			{hasAccepted: true, acceptedBallot: 9, acceptedValue: []byte("high")},
			{hasAccepted: true, acceptedBallot: 3, acceptedValue: []byte("low")},
		}
		got, adopted = chooseValue(highFirst, own)
		if !adopted || !bytes.Equal(got, []byte("high")) {
			t.Fatalf("high-first: expected %q, got %q", "high", got)
		}
	})
}

// TestAcceptorPredicates_ThroughProtocol exercises the acceptor state
// machine (promise then accept) through a minimal Protocol value (no
// runtime), confirming the durable-state transitions and the NACK ballots.
func TestAcceptorPredicates_ThroughProtocol(t *testing.T) {
	p := &Protocol{cfg: Config{Storage: NewMemoryStorage()}}

	// Promise ballot 5: succeeds, raises promised.
	if ok, mb, _ := p.acceptPrepare(5); !ok || mb != 5 {
		t.Fatalf("acceptPrepare(5) = ok=%v mb=%d, want true/5", ok, mb)
	}
	// Promise ballot 3: rejected, NACK carries the promised 5.
	if ok, mb, _ := p.acceptPrepare(3); ok || mb != 5 {
		t.Fatalf("acceptPrepare(3) = ok=%v mb=%d, want false/5", ok, mb)
	}
	// Accept ballot 5 (== promised): succeeds.
	if ok, _ := p.acceptAccept(5, []byte("v5")); !ok {
		t.Fatal("acceptAccept(5) should succeed at promised==5")
	}
	if !p.hasAccepted || p.acceptedBallot != 5 || string(p.acceptedValue) != "v5" {
		t.Fatalf("accepted state wrong: %+v", p)
	}
	// Accept ballot 4 (< promised): rejected.
	if ok, mb := p.acceptAccept(4, []byte("v4")); ok || mb != 5 {
		t.Fatalf("acceptAccept(4) = ok=%v mb=%d, want false/5", ok, mb)
	}
	// A later promise returns the accepted (ballot, value) for adoption.
	ok, _, info := p.acceptPrepare(9)
	if !ok || !info.hasAccepted || info.acceptedBallot != 5 || string(info.acceptedValue) != "v5" {
		t.Fatalf("acceptPrepare(9) info = %+v, want accepted (5, v5)", info)
	}
}
