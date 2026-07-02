package transport

import "testing"

// TestAddress_HostSatisfies is a compile-time + runtime check that
// Host implements Address. Removed methods or wrong signatures stop
// the test binary from building.
func TestAddress_HostSatisfies(t *testing.T) {
	var _ Address = Host{}
	var _ Address = (*Host)(nil)

	a := Host{IP: "127.0.0.1", Port: 5001}
	var addr Address = a
	if addr.String() != "127.0.0.1:5001" {
		t.Errorf("String(): got %q, want 127.0.0.1:5001", addr.String())
	}
	if !addr.Equal(a) {
		t.Errorf("Equal: same Host should report equal")
	}
	if addr.Equal(Host{IP: "127.0.0.1", Port: 5002}) {
		t.Errorf("Equal: different ports should not report equal")
	}
}

// fakeAddr is a custom Address implementation used to verify the
// interface is genuinely open: a non-Host type can plug in.
type fakeAddr struct{ id string }

func (f fakeAddr) String() string { return "fake://" + f.id }
func (f fakeAddr) Equal(other Address) bool {
	o, ok := other.(fakeAddr)
	return ok && o.id == f.id
}

// TestAddress_CustomImpl verifies a hand-written Address type works
// the same way Host does: string-formatting and equality both route
// through the interface.
func TestAddress_CustomImpl(t *testing.T) {
	var a Address = fakeAddr{id: "alpha"}
	if got := a.String(); got != "fake://alpha" {
		t.Errorf("String(): got %q, want fake://alpha", got)
	}
	if !a.Equal(fakeAddr{id: "alpha"}) {
		t.Errorf("Equal: same id should report equal")
	}
	if a.Equal(fakeAddr{id: "beta"}) {
		t.Errorf("Equal: different id should not report equal")
	}
}

// TestAddress_CrossTypeMismatch verifies Equal across different
// concrete Address types reports unequal: Host should never claim
// equality with a fakeAddr regardless of whether their String()
// outputs collide.
func TestAddress_CrossTypeMismatch(t *testing.T) {
	h := Host{IP: "alpha", Port: 0}
	f := fakeAddr{id: "alpha"}

	if h.Equal(f) {
		t.Errorf("Host.Equal(fakeAddr): expected false")
	}
	if f.Equal(h) {
		t.Errorf("fakeAddr.Equal(Host): expected false")
	}
}

// TestAddress_NilOther verifies Equal handles a nil Address argument
// without panicking, a reasonable contract for an interface method
// that may be invoked from generic plumbing.
func TestAddress_NilOther(t *testing.T) {
	var nilAddr Address
	h := Host{IP: "127.0.0.1", Port: 8080}
	if h.Equal(nilAddr) {
		t.Errorf("Host.Equal(nil): expected false")
	}
}
