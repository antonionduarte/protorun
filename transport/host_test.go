package transport

import (
	"bytes"
	"fmt"
	"testing"
)

// TestHost_String confirms Host satisfies fmt.Stringer with the
// expected "ip:port" formatting. Used in slog attributes throughout
// the codebase, so this is a public-API contract.
func TestHost_String(t *testing.T) {
	cases := []struct {
		host Host
		want string
	}{
		{Host{IP: "127.0.0.1", Port: 8080}, "127.0.0.1:8080"},
		{Host{IP: "2001:db8::1", Port: 9999}, "2001:db8::1:9999"},
		{Host{IP: "host.example.com", Port: 443}, "host.example.com:443"},
		{Host{IP: "", Port: 0}, ":0"},
	}
	for _, tc := range cases {
		if got := tc.host.String(); got != tc.want {
			t.Errorf("Host{%q, %d}.String() = %q, want %q",
				tc.host.IP, tc.host.Port, got, tc.want)
		}
	}
}

// TestHost_StringerInterface verifies Host implements fmt.Stringer at
// compile time; slog formatting and many fmt.Sprintf calls depend on
// this.
func TestHost_StringerInterface(t *testing.T) {
	var _ fmt.Stringer = Host{}
}

func TestHost_RoundTrip_IPv4(t *testing.T) {
	original := Host{IP: "192.168.1.42", Port: 6443}
	var buf bytes.Buffer
	if err := WriteHost(&buf, original); err != nil {
		t.Fatalf("WriteHost: %v", err)
	}
	got, err := ReadHost(&buf)
	if err != nil {
		t.Fatalf("ReadHost: %v", err)
	}
	if got != original {
		t.Fatalf("round-trip: got %+v, want %+v", got, original)
	}
}

func TestHost_RoundTrip_IPv6(t *testing.T) {
	original := Host{IP: "2001:db8::1", Port: 9999}
	var buf bytes.Buffer
	if err := WriteHost(&buf, original); err != nil {
		t.Fatalf("WriteHost: %v", err)
	}
	got, err := ReadHost(&buf)
	if err != nil {
		t.Fatalf("ReadHost: %v", err)
	}
	if got != original {
		t.Fatalf("round-trip: got %+v, want %+v", got, original)
	}
}

func TestHost_RoundTrip_Hostname(t *testing.T) {
	original := Host{IP: "node-3.example.com", Port: 443}
	var buf bytes.Buffer
	if err := WriteHost(&buf, original); err != nil {
		t.Fatalf("WriteHost: %v", err)
	}
	got, err := ReadHost(&buf)
	if err != nil {
		t.Fatalf("ReadHost: %v", err)
	}
	if got != original {
		t.Fatalf("round-trip: got %+v, want %+v", got, original)
	}
}
