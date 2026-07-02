package transport

import (
	"fmt"
	"io"

	"github.com/antonionduarte/protorun/pkg/wire"
)

// Host identifies a peer by string IP-or-hostname plus port. The IP field
// may hold an IPv4 dotted-decimal address, an IPv6 address, or a hostname
// (net.Dial handles all three).
type Host struct {
	Port int
	IP   string
}

func NewHost(port int, ip string) Host {
	return Host{Port: port, IP: ip}
}

// String formats the host as "ip:port". Satisfies fmt.Stringer, so
// passing a Host as a slog attribute auto-formats correctly without
// the caller having to wrap it.
func (h Host) String() string {
	return fmt.Sprintf("%s:%d", h.IP, h.Port)
}

// WriteHost writes a Host as [uint16 LE host-len][host bytes][uint16 LE
// port]. Supports IPv4 dotted-decimal, IPv6, and FQDN.
func WriteHost(w io.Writer, h Host) error {
	if len(h.IP) > 0xFFFF {
		return fmt.Errorf("WriteHost: host string too long: %d bytes", len(h.IP))
	}
	if err := wire.WriteUint16(w, uint16(len(h.IP))); err != nil {
		return err
	}
	if _, err := w.Write([]byte(h.IP)); err != nil {
		return err
	}
	return wire.WriteUint16(w, uint16(h.Port))
}

// ReadHost reads a Host written by WriteHost.
func ReadHost(r io.Reader) (Host, error) {
	var h Host
	hostLen, err := wire.ReadUint16(r)
	if err != nil {
		return h, err
	}
	if hostLen > 0 {
		buf := make([]byte, hostLen)
		if _, err := io.ReadFull(r, buf); err != nil {
			return h, fmt.Errorf("ReadHost: %w", err)
		}
		h.IP = string(buf)
	}
	port, err := wire.ReadUint16(r)
	if err != nil {
		return h, err
	}
	h.Port = int(port)
	return h, nil
}
