// Command gossip is a runnable two-layer demo on top of the protorun
// framework: a membership protocol tracks which peers we are
// session-connected to, and a gossip protocol layered on top
// broadcasts payloads to every node in the network using eager push.
//
// Run multiple instances on different ports, each pointing at a
// couple of contacts, to form a small cluster. Each node will
// broadcast a "hello from <addr>" message every five seconds via
// gossip's optional periodic-broadcast hook.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/antonionduarte/protorun"
	"github.com/antonionduarte/protorun/cmd/gossip/gossip"
	"github.com/antonionduarte/protorun/cmd/gossip/membership"
	"github.com/antonionduarte/protorun/transport"
)

// portList is a flag.Value that accumulates -contact-port flags into a slice.
type portList []int

func (p *portList) String() string { return fmt.Sprintf("%v", []int(*p)) }
func (p *portList) Set(s string) error {
	port, err := strconv.Atoi(s)
	if err != nil {
		return err
	}
	*p = append(*p, port)
	return nil
}

func main() {
	selfIP := flag.String("self-ip", "127.0.0.1", "IP address for this node")
	selfPort := flag.Int("self-port", 0, "TCP port for this node (required)")
	contactIP := flag.String("contact-ip", "127.0.0.1", "IP address shared by all contact peers")
	var contactPorts portList
	flag.Var(&contactPorts, "contact-port", "TCP port of a contact peer (repeatable)")
	flag.Parse()

	if *selfPort == 0 {
		fmt.Fprintln(os.Stderr, "self-port is required (use -self-port N)")
		os.Exit(2)
	}

	logger := slog.Default()
	self := transport.NewHost(*selfPort, *selfIP)

	contacts := make([]transport.Host, 0, len(contactPorts))
	for _, p := range contactPorts {
		contacts = append(contacts, transport.NewHost(p, *contactIP))
	}

	logger.Info("gossip node starting", "self", self.String(), "contacts", len(contacts))

	heartbeat := []byte("hello from " + self.String())
	rt := protorun.New(self,
		protorun.WithLogger(logger),
		protorun.WithTCPTransport(context.Background()),
	)
	rt.Register(membership.New(contacts))
	rt.Register(gossip.New(func(payload []byte) {
		logger.Info("gossip delivered", "payload", string(payload))
	}).EnablePeriodic(5*time.Second, func() []byte { return heartbeat }))

	if err := rt.Run(); err != nil {
		logger.Error("runtime exited with error", "err", err)
		os.Exit(1)
	}
}
