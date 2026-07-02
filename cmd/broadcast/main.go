// Command broadcast is the flagship protorun demo: it stacks Plumtree
// (epidemic broadcast trees) over HyParView (partial-view membership)
// over TCP. Each node maintains a self-healing overlay via HyParView and
// disseminates messages over a self-optimising spanning tree via
// Plumtree — the two protocols coordinate only through the
// protocols/membership IPC contract, never by direct calls.
//
// Type a line on stdin and it is broadcast to the whole cluster; every
// node prints the lines it delivers. Run several instances on different
// ports, each pointing at one or two contacts, to form a cluster:
//
//	go run ./cmd/broadcast -self-port 8001
//	go run ./cmd/broadcast -self-port 8002 -contact-port 8001
//	go run ./cmd/broadcast -self-port 8003 -contact-port 8002
//
// Then type into any node's terminal and watch it appear on all of them.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/antonionduarte/protorun/pkg/protocols/hyparview"
	"github.com/antonionduarte/protorun/pkg/protocols/plumtree"
	"github.com/antonionduarte/protorun/pkg/protorun"
	"github.com/antonionduarte/protorun/pkg/transport"
)

// shutdownTimeout bounds teardown when stdin closes.
const shutdownTimeout = 5 * time.Second

// portList accumulates repeated -contact-port flags.
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
	contactIP := flag.String("contact-ip", "127.0.0.1", "IP shared by all contact peers")
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
	logger.Info("broadcast node starting", "self", self.String(), "contacts", len(contacts))

	bc := &broadcaster{}
	rt := protorun.New(self,
		protorun.WithLogger(logger),
		protorun.WithTCPTransport(context.Background()),
	)
	rt.Register(hyparview.New(self, hyparview.Config{Contacts: contacts}))
	rt.Register(plumtree.New(self, plumtree.Config{}))
	rt.Register(&printer{self: self})
	rt.Register(bc)

	if err := rt.Start(); err != nil {
		logger.Error("runtime failed to start", "err", err)
		os.Exit(1)
	}

	// Read stdin lines and broadcast each. Blocks until EOF (Ctrl-D or a
	// closed pipe), then shuts the runtime down cleanly.
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		bc.broadcast([]byte(line))
	}
	if err := rt.Shutdown(shutdownTimeout); err != nil {
		logger.Error("runtime shutdown error", "err", err)
		os.Exit(1)
	}
}

// printer prints every broadcast this node delivers.
type printer struct{ self transport.Host }

func (*printer) Start(ctx protorun.ProtocolContext) {
	protorun.SubscribeNotification(ctx, func(ev plumtree.Delivered) {
		fmt.Printf("[delivered from %s] %s\n", ev.From.String(), string(ev.Payload))
	})
}
func (*printer) Init(protorun.ProtocolContext) {}

// broadcaster turns an application-goroutine call (broadcast, invoked
// from the stdin loop) into an on-loop plumtree.Broadcast request — the
// same "cross-boundary coordination is IPC" pattern as the gossip
// example's TriggerBroadcast.
type broadcaster struct{ ctx protorun.ProtocolContext }

func (b *broadcaster) Start(ctx protorun.ProtocolContext) { b.ctx = ctx }
func (*broadcaster) Init(protorun.ProtocolContext)        {}

func (b *broadcaster) broadcast(payload []byte) {
	protorun.SendRequest(b.ctx, &plumtree.Broadcast{Payload: payload},
		func(*plumtree.BroadcastAck, error) {})
}
