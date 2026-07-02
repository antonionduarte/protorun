package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"

	"github.com/antonionduarte/protorun"
	"github.com/antonionduarte/protorun/cmd/pingpong/protocol"
	"github.com/antonionduarte/protorun/config"
	"github.com/antonionduarte/protorun/transport"
)

func main() {
	configPath := flag.String("config", "", "YAML config file for logging (required)")
	selfIP := flag.String("self-ip", "127.0.0.1", "IP address for this pingpong node")
	selfPort := flag.Int("self-port", 0, "TCP port for this pingpong node (required)")
	peerIP := flag.String("peer-ip", "127.0.0.1", "IP address for the peer pingpong node")
	peerPort := flag.Int("peer-port", 0, "TCP port for the peer pingpong node (required)")
	flag.Parse()

	if *configPath == "" {
		panic("config file is required (use -config path/to/config.yaml)")
	}
	if *selfPort == 0 {
		panic("self-port is required (use -self-port N)")
	}
	if *peerPort == 0 {
		panic("peer-port is required (use -peer-port N)")
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		panic(err)
	}

	// The pingpong protocol's own knobs live under the "pingpong:"
	// section; a missing section just means the zero-value Config
	// (StartSeq 0), not an error. This is the "no framework magic"
	// wiring config's package doc describes: main reads the section
	// and hands it to the protocol's own constructor.
	pingCfg, err := config.Section[protocol.Config](cfg, "pingpong")
	if err != nil && !errors.Is(err, config.ErrSectionNotFound) {
		panic(err)
	}

	myself := transport.NewHost(*selfPort, *selfIP)
	peer := transport.NewHost(*peerPort, *peerIP)

	rt := protorun.New(myself,
		append(cfg.Runtime().Options(), protorun.WithTCPTransport(context.Background()))...,
	)
	slog.SetDefault(rt.Logger())
	rt.Logger().Info("starting pingpong node",
		"self", myself.String(),
		"peer", peer.String(),
		"startSeq", pingCfg.StartSeq,
	)

	rt.Register(protocol.NewPingPongProtocol(peer, pingCfg))

	if err := rt.Run(); err != nil {
		panic(err)
	}
}
