package main

import (
	"context"
	"flag"
	"log/slog"

	"github.com/antonionduarte/protorun"
	rtconfig "github.com/antonionduarte/protorun/cmd/pingpong/config"
	"github.com/antonionduarte/protorun/cmd/pingpong/protocol"
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

	cfg, err := rtconfig.LoadConfig(*configPath)
	if err != nil {
		panic(err)
	}

	logger := protorun.NewLoggerFromConfig(cfg.Logging)
	slog.SetDefault(logger)

	myself := transport.NewHost(*selfPort, *selfIP)
	peer := transport.NewHost(*peerPort, *peerIP)

	logger.Info("starting pingpong node",
		"self", myself.String(),
		"peer", peer.String(),
	)

	rt := protorun.New(myself,
		protorun.WithLogger(logger),
		protorun.WithTCPTransport(context.Background()),
	)
	rt.Register(protocol.NewPingPongProtocol(peer))

	if err := rt.Run(); err != nil {
		panic(err)
	}
}
