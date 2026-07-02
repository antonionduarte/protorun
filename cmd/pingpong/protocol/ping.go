package protocol

import (
	"log/slog"

	"github.com/antonionduarte/protorun"
	"github.com/antonionduarte/protorun/transport"
)

type PingPongProtocol struct {
	peer transport.Host
	seq  uint64

	logger *slog.Logger
	ctx    protorun.ProtocolContext
}

// Config is the pingpong protocol's own section of the node's YAML
// config (see the "pingpong:" key in pingpong.example.yaml), decoded
// via config.Section[Config](f, "pingpong"). It carries no framework
// meaning; NewPingPongProtocol is the only thing that reads it.
type Config struct {
	// StartSeq is the sequence number the first Ping carries.
	StartSeq uint64 `yaml:"startSeq"`
}

func NewPingPongProtocol(peer transport.Host, cfg Config) *PingPongProtocol {
	return &PingPongProtocol{peer: peer, seq: cfg.StartSeq}
}

func (p *PingPongProtocol) Start(ctx protorun.ProtocolContext) {
	p.logger = ctx.Logger()
	p.ctx = ctx

	protorun.Handle(ctx, p.HandlePing)
	protorun.Handle(ctx, p.HandlePong)
}

func (p *PingPongProtocol) Init(ctx protorun.ProtocolContext) {
	if err := ctx.Connect(p.peer); err != nil {
		p.logger.Error("initial Connect failed", "peer", p.peer.String(), "err", err)
	}
}

func (p *PingPongProtocol) OnSessionConnected(h transport.Host) {
	if h != p.peer {
		return
	}
	p.seq++
	p.logger.Info("session established with peer, sending initial Ping",
		"peer", p.peer.String(), "seq", p.seq)
	if err := p.ctx.Send(NewPingMessage(p.seq), p.peer); err != nil {
		p.logger.Error("failed to send initial Ping", "err", err)
	}
}

func (p *PingPongProtocol) OnSessionDisconnected(h transport.Host) {
	if h == p.peer {
		p.logger.Warn("session with peer disconnected", "peer", p.peer.String())
	}
}

func (p *PingPongProtocol) HandlePing(ping *PingMessage, from transport.Host) {
	p.logger.Info("Ping received", "from", from.String(), "seq", ping.Seq)
	if err := p.ctx.Send(NewPongMessage(ping.Seq), p.peer); err != nil {
		p.logger.Error("failed to send Pong", "err", err)
	}
}

func (p *PingPongProtocol) HandlePong(pong *PongMessage, from transport.Host) {
	p.logger.Info("Pong received", "from", from.String(), "seq", pong.Seq)
	p.seq++
	if err := p.ctx.Send(NewPingMessage(p.seq), p.peer); err != nil {
		p.logger.Error("failed to send Ping", "err", err)
	}
}
