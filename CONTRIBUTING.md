# Contributing to protorun

Thanks for taking a look. The framework is small enough that contributing
mostly means landing focused changes that respect the existing
conventions. Here's what to know before you open a PR.

## Local setup

Go 1.26+. No system dependencies beyond a Go toolchain.

```bash
git clone https://github.com/antonionduarte/protorun
cd protorun
make tools-install   # installs govulncheck; prints golangci-lint hint
make hooks-install   # installs the pre-commit hook (lint + tests on staged Go)
```

## Build and test

```bash
make build           # go build ./...
make test            # go test ./...
make test-race       # go test -race ./...
make lint            # golangci-lint run ./..., must report 0 issues
make modernize-check # gopls modernize analyzer, should report nothing
make coverage        # go test -coverprofile=coverage.out + summary
make deadcode        # golang.org/x/tools/cmd/deadcode, should report nothing
```

Run a single test:

```bash
go test -race . -run TestIPC_RequestReply_HappyPath
```

The 10k-node scale probe is gated behind an env var because it's slow and
resource-heavy:

```bash
GOSSIP_SCALE_10K=1 go test -run TestGossip_10000Nodes_Scale -timeout 10m ./cmd/gossip/
```

## Conventions

### Protocol authoring

- Protocols only interact with the runtime through `ProtocolContext`. No
  package-level helpers, no reaching into other protocols' Go methods.
- Cross-protocol coordination is IPC: `RegisterRequestHandler` /
  `SendRequest` for synchronous fetches, `SubscribeNotification` /
  `PublishNotification` for fan-out. Direct Go method calls between
  protocols are an anti-pattern; the test driver in `cmd/gossip` uses a
  `triggerer` harness protocol for exactly this reason.
- All protocol state is event-loop-owned; no locks unless you have a
  public method callable from another goroutine, and even then prefer
  routing through self-IPC over a mutex.
- Public methods on a protocol type beyond `Start` / `Init` /
  `On*Session*` / `OnPanic` are usually a code smell. If you need an
  external entry point, expose it as an IPC type and let callers go
  through `SendRequest`.

### Logging

The runtime uses `log/slog` with four component tags: `runtime`,
`session`, `transport`, `protocol`. Every log line should have a
component attribute (the framework adds it automatically for the layers
it owns; protocol-side logs come from `ctx.Logger()` which is already
tagged). Prefer structured attributes over interpolated strings:

```go
ctx.Logger().Info("session up", "peer", peer, "rtt_ms", rtt)
```

`Host` satisfies `fmt.Stringer`, so passing it directly as a slog
attribute formats it as `ip:port` automatically.

### Tests

- Use `goleak.VerifyTestMain` in `TestMain` so leaked goroutines fail
  loudly. Existing `main_test.go` (root package) and
  `cmd/gossip/main_test.go` are the templates.
- Run new code under `-race`. The CI workflow already does this on every
  PR.
- Integration tests across multiple runtimes use a base port atomic
  (`reservePorts`) so they can run with `-count=N` without colliding.
  See `cmd/gossip/integration_test.go` for the pattern.
- For framework-internal tests, prefer real `SessionLayer` over mocking
  it. Most lifecycle bugs the suite has caught lived at the
  session-layer boundary.

### Adding a new IPC type

Define the request and reply as Go types embedding `BaseRequest` /
`BaseReply`:

```go
type Lookup struct {
    protorun.BaseRequest
    Key string
}
type LookupReply struct {
    protorun.BaseReply
    Value string
    Found bool
}
```

Register the handler in your protocol's `Start`:

```go
protorun.RegisterRequestHandler(ctx, func(req *Lookup, r protorun.Responder[*LookupReply]) {
    v, ok := store[req.Key]
    r.Reply(&LookupReply{Value: v, Found: ok})
})
```

Callers (other protocols on the same runtime) issue the request:

```go
protorun.SendRequest(ctx, &Lookup{Key: "foo"}, func(rep *LookupReply, err error) {
    // ...
})
```

For notifications, use `BaseNotification` plus
`SubscribeNotification` / `PublishNotification`. Notifications are
fan-out: one publisher, many subscribers per type.

## Pull request checklist

Before submitting:

- [ ] `go build ./...` clean
- [ ] `go vet ./...` clean
- [ ] `make lint` reports 0 issues
- [ ] `go test -race ./...` passes
- [ ] New public surface ships with happy-path + failure-path tests
- [ ] If touching shutdown / lifecycle: `goleak` doesn't surface new
      leaks
- [ ] Behavioral changes mentioned in the PR description

CI runs all of the above on every PR and gates merges on lint passing
for new code; existing violations are not blocking, only regressions.

## Communication style

The codebase favors short focused commits over batched ones. Commit
messages are imperative-mood subject lines (`add foo bar`, `fix race in
shutdown`) with a body explaining *why* when the *what* isn't obvious
from the diff.
