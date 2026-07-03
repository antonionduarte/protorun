.PHONY: build test test-race run bench \
	lint lint-fix lint-new \
	modernize modernize-check \
	coverage vulncheck deadcode \
	tools-install hooks-install

# MODULES is every Go module in the repo. The core module (.) stays
# zero-dependency; pkg/codec/protobuf, pkg/transport/quic, pkg/config, and
# pkg/otel are nested modules with their own go.mod for the third-party deps that
# would otherwise break that guarantee. go.work (tracked) ties them
# together for local builds. Targets that must cover the whole repo
# loop over this list; root-only helpers (run, bench, coverage gate)
# stay on the core module.
MODULES := . cmd pkg/codec/protobuf pkg/transport/quic pkg/config pkg/otel

# -----------------------------------------------------------------------
# Build / run
# -----------------------------------------------------------------------

build:
	@for m in $(MODULES); do echo "==> build $$m"; (cd $$m && go build ./...) || exit 1; done

test:
	@for m in $(MODULES); do echo "==> test $$m"; (cd $$m && go test ./...) || exit 1; done

test-race:
	@for m in $(MODULES); do echo "==> test-race $$m"; (cd $$m && go test -race ./...) || exit 1; done

bench:
	go test -bench=. -run=^$$ -benchmem ./...

run:
	go run ./cmd/pingpong \
	  -config cmd/pingpong/pingpong.example.yaml \
	  -self-port 5001 -peer-port 5002

# -----------------------------------------------------------------------
# Code quality — lint, coverage, vulnerability scan, dead code
#
# Thresholds in .golangci.yml are anchored on Go-community conventions
# (gocyclo/cyclop/gocognit/funlen/lll/dupl defaults and Uber Go Style
# Guide).
# -----------------------------------------------------------------------

lint:
	@for m in $(MODULES); do echo "==> lint $$m"; (cd $$m && golangci-lint run ./...) || exit 1; done

lint-fix:
	@for m in $(MODULES); do (cd $$m && golangci-lint run --fix ./...) || exit 1; done

# lint-new is the CI gate: fail only on issues in code changed against
# origin/main. Old code is surfaced but not blocked.
lint-new:
	@for m in $(MODULES); do (cd $$m && golangci-lint run --new-from-rev=origin/main ./...) || exit 1; done

coverage:
	go test ./... -coverprofile=coverage.out -covermode=atomic
	go tool cover -func=coverage.out | tail -1

vulncheck:
	@for m in $(MODULES); do echo "==> vulncheck $$m"; (cd $$m && go run golang.org/x/vuln/cmd/govulncheck@latest ./...) || exit 1; done

# deadcode exits 0 even when it reports findings, so the gate fails on
# any output instead of on the exit code.
deadcode:
	@for m in $(MODULES); do \
		echo "==> deadcode $$m"; \
		out=$$(cd $$m && go run golang.org/x/tools/cmd/deadcode@latest -test ./...) || exit 1; \
		if [ -n "$$out" ]; then echo "$$out"; exit 1; fi; \
	done

# Apply gopls' modernize fixes in place (sync.WaitGroup.Go, range-over-int,
# t.Context(), maps/slices helpers, etc.). Idempotent — safe to re-run.
# `go modernize` itself doesn't exist; the tool ships inside gopls.
modernize:
	@for m in $(MODULES); do (cd $$m && go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest -fix ./...) || exit 1; done

# Same analyzer in report-only mode. Useful in CI to fail when new code
# would be modernized.
modernize-check:
	@for m in $(MODULES); do echo "==> modernize-check $$m"; (cd $$m && go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest ./...) || exit 1; done

# -----------------------------------------------------------------------
# One-shot setup: install developer-side quality tools and git hooks
# -----------------------------------------------------------------------

tools-install:
	go install golang.org/x/vuln/cmd/govulncheck@latest
	@echo "Also install golangci-lint (see https://golangci-lint.run/welcome/install/)"

hooks-install:
	./scripts/install-hooks.sh
