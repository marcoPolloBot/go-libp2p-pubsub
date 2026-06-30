# AGENTS.md

## Cursor Cloud specific instructions

This repo is a Go **library** (`github.com/libp2p/go-libp2p-pubsub`), not a runnable
application. There is no server/daemon to start; "running" it means building it,
running its tests, or using it from a small program.

### Toolchain
- `go.mod` requires **Go 1.25**. The system package `/usr/bin/go` is older (1.22), so
  Go 1.25 is installed at `/usr/local/go` and prepended to `PATH` via `~/.bashrc`.
  In a fresh non-login shell, prefer `/usr/local/go/bin/go` if `go version` reports < 1.25.
- Do not rely on `GOTOOLCHAIN=auto` to fetch 1.25 with the old system `go`; that
  auto-download fails ("toolchain not available") in this environment.

### Common commands (run from repo root)
- Build: `go build ./...`
- Lint/vet: `go vet ./...` and `gofmt -l .` (CI uses the ipdxco unified go-check workflow).
- Tests: `go test ./...`. The full suite is large and networking/timing heavy
  (CI allows up to 30m). Prefer targeting specific tests with `-run` and `-count=1`
  while iterating, e.g. `go test -run TestBasicFloodsub -count=1 .`.
- Race tests (CI runs these): `go test -race ./...` — significantly slower.

### Notes
- Tests spin up in-process libp2p hosts over loopback TCP; no external services,
  databases, or credentials are required.
- Gossipsub needs the mesh to form before delivery; when writing demos/tests that
  publish-then-receive, allow a short delay (~1–2s) after subscribing.
