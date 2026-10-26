# CI / CD — go-sequent

GitHub Actions for this **private** module. No public GOPROXY publish step; no Docker registry.

## Workflows

| File | When | What |
|------|------|------|
| [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) | `push` / `pull_request` to `main` or `master`; also called from Release | `go mod tidy` check, `go vet`, **golangci-lint**, `go test -race./...`, Postgres ITs |
| [`.github/workflows/release.yml`](../.github/workflows/release.yml) | push tag `v*` | Re-runs full CI, then **GoReleaser** uploads `cmd/go-sequent` binaries |

## Jobs (CI)

1. **checks** — `go mod tidy` + `git diff --exit-code`; `go vet./...`; golangci-lint (config: [`.golangci.yml`](../.golangci.yml))
2. **test** — `go test -race -count=1./...` without Postgres (`GO_SEQUENT_PG_DSN` unset → PG tests skip)
3. **test-postgres** — Postgres 16 service; `GO_SEQUENT_PG_DSN=postgres://sequent:sequent@localhost:5432/sequent?sslmode=disable`; tests:
 - `./eventstore/postgres/`
 - `./examples/bank/` (includes postgres IT)
 - `./examples/bankpg/`

Go version comes from [`go.mod`](../go.mod) via `actions/setup-go` `go-version-file` (module cache enabled).

## Release (CD)

Tag a version (SemVer):

```bash
git tag v0.1.0
git push origin v0.1.0
```

GoReleaser ([`.goreleaser.yaml`](../.goreleaser.yaml)) builds `go-sequent` for linux/darwin/windows × amd64/arm64 and attaches archives to the GitHub Release.

### Secrets

**None required for CI.** Release uses the default `GITHUB_TOKEN` (workflow `permissions: contents: write`). Do not add Docker Hub / GOPROXY tokens unless you intentionally extend CD later.

Private module consumers still use `GOPRIVATE` / `replace` / git — Actions does not publish to the public module mirror.

## Local

```bash
go test -race./...
GO_SEQUENT_PG_DSN='postgres://…' go test./eventstore/postgres/./examples/bank/./examples/bankpg/
# optional
golangci-lint run
# optional YAML check
actionlint
```

中文：[`ci.zh.md`](./ci.zh.md)
