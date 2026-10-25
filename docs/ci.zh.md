# CI / CD — go-sequent

本仓库为**私有**模块（Q10）的 GitHub Actions。不向公共 GOPROXY 发布；不推 Docker 镜像仓库。

## 工作流

| 文件 | 触发 | 内容 |
|------|------|------|
| [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) | 向 `main` / `master` 的 `push` / `pull_request`；Release 也会调用 | `go mod tidy` 检查、`go vet`、**golangci-lint**、`go test -race ./...`、Postgres 集成测 |
| [`.github/workflows/release.yml`](../.github/workflows/release.yml) | 推送 `v*` 标签 | 再跑完整 CI，再用 **GoReleaser** 上传 `cmd/go-sequent` 二进制 |

## CI 任务

1. **checks** — `go mod tidy` + `git diff --exit-code`；`go vet ./...`；golangci-lint（[`.golangci.yml`](../.golangci.yml)）
2. **test** — 无 Postgres 的 `go test -race -count=1 ./...`（未设 `GO_SEQUENT_PG_DSN` 时 PG 用例 skip）
3. **test-postgres** — Postgres 16 服务容器；DSN：`postgres://sequent:sequent@localhost:5432/sequent?sslmode=disable`；包：
   - `./eventstore/postgres/`
   - `./examples/bank/`
   - `./examples/bankpg/`

Go 版本取自 [`go.mod`](../go.mod)（`setup-go` 的 `go-version-file`），并启用模块缓存。

## 发布（CD）

```bash
git tag v0.1.0
git push origin v0.1.0
```

GoReleaser（[`.goreleaser.yaml`](../.goreleaser.yaml)）为 linux/darwin/windows × amd64/arm64 构建 `go-sequent`，并挂到 GitHub Release。

### Secrets

**CI 不需要任何 secrets。** Release 使用默认 `GITHUB_TOKEN`（`permissions: contents: write`）。除非以后扩展 CD，否则不要加 Docker Hub / GOPROXY 令牌。

私有模块消费方仍用 `GOPRIVATE` / `replace` / git；Actions **不会**发布到公共 module mirror。

## 本地

```bash
go test -race ./...
GO_SEQUENT_PG_DSN='postgres://…' go test ./eventstore/postgres/ ./examples/bank/ ./examples/bankpg/
golangci-lint run   # 可选
actionlint          # 可选
```

English: [`ci.md`](./ci.md)
