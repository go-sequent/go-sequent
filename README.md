# go-sequent

Go CQRS + Event Sourcing library for financial systems, inspired by [Sequent](https://sequent.io/) (conceptual only — no Ruby DB compatibility).

**Status:** Phase 2 (Sequent-parity core). Private / not open-source.

## Module

```text
github.com/wenweih/go-sequent
```

## Quick start

See [docs/getting-started.md](docs/getting-started.md) (中文: [getting-started.zh.md](docs/getting-started.zh.md)).

Design: [docs/design.md](docs/design.md).

## Test

```bash
go test ./...
```

Optional Postgres integration tests:

```bash
export GO_SEQUENT_PG_DSN='postgres://user:pass@localhost:5432/go_sequent?sslmode=disable'
go test ./eventstore/postgres/ -count=1
```

## Ops CLI

```bash
go run ./cmd/go-sequent migrate -dsn "$GO_SEQUENT_PG_DSN"
go run ./cmd/go-sequent replay -dsn "$DSN" -projector NAME -stage prepare -tables t1,t2
```

## Layout

| Package | Role |
|---------|------|
| `sequent` | App façade |
| `command` | Commands, Service, middleware, router |
| `event` / `event/upcast` | Events, registry, upcasters v0 |
| `aggregate` | Root base, UoW, repository (+ snapshot load) |
| `eventstore` | Store; `memory` + `postgres` (+ snapshots, unique keys) |
| `snapshot` | JSON snapshots + background snapshotter |
| `publish` | `SyncInTxn` (projectors + workflows) |
| `projector` / `projector/replay` | Projector helpers + rebuild pipeline |
| `workflow` | Workflows + AfterCommit |
| `types` | `Money` (decimal) |
| `tenancy` | shared schema + `tenant_id` hooks |
| `testkit` | Given / When / Expect |
| `cmd/go-sequent` | migrate / replay CLI |
| `examples/bank` | Ledger demo |
