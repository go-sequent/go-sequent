# go-sequent

Go CQRS + Event Sourcing library for financial systems, inspired by [Sequent](https://sequent.io/) (conceptual only — no Ruby DB compatibility).

**Status:** Phase 1 MVP (pure library). Private / not open-source.

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

## Layout (Phase 1)

| Package | Role |
|---------|------|
| `sequent` | App façade |
| `command` | Commands, Service, middleware, router |
| `event` | Events + type registry |
| `aggregate` | Root base, UoW, repository |
| `eventstore` | Store interface; `memory` + `postgres` |
| `publish` | `SyncInTxn` publisher |
| `projector` | Projector helpers |
| `types` | `Money` (decimal) |
| `tenancy` | shared schema + `tenant_id` hooks |
| `testkit` | Given / When / Expect |
| `examples/bank` | Ledger demo + sync balance projector |
