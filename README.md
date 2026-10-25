# go-sequent

Go CQRS + Event Sourcing library for financial systems, inspired by [Sequent](https://sequent.io/) (conceptual only — no Ruby DB compatibility).

**Status:** Phase 3 (finance-ready). Private / not open-source.

## Module

```text
github.com/wenweih/go-sequent
```

## Docs

- [docs/getting-started.md](docs/getting-started.md) · [getting-started.zh.md](docs/getting-started.zh.md)
- [docs/design.md](docs/design.md)

## Test

```bash
go test ./...
```

## Ops CLI

```bash
go run ./cmd/go-sequent migrate -dsn "$GO_SEQUENT_PG_DSN"
go run ./cmd/go-sequent outbox -dsn "$DSN" -once   # Postgres table polling (Q6)
```

## Layout (Phase 3 additions)

| Package | Role |
|---------|------|
| `idempotency` | Tenant-scoped idempotency keys |
| `security` | AuthZ at command boundary |
| `crypto` | PayloadProtector hooks (no shredding product) |
| `tombstone` | GDPR tombstone-only helpers |
| `publish/outbox` | Outbox writer + poller |
| `obs` | OpenTelemetry metrics/traces |
| `tenancy` | UniqueScope / partition helpers |

Ledger projectors remain **SyncInTxn** (Q1). No event hash-chain/signatures (Q3).
