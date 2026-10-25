# Operations runbooks — go-sequent

Production hardening guides for the event store and projections.

| Doc | Topic |
|-----|--------|
| [failover.md](./failover.md) | PostgreSQL primary / replica notes |
| [projection-rebuild.md](./projection-rebuild.md) | prepare → replay → catchup → golive |
| [backup-restore.md](./backup-restore.md) | Event store backup & verified restore |
| [partitioning.md](./partitioning.md) | Shared-schema partitioning at scale (Q4) |
| [sql/](./sql/) | Optional partitioning SQL examples |

中文：[`README.zh.md`](./README.zh.md) 及同名 `*.zh.md`。

**Locked defaults:** ledger projectors = SyncInTxn (Q1); integrity = DB controls only (Q3); tenancy = shared schema + `tenant_id` (Q4); outbox = table polling (Q6); no Ruby Sequent DB import (Q9).
