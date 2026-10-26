# Operations runbooks — go-sequent

Production hardening guides for the event store and projections.

| Doc | Topic |
|-----|--------|
| [failover.md](./failover.md) | PostgreSQL primary / replica notes |
| [projection-rebuild.md](./projection-rebuild.md) | prepare → replay → catchup → golive |
| [backup-restore.md](./backup-restore.md) | Event store backup & verified restore |
| [partitioning.md](./partitioning.md) | Shared-schema partitioning at scale |
| [sql/](./sql/) | Optional partitioning SQL examples |

中文：[`README.zh.md`](./README.zh.md) 及同名 `*.zh.md`。

**Locked defaults:** ledger projectors = SyncInTxn ; integrity = hash chain ; tenancy = shared schema + `tenant_id` ; outbox = table polling ; no Ruby Sequent DB import.
