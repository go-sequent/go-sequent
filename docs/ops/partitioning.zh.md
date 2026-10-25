# Schema 分区指引（共享 schema + tenant_id）

**模型（Q4）：** 共享 schema + `tenant_id` 列/钩子 — **不是** schema-per-tenant / DB-per-tenant。

物理分区可选，且应落在**同一共享 schema** 内。Phase 1–3 默认迁移**不**建分区；[`sql/`](./sql/) 仅为运维示例（容量评估后手动执行）。

## 何时分区

| 现象 | 候选 |
|------|------|
| `events` 过大 / vacuum 压力 | 按 `partition_key`（常为 `tenant_id` 或 tenant+hash） |
| `commands` 很大 | 按时间 range 或 id hash |
| 少跨租户扫描 | 与加载过滤对齐以便裁剪 |

应用侧用 `tenancy.PartitionKey` / `PartitionKeyHashed` 设置聚合 `PartitionKey()`。唯一键 scope 应用 `tenancy.UniqueScope` 带上租户。

## 注意

- 不要把「每租户一个 schema」当成产品模型。
- RLS 可选；库侧仍要做租户校验。
- `Migrate` **不会**自动应用示例 SQL。
