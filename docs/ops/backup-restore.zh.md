# 运维手册 — 事件库备份与恢复

PostgreSQL 事件库是**核心资产**。投影可重建；事件（+ 命令）不可丢。

## 备份对象

| 对象 | 优先级 |
|------|--------|
| `commands` / `events` / `aggregates` / `*_types` | **关键** |
| `aggregate_unique_keys` / `idempotency_keys` | 正确性关键 |
| `snapshot_records` | 高（可由事件重建） |
| `outbox` | 有异步集成时高 |
| 投影表 | 低 — 用 replay 重建 |
| `event_tombstones` | 中（GDPR 审计） |

完整性 = **哈希链**+ ACL/备份/可选 WORM；**无**事件签名。

## 持续备份

1. 开启 WAL 归档 / 云 PITR。
2. 定期基础备份（pgBackRest / Barman / 云自动备份）。
3. 备份加密与恢复权限最小化。
4. 留存策略与墓碑策略对齐（仅墓碑产品路径）。

## 验证恢复演练（建议每季）

1. 恢复到隔离实例。
2. 冒烟：`LoadStream`、一次 SyncInTxn `Execute`、租户幂等键。
3. 可选：投影 replay 重建。
4. 记录实际 RTO/RPO。

## PITR

- 恢复到事故前；**永不**导入 Ruby Sequent dump。
- 快照版本不兼容时删除后由 snapshotter 重建。
