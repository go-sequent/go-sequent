# 运维手册 — PostgreSQL 故障切换

## 目标

- 事件库**单写主库**，避免静默双主。
- 同步提交 / 仲裁时可做到 RPO≈0（运维选型）。
- 投影**读**仅在可接受异步延迟时使用副本。

## 路径建议

| 路径 | 建议 |
|------|------|
| Command + SyncInTxn 投影（Q1） | 始终打**主库**（账本读己之写） |
| 异步 Outbox 消费（Q6） | 非账本视图可用延迟可容忍的副本 |
| Replay / 重建 | 优先主库或专用重建角色 |

前置：Patroni / Cloud SQL HA / RDS Multi-AZ + PgBouncer（或云侧连接池）。

## 切换检查清单

1. 判定主库失联。
2. 提升同步备库；确认新主 `pg_is_in_recovery() = false`。
3. 切换连接池 / DSN；必要时滚动重启应用。
4. 验证写入：一次 append + SyncInTxn 投影。
5. 确认旧主不再接受写入（fencing）。
6. 从新主重建备库并监控延迟。

## 应用注意

- 事务尽量短（一次 `Execute` 批次）。
- `ErrOptimisticConcurrency` 退避重试（切换后冲突风暴常见）。
- 禁止跨协程共享 UoW。

库本身不做多主合并或内置 DSN 故障转移。
