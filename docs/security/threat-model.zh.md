# 威胁模型刷新 — go-sequent（Phase 4）

与已锁定 对齐。

## 资产

事件库（事实来源）· 唯一键/幂等键 · 可丢弃投影 · Outbox · 版本化 JSON 快照· 载荷中的 PII（加密钩子；GDPR = **仅墓碑**）。

## 信任边界

1. 认证在宿主应用完成；`UserID` / `AuthContext` 不可盲信。
2. `security.Authorizer` 在 handler 前拦截。
3. 单写主库；完整性 = **哈希链**+ DB ACL/备份；无事件签名。
4. 账本 SyncInTxn；Outbox 至少一次 + 幂等消费。

## 威胁与缓解（摘要）

| 威胁 | 缓解 |
|------|------|
| 重放 / 双提交 | 租户幂等键 |
| 伪造命令提权 | AuthZ；敏感类型默认拒绝 |
| 事件库篡改 | 哈希链校验 + DB ACL / WORM 备份 |
| 跨租户 | `tenant_id` + UniqueScope + 加载校验 |
| PII 泄露 | 脱敏、PayloadProtector、墓碑 |
| 投影注入 | 仅参数化 Persistor |
| Sequent 库导入 | **永不** |

应用检查清单见英文版 [threat-model.md](./threat-model.md)。事件演进政策见 [event-evolution.zh.md](./event-evolution.zh.md)。
