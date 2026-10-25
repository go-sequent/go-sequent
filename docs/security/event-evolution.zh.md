# 事件演进兼容政策

## 原则

1. 事件是不可变事实；优先新类型或 upcaster，而非改写历史。
2. 默认 JSON 编解码；信封在库列，载荷保持扁平。
3. **永不导入 Ruby Sequent 数据库**（Q9）。

## Upcaster v0 规则

- 按存储类型名 `FromType` 注册；链至稳定（默认最多 8 跳）。
- 最终类型必须在 `event.Registry` 注册；变换纯函数、可判定。
- 加载前 `store.SetUpcasters(chain)`。

## 策略摘要

| 变更 | 做法 |
|------|------|
| 新增可选字段 | applier 默认值即可 |
| 改名/改形 | upcaster |
| 语义破坏 | 新事件类型 + 补偿事件 |
| 快照不兼容 | 提升 `SnapshotVersion`，丢弃旧快照 |

金额保持 decimal 字符串（Q2）；禁止 float64。禁止静默改事件行、框架哈希链/签名（Q3）、crypto-shredding 产品化（Q5）、Sequent dump 导入（Q9）。

详见英文版 [event-evolution.md](./event-evolution.md) 与运维 [projection-rebuild.zh.md](../ops/projection-rebuild.zh.md)。
