# 运维手册 — 投影重建

对齐 Sequent 8：**prepare → replay → catchup → optimize → golive**（可 **abort**）。

需强一致的账本/余额投影在**线上写路径**保持 **SyncInTxn**。重建针对可丢弃读模型。

## 库 API

使用 `projector/replay.Pipeline`，经 `PublishProjectorsOnly` 排除 Workflow。

## CLI（schema 阶段）

```bash
go run./cmd/go-sequent migrate -dsn "$GO_SEQUENT_PG_DSN"
go run./cmd/go-sequent replay -dsn "$DSN" -projector balances \
 -stage prepare -tables account_balances
```

完整事件流回放需在应用二进制中注册 Projector（CLI 不知情）。

## 阶段

| 阶段 | 动作 |
|------|------|
| prepare | 建 `replay_*` schema，复制托管表 DDL |
| replay | 历史事件灌入 replay 投影 |
| catchup | 追平高水位期间新增事件 |
| optimize | `ANALYZE` / 索引 |
| golive | 短窗口交换表到 view schema |
| abort | 删除 replay schema |

Golive 期间必要时短暂串行写；账本线上仍以 SyncInTxn 为准。
