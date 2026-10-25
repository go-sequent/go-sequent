# Sequent（Ruby）研究笔记

> **本文是 [`sequent-research.md`](./sequent-research.md) 的中文版。**  
> 面向 go-sequent 设计的研究材料。来源：[sequent.io](https://sequent.io/)、GitHub `zilverline/sequent`（master 分支 zip 快照，2026 年 10 月）。  
> 框架版本语境：Sequent 8+ 模式（分区表 `events` / `commands` / `aggregates`）。

## 1. 定位

Sequent 是面向 Ruby 的**有主见的 CQRS + Event Sourcing** 框架。它聚焦领域逻辑，且**与 Web 框架无关**（可用 Rails、Sinatra 等）。持久化以 **PostgreSQL 为中心**，大量使用 PL/pgSQL 过程，在同一事务中原子提交 commands、events 与 unique keys。

## 2. 基本流程（Happy Path）

1. 客户端构造 `Command`（表达意图）。
2. `CommandService.execute_commands(...)` 开启**单一事务**。
3. 对每个 command：执行 **filters** → **validate** → **parse types** → 派发给匹配的 `CommandHandler`。
4. Handler 通过 `AggregateRepository`（Unit-of-Work + Identity Map）加载或创建 `AggregateRoot`。
5. Aggregate 通过 `apply` 产生 Event（领域逻辑在方法中；状态变更仅发生在 `on Event` 处理块中）。
6. Repository `commit` → `EventStore.commit_events` 先持久化 command 与 events，再在**同一事务内 publish** 给 `Projector` / `Workflow`（默认读模型强一致）。
7. Workflow 可再入队更多 command（在同一事务队列中、于当前已调度 command 之后处理）。
8. 成功则提交并清空 UoW；失败则整事务回滚。

## 3. 核心概念

| 概念 | 职责 |
|------|------|
| **Command** | 领域 API / 意图（如 `CreateUser`）。校验后写入 `commands`。 |
| **Event** | 过去时事实（如 `UserCreated`）。主键 = `(aggregate_id, sequence_number)`。 |
| **ValueObject** | 嵌套的不可变属性组合。 |
| **AggregateRoot** | 封装不变量；状态 = 事件流折叠。禁止跨聚合读取状态。 |
| **CommandHandler** | 将 command 翻译为对聚合方法的调用。 |
| **CommandService** | 统一入口：事务、校验、中间件、提交、命令队列。 |
| **AggregateRepository** | UoW + 身份映射；提交时乐观并发控制。 |
| **EventStore** | 持久化/加载流、快照、唯一键；发布事件。 |
| **Projector** | 构建读模型（`Records`）；一张 Record 表仅由一个 Projector 管理。 |
| **Workflow** | 副作用 / 后续 command；**不**在 projector 迁移重放时运行。 |
| **Migrations / Replay** | 通过 prepare → replay → catchup → optimize → golive 重建视图模式。 |
| **Snapshotting** | 可选；基于阈值；快照格式带版本。 |
| **UniqueKeys** | 通过 `aggregate_unique_keys` 表实现跨聚合业务唯一性。 |
| **Secret** | Command 侧用 BCrypt 处理密码；校验失败时清空。 |
| **Middleware / Filters** | 在 command 边界的横切能力。 |

## 4. Event Store 模式（Sequent 8）

主要表（注明处分区）：

- `commands` — 成功执行的 command（`command_json`，可选 `user_id`，因果字段 `event_aggregate_id` / `event_sequence_number`）。
- `aggregates` — 流元数据（`aggregate_type_id`、`events_partition_key`、`snapshot_threshold`）。
- `events` — 主键 `(partition_key, aggregate_id, sequence_number)`；`event_json`；外键指向 command；`xact_id`。
- `command_types` / `event_types` / `aggregate_types` — 类型注册表（SMALLINT id）。
- `aggregate_unique_keys` — `UNIQUE (scope, key)`，约束业务唯一性。
- `snapshot_records`、`aggregates_that_need_snapshots`。
- `saved_event_records` — 对 events 的 U/D 审计（GDPR / 运维）。

**乐观锁**：`store_events` 过程在聚合行上使用 `FOR NO KEY UPDATE`，检查 `sequence_number` 连续；主键防重 → 并发写冲突 → integrity / `OptimisticLockingError`。

**发布顺序**：按 UoW 中聚合的**加载顺序**发布，再按各聚合内 sequence（而非跨聚合 `apply` 调用的交错顺序）。

## 5. Projector 与迁移

- 视图表位于 **view schema**；Events 是唯一事实来源。
- Projector 使用抽象 Persistor API，以便 **replay** 走批量优化路径。
- 重放流水线（Sequent 8）：`prepare`（replay schema）→ `replay` → `catchup` → `optimize` → `golive`（切到 view，旧表进 archive）。
- 约束：视图模式中不要跨聚合使用 `belongs_to` / FK；仅保证**单个聚合内**事件顺序。

## 6. GDPR 说明（Sequent）

无开箱即用的擦除能力。讨论过的策略：**tombstoning（墓碑事件）**、有选择的 **event deletion**（需谨慎保证流完整性）、通过墓碑传播做清洗。`saved_event_records` 可追踪变更。

## 7. 对 Go 移植的启示

- 优先使用**接口 + 组合**，而非 Ruby 的继承 / `on` DSL；消息路由可用 codegen 或 registry。
- 首先坚持 **PostgreSQL + 存储过程**（或等价 SQL）保证原子提交语义；用抽象 `EventStore` 接口为日后其他后端留口。
- 默认 **写事务内同步 projector** 与 Sequent 一致；金融场景可能需要 **异步 outbox** 以换取 HA/延迟 — 设计为可插拔 publisher。
- 线程本地 UoW → Go：使用**请求/上下文作用域**的 UoW（`context.Context` + 显式 `UnitOfWork`），禁止 goroutine 全局可变映射。
- Money/decimal、authz、多租户、事件签名：超出 Sequent 范围；在 go-sequent 中作为金融目标的一等能力。
