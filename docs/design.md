# 设计说明：状态机、调度与恢复

本文档补充 README 中的规则细节，作为实现与测试的判定基准。

## 1. 状态转换表

### 作业（JobStatus）

| from | event（判定依据） | to |
| --- | --- | --- |
| PENDING | 首个调度 tick | RUNNING |
| RUNNING | 全部步骤 SUCCEEDED | SUCCEEDED |
| RUNNING | 存在步骤 FAILED 或 BLOCKED | FAILED（`error` 汇总归因） |
| RUNNING/PENDING | `cancel_requested=true` 且仍有未终态步骤（且取消先于预算到点） | CANCELLED |
| RUNNING/PENDING | 墙钟到达 `deadline_at` 且仍有未终态步骤（且到点先于取消） | TIMED_OUT |
| 任意非终态 | 进程重启 | 状态不变，重建调度器继续推进；若 `deadline_at` 已过则直接落 TIMED_OUT（取消在先则 CANCELLED） |
| 终态 | 任意事件 | 终态（不可变更，取消返回 `job_already_terminal`） |

### 步骤（StepStatus）

| from | 条件 | to | fail_reason |
| --- | --- | --- | --- |
| PENDING | 全部依赖 SUCCEEDED | READY | - |
| PENDING | 任一依赖 ∈ {FAILED, BLOCKED, CANCELLED, TIMED_OUT} | BLOCKED | `upstream_failed` |
| READY / RETRYING(到点) | 获取并发槽，attempt += 1 | RUNNING | - |
| RUNNING | 执行返回成功 | SUCCEEDED | -（清空 error） |
| RUNNING | 失败且 attempt < max_attempts | RETRYING | -（设置 run_at=now+backoff） |
| RUNNING | 失败且 attempt >= max_attempts | FAILED | `retry_exhausted` |
| RUNNING/RETRYING/READY/PENDING | 作业取消 | CANCELLED | `cancelled` |
| RUNNING/RETRYING/READY/PENDING | 作业预算到点 | TIMED_OUT | `budget_exhausted` |
| RUNNING/RETRYING | 进程中断后重启 | READY（重调度，attempt 保留） | - |
| SUCCEEDED | 任意 | SUCCEEDED（**不重复执行**） | - |
| BLOCKED | 依赖链上的失败会继续向其下游传播 | BLOCKED | `upstream_failed` |

退避公式：`delay = min(backoff_cap, backoff_base * 2**(attempt-1))`。

## 2. 调度循环

每个作业一个后台 task（`_JobRunner.run`），循环执行：

1. 若作业终态 → 退出。
2. 取消/预算仲裁（详见第 3.1 节）：取消在先 → 取消传播；预算到点在先 →
   超时清扫；随后终结作业。
3. 依赖判定：PENDING 步骤根据依赖状态转 READY 或 BLOCKED（每个决策点立即落库）。
4. 启动到点的 READY / RETRYING 步骤，受作业级
   `Semaphore(max_concurrency)` 约束；每个步骤一个执行 task。
5. PENDING→RUNNING 的作业状态提升。
6. 全部步骤终态时，按“全成功 / 有超时 / 有取消 / 有失败阻断”归并作业终态。

循环由 `asyncio.Condition` 唤醒（步骤完成、取消、提交），并以 50ms 超时轮询
兜底 RETRYING 步骤的 `run_at` 到点与预算 `deadline_at` 到点（因此预算到期
最多延迟一个轮询周期被发现）。Condition 的通知不会因“发出早于等待”而丢失。

## 3.1 整体时间预算

- 提交时若带 `budget_seconds`，接受时刻计算
  `deadline_at = accepted_at + budget_seconds` 并随作业行落库；
  计时为**墙钟**，停机期间自然流逝，重启无需任何补偿逻辑。
- 调度循环每个 tick 先比较 `now >= deadline_at`：到点且仍有未终态步骤 →
  超时清扫（`_propagate_timeout`）：全部未终态步骤 → `TIMED_OUT`
  （`fail_reason=budget_exhausted`），在途任务收到 `CancelledError` 立即中断，
  作业 → `TIMED_OUT`。退避中的步骤因此永远不会再到点重跑；
  排队等槽位的 READY 步骤被标记终态后不会再被放行（执行任务在槽内还有
  一次预算二次检查兜底）。
- 预算只约束“未完成的步骤”：若全部步骤已 SUCCEEDED，作业仍为 SUCCEEDED，
  到点不影响已成功结果。
- **与取消的仲裁**：取消落盘时记录 `cancel_requested_at`（定向 UPDATE，
  只写标记列，不触碰状态列，避免与调度器的终态写互相覆盖）。
  两者同时悬置时按事件先后判定，与 tick 时机无关：
  `cancel_requested_at <= deadline_at` → CANCELLED，否则 TIMED_OUT。
  恢复路径（重启时发现已过期）使用同一条比较规则，保证运行期与重启后
  的归因一致。
- 重启恢复时若 `deadline_at` 已过：不重建调度器，直接在恢复扫描中把
  未终态步骤与作业落为 TIMED_OUT（或取消在先时的 CANCELLED）。
- 不带 `budget_seconds` 的作业 `deadline_at` 为 NULL，所有预算检查短路，
  行为与引入预算特性前完全一致。

## 3. 取消的执行模型

- 取消是**先持久化意图**（`jobs.cancel_requested=1`）再由调度循环传播，
  因此即使取消信号与进程崩溃竞态，重启后取消仍会继续生效。
- 运行中的执行 task 通过 `task.cancel()` 收到 `asyncio.CancelledError`：
  - `sleep` action 在每个 20ms 分片检查中断，立即抛出；
  - 执行器在入口检查取消（`CancellationError`）。
- 终态标记与取消运行任务在同一传播扫描中完成；已成功步骤不受影响。

## 4. 持久化与并发

- SQLite 单文件、WAL 日志、`synchronous=FULL`；单连接 + `threading.RLock`，
  写操作使用 `BEGIN IMMEDIATE`（作业行与全部步骤行在同一事务插入）。
- `idempotency_key` 上有 UNIQUE 约束，构成去重的数据库级兜底；
  应用层另用 `asyncio.Lock` 串行化“查重→创建”，使并发同键提交确定性返回同一作业。
- 步骤 spec 以排序键 JSON 落库；幂等一致性比对使用规范化请求体
  （`canonical_request`），与 JSON 字段书写顺序无关。

## 5. 恢复语义的边界

恢复保证：

1. 已 SUCCEEDED 的步骤不重复执行，结果可查询；
2. 未成功的步骤继续推进，最终到达确定终态；
3. 重试预算（attempt）跨重启连续，不会因重启获得额外重试；
4. 终态作业与幂等记录在重启后保持一致。

恢复**不**保证：进程在步骤产生外部副作用之后、成功落盘之前崩溃时，
该副作用不会发生（at-least-once 调度）。本服务内置 action 均无外部副作用，
实际接入外部系统时应由动作自身保证幂等。

## 6. 测试矩阵

| 文件 | 覆盖场景 |
| --- | --- |
| `test_idempotency_concurrency.py` | 无键并发独立执行；同键并发去重只执行一次；完成后同键重放；请求体冲突（步骤内容/并发度变化）；HTTP 409 原因码 |
| `test_orchestration_retry.py` | 并发上限的时间窗验证；下游等待上游成功；上游失败→下游 BLOCKED 与链式阻断；重试中 RETRYING 与耗尽 FAILED；`max_attempts=1`；非法依赖/环拒绝 |
| `test_cancel.py` | 运行中取消传播到运行与未启动步骤；已成功结果保留；终态取消 409；未知作业 404 |
| `test_recovery.py` | 部分成功+在途运行时 kill 后续跑且成功步骤不重跑；终态作业重启不变；RETRYING 中重启后沿用 attempt 预算 |
| `test_budget.py` | 运行中被预算掐断且已成功步骤保留；退避等待期间预算耗尽不再重跑；排队等槽位步骤不再放行；停机期间跨过预算恢复后直接 TIMED_OUT；部分预算跨重启连续扣减；取消先于到点→CANCELLED；到点先于取消→TIMED_OUT 且终态后再取消 409；预算纳入幂等请求体；HTTP 提交/查询与无预算兼容 |
