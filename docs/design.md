# 设计说明：状态机、调度与恢复

本文档补充 README 中的规则细节，作为实现与测试的判定基准。

## 1. 状态转换表

### 作业（JobStatus）

| from | event（判定依据） | to |
| --- | --- | --- |
| PENDING | 首个调度 tick | RUNNING |
| RUNNING | 全部步骤 SUCCEEDED | SUCCEEDED |
| RUNNING | 存在步骤 FAILED 或 BLOCKED | FAILED（`error` 汇总归因） |
| RUNNING/PENDING | `cancel_requested=true` 且仍有未终态步骤 | CANCELLED |
| RUNNING/PENDING | 墙钟 `time >= budget_deadline` 且仍有未终态步骤 | TIMED_OUT |
| 任意非终态 | 重启时发现截止点已在过去（含停机期间跨过） | TIMED_OUT（恢复路径直接落终态） |
| 任意非终态 | 进程重启且预算仍有效 | 状态不变，重建调度器继续推进 |
| 终态 | 任意事件 | 终态（不可变更，取消返回 `job_already_terminal`） |

### 步骤（StepStatus）

| from | 条件 | to | fail_reason |
| --- | --- | --- | --- |
| PENDING | 全部依赖 SUCCEEDED | READY | - |
| PENDING | 任一依赖 ∈ {FAILED, BLOCKED, CANCELLED} | BLOCKED | `upstream_failed` |
| READY / RETRYING(到点) | 获取并发槽，attempt += 1 | RUNNING | - |
| RUNNING | 执行返回成功 | SUCCEEDED | -（清空 error） |
| RUNNING | 失败且 attempt < max_attempts | RETRYING | -（设置 run_at=now+backoff） |
| RUNNING | 失败且 attempt >= max_attempts | FAILED | `retry_exhausted` |
| RUNNING/RETRYING/READY/PENDING | 作业取消 | CANCELLED | `cancelled` |
| RUNNING/RETRYING/READY/PENDING | 作业整体预算耗尽 | TIMED_OUT | `budget_exhausted` |
| RUNNING/RETRYING | 进程中断后重启（预算仍有效） | READY（重调度，attempt 保留） | - |
| SUCCEEDED | 任意 | SUCCEEDED（**不重复执行**） | - |
| BLOCKED | 依赖链上的失败会继续向其下游传播 | BLOCKED | `upstream_failed` |

退避公式：`delay = min(backoff_cap, backoff_base * 2**(attempt-1))`。

> 注：TIMED_OUT **不**计入 `STEP_FAILING`，即“上游超预算”本身不会按
> upstream_failed 再向下游传染——预算收尾扫描会一次性把作业内所有未终态
> 步骤统一标为 TIMED_OUT，下游得到的是明确的 `budget_exhausted` 受阻终态，
> 既不静默跳过，也不与上游失败归因混淆。

## 2. 调度循环

每个作业一个后台 task（`_JobRunner.run`），循环执行：

1. 若作业终态 → 退出。
2. 若 `cancel_requested` → 取消传播扫描并终结作业。
3. 若带预算且 `time.time() >= budget_deadline` → 预算收尾扫描并终结作业。
   （取消判定排在预算判定之前，见 §3.1 的撞车规则。）
4. 依赖判定：PENDING 步骤根据依赖状态转 READY 或 BLOCKED（每个决策点立即落库）。
5. 启动到点的 READY / RETRYING 步骤，受作业级
   `Semaphore(max_concurrency)` 约束；预算已过时**一律不再放行新尝试**
   （退避等到点、排队等额度的步骤都不会再跑）。
6. PENDING→RUNNING 的作业状态提升。
7. 全部步骤终态时，按“全成功 / 有取消 / 有超时 / 有失败阻断”归并作业终态。

循环由 `asyncio.Condition` 唤醒（步骤完成、取消、提交），并以 50ms 超时轮询
兜底 RETRYING 步骤的 `run_at` 到点；带预算时该等待还会被“距截止点的剩余
时间”封顶，因此预算到点最迟在同一时间切片内被扫到。Condition 的通知不会因
“发出早于等待”而丢失。

## 2.1 整体时间预算（墙钟）

语义：

- 提交时可带 `time_budget_seconds`（正实数秒）。缺省 / `null` 表示不带预算，
  作业行为与历史**完全一致**（不产生任何预算判定）。
- 预算从**作业被接受**（服务端接受提交、`created_at` 落盘那一刻）开始按
  **真实经过的墙钟时间**（`time.time()`）扣减，不使用 CPU 时间，也不因步骤
  退避、排队、等待并发槽或进程停机而暂停。
- 持久化两个字段：`time_budget_seconds`（预算总额，用于解释与展示）与
  `budget_deadline`（= 接受时刻 + 预算，绝对时间戳）。跨进程重启后直接按
  绝对截止点判定，已经用掉的时间接着扣，绝不重新计时。
- 预算耗尽的收尾（`_propagate_budget_timeout`）：
  - 已 SUCCEEDED 的步骤：状态与 `result` 原样保留；
  - 其余所有未终态步骤（RUNNING / RETRYING / READY / PENDING，包括排队等
    额度的步骤与尚未满足依赖的下游）统一转 **TIMED_OUT**
    （`fail_reason=budget_exhausted`，`attempt` 定格在已开始的次数）；
  - 正在执行的 task 收到 `CancelledError` 立即中断；步骤的终态标记由扫描
    统一完成，执行路径在预算模式下不会再改写成 CANCELLED；
  - 作业落 **TIMED_OUT**，`error` 形如
    `timed out: wall-clock budget 0.3s exhausted`。
- 步骤在获取并发槽后、真正发起尝试前还有一次预算二次检查，杜绝“刚好在
  截止点之后拿到槽位却又跑了一次”的竞态。

与单步 `timeout` 的关系：单步 `timeout` 限定的是**一次尝试**的时长，超时按
一次可重试失败处理；整体预算限定的是**整个作业**的墙钟寿命，耗尽即终态，
两者正交、可同时使用。

幂等：`time_budget_seconds` 是规范化请求体的一部分，同幂等键但预算不同的
重放按 `idempotency_conflict`（409）拒绝。

## 3.1 取消与预算撞车的边界规则

两种收尾都是“先持久化意图/事实，再由调度器传播”，终态一经写定不可变更。
冲突时按下列固定规则归并，保证结果稳定、可解释：

| 时序 | 结果 | 依据 |
| --- | --- | --- |
| 先取消，之后预算才到点 | **CANCELLED** | tick 内取消判定先于预算判定；取消扫描先把作业落终态，到点时作业已终态不再扫描 |
| 取消先持久化，进程在传播完成前崩溃，停机期间预算也走过 | **CANCELLED** | 恢复扫描同样“取消先于预算”，重启后兑现已落盘的取消意图 |
| 预算先到点，之后再调取消 | **TIMED_OUT**，取消请求 **409 `job_already_terminal`** | 预算扫描已落终态；终态拒绝取消，`cancel_requested` 不会被置位 |
| 所有步骤恰好在信号到达前全部 SUCCEEDED | **SUCCEEDED** | 终态归并中“全成功”优先级最高，取消/超时都“到达太晚”（运行期与重启恢复路径一致） |
| 对任意终态（SUCCEEDED/FAILED/CANCELLED/TIMED_OUT）再调取消 | **409**，原状态不变 | 终态不可变 |

状态与归因区分：CANCELLED 步骤 `fail_reason=cancelled`；TIMED_OUT 步骤
`fail_reason=budget_exhausted`；作业状态分别为 `cancelled` / `timed_out`，
二者在 API 与存储层都不会互相覆盖。

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
4. 终态作业与幂等记录在重启后保持一致；
5. **时间预算跨重启连续**：预算以绝对截止墙钟时间戳持久化，停机时间照扣。
   启动扫描时若发现 `now >= budget_deadline`（预算在停机期间就已走完），
   作业**不重建调度器、不重跑任何步骤**，直接走预算收尾：未终态步骤全部
   TIMED_OUT（trace 依据 `budget already exhausted before process
   restarted`），作业落 TIMED_OUT；已成功结果原样保留。预算仍有效时才按
   常规路径恢复，剩余预算继续扣。

恢复**不**保证：进程在步骤产生外部副作用之后、成功落盘之前崩溃时，
该副作用不会发生（at-least-once 调度）。本服务内置 action 均无外部副作用，
实际接入外部系统时应由动作自身保证幂等。

## 6. 测试矩阵

| 文件 | 覆盖场景 |
| --- | --- |
| `test_idempotency_concurrency.py`（6） | 无键并发独立执行；同键并发去重只执行一次；完成后同键重放；请求体冲突（步骤内容/并发度变化）；HTTP 409 原因码 |
| `test_orchestration_retry.py`（6） | 并发上限的时间窗验证；下游等待上游成功；上游失败→下游 BLOCKED 与链式阻断；重试中 RETRYING 与耗尽 FAILED；`max_attempts=1`；非法依赖/环拒绝 |
| `test_cancel.py`（5） | 运行中取消传播到运行与未启动步骤；已成功结果保留；终态取消 409；未知作业 404；HTTP 终态取消冲突 |
| `test_recovery.py`（4） | 部分成功+在途运行时 kill 后续跑且成功步骤不重跑；终态作业重启不变；RETRYING 中重启后沿用 attempt 预算；优雅关闭后状态可恢复 |
| `test_time_budget.py`（14） | 运行中掐断且成功结果保留/下游显式受阻；退避期耗尽 attempt 定格；排队步骤不放行；停机跨预算恢复即终态；短重启后续扣且正常成功；预算内完成；不带预算完全兼容；先取消后到点为 CANCELLED（运行期与跨重启均覆盖）；先到点后取消为 TIMED_OUT+409；到点瞬间全成功仍 SUCCEEDED；预算场景与失败/阻断归因区分；HTTP 预算字段透传与非法值 422；预算参与幂等冲突 |
