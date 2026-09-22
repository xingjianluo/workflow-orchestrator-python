# 异步工作流编排服务（Python）

在 FastAPI 之上实现的一套可本地运行的异步工作流编排能力：作业由带依赖关系的
步骤（DAG）组成，支持**幂等提交、并发上限、依赖编排、失败退避重试、取消传播、
进程重启恢复**。作业与步骤的每次状态跃迁都落 SQLite 并打印可解释的 trace 日志。

## 1. 快速开始

```bash
uv sync
uv run uvicorn main:app            # 默认持久化到 ./workflow.db
WORKFLOW_DB=/tmp/wf.db uv run uvicorn main:app --port 8000
```

健康检查：`GET /` → `{"service":"workflow-orchestrator"}`
交互式文档：打开 http://127.0.0.1:8000/docs

### 提交一个作业

```bash
curl -s -X POST localhost:8000/jobs -H 'Content-Type: application/json' -d '{
  "idempotency_key": "demo-1",
  "max_concurrency": 2,
  "steps": [
    {"key": "a", "action": "flaky", "params": {"fail_times": 2},
     "max_attempts": 3, "backoff_base": 0.1},
    {"key": "b", "action": "echo", "params": {"v": "ok"}, "depends_on": ["a"]},
    {"key": "c", "action": "fail", "max_attempts": 2},
    {"key": "d", "action": "succeed", "depends_on": ["c"]}
  ]
}'
```

| 接口 | 说明 |
| --- | --- |
| `POST /jobs` | 提交作业（201）。同幂等键+同请求体返回同一作业；请求体不一致返回 409 |
| `GET /jobs/{job_id}` | 查询作业与全部步骤的实时状态 |
| `POST /jobs/{job_id}/cancel` | 请求取消；终态作业返回 409，未知作业返回 404 |

错误响应体形如：
`{"detail": {"detail": "...", "reason": "<可区分原因码>"}}`

原因码 `reason`：

| reason | HTTP | 含义 |
| --- | --- | --- |
| `idempotency_conflict` | 409 | 幂等键被复用，但规范化请求体与首次不一致 |
| `job_already_terminal` | 409 | 作业已在终态，不可取消 |
| `job_not_found` | 404 | 作业不存在 |
| `invalid_dependency` | 400 | 依赖了不存在的步骤、自依赖或依赖成环 |

### 内置步骤 action

| action | params | 行为 |
| --- | --- | --- |
| `succeed` | `result`（可选） | 立即成功 |
| `fail` | `message` / `reason` | 每次尝试都失败，用于验证重试 |
| `flaky` | `fail_times`（默认 1） | 前 N 次失败、之后成功，用于验证重试后恢复 |
| `sleep` | `seconds` | 休眠，可被取消打断，用于验证并发与取消 |
| `echo` | 任意 | 原样返回 params，用于断言编排结果 |

## 2. 状态模型

### 作业状态 `JobStatus`

```
PENDING ──> RUNNING ──> SUCCEEDED     全部步骤 SUCCEEDED
                     ├─> FAILED        存在 FAILED/BLOCKED 终态步骤
                     └─> CANCELLED     收到取消且仍有未完成步骤
```

`SUCCEEDED / FAILED / CANCELLED` 为终态。`error` 字段在 FAILED 时汇总各失败
步骤及其归因，例如：
`c(failed:retry_exhausted); d(blocked:upstream_failed)`。

### 步骤状态 `StepStatus`

```
PENDING ──依赖满足──> READY ──获得并发槽──> RUNNING ──成功──> SUCCEEDED(终态)
   │                   ▲                      │
   │                   │                      ├─可重试失败─> RETRYING ─退避到点─┘
   │                   │                      │
   │                   │                      └─attempt>=max_attempts─> FAILED(终态, retry_exhausted)
   │                   │
   └──上游 FAILED/BLOCKED/CANCELLED──> BLOCKED(终态, upstream_failed)
   └──作业取消且本步骤未完成──────────> CANCELLED(终态, cancelled)
```

终态集合：`SUCCEEDED / FAILED / BLOCKED / CANCELLED`。

关键不变量：

- **下游不会被静默跳过**：任一依赖进入 `FAILED/BLOCKED/CANCELLED`，该步骤立即
  转为 `BLOCKED`（`fail_reason=upstream_failed`，`attempt=0`），阻断沿依赖链传播。
- **失败原因可区分**：步骤终态携带 `fail_reason`，取值
  `retry_exhausted`（重试耗尽）/ `upstream_failed`（被上游阻断）/ `cancelled`（取消）；
  `error` 字段保留最近一次失败的底层原因（如 `forced_failure: boom`）。
- 状态以字符串枚举落库（`pending/running/...`），API 输出稳定字符串。

## 3. 并发与重试规则

### 并发控制

- 每个作业有独立信号量 `asyncio.Semaphore(max_concurrency)`；无依赖（或依赖已
  成功）的步骤在限额内**真正并行**，超出限额的 READY 步骤排队。
- 不同作业之间互不影响，各自独立调度。
- 依赖判定优先于并发调度：步骤只有在全部依赖 `SUCCEEDED` 后才从 PENDING→READY。

### 重试与退避

- 每次业务失败（`StepExecutionError`、超时、未预期异常）都按同一策略处理。
- 第 `attempt`（从 1 起）次失败后的等待：
  `delay = min(backoff_cap, backoff_base * 2**(attempt-1))`
  （默认 base=0.05s、cap=1s 的指数退避）。
- 退避期间步骤状态为 `RETRYING`，`run_at` 记录最早可再次执行的墙钟时间。
- 当 `attempt >= max_attempts`（默认 3，含首次执行）时进入
  `FAILED` + `fail_reason=retry_exhausted`，不再重试。
- 单步可用 `timeout` 限定单次尝试时长，超时按一次失败计入重试。
- `max_attempts=1` 表示不重试，一次失败即终态。

## 4. 幂等提交语义

- 请求体被**规范化**为 `{max_concurrency, steps[...]}`（字段顺序无关），与
  `idempotency_key` 一起持久化。
- 同键重复提交：
  - 请求体一致 → 返回**同一个作业**（同 `job_id`），无论作业在运行中还是已终态，
    都绝不会创建或执行第二个作业；
  - 请求体不一致（步骤内容、依赖、并发度、重试参数等任一差异）→ 返回
    **409 `idempotency_conflict`**，且不影响已存在的作业。
- 不带幂等键的提交每次创建新作业。
- 并发提交由引擎内互斥锁 + 数据库唯一约束双重保护：同键并发 N 次只创建一个作业。

## 5. 取消语义

- `POST /jobs/{id}/cancel` 先持久化 `cancel_requested=true` 并唤醒调度器。
- 调度器执行取消传播扫描：**所有尚未终态的步骤**→ `CANCELLED`
  （`fail_reason=cancelled`），正在执行的任务收到 `CancelledError` 尽快停止
  （`sleep` 类步骤会立刻中断而非等到超时）。
- 已 `SUCCEEDED` 的步骤结果原样保留、不被篡改；作业 → `CANCELLED`。
- 对已终态作业取消返回 409 `job_already_terminal`。

## 6. 重启恢复语义（at-least-once 调度 + 不重复成功）

- 所有状态变更在决策点同步写入 SQLite（WAL 模式，`synchronous=FULL`，写事务加锁），
  步骤的 spec 以 JSON 随步骤行持久化。
- 服务启动（`engine.start()`）时扫描全部非终态作业并重建调度器：
  - `SUCCEEDED` 步骤：结果、`attempt` 原样保留，**绝不重新执行**；
  - `PENDING/READY`：按依赖关系继续调度；
  - 崩溃瞬间处于 `RUNNING`/`RETRYING` 的步骤：由于该次尝试没有成功结果落盘，
    重置为 `READY` 并重新调度（trace 事件 `step_recovered`），`attempt` 计数保留，
    因此重试上限不会因重启被放宽或重置；
  - 已终态作业保持终态，其幂等键在新进程中仍然有效。
- 语义说明：中断的在途尝试可能产生外部副作用，因此恢复保证的是
  **“已成功步骤不重复执行、未成功步骤继续到确定终态”**，而非恰好一次的外部副作用。

## 7. 可观测性：trace 日志

引擎在每个决策点输出 `workflow.engine` 的 INFO 日志，事件包含
`job_id / step / status / attempt / basis（判定依据）`，例如：

```
trace event=step_ready     ... status=ready    basis=all dependencies succeeded
trace event=step_retrying  ... attempt=1 backoff_seconds=0.05 next_attempt=2
trace event=step_blocked   ... status=blocked  basis=upstream in {failed,blocked,cancelled}
trace event=step_failed    ... status=failed   basis=attempt >= max_attempts
trace event=submit_rejected ... reason=idempotency_conflict basis=same key, different ...
trace event=step_recovered ... basis=process restart; no success result, reschedule
```

## 8. 本地验证

```bash
uv run pytest -q                 # 全部 20 个用例
uv run pytest tests/test_idempotency_concurrency.py   # 并发提交 / 幂等冲突
uv run pytest tests/test_orchestration_retry.py       # 依赖 / 并发 / 阻断 / 重试耗尽
uv run pytest tests/test_cancel.py                    # 取消传播
uv run pytest tests/test_recovery.py                  # 进程重启恢复
```

每个用例都会打印：

- `[input]`：提交输入（步骤、依赖、重试参数、并发度、操作时序）；
- `[state]`：作业/步骤的状态跃迁与 `attempt`、`fail_reason`、`error`；
- `[judge]`：断言所依据的判定（如去重后作业数、退避时间窗、终态归因）；
- 末尾的 `---- state transition trace ----`：按时间顺序汇总的完整跃迁轨迹。

### 手动验证重启恢复

```bash
WORKFLOW_DB=./wf.db uv run uvicorn main:app --port 8000
# 1) 提交含 sleep 长步骤的作业
# 2) 在作业运行中 kill -9 <uvicorn pid>
# 3) 用同一 WORKFLOW_DB 重新启动
# 4) GET /jobs/{id}：已成功步骤 attempt 不变、结果保留；
#    中断的长步骤被 step_recovered 重新调度，作业继续到 SUCCEEDED
```

## 9. 代码结构

```
main.py              FastAPI 入口与 lifespan（启动即恢复、关闭落盘）
app/models.py        状态枚举、作业/步骤领域模型、拒绝原因码
app/schemas.py       HTTP 请求/响应 pydantic 模型
app/store.py         SQLite 持久化（WAL、写锁、JSON 字段）
app/executor.py      内置 action 执行器（succeed/fail/flaky/sleep/echo）
app/engine.py        编排引擎：幂等、DAG 调度、信号量、退避重试、取消、恢复
app/api.py           /jobs 提交/查询/取消路由
tests/               四类场景测试 + 打印输入/跃迁/判定依据的夹具
```
