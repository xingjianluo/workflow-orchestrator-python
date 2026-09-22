# 异步工作流编排服务（Python / FastAPI）

在现有 Python HTTP 服务骨架上实现的一套**本地可运行**的异步工作流编排能力：
带依赖关系的作业在并发提交、失败重试、取消和进程重启后，都能得到**一致且可解释**的结果。

- **幂等提交**：同一幂等键的重复提交返回同一个作业，不重新执行；同键不同体被拒绝（409 `IDEMPOTENCY_CONFLICT`）。
- **DAG 依赖编排**：无依赖步骤受并发上限约束并行；有依赖步骤在前置成功后才开始；前置最终失败则下游为 `BLOCKED`，不静默跳过。
- **重试 / 取消 / 恢复**：可重试失败按指数退避重试，耗尽入确定终态；取消传播到所有未完成步骤；重启后从 SQLite 持久化状态继续，**已成功步骤绝不重复执行**。
- **可解释性**：每次提交、状态跃迁、拒绝、恢复都写入 `events` 表，引擎日志打印输入、跃迁与判定依据。

---

## 1. 快速开始

```bash
uv sync
uv run uvicorn main:app --port 18765 --reload
```

启动后会自动执行重启恢复（`recover_all`）并在 `./workflow.db` 自动建表。
交互式文档：<http://localhost:18765/docs>。

环境变量：

| 变量 | 默认 | 说明 |
|---|---|---|
| `WORKFLOW_DB_PATH` | `./workflow.db` | SQLite 文件路径 |
| `WORKFLOW_GLOBAL_CONCURRENCY` | `8` | 跨作业全局并发槽 |
| `WORKFLOW_LOG_LEVEL` | `INFO` | 日志级别 |

---

## 2. HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/jobs` | 提交作业（`body.idempotency_key` 或 `IDEMPOTENCY-KEY` 头）→ 202 |
| `GET`  | `/jobs/{id}` | 作业详情：`job` + `steps` + 完整 `events` 轨迹 |
| `POST` | `/jobs/{id}/cancel` | 请求取消 → 202（已终态 → 409） |
| `GET`  | `/healthz` | 健康检查 |

**提交请求体**：

```json
{
  "idempotency_key": "order-123",
  "max_concurrency": 4,
  "steps": [
    {"id": "a", "kind": "noop", "params": {"value": 1}},
    {"id": "b", "kind": "flaky", "params": {"fail_times": 2, "counter": "b"},
     "max_retries": 3, "retry_backoff_seconds": 0.2, "depends_on": ["a"]},
    {"id": "c", "kind": "slow", "params": {"sleep": 0.1}, "depends_on": ["a"]}
  ]
}
```

响应：`{"job_id", "state", "created", "idempotency_key"}`。`created=false` 表示幂等命中已有作业。

### 可区分的原因码（`detail.reason` / 步骤 `reason` 字段）

| 代码 | HTTP | 含义 |
|---|---|---|
| `IDEMPOTENCY_CONFLICT` | 409 | 幂等键复用但请求内容不一致 |
| `DAG_CYCLE` | 422 | 依赖图有环（也用于步骤 ID 重复） |
| `DAG_UNKNOWN_DEP` | 422 | 依赖了不存在的步骤 |
| `UNKNOWN_STEP_KIND` | 422 | 步骤类型未注册 |
| `JOB_NOT_FOUND` | 404 | 作业不存在 |
| `JOB_ALREADY_TERMINAL` | 409 | 已终态作业不能取消 |
| `RETRIES_EXHAUSTED` | —（步骤） | 重试预算耗尽 |
| `UPSTREAM_FAILED` | —（步骤） | 前置最终失败，步骤受阻 |
| `EXECUTOR_ERROR` | —（步骤） | 执行器抛出不可重试异常 |
| `CANCELLED` | —（步骤） | 因作业取消而中止 |

---

## 3. 状态模型

### 步骤状态机

```
PENDING ──无依赖 / 全部依赖成功──▶ READY ──获取并发槽──▶ RUNNING ──成功──▶ SUCCEEDED(终态)
   │                                 ▲   │                  │
   │                                 │   └──可重试失败──────┘（attempt+1，指数退避后回 READY）
   │                                 │
   │                                 └──────────────────────▶ FAILED(终态, RETRIES_EXHAUSTED / EXECUTOR_ERROR)
   ├──任一上游进入 {FAILED, BLOCKED, CANCELLED}──▶ BLOCKED(终态, UPSTREAM_FAILED)
   └──作业取消且步骤尚未完成──────────────────────▶ CANCELLED(终态, CANCELLED)
```

- **`READY`**：可运行但可能在并发槽排队，或处于重试退避等待（`run_after_ts`）。
- **`BLOCKED`** 是显式终态而非“被跳过”：前置最终失败（`FAILED`/`BLOCKED`/`CANCELLED`）时落盘并给出 `UPSTREAM_FAILED` 原因。
- 只有 `SUCCEEDED/FAILED/BLOCKED/CANCELLED` 是步骤终态。

### 作业状态机

```
PENDING ─▶ RUNNING ─┬─ 全部步骤 SUCCEEDED ─────────────▶ SUCCEEDED
                    ├─ 出现 FAILED/BLOCKED ────────────▶ FAILED
                    └─ cancel() ─▶ CANCELLING ─全部步骤终态─▶ CANCELLED
```

取消时已 `SUCCEEDED` 的步骤**保留其结果**，其余步骤收为 `CANCELLED`，作业终态 `CANCELLED`。
若取消与步骤失败竞争，终态以调度循环实际收敛结果为准（两种终态都有明确事件轨迹可解释）。

---

## 4. 并发与重试规则

**并发**采用两层信号量：

1. **作业级** `max_concurrency`：单个作业内同时 `RUNNING` 的步骤上限；
2. **全局级** `WORKFLOW_GLOBAL_CONCURRENCY`：跨所有作业的总上限。

无依赖步骤在两层槽位都可用时并行启动；不满足依赖的步骤保持 `PENDING`，不占槽。

**重试**：

- 执行器抛 `workflow.executors.RetryableError`（或步骤超时）→ 可重试；其它异常 → 永久失败，不重试。
- 每步 `max_retries` 表示“初次之外的额外尝试次数”，总尝试次数 = `max_retries + 1`。
- 退避：`backoff * 2^(attempt-1)`（attempt 从 1 起），等待期间步骤为 `READY` + `run_after_ts`。
- 超过预算：步骤终态 `FAILED`，reason=`RETRIES_EXHAUSTED`；其下游 `BLOCKED/UPSTREAM_FAILED`；作业 `FAILED`。
- 退避窗口内取消：不再发起下一次尝试，步骤直接 `CANCELLED`。

---

## 5. 恢复语义（进程重启 / 崩溃）

所有状态跃迁遵循“**先持久化、后推进**”：先写 SQLite（WAL + `synchronous=FULL`）并追加事件，再执行后续调度。

启动时 `JobService.recover_all()` 接管所有非终态作业：

| 崩溃时状态 | 恢复行为 |
|---|---|
| 步骤 `SUCCEEDED` | **保持不变，绝不重新执行**（结果已落盘） |
| 步骤 `RUNNING` | 在途尝试结果未知 → 回到 `READY` 重跑，**保留 `attempt` 计数**（at-least-once，会消耗重试预算） |
| 步骤 `READY`（退避中） | 按剩余退避时间继续等待后执行 |
| 步骤 `PENDING` | 正常参与依赖判定 |
| 作业 `CANCELLING` | 恢复后取消信号置位，继续把未完成步骤收为 `CANCELLED` |

> **语义说明**：这是 **at-least-once** 执行模型。成功步骤精确一次（成功即落盘终态）；崩溃时在途的那一次尝试可能已产生外部副作用，恢复后会再执行一次。需要 exactly-once 外部效果时，执行器应自带幂等（可用步骤 ID 作为幂等令牌）。

持久化三张表：`jobs`、`steps`、`events`（自增 `seq` 的完整跃迁/判定流水，`GET /jobs/{id}` 直接返回）。

---

## 6. 本地验证方法

### 自动化测试

```bash
uv run pytest -q                       # 全部 29 个用例
uv run pytest -s tests/test_retry.py  # 查看输入/跃迁/判定日志与事件轨迹
```

覆盖场景：

| 文件 | 覆盖 |
|---|---|
 `tests/test_concurrent_submit.py` | 同键并发 20 次只建 1 个作业；异键并发全部成功；同键异体并发冲突 |
| `tests/test_idempotency.py` | 重复提交返回同一作业且步骤只跑一次；同键异体 409；改并发上限也算冲突 |
| `tests/test_dag_concurrency.py` | 并发上限采样不超限；依赖先后顺序；上游失败下游 BLOCKED；环/未知依赖 422；全局槽跨作业生效 |
| `tests/test_retry.py` | 重试成功、**重试耗尽** `RETRIES_EXHAUSTED`、退避时长、永久失败不重试 |
| `tests/test_cancel.py` | 取消传播到 RUNNING/PENDING、保留已成功步骤、终态再取消 409、退避中取消 |
| `tests/test_recovery.py` | 崩溃重启后成功步骤不重跑（计数器=1）、在途步骤恢复、`CANCELLING` 恢复收割 |
| `tests/test_api.py` | HTTP 全链路状态码与原因码 |

测试日志通过 logging 打印**输入参数、每次状态跃迁（from→to）、判定依据**；`print_trace` 还会输出持久化事件流。

### 手动 curl 验证

```bash
uv run uvicorn main:app --port 18765 &
# 1) 提交
curl -s -X POST localhost:18765/jobs -H 'Content-Type: application/json' -d '{
  "idempotency_key":"demo","max_concurrency":2,"steps":[
    {"id":"a","kind":"flaky","params":{"fail_times":2},"max_retries":3,"retry_backoff_seconds":0.2},
    {"id":"b","kind":"fail_permanent","depends_on":[]},
    {"id":"c","kind":"noop","depends_on":["b"]}]}'
# 2) 查询（观察 a 重试后成功、b FAILED/EXECUTOR_ERROR、c BLOCKED/UPSTREAM_FAILED）
curl -s localhost:18765/jobs/<job_id> | python -m json.tool
# 3) 重复提交 -> created=false，同一 job_id；改任一参数 -> 409 IDEMPOTENCY_CONFLICT
# 4) 取消
curl -s -X POST localhost:18765/jobs/<job_id>/cancel
```

**重启恢复手动验证**：提交含 `slow`（如 `{"sleep":300}`）的作业 → `kill -9` 服务进程 →
重新 `uvicorn` 启动 → 日志出现“重启后恢复了 N 个未完成作业”，
`GET` 可见在途步骤经历 `RUNNING -> READY -> RUNNING`（attempt 保留），已成功步骤保持 `SUCCEEDED`。

---

## 7. 代码结构

| 模块 | 职责 |
|---|---|
| `workflow/models.py` | 状态枚举、原因码、`JobSpec/StepSpec/JobRecord/StepRecord`、`WorkflowError` |
| `workflow/store.py` | SQLite 持久化（作业/步骤/事件），线程安全 |
| `workflow/executors.py` | 执行器注册表、`RetryableError`、内置执行器（noop/slow/flaky/fail_permanent/counted） |
| `workflow/scheduler.py` | `JobRunner`：调度循环、两层并发槽、退避重试、取消传播、重启在途修正 |
| `workflow/service.py` | `JobService`：规范化请求哈希、DAG 校验、幂等提交、取消、`recover_all` |
| `workflow/api.py` | FastAPI 路由、Pydantic 模型、原因码→HTTP 状态映射 |
| `main.py` | 入口与 lifespan（建表 + 恢复 + 优雅关停） |
| `tests/` | 7 个测试文件、29 个用例 |

### 注册自定义执行器

```python
from workflow.executors import ExecutorRegistry, RetryableError

async def my_step(step_id, params, cancel: asyncio.Event):
    if cancel.is_set():
        raise asyncio.CancelledError
    # raise RetryableError("临时故障")  # 会按退避重试
    return {"ok": True}

reg = ExecutorRegistry()
reg.register("my_kind", my_step)
```
