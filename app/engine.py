"""异步工作流编排引擎。

职责
----
1. 提交与幂等：幂等键命中时，请求体规范化比对：
   - 一致 -> 返回同一作业（绝不重新执行）；
   - 不一致 -> 抛 IdempotencyConflict（reason=idempotency_conflict）。
2. 依赖编排与并发控制：
   - 无依赖（或依赖已 SUCCEEDED）的步骤进入 READY，在作业级
     asyncio.Semaphore(max_concurrency) 下并行执行；
   - 任一依赖进入 FAILED/BLOCKED/CANCELLED，步骤立刻 BLOCKED（fail_reason=upstream_failed）。
3. 重试：可重试异常按指数退避（backoff_base*2**(attempt-1)，上限 backoff_cap）
   等待至 run_at；attempt 超过 max_attempts -> FAILED（fail_reason=retry_exhausted）。
4. 取消：cancel() 落 cancel_requested 后唤醒调度器，所有未在终态的步骤
   被标记 CANCELLED，运行中的任务收到 CancelledError；作业 -> CANCELLED。
5. 整体时间预算：提交时可带 time_budget_seconds（墙钟秒）。预算自作业被
   接受起按 time.time() 扣减，截止点 budget_deadline 以绝对时间戳持久化。
   截止点到（含重启时发现其已在过去）时，所有未终态步骤 -> TIMED_OUT
   (fail_reason=budget_exhausted)，已成功结果保留，作业 -> TIMED_OUT；
   退避/排队中的步骤不再被放行。取消与预算谁先谁后只按真实墙钟时刻判定
   （cancel_requested_at < budget_deadline 才算取消在先），与处理顺序、
   是否重启无关：取消在先稳定 CANCELLED；到点之后才到达的取消不被接受
   （运行期 409 job_budget_deadline_reached，恢复时清除迟到标记），
   终态稳定 TIMED_OUT；终态不可变更。
6. 重启恢复：start() 时扫描存储，预算已耗尽的非终态作业直接落 TIMED_OUT，
   其余非终态作业重新装载并调度；SUCCEEDED 步骤原样保留绝不重跑，
   RUNNING/RETRYING/PENDING/READY 继续调度。

线程/并发模型：FastAPI 的 async 处理器与引擎运行在同一事件循环；
SQLite 调用耗时极短（本地文件），直接在事件循环中调用并由 store 内锁保护。
"""

from __future__ import annotations

import asyncio
import contextvars
import logging
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from app.executor import StepExecutionError, StepExecutor
from app.models import (
    FAIL_REASON_BUDGET_EXHAUSTED,
    FAIL_REASON_CANCELLED,
    FAIL_REASON_RETRY_EXHAUSTED,
    FAIL_REASON_UPSTREAM_FAILED,
    JOB_TERMINAL,
    STEP_FAILING,
    STEP_TERMINAL,
    CancellationError,
    JobState,
    JobStatus,
    RejectReason,
    StepSpec,
    StepState,
    StepStatus,
)
from app.store import JobStore

logger = logging.getLogger("workflow.engine")
if not logger.handlers:
    # 独立 handler 并关闭向上传播：本地 uvicorn / pytest 下均可见且不重复
    _h = logging.StreamHandler()
    _h.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    )
    logger.addHandler(_h)
logger.setLevel(logging.INFO)
logger.propagate = False

# 测试/判定日志中打印输入与状态跃迁
_trace_var: contextvars.ContextVar[Any] = contextvars.ContextVar(
    "trace_sink", default=None
)


class WorkflowError(Exception):
    def __init__(self, reason: RejectReason, message: str) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message


class IdempotencyConflict(WorkflowError):
    pass


def _trace(event: str, **fields: Any) -> None:
    """状态跃迁/判定依据日志；同时进 logging 与测试 trace sink。"""
    logger.info("trace event=%s %s", event, fields)
    sink = _trace_var.get()
    if sink is not None:
        sink(event, fields)


def canonical_request(
    specs: list[StepSpec],
    max_concurrency: int,
    time_budget_seconds: float | None = None,
) -> dict[str, Any]:
    """规范化请求体（不含幂等键），用于幂等一致性判定。"""
    return {
        "max_concurrency": max_concurrency,
        "time_budget_seconds": time_budget_seconds,
        "steps": [s.request_payload() for s in specs],
    }


def cancel_outranks_budget(job: JobState) -> bool:
    """取消请求是否“真实先于”预算到点发生。

    判定只依据两个已持久化的墙钟时刻：取消被接受的时刻
    ``cancel_requested_at`` 与预算截止点 ``budget_deadline``。
    因此结论与调度器先处理哪个信号、中间是否重启过都无关：

    - 取消时刻严格早于截止点 -> 取消赢（终态 CANCELLED）；
    - 取消时刻等于/晚于截止点 -> 预算赢（终态 TIMED_OUT），
      到点那一刻才到达的取消视为“到得太晚”；
    - 未带预算：取消恒赢（与历史无预算作业行为一致）；
    - 旧库兼容：cancel_requested=True 但 cancel_requested_at 缺失
      （历史版本已落盘的取消意图）时保守承认取消在先。
    """
    if not job.cancel_requested:
        return False
    if job.budget_deadline is None:
        return True
    if job.cancel_requested_at is None:
        return True
    return job.cancel_requested_at < job.budget_deadline


def validate_specs(specs: list[StepSpec]) -> None:
    keys: set[str] = set()
    for s in specs:
        if not s.key:
            raise WorkflowError(RejectReason.INVALID_DEPENDENCY, "empty step key")
        if s.key in keys:
            raise WorkflowError(
                RejectReason.INVALID_DEPENDENCY, f"duplicate step key: {s.key}"
            )
        keys.add(s.key)
    for s in specs:
        for dep in s.depends_on:
            if dep not in keys:
                raise WorkflowError(
                    RejectReason.INVALID_DEPENDENCY,
                    f"step {s.key!r} depends on unknown step {dep!r}",
                )
            if dep == s.key:
                raise WorkflowError(
                    RejectReason.INVALID_DEPENDENCY,
                    f"step {s.key!r} depends on itself",
                )
    # 环检测（Kahn 拓扑排序）
    indeg = {s.key: len(set(s.depends_on)) for s in specs}
    deps_of = {s.key: set(s.depends_on) for s in specs}
    queue = [k for k, d in indeg.items() if d == 0]
    seen = 0
    while queue:
        k = queue.pop()
        seen += 1
        for s in specs:
            if k in deps_of[s.key]:
                deps_of[s.key].discard(k)
                indeg[s.key] -= 1
                if indeg[s.key] == 0:
                    queue.append(s.key)
    if seen != len(specs):
        raise WorkflowError(
            RejectReason.INVALID_DEPENDENCY, "dependency cycle detected"
        )


class _JobRunner:
    """单个作业的调度器：一个后台 task 跑到作业终态。"""

    def __init__(
        self,
        store: JobStore,
        executor: StepExecutor,
        job_id: str,
        cond: asyncio.Condition,
        tick_gate: Callable[[str], Awaitable[None]] | None = None,
    ) -> None:
        self._store = store
        self._executor = executor
        self._job_id = job_id
        self._cond = cond
        # 每个调度 tick 开头等待的可选挂点（测试用：模拟“到点那一刻调度被
        # 打断”，让取消恰好夹在截止点与收尾扫描之间到达）。生产恒为 None。
        self._tick_gate = tick_gate
        self._sem: asyncio.Semaphore | None = None
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._stop = False
        self._shutting_down = False
        # True 表示在途任务被撤销是因为预算耗尽（区别于引擎关闭与人工取消）
        self._budget_timeout = False

    # ---------- 持久化 + trace 的小工具 ----------
    def _job(self) -> JobState:
        job = self._store.get_job(self._job_id)
        assert job is not None
        return job

    def _steps(self) -> dict[str, StepState]:
        return {s.key: s for s in self._store.list_steps(self._job_id)}

    def _save_step(self, step: StepState, event: str, **why: Any) -> None:
        self._store.update_step(step)
        _trace(
            event,
            job_id=self._job_id,
            step=step.key,
            status=step.status.value,
            attempt=step.attempt,
            **why,
        )

    def _save_job(self, job: JobState, event: str, **why: Any) -> None:
        self._store.update_job(job)
        _trace(
            event,
            job_id=job.id,
            status=job.status.value,
            cancel_requested=job.cancel_requested,
            **why,
        )

    # ---------- 主调度循环 ----------
    async def run(self) -> None:
        job = self._job()
        self._sem = asyncio.Semaphore(job.max_concurrency)
        _trace("job_scheduler_start", job_id=self._job_id, reason="new or recovered")
        try:
            while not self._stop:
                if self._tick_gate is not None:
                    await self._tick_gate(self._job_id)
                done = await self._tick()
                if done:
                    return
                # Condition.notify 不会因“早于等待”而丢失；
                # 另用 0.05s 超时兜底重试退避到点（run_at）的步骤。
                # 有整体预算时，等待时长额外被“距预算截止的剩余时间”封顶，
                # 保证预算一到点最迟在同一切片内被扫到。
                timeout = 0.05
                latest = self._job()
                if latest.budget_deadline is not None:
                    timeout = max(0.0, min(timeout, latest.budget_deadline - time.time()))
                async with self._cond:
                    try:
                        await asyncio.wait_for(self._cond.wait(), timeout=timeout)
                    except asyncio.TimeoutError:
                        pass
        finally:
            _trace("job_scheduler_stop", job_id=self._job_id)

    async def _tick(self) -> bool:
        """执行一轮调度决策。返回 True 表示作业已到终态。"""
        job = self._job()
        if job.status in JOB_TERMINAL:
            return True

        steps = self._steps()

        # 1) 取消传播：仍有未终态步骤时才扫描取消；
        #    若全部步骤恰好已终态（取消到达太晚），交给终态归并判定。
        #    取消与预算到点谁先谁后，按各自真实发生的墙钟时刻判定，
        #    而不是按“本 tick 先看到谁”：取消时刻晚于/等于截止点的，
        #    取消请求本就不会被接受（见 WorkflowEngine.cancel）；若该取消
        #    标记是崩溃前落盘、重启后才被发现的，则在恢复路径处理。
        #    因此这里能成立的 cancel_requested 必然先于到点，取消优先。
        if (
            cancel_outranks_budget(job)
            and any(s.status not in STEP_TERMINAL for s in steps.values())
        ):
            await self._propagate_cancel(job, steps)
            return True

        # 2) 整体时间预算：预算到点且仍有未终态步骤时立即收尾。
        #    晚于截止点到达的取消不改变结论：此时若 cancel_requested
        #    已被置位（理论上只可能来自旧版本写入），按预算收尾，
        #    并清掉这条“到得太晚”的取消标记，终态稳定为 TIMED_OUT。
        if (
            job.budget_deadline is not None
            and time.time() >= job.budget_deadline
            and any(s.status not in STEP_TERMINAL for s in steps.values())
        ):
            if job.cancel_requested:
                job.cancel_requested = False
                job.cancel_requested_at = None
            await self._propagate_budget_timeout(job, steps)
            return True

        # 2) 依赖判定：失败上游 -> BLOCKED；满足依赖 -> READY
        progress = False
        for step in steps.values():
            if step.status != StepStatus.PENDING:
                continue
            dep_states = [steps[d].status for d in step.spec.depends_on]
            if any(x in STEP_FAILING for x in dep_states):
                step.status = StepStatus.BLOCKED
                step.fail_reason = FAIL_REASON_UPSTREAM_FAILED
                step.error = (
                    "blocked: upstream step "
                    + ",".join(
                        d for d in step.spec.depends_on
                        if steps[d].status in STEP_FAILING
                    )
                    + " did not succeed"
                )
                step.finished_at = time.time()
                self._save_step(
                    step,
                    "step_blocked",
                    basis="upstream in {failed,blocked,cancelled}",
                    depends_on=list(step.spec.depends_on),
                )
                progress = True
                continue
            if all(x == StepStatus.SUCCEEDED for x in dep_states):
                step.status = StepStatus.READY
                step.run_at = 0.0
                self._save_step(
                    step,
                    "step_ready",
                    basis="all dependencies succeeded",
                )
                progress = True

        if progress:
            steps = self._steps()

        # 3) 启动到点的 READY/RETRYING 步骤（受并发信号量约束）。
        #    预算已耗尽时不得再放行：排队等槽位的步骤、退避等到点的步骤
        #    都不会再跑，统一由下一次 tick 的预算扫描收尾。
        now = time.time()
        budget_alive = job.budget_deadline is None or now < job.budget_deadline
        for step in steps.values():
            if step.key in self._tasks:
                continue
            if not budget_alive:
                continue
            if step.status == StepStatus.READY or (
                step.status == StepStatus.RETRYING and now >= step.run_at
            ):
                task = asyncio.create_task(self._run_step(step.key))
                self._tasks[step.key] = task
                task.add_done_callback(lambda t, k=step.key: self._tasks.pop(k, None))

        # 4) 作业状态机：PENDING -> RUNNING
        if job.status == JobStatus.PENDING:
            job.status = JobStatus.RUNNING
            self._save_job(job, "job_running", basis="first scheduling tick")

        # 5) 终态判定
        steps = self._steps()
        return await self._maybe_finalize(steps)

    async def _maybe_finalize(self, steps: dict[str, StepState]) -> bool:
        if not all(s.status in STEP_TERMINAL for s in steps.values()):
            return False
        job = self._job()
        # 归并顺序需消除“收尾信号与自然完成同时发生”的竞态歧义：
        # 1) 若所有步骤都已 SUCCEEDED，则作业视为成功（取消/预算到点到达得太晚）；
        # 2) 否则人工取消优先——但仅当取消真实先于预算到点（或作业不带预算）；
        #    到点之后才到达的取消不能把 TIMED_OUT 步骤改写为 CANCELLED；
        # 3) 否则预算耗尽（TIMED_OUT 步骤只可能来自预算收尾扫描）；
        # 4) 其余按失败归并。
        if all(s.status == StepStatus.SUCCEEDED for s in steps.values()):
            job.status = JobStatus.SUCCEEDED
            self._save_job(job, "job_succeeded", basis="all steps succeeded")
        elif cancel_outranks_budget(job) and (
            job.cancel_requested
            or any(s.status is StepStatus.CANCELLED for s in steps.values())
        ):
            job.status = JobStatus.CANCELLED
            self._save_job(
                job,
                "job_cancelled",
                basis="cancel requested before budget deadline propagated to unfinished steps",
            )
        elif any(s.status is StepStatus.TIMED_OUT for s in steps.values()):
            job.status = JobStatus.TIMED_OUT
            self._save_job(
                job,
                "job_timed_out",
                basis="wall-clock time budget exhausted before all steps finished",
            )
        else:
            job.status = JobStatus.FAILED
            failed = [
                f"{s.key}({s.status.value}:{s.fail_reason or s.error or ''})"
                for s in steps.values()
                if s.status in (StepStatus.FAILED, StepStatus.BLOCKED)
            ]
            job.error = "; ".join(failed)
            self._save_job(
                job,
                "job_failed",
                basis="exists failed/blocked terminal step",
                failed=failed,
            )
        # 等待运行中的任务自然收尾（终态标记后它们会观察到并退出）
        self._stop = True
        return True

    # ---------- 取消 / 时间预算收尾 ----------
    async def _propagate_cancel(
        self, job: JobState, steps: dict[str, StepState]
    ) -> None:
        running = [t for k, t in self._tasks.items() if not t.done()]
        for step in steps.values():
            if step.status in STEP_TERMINAL:
                continue
            step.status = StepStatus.CANCELLED
            step.fail_reason = FAIL_REASON_CANCELLED
            step.error = "cancelled before completion"
            step.finished_at = time.time()
            self._save_step(
                step,
                "step_cancelled",
                basis="job cancel_requested while step unfinished",
            )
        job.status = JobStatus.CANCELLED
        job.error = "cancelled by request"
        self._save_job(job, "job_cancelled", basis="cancel propagation sweep")
        for t in running:
            t.cancel()
        if running:
            await asyncio.gather(*running, return_exceptions=True)
        self._stop = True

    async def _propagate_budget_timeout(
        self, job: JobState, steps: dict[str, StepState]
    ) -> None:
        """预算耗尽收尾：未终态步骤统一 TIMED_OUT，运行中任务被撤销。

        已 SUCCEEDED/FAILED/BLOCKED/CANCELLED/TIMED_OUT 的步骤原样保留；
        下游不会被静默跳过——全部进入显式 TIMED_OUT 终态。
        """
        assert job.budget_deadline is not None
        elapsed = time.time() - job.created_at
        running = [t for k, t in self._tasks.items() if not t.done()]
        self._budget_timeout = True
        for step in steps.values():
            if step.status in STEP_TERMINAL:
                continue
            step.status = StepStatus.TIMED_OUT
            step.fail_reason = FAIL_REASON_BUDGET_EXHAUSTED
            step.error = (
                f"timed out: job wall-clock budget "
                f"{job.time_budget_seconds}s exhausted"
            )
            step.finished_at = time.time()
            self._save_step(
                step,
                "step_timed_out",
                basis="job budget deadline reached while step unfinished",
                deadline=job.budget_deadline,
                elapsed=round(elapsed, 6),
            )
        job.status = JobStatus.TIMED_OUT
        job.error = (
            f"timed out: wall-clock budget {job.time_budget_seconds}s exhausted"
        )
        self._save_job(
            job,
            "job_timed_out",
            basis="budget timeout propagation sweep",
            deadline=job.budget_deadline,
            elapsed=round(elapsed, 6),
        )
        for t in running:
            t.cancel()
        if running:
            await asyncio.gather(*running, return_exceptions=True)
        self._stop = True

    # ---------- 单步骤执行（含重试/退避/取消） ----------
    async def _run_step(self, key: str) -> None:
        assert self._sem is not None
        async with self._sem:
            await self._execute_with_retry(key)
        await self._notify()

    async def _notify(self) -> None:
        async with self._cond:
            self._cond.notify_all()

    def cancel_inflight(self) -> None:
        """引擎关闭时撤销所有在途步骤任务（区别于用户取消：不写 CANCELLED）。"""
        self._shutting_down = True
        self._stop = True  # 调度循环不再派生新任务
        for t in self._tasks.values():
            if not t.done():
                t.cancel()

    async def _execute_with_retry(self, key: str) -> None:
        step = self._steps()[key]
        # 进入执行前的并发槽内二次检查（可能已被取消/阻断/预算收尾）
        if step.status in STEP_TERMINAL:
            return
        job = self._job()
        if cancel_outranks_budget(job):
            return
        # 并发槽可能恰好等到预算耗尽才拿到：此时不得再发起一次尝试
        if job.budget_deadline is not None and time.time() >= job.budget_deadline:
            return

        is_retry = step.status == StepStatus.RETRYING
        step.status = StepStatus.RUNNING
        if step.started_at is None:
            step.started_at = time.time()
        step.attempt += 1
        self._save_step(
            step,
            "step_running",
            basis="acquired concurrency slot",
            slot=self._job().max_concurrency,
            retry=is_retry,
        )

        try:
            async with asyncio.timeout(step.spec.timeout):
                result = await self._executor.run_with_attempt(
                    step.spec.action, step.spec.params, step.attempt
                )
        except (CancellationError, asyncio.CancelledError):
            if self._shutting_down:
                # 引擎关闭模拟进程中断：不落 CANCELLED，保留 RUNNING/RETRYING，
                # 交由下次启动时的 step_recovered 重调度。
                raise
            if self._budget_timeout:
                # 预算收尾扫描已把该步骤落为 TIMED_OUT，无需再改状态
                await self._notify()
                raise
            # 作业取消：交由调度循环统一标记 CANCELLED（若尚未标记）
            latest = self._steps()[key]
            if latest.status not in STEP_TERMINAL:
                latest.status = StepStatus.CANCELLED
                latest.fail_reason = FAIL_REASON_CANCELLED
                latest.error = "cancelled during execution"
                latest.finished_at = time.time()
                self._save_step(
                    latest,
                    "step_cancelled",
                    basis="CancelledError while running",
                )
            await self._notify()
            raise
        except TimeoutError as exc:  # asyncio.timeout 超时
            failure = StepExecutionError("timeout", f"timeout after {step.spec.timeout}s")
            self._handle_failure(step, failure)
        except StepExecutionError as exc:
            self._handle_failure(step, exc)
        except Exception as exc:  # 未知异常也视为可重试失败，原因可区分
            wrapped = StepExecutionError("unexpected_error", repr(exc))
            self._handle_failure(step, wrapped)
        else:
            step.result = result
            step.status = StepStatus.SUCCEEDED
            step.error = None
            step.fail_reason = None
            step.finished_at = time.time()
            self._save_step(
                step,
                "step_succeeded",
                basis="executor returned without error",
                result=result,
            )

    def _handle_failure(self, step: StepState, exc: StepExecutionError) -> None:
        step.error = f"{exc.reason}: {exc.message}"
        if step.attempt >= step.spec.max_attempts:
            step.status = StepStatus.FAILED
            step.fail_reason = FAIL_REASON_RETRY_EXHAUSTED
            step.finished_at = time.time()
            self._save_step(
                step,
                "step_failed",
                basis="attempt >= max_attempts",
                max_attempts=step.spec.max_attempts,
                last_error=step.error,
            )
            return
        delay = min(
            step.spec.backoff_cap,
            step.spec.backoff_base * (2 ** (step.attempt - 1)),
        )
        step.status = StepStatus.RETRYING
        step.fail_reason = None
        step.run_at = time.time() + delay
        self._save_step(
            step,
            "step_retrying",
            basis="attempt < max_attempts; exponential backoff scheduled",
            backoff_seconds=round(delay, 6),
            next_attempt=step.attempt + 1,
            last_error=step.error,
        )


class WorkflowEngine:
    def __init__(
        self,
        store: JobStore,
        executor: StepExecutor | None = None,
        tick_gate: Callable[[str], Awaitable[None]] | None = None,
    ) -> None:
        self._store = store
        self._executor = executor or StepExecutor()
        # 测试挂点：在每个作业 runner 的每个调度 tick 开头 await(job_id)。
        # 用于确定性制造“预算到点那一刻调度被打断、取消随后才到”的窗口。
        self._tick_gate = tick_gate
        self._runners: dict[
            str, tuple[_JobRunner, asyncio.Task[None], asyncio.Condition]
        ] = {}
        self._closed = False
        self._lock = asyncio.Lock()
        self._started = False

    # ---------------- 生命周期 ----------------
    async def start(self) -> None:
        if self._closed:
            raise RuntimeError("engine closed")
        if self._started:
            return
        self._started = True
        recovered = 0
        timed_out_on_recovery = 0
        cancelled_on_recovery = 0
        now = time.time()
        for job in self._store.list_all_jobs():
            if job.status in JOB_TERMINAL:
                continue
            steps = self._store.list_steps(job.id)
            unfinished = [s for s in steps if s.status not in STEP_TERMINAL]
            # 停机恢复的撞车判定与运行期完全一致：只按两个已持久化的墙钟
            # 时刻判“谁先谁后”，不因重启、不因谁先被扫描而改变结论。
            deadline_reached = (
                job.budget_deadline is not None and now >= job.budget_deadline
            )
            # 第一判定：取消真实先于预算到点（崩溃前已持久化、传播扫描未
            # 走完）——即使停机期间预算也已经走过，重启后仍兑现 CANCELLED。
            if (
                job.cancel_requested
                and unfinished
                and (not deadline_reached or cancel_outranks_budget(job))
            ):
                self._finalize_cancel_on_recovery(job, steps, now)
                cancelled_on_recovery += 1
                continue
            # 第二判定：预算按墙钟照扣。若取消标记是在到点之后才落盘的
            # （崩溃窗口夹在截止点与收尾之间），该取消不被接受：清掉标记，
            # 作业稳定落 TIMED_OUT。
            if deadline_reached:
                if unfinished:
                    if job.cancel_requested:
                        _trace(
                            "cancel_rejected",
                            job_id=job.id,
                            reason=RejectReason.JOB_BUDGET_DEADLINE_REACHED.value,
                            basis=(
                                "cancel persisted at/after deadline before crash; "
                                "timeout wins on recovery"
                            ),
                            cancel_requested_at=job.cancel_requested_at,
                            deadline=job.budget_deadline,
                        )
                    self._finalize_budget_timeout_on_recovery(job, now)
                    timed_out_on_recovery += 1
                    continue
                # 全部步骤恰好已终态（预算到点太晚）：重建调度器，让首轮
                # tick 的终态归并按步骤现状定论（全 SUCCEEDED -> SUCCEEDED）。
                self._spawn(job.id)
                recovered += 1
                continue
            # 预算仍有效：若取消标记在截止点之前落盘但步骤恰已全终态，
            # 交给重建后的调度器按终态归并处理。
            # 崩溃恢复：进程中断时 RUNNING 的尝试不算成功结果，
            # 重置为 READY 继续调度（attempt 保留，退避到点立即跑）。
            for step in steps:
                if step.status in (StepStatus.RUNNING, StepStatus.RETRYING):
                    step.status = StepStatus.READY
                    step.run_at = 0.0
                    self._store.update_step(step)
                    _trace(
                        "step_recovered",
                        job_id=job.id,
                        step=step.key,
                        status=step.status.value,
                        basis="process restart; no success result, reschedule",
                        attempt=step.attempt,
                    )
            self._spawn(job.id)
            recovered += 1
        _trace(
            "engine_started",
            recovered_jobs=recovered,
            timed_out_on_recovery=timed_out_on_recovery,
            cancelled_on_recovery=cancelled_on_recovery,
        )

    def _finalize_cancel_on_recovery(
        self, job: JobState, steps: list[StepState], now: float
    ) -> None:
        """恢复路径的取消收尾：兑现崩溃前已持久化的取消意图。"""
        for step in steps:
            if step.status in STEP_TERMINAL:
                continue
            step.status = StepStatus.CANCELLED
            step.fail_reason = FAIL_REASON_CANCELLED
            step.error = "cancelled before completion (finalized on recovery)"
            step.finished_at = now
            self._store.update_step(step)
            _trace(
                "step_cancelled",
                job_id=job.id,
                step=step.key,
                status=step.status.value,
                basis="cancel_requested persisted before crash; finalized on restart",
            )
        job.status = JobStatus.CANCELLED
        job.error = "cancelled by request"
        self._store.update_job(job)
        _trace(
            "job_cancelled",
            job_id=job.id,
            status=job.status.value,
            cancel_requested=job.cancel_requested,
            basis="cancel intent survived restart; takes precedence over budget",
        )

    def _finalize_budget_timeout_on_recovery(self, job: JobState, now: float) -> None:
        """恢复路径的预算收尾：不重建调度器，直接把未终态步骤与作业落终态。"""
        steps = self._store.list_steps(job.id)
        elapsed = now - job.created_at
        # 取消标记若晚于截止点落盘则不被接受：清掉，保持终态证据自洽。
        late_cancel = job.cancel_requested and not cancel_outranks_budget(job)
        if late_cancel:
            job.cancel_requested = False
            job.cancel_requested_at = None
        for step in steps:
            if step.status in STEP_TERMINAL:
                continue
            step.status = StepStatus.TIMED_OUT
            step.fail_reason = FAIL_REASON_BUDGET_EXHAUSTED
            step.error = (
                f"timed out: job wall-clock budget "
                f"{job.time_budget_seconds}s exhausted during downtime"
            )
            step.finished_at = now
            self._store.update_step(step)
            _trace(
                "step_timed_out",
                job_id=job.id,
                step=step.key,
                status=step.status.value,
                basis="budget already exhausted before process restarted",
                deadline=job.budget_deadline,
                elapsed=round(elapsed, 6),
            )
        job.status = JobStatus.TIMED_OUT
        job.error = (
            f"timed out: wall-clock budget {job.time_budget_seconds}s exhausted "
            f"before restart"
        )
        self._store.update_job(job)
        _trace(
            "job_timed_out",
            job_id=job.id,
            status=job.status.value,
            cancel_requested=job.cancel_requested,
            basis="deadline already in the past on recovery; no step rescheduled",
            deadline=job.budget_deadline,
            elapsed=round(elapsed, 6),
        )

    async def stop(self) -> None:
        # 先撤销所有在途步骤任务（模拟进程退出；未完成尝试将在重启后重调度），
        # 避免它们在 SQLite 连接关闭后继续写库。
        for runner, _, _ in self._runners.values():
            runner.cancel_inflight()
        tasks = [t for _, t, _ in self._runners.values()]
        for _, _, cond in self._runners.values():
            async with cond:
                cond.notify_all()
        if tasks:
            # 正常情况下 runner 在当前 tick 收尾后立刻自然退出；给一个短宽限。
            # 若调度协程被阻塞在无法解除的等待上（例如测试门控、异常的运行
            # 环境），宽限后强制撤销调度任务本身——这与“进程被杀”等价，
            # 持久化状态由重启恢复路径收敛到确定终态。
            _, pending = await asyncio.wait(tasks, timeout=0.5)
            for t in pending:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        self._runners.clear()
        self._started = False
        _trace("engine_stopped")

    async def aclose(self) -> None:
        """停止调度并关闭持久化连接。"""
        if self._closed:
            return
        await self.stop()
        self._store.close()
        self._closed = True

    def _spawn(self, job_id: str) -> None:
        cond = asyncio.Condition()
        runner = _JobRunner(
            self._store, self._executor, job_id, cond, tick_gate=self._tick_gate
        )
        task = asyncio.create_task(runner.run(), name=f"job-{job_id}")
        self._runners[job_id] = (runner, task, cond)
        # 无需提前 notify：runner 的首轮 _tick 在其第一次挂起前已同步执行。

    async def _wait_terminal(self, job_id: str) -> None:
        """等待某作业 runner 退出（供测试与关闭使用）。"""
        entry = self._runners.get(job_id)
        if entry is not None:
            await asyncio.gather(entry[1], return_exceptions=True)

    # ---------------- 提交 / 取消 / 查询 ----------------
    async def submit(
        self,
        specs: list[StepSpec],
        idempotency_key: str | None,
        max_concurrency: int,
        time_budget_seconds: float | None = None,
        request_payload: dict[str, Any] | None = None,
    ) -> JobState:
        if not self._started:
            await self.start()
        validate_specs(specs)
        if time_budget_seconds is not None and time_budget_seconds <= 0:
            raise WorkflowError(
                RejectReason.INVALID_DEPENDENCY,
                "time_budget_seconds must be positive",
            )
        payload = request_payload or canonical_request(
            specs, max_concurrency, time_budget_seconds
        )

        async with self._lock:
            if idempotency_key is not None:
                existing = self._store.get_job_by_idempotency_key(idempotency_key)
                if existing is not None:
                    if existing.request_payload != payload:
                        _trace(
                            "submit_rejected",
                            idempotency_key=idempotency_key,
                            reason=RejectReason.IDEMPOTENCY_CONFLICT.value,
                            basis="same key, different canonical request payload",
                            existing=existing.request_payload,
                            incoming=payload,
                        )
                        raise IdempotencyConflict(
                            RejectReason.IDEMPOTENCY_CONFLICT,
                            "idempotency key reused with a different request payload",
                        )
                    _trace(
                        "submit_deduplicated",
                        idempotency_key=idempotency_key,
                        job_id=existing.id,
                        basis="same key and identical canonical payload",
                    )
                    return existing

            accepted_at = time.time()
            deadline = (
                accepted_at + time_budget_seconds
                if time_budget_seconds is not None
                else None
            )
            job = JobState(
                id=uuid.uuid4().hex[:12],
                idempotency_key=idempotency_key,
                status=JobStatus.PENDING,
                max_concurrency=max_concurrency,
                request_payload=payload,
                time_budget_seconds=time_budget_seconds,
                # 预算从作业被接受开始按墙钟扣：截止点以绝对时间戳持久化，
                # 使“已用时间”跨进程重启接着扣（停机时间照扣）。
                budget_deadline=deadline,
            )
            job.created_at = accepted_at
            job.updated_at = accepted_at
            self._store.create_job(job, specs)
            _trace(
                "job_created",
                job_id=job.id,
                idempotency_key=idempotency_key,
                steps=[s.key for s in specs],
                max_concurrency=max_concurrency,
                time_budget_seconds=time_budget_seconds,
                budget_deadline=deadline,
                basis="budget measured in wall-clock seconds from acceptance",
            )
            self._spawn(job.id)
            return job

    async def cancel(self, job_id: str) -> JobState:
        job = self._store.get_job(job_id)
        if job is None:
            raise WorkflowError(RejectReason.JOB_NOT_FOUND, f"unknown job: {job_id}")
        if job.status in JOB_TERMINAL:
            # 终态不可变更：SUCCEEDED/FAILED/CANCELLED/TIMED_OUT 均返回 409，
            # “已超预算后再取消”不会把 TIMED_OUT 搅成别的状态。
            raise WorkflowError(
                RejectReason.JOB_ALREADY_TERMINAL,
                f"job already terminal: {job.status.value}",
            )
        # 非终态但预算已经到点：终态按规则应是 TIMED_OUT（收尾可能正在进行，
        # 也可能恰好被调度打断/进程停在收尾前）。此时取消“到得太晚”，
        # 不接受、不落 cancel_requested，返回可区分的 409 原因码。
        if job.budget_deadline is not None and time.time() >= job.budget_deadline:
            _trace(
                "cancel_rejected",
                job_id=job_id,
                reason=RejectReason.JOB_BUDGET_DEADLINE_REACHED.value,
                basis="cancel arrived at/after wall-clock budget deadline; timeout wins",
                deadline=job.budget_deadline,
                now=time.time(),
            )
            raise WorkflowError(
                RejectReason.JOB_BUDGET_DEADLINE_REACHED,
                "job time budget already exhausted at cancel time; "
                "terminal state is timed_out",
            )
        accepted_at = time.time()
        job.cancel_requested = True
        # 仅在尚未记录时锁定首次接受时刻：重复取消不应改写“真实先后”的证据。
        if job.cancel_requested_at is None:
            job.cancel_requested_at = accepted_at
        self._store.update_job(job)
        _trace(
            "cancel_requested",
            job_id=job_id,
            basis="client cancel request accepted before budget deadline",
            cancel_requested_at=job.cancel_requested_at,
            deadline=job.budget_deadline,
        )
        entry = self._runners.get(job_id)
        if entry is not None:
            async with entry[2]:
                entry[2].notify_all()
        return job

    def get_job_state(self, job_id: str) -> JobState | None:
        return self._store.get_job(job_id)

    def get_steps_state(self, job_id: str) -> list[StepState]:
        return self._store.list_steps(job_id)
