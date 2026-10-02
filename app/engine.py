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
5. 重启恢复：start() 时扫描存储，所有非终态作业重新装载并调度；
   SUCCEEDED 步骤原样保留绝不重跑，RUNNING/RETRYING/PENDING/READY 继续调度。

线程/并发模型：FastAPI 的 async 处理器与引擎运行在同一事件循环；
SQLite 调用耗时极短（本地文件），直接在事件循环中调用并由 store 内锁保护。
"""

from __future__ import annotations

import asyncio
import contextvars
import logging
import time
import uuid
from typing import Any

from app.executor import StepExecutionError, StepExecutor
from app.models import (
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
    budget_seconds: float | None = None,
) -> dict[str, Any]:
    """规范化请求体（不含幂等键），用于幂等一致性判定。"""
    return {
        "max_concurrency": max_concurrency,
        "budget_seconds": budget_seconds,
        "steps": [s.request_payload() for s in specs],
    }


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
    ) -> None:
        self._store = store
        self._executor = executor
        self._job_id = job_id
        self._cond = cond
        self._sem: asyncio.Semaphore | None = None
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._stop = False
        self._shutting_down = False

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
                done = await self._tick()
                if done:
                    return
                # Condition.notify 不会因“早于等待”而丢失；
                # 另用 0.05s 超时兜底重试退避到点（run_at）的步骤。
                async with self._cond:
                    try:
                        await asyncio.wait_for(self._cond.wait(), timeout=0.05)
                    except TimeoutError:
                        pass
        finally:
            _trace("job_scheduler_stop", job_id=self._job_id)

    async def _tick(self) -> bool:
        """执行一轮调度决策。返回 True 表示作业已到终态。"""
        job = self._job()
        if job.status in JOB_TERMINAL:
            return True

        steps = self._steps()

        # 1) 取消/预算仲裁：仍有未终态步骤时才扫描；
        #    若全部步骤恰好已终态（信号到达太晚），交给终态归并判定。
        #    两者同时悬置时按“事件发生先后”仲裁，结果与 tick 时机无关：
        #    cancel_requested_at <= deadline_at -> CANCELLED，否则 TIMED_OUT。
        if any(s.status not in STEP_TERMINAL for s in steps.values()):
            budget_expired = (
                job.deadline_at is not None and time.time() >= job.deadline_at
            )
            if job.cancel_requested and (
                not budget_expired
                or (job.cancel_requested_at or 0.0) <= (job.deadline_at or 0.0)
            ):
                await self._propagate_cancel(job, steps)
                return True
            if budget_expired:
                await self._propagate_timeout(job, steps)
                return True

        # 2) 依赖判定：失败上游 -> BLOCKED；满足依赖 -> READY
        progress = False
        for step in steps.values():
            if step.status != StepStatus.PENDING:
                continue
            dep_states = [steps[d].status for d in step.spec.depends_on]
            if any(x in STEP_FAILING for x in dep_states):
                step.status = StepStatus.BLOCKED
                step.fail_reason = "upstream_failed"
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

        # 3) 启动到点的 READY/RETRYING 步骤（受并发信号量约束）
        now = time.time()
        for step in steps.values():
            if step.key in self._tasks:
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
        # 归并顺序需消除“取消与收尾同时发生”的竞态歧义：
        # 若所有步骤都已 SUCCEEDED，则作业视为成功（取消到达得太晚）；
        # 否则只要存在被取消的步骤或取消请求，即为 CANCELLED。
        if all(s.status == StepStatus.SUCCEEDED for s in steps.values()):
            job.status = JobStatus.SUCCEEDED
            self._save_job(job, "job_succeeded", basis="all steps succeeded")
        elif any(s.status is StepStatus.TIMED_OUT for s in steps.values()):
            # 防御分支：正常路径上 _propagate_timeout 已终结作业；
            # 仅当超时清扫与步骤自然完成交错时才可能走到这里。
            job.status = JobStatus.TIMED_OUT
            job.error = (
                f"budget_exhausted: job time budget of {job.budget_seconds}s "
                "exhausted (wall clock)"
            )
            self._save_job(
                job,
                "job_timed_out",
                basis="exists timed_out terminal step",
            )
        elif job.cancel_requested or any(
            s.status is StepStatus.CANCELLED for s in steps.values()
        ):
            job.status = JobStatus.CANCELLED
            self._save_job(
                job,
                "job_cancelled",
                basis="cancel requested propagated to unfinished steps",
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

    # ---------- 取消 ----------
    async def _propagate_cancel(
        self, job: JobState, steps: dict[str, StepState]
    ) -> None:
        running = [t for k, t in self._tasks.items() if not t.done()]
        for step in steps.values():
            if step.status in STEP_TERMINAL:
                continue
            step.status = StepStatus.CANCELLED
            step.fail_reason = "cancelled"
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

    # ---------- 预算耗尽 ----------
    async def _propagate_timeout(
        self, job: JobState, steps: dict[str, StepState]
    ) -> None:
        """预算到期清扫：所有未终态步骤 -> TIMED_OUT，作业 -> TIMED_OUT。

        与取消传播结构对称，但状态与归因完全独立：
        步骤 fail_reason=budget_exhausted，作业状态 TIMED_OUT，
        已成功步骤的结果原样保留。
        """
        running = [t for k, t in self._tasks.items() if not t.done()]
        now = time.time()
        for step in steps.values():
            if step.status in STEP_TERMINAL:
                continue
            step.status = StepStatus.TIMED_OUT
            step.fail_reason = "budget_exhausted"
            step.error = "job time budget exhausted before step completed"
            step.finished_at = now
            self._save_step(
                step,
                "step_timed_out",
                basis="wall-clock deadline_at reached while step unfinished",
                deadline_at=job.deadline_at,
            )
        job.status = JobStatus.TIMED_OUT
        job.error = (
            f"budget_exhausted: job time budget of {job.budget_seconds}s "
            "exhausted (wall clock)"
        )
        self._save_job(
            job,
            "job_timed_out",
            basis="now >= deadline_at with unfinished steps",
            budget_seconds=job.budget_seconds,
            deadline_at=job.deadline_at,
        )
        # 运行中的任务立即中断；退避等待与排队等槽位的步骤已被标记终态，
        # 不会再被到点唤醒或放行。
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
        # 进入执行前的并发槽内二次检查（可能已被取消/阻断/超时清扫）
        if step.status in STEP_TERMINAL:
            return
        job = self._job()
        if job.cancel_requested:
            return
        if job.deadline_at is not None and time.time() >= job.deadline_at:
            # 预算在排队等槽位期间耗尽：不启动本次尝试，
            # 保持非终态，交由调度循环的 _propagate_timeout 统一收尾。
            _trace(
                "step_skipped_budget_exhausted",
                job_id=self._job_id,
                step=key,
                basis="deadline reached while waiting for concurrency slot",
                deadline_at=job.deadline_at,
            )
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
            # 作业取消：交由调度循环统一标记 CANCELLED（若尚未标记）
            latest = self._steps()[key]
            if latest.status not in STEP_TERMINAL:
                latest.status = StepStatus.CANCELLED
                latest.fail_reason = "cancelled"
                latest.error = "cancelled during execution"
                latest.finished_at = time.time()
                self._save_step(
                    latest,
                    "step_cancelled",
                    basis="CancelledError while running",
                )
            await self._notify()
            raise
        except TimeoutError:  # asyncio.timeout 超时
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
            step.fail_reason = "retry_exhausted"
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
    ) -> None:
        self._store = store
        self._executor = executor or StepExecutor()
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
        expired = 0
        for job in self._store.list_all_jobs():
            if job.status in JOB_TERMINAL:
                continue
            # 预算在停机期间已经走完：直接落到终态，不重建调度器、
            # 不再执行任何步骤。若取消请求先于预算到点，与运行期仲裁
            # 规则一致地落 CANCELLED，否则落 TIMED_OUT。
            if job.deadline_at is not None and time.time() >= job.deadline_at:
                cancel_first = job.cancel_requested and (
                    job.cancel_requested_at or 0.0
                ) <= job.deadline_at
                self._finalize_recovery_terminal(job, cancelled=cancel_first)
                expired += 1
                continue
            # 崩溃恢复：进程中断时 RUNNING 的尝试不算成功结果，
            # 重置为 READY 继续调度（attempt 保留，退避到点立即跑）。
            for step in self._store.list_steps(job.id):
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
        _trace("engine_started", recovered_jobs=recovered, expired_jobs=expired)

    def _finalize_recovery_terminal(self, job: JobState, *, cancelled: bool) -> None:
        """重启时发现预算已过期：原地清扫为终态（与运行期清扫同语义）。

        cancelled=True 表示取消请求先于预算到点，落 CANCELLED；
        否则落 TIMED_OUT（budget_exhausted）。
        """
        now = time.time()
        step_status = StepStatus.CANCELLED if cancelled else StepStatus.TIMED_OUT
        fail_reason = "cancelled" if cancelled else "budget_exhausted"
        event = "step_cancelled" if cancelled else "step_timed_out"
        for step in self._store.list_steps(job.id):
            if step.status in STEP_TERMINAL:
                continue
            step.status = step_status
            step.fail_reason = fail_reason
            step.error = (
                "cancelled before completion"
                if cancelled
                else "job time budget exhausted before step completed"
            )
            step.finished_at = now
            self._store.update_step(step)
            _trace(
                event,
                job_id=job.id,
                step=step.key,
                status=step.status.value,
                basis="finalized on recovery after downtime",
                deadline_at=job.deadline_at,
            )
        if cancelled:
            job.status = JobStatus.CANCELLED
            job.error = "cancelled by request"
            job_event = "job_cancelled"
            basis = "cancel requested before deadline; finalized on recovery"
        else:
            job.status = JobStatus.TIMED_OUT
            job.error = (
                f"budget_exhausted: job time budget of {job.budget_seconds}s "
                "exhausted (wall clock, incl. downtime)"
            )
            job_event = "job_timed_out"
            basis = "deadline_at already passed at engine start"
        self._store.update_job(job)
        _trace(
            job_event,
            job_id=job.id,
            status=job.status.value,
            basis=basis,
            budget_seconds=job.budget_seconds,
            deadline_at=job.deadline_at,
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
        runner = _JobRunner(self._store, self._executor, job_id, cond)
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
        request_payload: dict[str, Any] | None = None,
        budget_seconds: float | None = None,
    ) -> JobState:
        if not self._started:
            await self.start()
        validate_specs(specs)
        if budget_seconds is not None and budget_seconds <= 0:
            raise WorkflowError(
                RejectReason.INVALID_DEPENDENCY,
                "budget_seconds must be positive",
            )
        payload = request_payload or canonical_request(
            specs, max_concurrency, budget_seconds
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
            job = JobState(
                id=uuid.uuid4().hex[:12],
                idempotency_key=idempotency_key,
                status=JobStatus.PENDING,
                max_concurrency=max_concurrency,
                request_payload=payload,
                created_at=accepted_at,
                budget_seconds=budget_seconds,
                # 墙钟到期点：接受时刻 + 预算。落盘后跨重启连续计时，
                # 停机期间的时间自然计入（重启时比较 now 与 deadline_at）。
                deadline_at=(
                    accepted_at + budget_seconds if budget_seconds is not None else None
                ),
            )
            self._store.create_job(job, specs)
            _trace(
                "job_created",
                job_id=job.id,
                idempotency_key=idempotency_key,
                steps=[s.key for s in specs],
                max_concurrency=max_concurrency,
                budget_seconds=budget_seconds,
                deadline_at=job.deadline_at,
            )
            self._spawn(job.id)
            return job

    async def cancel(self, job_id: str) -> JobState:
        job = self._store.get_job(job_id)
        if job is None:
            raise WorkflowError(RejectReason.JOB_NOT_FOUND, f"unknown job: {job_id}")
        # 定向 UPDATE：仅在非终态时落 cancel_requested + 请求时刻，
        # 不触碰状态列，避免与调度器的终态写互相覆盖。
        requested_at = time.time()
        updated = self._store.try_set_cancel_requested(job_id, requested_at)
        if updated is None:
            raise WorkflowError(
                RejectReason.JOB_ALREADY_TERMINAL,
                f"job already terminal: {job.status.value}",
            )
        _trace(
            "cancel_requested",
            job_id=job_id,
            basis="client cancel request accepted",
            cancel_requested_at=requested_at,
        )
        entry = self._runners.get(job_id)
        if entry is not None:
            async with entry[2]:
                entry[2].notify_all()
        return updated

    def get_job_state(self, job_id: str) -> JobState | None:
        return self._store.get_job(job_id)

    def get_steps_state(self, job_id: str) -> list[StepState]:
        return self._store.list_steps(job_id)
