"""单作业运行时：调度循环、并发槽、重试退避、取消传播。

一个 :class:`JobRunner` 对应一个作业，生命周期::

    run() 入口
      -> 重启修正（上次崩溃时停在 RUNNING 的步骤回到 READY）
      -> 循环：
           1. 依据依赖与取消信号计算步骤跃迁（PENDING→READY/BLOCKED/CANCELLED）
           2. 为到期的 READY 步骤启动执行协程（受作业级+全局并发槽约束）
           3. 任一步骤完成或退避计时到期后被唤醒，重新评估
           4. 全部步骤终态 => 作业终态（SUCCEEDED / FAILED / CANCELLED）

持久化先于内存判定：每一次状态跃迁都先写 SQLite 并追加事件，再继续调度，
因此进程在任意时刻崩溃都不会丢失已完成步骤，也不会产生“幻读”的终态。
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

from .executors import ExecutorRegistry, RetryableError
from .models import (
    JOB_TERMINAL,
    Reason,
    StepState,
    JobRecord,
    JobState,
    StepRecord,
)
from .store import Store

logger = logging.getLogger("workflow.scheduler")

# 判定依赖“最终失败”的步骤状态（下游据此进入 BLOCKED）
UPSTREAM_FATAL = frozenset(
    {StepState.FAILED, StepState.BLOCKED, StepState.CANCELLED}
)


class JobRunner:
    def __init__(
        self,
        store: Store,
        executors: ExecutorRegistry,
        job: JobRecord,
        global_sem: asyncio.Semaphore,
    ):
        self.store = store
        self.executors = executors
        self.job = job
        self._global_sem = global_sem
        self._local_sem = asyncio.Semaphore(max(1, job.max_concurrency))
        self._cancel = asyncio.Event()
        self._wakeup = asyncio.Event()
        self._tasks: dict[str, asyncio.Task] = {}
        self._ready_monotonic: dict[str, float] = {}

    @property
    def job_id(self) -> str:
        return self.job.id

    # -- 取消 --------------------------------------------------------------
    def request_cancel(self) -> None:
        """请求取消（幂等）。RUNNING 步骤协作式响应，其余步骤由循环收割。"""
        if self.job.state in JOB_TERMINAL:
            return
        self._cancel.set()
        self._wakeup.set()
        if self.job.state is not JobState.CANCELLING:
            self._transition_job(JobState.CANCELLING, Reason.CANCELLED, "用户请求取消")

    # -- 小工具：持久化跃迁 -------------------------------------------------
    def _transition_step(
        self,
        step: StepRecord,
        to_state: StepState,
        reason: str = "",
        detail: str = "",
        **fields: Any,
    ) -> None:
        frm = step.state.value
        self.store.update_step(self.job_id, step.id, state=to_state, reason=reason, **fields)
        self.store.append_event(
            self.job_id,
            "step_transition",
            step_id=step.id,
            from_state=frm,
            to_state=to_state.value,
            reason=reason,
            detail=detail,
        )
        step.state = to_state
        step.reason = reason
        for k, v in fields.items():
            setattr(step, k, v)
        logger.info(
            "[%s] 步骤 %s 状态跃迁 %s -> %s | 原因=%s | 依据=%s",
            self.job_id, step.id, frm, to_state.value, reason or "-", detail or "-",
        )

    def _transition_job(
        self, to_state: JobState, reason: str = "", detail: str = ""
    ) -> None:
        frm = self.job.state.value
        self.store.update_job(self.job_id, state=to_state, reason=reason)
        self.store.append_event(
            self.job_id,
            "job_transition",
            from_state=frm,
            to_state=to_state.value,
            reason=reason,
            detail=detail,
        )
        logger.info(
            "[%s] 作业状态跃迁 %s -> %s | 原因=%s | 依据=%s",
            self.job_id, frm, to_state.value, reason or "-", detail or "-",
        )
        self.job.state = to_state
        self.job.reason = reason

    # -- 重启修正 -----------------------------------------------------------
    def _recover_inflight(self, steps: list[StepRecord]) -> None:
        """进程崩溃时 RUNNING 的尝试结果未知：回到 READY 重跑（attempt 保留）。

        SUCCEEDED 步骤不触碰，保证“已成功步骤绝不重复执行”。
        """
        for s in steps:
            if s.state is StepState.RUNNING:
                remaining = max(0.0, s.run_after_ts - time.time()) if s.run_after_ts else 0.0
                self._ready_monotonic[s.id] = time.monotonic() + remaining
                self._transition_step(
                    s,
                    StepState.READY,
                    reason="",
                    detail=f"重启恢复：RUNNING 的在途尝试结果未知，回到 READY（已保留 attempt={s.attempt}）",
                    run_after_ts=0.0,
                )
            elif s.state is StepState.READY:
                remaining = max(0.0, s.run_after_ts - time.time()) if s.run_after_ts else 0.0
                self._ready_monotonic[s.id] = time.monotonic() + remaining
                self.store.update_step(self.job_id, s.id, run_after_ts=0.0)
                s.run_after_ts = 0.0

    # -- 主循环 -------------------------------------------------------------
    async def run(self) -> JobState:
        if self.job.state is JobState.PENDING:
            self._transition_job(JobState.RUNNING, detail="调度器启动")
        steps = self.store.list_steps(self.job_id)
        by_id = {s.id: s for s in steps}
        self._recover_inflight(steps)

        if self.job.state is JobState.CANCELLING:
            self._cancel.set()

        while True:
            steps = self.store.list_steps(self.job_id)
            by_id = {s.id: s for s in steps}

            # 1) 取消收割：所有尚未开始的步骤 -> CANCELLED
            if self._cancel.is_set():
                for s in steps:
                    if s.state in (StepState.PENDING, StepState.READY):
                        self._transition_step(
                            s, StepState.CANCELLED,
                            reason=Reason.CANCELLED.value,
                            detail="取消信号已置位且步骤尚未完成",
                        )

            # 2) 依赖判定：PENDING -> READY / BLOCKED
            for s in steps:
                if s.state is not StepState.PENDING:
                    continue
                deps = json.loads(s.depends_on_json)
                fatals = [d for d in deps if by_id[d].state in UPSTREAM_FATAL] if deps else []
                if fatals:
                    self._transition_step(
                        s, StepState.BLOCKED,
                        reason=Reason.UPSTREAM_FAILED.value,
                        detail=f"前置步骤最终失败: {fatals}",
                        error=f"上游失败: {fatals}",
                    )
                elif deps and all(by_id[d].state is StepState.SUCCEEDED for d in deps):
                    self._transition_step(
                        s, StepState.READY,
                        detail=f"前置步骤 {deps} 全部 SUCCEEDED",
                    )
                    self._ready_monotonic.setdefault(s.id, time.monotonic())
                elif not deps:
                    self._transition_step(
                        s, StepState.READY, detail="无依赖，立即可调度"
                    )
                    self._ready_monotonic.setdefault(s.id, time.monotonic())

            # 3) 启动到期的 READY 步骤
            now = time.monotonic()
            for s in list(by_id.values()):
                if s.state is not StepState.READY or s.id in self._tasks:
                    continue
                ready_at = self._ready_monotonic.get(s.id, now)
                if ready_at <= now:
                    self._tasks[s.id] = asyncio.create_task(self._execute(s.id))

            # 4) 终态判定
            alive = [s for s in by_id.values() if s.state not in
                     {StepState.SUCCEEDED, StepState.FAILED,
                      StepState.BLOCKED, StepState.CANCELLED}]
            pending_tasks = [t for t in self._tasks.values() if not t.done()]
            if not alive and not pending_tasks:
                return self._finalize(by_id)

            # 5) 等待：最近的退避到期、任一步骤完成、或取消信号
            self._wakeup.clear()
            timeout = 0.05
            for sid, mono in self._ready_monotonic.items():
                st = by_id.get(sid)
                if st and st.state is StepState.READY and sid not in self._tasks:
                    timeout = max(0.005, min(timeout, mono - now))
            if self._cancel.is_set():
                timeout = 0.01
            try:
                await asyncio.wait_for(self._wakeup.wait(), timeout=timeout)
            except TimeoutError:
                pass

    # -- 单步执行 -----------------------------------------------------------
    async def _execute(self, step_id: str) -> None:
        try:
            async with self._local_sem:
                await self._global_sem.acquire()
                try:
                    step = self.store.get_step(self.job_id, step_id)
                    if step.state is not StepState.READY or self._cancel.is_set():
                        if step.state is StepState.READY:
                            self._transition_step(
                                step, StepState.CANCELLED,
                                reason=Reason.CANCELLED.value,
                                detail="获得并发槽时取消信号已置位",
                            )
                        return
                    await self._attempt(step)
                finally:
                    self._global_sem.release()
        finally:
            # 执行协程结束（成功/失败/重试等待/取消）后必须摘除，
            # 否则该步骤会被永久误判为“运行中”，无法被再次调度。
            self._tasks.pop(step_id, None)
            self._wakeup.set()

    async def _attempt(self, step: StepRecord) -> None:
        attempt = step.attempt + 1
        params = json.loads(step.params_json)
        self.store.update_step(
            self.job_id, step.id, state=StepState.RUNNING,
            attempt=attempt, error="",
        )
        self.store.append_event(
            self.job_id, "step_transition", step_id=step.id,
            from_state=StepState.READY.value, to_state=StepState.RUNNING.value,
            detail=f"第 {attempt} 次尝试开始，输入参数={params}",
        )
        step.state = StepState.RUNNING
        step.attempt = attempt
        logger.info(
            "[%s] 步骤 %s READY -> RUNNING | 第 %d 次尝试 | 输入=%s",
            self.job_id, step.id, attempt, params,
        )

        executor = self.executors.get(step.kind)
        started = time.monotonic()
        try:
            if step.timeout_seconds:
                result = await asyncio.wait_for(
                    executor(step.id, params, self._cancel),
                    timeout=step.timeout_seconds,
                )
            else:
                result = await executor(step.id, params, self._cancel)
        except asyncio.CancelledError:
            self._fail_or_cancel(step, cancelled=True,
                                 detail="执行器响应取消信号中止", error="cancelled")
            raise
        except RetryableError as exc:
            self._handle_retryable(step, str(exc), started)
        except (TimeoutError, asyncio.TimeoutError) as exc:
            self._handle_retryable(step, f"执行超时: {exc}", started)
        except Exception as exc:  # noqa: BLE001 - 业务异常必须落盘为终态
            self._transition_step(
                step, StepState.FAILED,
                reason=Reason.EXECUTOR_ERROR.value,
                detail=f"永久失败（不可重试异常 {type(exc).__name__}）",
                error=f"{type(exc).__name__}: {exc}",
            )
        else:
            self._transition_step(
                step, StepState.SUCCEEDED,
                detail=f"执行成功，结果已持久化，耗时 {time.monotonic() - started:.3f}s",
                result_json=json.dumps(result, ensure_ascii=False, default=str),
            )

    def _handle_retryable(self, step: StepRecord, message: str, started: float) -> None:
        if self._cancel.is_set():
            self._transition_step(
                step, StepState.CANCELLED,
                reason=Reason.CANCELLED.value,
                detail=f"可重试失败发生但取消已置位，不再重试: {message}",
                error=message,
            )
            return
        if step.attempt <= step.max_retries:
            backoff = step.retry_backoff_seconds * (2 ** (step.attempt - 1))
            ready_at = time.monotonic() + backoff
            run_after = time.time() + backoff
            self._ready_monotonic[step.id] = ready_at
            self._transition_step(
                step, StepState.READY,
                detail=(
                    f"可重试失败: {message}；第 {step.attempt}/{step.max_retries} 次重试"
                    f"将在 {backoff:.3f}s 后（指数退避）"
                ),
                error=message,
                run_after_ts=run_after,
            )
        else:
            self._transition_step(
                step, StepState.FAILED,
                reason=Reason.RETRIES_EXHAUSTED.value,
                detail=(
                    f"可重试失败: {message}；已用尝试 {step.attempt} 次，"
                    f"上限 {step.max_retries + 1} 次，重试耗尽"
                ),
                error=message,
            )

    def _fail_or_cancel(self, step: StepRecord, *, cancelled: bool,
                        detail: str, error: str) -> None:
        if cancelled:
            self._transition_step(
                step, StepState.CANCELLED,
                reason=Reason.CANCELLED.value, detail=detail, error=error,
            )

    # -- 终态收敛 -----------------------------------------------------------
    def _finalize(self, by_id: dict[str, StepRecord]) -> JobState:
        states = {s.state for s in by_id.values()}
        if self._cancel.is_set():
            # 取消场景：已完成的保留 SUCCEEDED，其余应已被收割
            if states <= {StepState.CANCELLED, StepState.SUCCEEDED}:
                self._transition_job(
                    JobState.CANCELLED, Reason.CANCELLED.value,
                    detail=f"全部步骤终态: {sorted(s.value for s in states)}",
                )
                return JobState.CANCELLED
        if StepState.FAILED in states or StepState.BLOCKED in states:
            failed = [s.id for s in by_id.values() if s.state in
                      (StepState.FAILED, StepState.BLOCKED)]
            self._transition_job(
                JobState.FAILED, Reason.EXECUTOR_ERROR.value,
                detail=f"存在失败/受阻步骤: {failed}",
            )
            return JobState.FAILED
        self._transition_job(
            JobState.SUCCEEDED, detail="全部步骤 SUCCEEDED",
        )
        return JobState.SUCCEEDED
