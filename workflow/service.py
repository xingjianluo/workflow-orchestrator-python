"""应用服务层：提交（幂等）、查询、取消、重启恢复。

幂等规则（可区分的两种“重复提交”）::

    同 idempotency_key + 同 request_hash  => 返回同一个作业（绝不重复执行）
    同 idempotency_key + 不同 request_hash => 409 IDEMPOTENCY_CONFLICT

request_hash 是对提交载荷（步骤定义、依赖、重试参数、并发上限）的规范化
SHA-256，与作业运行结果无关。
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
import uuid
from typing import Any

from .executors import ExecutorRegistry
from .models import (
    JOB_TERMINAL,
    JobRecord,
    JobSpec,
    JobState,
    Reason,
    StepRecord,
    StepSpec,
    StepState,
    WorkflowError,
)
from .scheduler import JobRunner
from .store import Store

logger = logging.getLogger("workflow.service")


def canonical_request_hash(
    steps: tuple[StepSpec, ...], max_concurrency: int
) -> str:
    """对请求内容做规范化哈希（键排序、区分字段顺序）。"""
    payload = {
        "max_concurrency": max_concurrency,
        "steps": [
            {
                "id": s.id,
                "kind": s.kind,
                "params": s.params,
                "depends_on": list(s.depends_on),
                "max_retries": s.max_retries,
                "retry_backoff_seconds": s.retry_backoff_seconds,
                "timeout_seconds": s.timeout_seconds,
            }
            for s in steps
        ],
    }
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def validate_dag(steps: tuple[StepSpec, ...]) -> None:
    """校验步骤 DAG：ID 唯一、依赖存在、无环。"""
    ids = [s.id for s in steps]
    if len(ids) != len(set(ids)):
        dup = sorted({i for i in ids if ids.count(i) > 1})
        raise WorkflowError(Reason.DAG_CYCLE, f"步骤 ID 重复: {dup}")
    known = set(ids)
    deps: dict[str, set[str]] = {}
    for s in steps:
        unknown = [d for d in s.depends_on if d not in known]
        if unknown:
            raise WorkflowError(
                Reason.DAG_UNKNOWN_DEP,
                f"步骤 {s.id} 依赖了不存在的步骤: {unknown}",
            )
        deps[s.id] = set(s.depends_on)
    # Kahn 拓扑排序检测环
    remaining = {k: set(v) for k, v in deps.items()}
    resolved = set()
    while remaining:
        ready = {k for k, v in remaining.items() if v <= resolved}
        if not ready:
            raise WorkflowError(
                Reason.DAG_CYCLE, f"依赖图存在环，涉及步骤: {sorted(remaining)}"
            )
        resolved |= ready
        for k in ready:
            remaining.pop(k)


class JobService:
    def __init__(
        self,
        store: Store,
        executors: ExecutorRegistry,
        global_max_concurrency: int = 8,
    ):
        self.store = store
        self.executors = executors
        self._global_sem = asyncio.Semaphore(max(1, global_max_concurrency))
        self._runners: dict[str, JobRunner] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()

    # -- 内部：启动 runner --------------------------------------------------
    def _spawn_runner(self, job: JobRecord) -> JobRunner:
        runner = JobRunner(self.store, self.executors, job, self._global_sem)
        self._runners[job.id] = runner
        task = asyncio.create_task(runner.run(), name=f"job-{job.id[:8]}")
        self._tasks[job.id] = task

        def _drop(t: asyncio.Task) -> None:
            self._runners.pop(job.id, None)
            self._tasks.pop(job.id, None)
            if t.exception():
                logger.error("[%s] runner 异常退出: %r", job.id, t.exception())

        task.add_done_callback(_drop)
        return runner

    # -- 提交 ---------------------------------------------------------------
    async def submit(self, spec: JobSpec) -> tuple[JobRecord, bool]:
        """返回 (作业记录, 是否新建)。重复提交返回已有作业。"""
        validate_dag(spec.steps)
        request_hash = canonical_request_hash(spec.steps, spec.max_concurrency)

        async with self._lock:
            existing = self.store.get_job_by_idempotency_key(
                spec.idempotency_key
            )
            if existing is not None:
                if existing.request_hash != request_hash:
                    self.store.append_event(
                        existing.id, "submit_rejected",
                        reason=Reason.IDEMPOTENCY_CONFLICT.value,
                        detail=(
                            f"幂等键 {spec.idempotency_key!r} 已存在但请求体不一致："
                            f"已存 hash={existing.request_hash[:12]} "
                            f"本次 hash={request_hash[:12]}"
                        ),
                    )
                    raise WorkflowError(
                        Reason.IDEMPOTENCY_CONFLICT,
                        (
                            f"幂等键 {spec.idempotency_key!r} 已被其他请求占用："
                            f"已存请求哈希 {existing.request_hash[:12]}...，"
                            f"本次请求哈希 {request_hash[:12]}..."
                        ),
                    )
                logger.info(
                    "[%s] 幂等命中：返回已有作业（不重复执行），key=%r",
                    existing.id, spec.idempotency_key,
                )
                return existing, False

            now = time.time()
            job_id = uuid.uuid4().hex
            spec_json = json.dumps(
                {
                    "idempotency_key": spec.idempotency_key,
                    "max_concurrency": spec.max_concurrency,
                    "steps": [
                        {
                            "id": s.id, "kind": s.kind, "params": s.params,
                            "depends_on": list(s.depends_on),
                            "max_retries": s.max_retries,
                            "retry_backoff_seconds": s.retry_backoff_seconds,
                            "timeout_seconds": s.timeout_seconds,
                        }
                        for s in spec.steps
                    ],
                },
                ensure_ascii=False,
            )
            job = JobRecord(
                id=job_id,
                idempotency_key=spec.idempotency_key,
                request_hash=request_hash,
                spec_json=spec_json,
                state=JobState.PENDING,
                max_concurrency=spec.max_concurrency,
                reason="",
                error="",
                created_ts=now,
                updated_ts=now,
            )
            step_records = [
                StepRecord(
                    id=s.id,
                    job_id=job_id,
                    kind=s.kind,
                    params_json=json.dumps(s.params, ensure_ascii=False),
                    depends_on_json=json.dumps(list(s.depends_on)),
                    state=StepState.PENDING,
                    attempt=0,
                    max_retries=s.max_retries,
                    retry_backoff_seconds=s.retry_backoff_seconds,
                    timeout_seconds=s.timeout_seconds,
                    reason="",
                    error="",
                    result_json="",
                    run_after_ts=0.0,
                )
                for s in spec.steps
            ]
            self.store.insert_job_with_steps(job, step_records)
            self.store.append_event(
                job_id, "job_submitted",
                detail=(
                    f"新作业提交：步骤数={len(step_records)}，"
                    f"作业并发上限={spec.max_concurrency}，hash={request_hash[:12]}"
                ),
            )
            logger.info(
                "[%s] 新作业提交 key=%r 步骤数=%d 并发上限=%d 输入=%s",
                job_id, spec.idempotency_key, len(step_records),
                spec.max_concurrency, spec_json,
            )

        self._spawn_runner(job)
        return job, True

    # -- 取消 ---------------------------------------------------------------
    async def cancel(self, job_id: str) -> JobRecord:
        async with self._lock:
            try:
                job = self.store.get_job(job_id)
            except KeyError:
                raise WorkflowError(Reason.JOB_NOT_FOUND, f"作业不存在: {job_id}") from None
            if job.state in JOB_TERMINAL:
                raise WorkflowError(
                    Reason.JOB_ALREADY_TERMINAL,
                    f"作业已处于终态 {job.state.value}，无法取消",
                )
            runner = self._runners.get(job_id)
            if runner is None:
                # runner 不在本进程（通常发生在恢复前）：直接落盘 CANCELLING，
                # 待 recover_all 启动后由 runner 收割所有未完成步骤。
                self.store.update_job(
                    job_id, state=JobState.CANCELLING,
                    reason=Reason.CANCELLED.value,
                )
                self.store.append_event(
                    job_id, "job_transition",
                    from_state=job.state.value,
                    to_state=JobState.CANCELLING.value,
                    reason=Reason.CANCELLED.value,
                    detail="取消时 runner 不在运行，标记 CANCELLING 等待恢复收割",
                )
                job.state = JobState.CANCELLING
                job.reason = Reason.CANCELLED.value
            else:
                runner.request_cancel()
        return self.store.get_job(job_id)

    # -- 查询 ---------------------------------------------------------------
    def get_job_detail(self, job_id: str) -> dict[str, Any]:
        try:
            job = self.store.get_job(job_id)
        except KeyError:
            raise WorkflowError(Reason.JOB_NOT_FOUND, f"作业不存在: {job_id}") from None
        steps = self.store.list_steps(job_id)
        return {
            "job": {
                "id": job.id,
                "idempotency_key": job.idempotency_key,
                "state": job.state.value,
                "reason": job.reason,
                "error": job.error,
                "max_concurrency": job.max_concurrency,
                "created_ts": job.created_ts,
                "updated_ts": job.updated_ts,
            },
            "steps": [
                {
                    "id": s.id,
                    "kind": s.kind,
                    "state": s.state.value,
                    "attempt": s.attempt,
                    "depends_on": json.loads(s.depends_on_json),
                    "reason": s.reason,
                    "error": s.error,
                    "result": json.loads(s.result_json) if s.result_json else None,
                    "run_after_ts": s.run_after_ts,
                }
                for s in steps
            ],
            "events": self.store.list_events(job_id),
        }

    # -- 重启恢复 ------------------------------------------------------------
    async def recover_all(self) -> int:
        """恢复所有未终态作业；已 SUCCEEDED 的步骤不会被再次执行。"""
        pending = self.store.list_non_terminal_jobs()
        recovered = 0
        for job in pending:
            if job.id in self._runners:
                continue
            self.store.append_event(
                job.id, "job_recovered",
                detail=(
                    f"进程重启后恢复作业（当前状态 {job.state.value}）；"
                    f"RUNNING 步骤将回到 READY，SUCCEEDED 步骤保持不变"
                ),
            )
            logger.info(
                "[%s] 重启恢复作业，状态=%s，runner 重新接管",
                job.id, job.state.value,
            )
            self._spawn_runner(job)
            recovered += 1
        return recovered

    async def wait_for_terminal(self, job_id: str, timeout: float = 30.0) -> JobRecord:
        """等待作业进入终态（测试/优雅关停辅助）。"""
        task = self._tasks.get(job_id)
        if task is not None:
            await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
        return self.store.get_job(job_id)

    async def aclose(self) -> None:
        """等待所有 runner 结束（优雅关停）。"""
        tasks = list(self._tasks.values())
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
