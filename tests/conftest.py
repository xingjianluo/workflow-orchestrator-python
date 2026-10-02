"""pytest 公共夹具。

单测日志要求：打印输入、状态跃迁与判定依据。
- workflow.engine 的 trace 事件同时通过 logging 输出（caplog 可见）；
- 本夹具把每条 trace 收集到 trace_records，并在测试结束后统一打印，
  内容包含事件、作业/步骤、目标状态以及 basis（判定依据）。
"""

from __future__ import annotations

import asyncio
import logging
import os
import tempfile

import pytest
import pytest_asyncio

from app.engine import WorkflowEngine, _trace_var
from app.executor import StepExecutor
from app.store import JobStore


@pytest.fixture
def db_path(tmp_path: str) -> str:
    return os.path.join(tmp_path, "workflow.db")


@pytest_asyncio.fixture
async def engine(db_path):
    store = JobStore(db_path)
    eng = WorkflowEngine(store, StepExecutor())

    records: list[dict] = []

    def sink(event: str, fields: dict) -> None:
        records.append({"event": event, **fields})

    token = _trace_var.set(sink)
    logging.getLogger("workflow.engine").setLevel(logging.DEBUG)
    await eng.start()
    try:
        yield eng
    finally:
        # 打印状态跃迁轨迹（输入在各用例中通过 print 显式打印）
        print("\n---- state transition trace ----")
        for r in records:
            head = f"{r['event']}"
            ctx = []
            for k in ("job_id", "step", "status", "attempt", "basis", "reason",
                      "backoff_seconds", "next_attempt", "idempotency_key",
                      "time_budget_seconds", "budget_deadline", "deadline",
                      "elapsed", "timed_out_on_recovery", "recovered_jobs",
                      "cancelled_on_recovery"):
                if k in r:
                    ctx.append(f"{k}={r[k]}")
            print(f"  {head}: " + " ".join(str(c) for c in ctx))
        print("---------------------------------")
        _trace_var.reset(token)
        await eng.aclose()


@pytest_asyncio.fixture
async def client(engine, db_path):
    """ASGI 测试客户端，与引擎共享同一事件循环。"""
    from httpx import ASGITransport, AsyncClient
    import main as main_module

    main_module._engine = engine  # 让 get_engine 返回测试引擎
    transport = ASGITransport(app=main_module.app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


async def wait_for_status(
    engine: WorkflowEngine, job_id: str, timeout: float = 5.0
) -> str:
    """轮询直到作业进入终态，返回终态值。"""
    deadline = asyncio.get_running_loop().time() + timeout
    from app.models import JOB_TERMINAL

    while asyncio.get_running_loop().time() < deadline:
        job = engine.get_job_state(job_id)
        if job is not None and job.status in JOB_TERMINAL:
            return job.status.value
        await asyncio.sleep(0.01)
    states = {s.key: s.status.value for s in engine.get_steps_state(job_id)}
    raise AssertionError(f"job {job_id} not terminal within {timeout}s: {states}")
