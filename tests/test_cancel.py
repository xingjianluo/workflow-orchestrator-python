"""取消传播：取消后所有未完成步骤进入 CANCELLED，已 SUCCEEDED 的步骤保留。"""
from __future__ import annotations

import asyncio

import pytest

from workflow.models import JobSpec, Reason, StepSpec, WorkflowError

from .conftest import print_trace


async def test_cancel_propagates_to_all_unfinished_steps(service):
    """输入: a 慢步骤(0.5s) 与 b 慢步骤并发；d 依赖 a（未开始）。
    在运行中取消。判定依据:
      a,b -> CANCELLED（响应取消信号）；d -> CANCELLED（PENDING 被收割）；
      作业 -> CANCELLING -> CANCELLED。
    """
    spec = JobSpec(
        idempotency_key="cancel-1", max_concurrency=4,
        steps=(
            StepSpec(id="a", kind="slow", params={"sleep": 1.0}),
            StepSpec(id="b", kind="slow", params={"sleep": 1.0}),
            StepSpec(id="d", kind="noop", params={}, depends_on=("a",)),
        ),
    )
    job, _ = await service.submit(spec)
    await asyncio.sleep(0.1)  # 确保 a,b 已 RUNNING
    running_before = [
        s.id for s in service.store.list_steps(job.id) if s.state.value == "RUNNING"
    ]
    print(f"\n[输入] 取消前 RUNNING 步骤={running_before}（应包含 a,b）")
    cancelled_job = await service.cancel(job.id)
    print(f"[状态跃迁] cancel 返回作业状态={cancelled_job.state.value}")
    assert cancelled_job.state.value == "CANCELLING"

    final = await service.wait_for_terminal(job.id, timeout=10)
    steps = {s.id: s for s in service.store.list_steps(job.id)}
    print(f"[判定] 最终作业={final.state.value}")
    for sid, s in steps.items():
        print(f"  步骤 {sid}: {s.state.value} reason={s.reason}")
    assert final.state.value == "CANCELLED"
    assert all(s.state.value == "CANCELLED" for s in steps.values())
    assert all(s.reason == Reason.CANCELLED.value for s in steps.values())
    print_trace(service.store, job.id, "取消传播")


async def test_cancel_preserves_succeeded_steps(service):
    """输入: a 很快成功，b 长时间运行；在 a 成功后取消。
    判定依据: a 保持 SUCCEEDED（结果不丢），b -> CANCELLED，作业 CANCELLED。
    """
    spec = JobSpec(
        idempotency_key="cancel-2", max_concurrency=4,
        steps=(
            StepSpec(id="a", kind="noop", params={"value": 42}),
            StepSpec(id="b", kind="slow", params={"sleep": 1.0}),
        ),
    )
    job, _ = await service.submit(spec)
    # 等待 a 成功
    for _ in range(100):
        a = service.store.get_step(job.id, "a")
        if a.state.value == "SUCCEEDED":
            break
        await asyncio.sleep(0.01)
    await service.cancel(job.id)
    final = await service.wait_for_terminal(job.id, timeout=10)
    steps = {s.id: s for s in service.store.list_steps(job.id)}
    print(f"\n[判定] a={steps['a'].state.value} b={steps['b'].state.value} job={final.state.value}")
    assert steps["a"].state.value == "SUCCEEDED"
    import json as _json
    assert _json.loads(steps["a"].result_json)["value"] == 42
    assert steps["b"].state.value == "CANCELLED"
    assert final.state.value == "CANCELLED"


async def test_cancel_terminal_job_rejected(service):
    """输入: 已 SUCCEEDED 的作业再次取消。判定依据: 409 JOB_ALREADY_TERMINAL。"""
    spec = JobSpec(
        idempotency_key="cancel-3",
        steps=(StepSpec(id="a", kind="noop", params={}),),
    )
    job, _ = await service.submit(spec)
    await service.wait_for_terminal(job.id, timeout=10)
    with pytest.raises(WorkflowError) as exc:
        await service.cancel(job.id)
    print(f"\n[判定] 终态作业取消被拒，原因={exc.value.reason.value}")
    assert exc.value.reason is Reason.JOB_ALREADY_TERMINAL


async def test_cancel_during_retry_backoff(service):
    """输入: flaky 在重试退避窗口中被取消。
    判定依据: 不再发起下一次尝试，步骤 CANCELLED，attempt 停留在已失败的次数。
    """
    spec = JobSpec(
        idempotency_key="cancel-4", max_concurrency=1,
        steps=(
            StepSpec(id="r", kind="flaky",
                     params={"fail_times": 99, "counter": "cancel-4-r"},
                     max_retries=5, retry_backoff_seconds=2.0),
        ),
    )
    job, _ = await service.submit(spec)
    await asyncio.sleep(0.15)  # 经历一次失败进入长退避
    r0 = service.store.get_step(job.id, "r")
    print(f"\n[输入] 取消时 r 状态={r0.state.value} attempt={r0.attempt}")
    await service.cancel(job.id)
    final = await service.wait_for_terminal(job.id, timeout=10)
    r = service.store.get_step(job.id, "r")
    print(f"[判定] r 最终={r.state.value} reason={r.reason} attempt={r.attempt}")
    assert final.state.value == "CANCELLED"
    assert r.state.value == "CANCELLED"
    assert r.attempt == 1  # 退避中取消，未进行第 2 次尝试
    print_trace(service.store, job.id, "退避中取消")
