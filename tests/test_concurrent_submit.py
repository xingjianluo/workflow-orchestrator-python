"""并发提交：同一幂等键的并发竞争必须收敛为一个作业。"""
from __future__ import annotations

import asyncio

from workflow.models import JobSpec, StepSpec


def _spec(key: str) -> JobSpec:
    return JobSpec(
        idempotency_key=key, max_concurrency=1,
        steps=(StepSpec(id="a", kind="noop", params={"value": key}),),
    )


async def test_racing_same_key_creates_one_job(service):
    """输入: 20 个协程同时提交同一幂等键。
    判定依据: 全部返回同一个 job_id；created=True 恰有一次；步骤只执行一次。
    """
    spec = _spec("race-same")
    results = await asyncio.gather(*[service.submit(spec) for _ in range(20)])
    ids = {j.id for j, _ in results}
    created = sum(1 for _, c in results if c)
    print(f"\n[输入] 20 个并发同键提交；不同 job_id 数={len(ids)} created 次数={created}")
    assert len(ids) == 1
    assert created == 1
    job_id = ids.pop()
    final = await service.wait_for_terminal(job_id, timeout=10)
    assert final.state.value == "SUCCEEDED"
    step = service.store.get_step(job_id, "a")
    assert step.attempt == 1


async def test_racing_distinct_keys_all_succeed(service):
    """输入: 10 个协程并发提交 10 个不同幂等键。判定依据: 产生 10 个独立作业且全部成功。"""
    keys = [f"race-distinct-{i}" for i in range(10)]
    results = await asyncio.gather(*[service.submit(_spec(k)) for k in keys])
    ids = {j.id for j, _ in results}
    print(f"\n[输入] 10 个不同键并发；作业数={len(ids)}（要求 10）")
    assert len(ids) == 10
    finals = await asyncio.gather(*[service.wait_for_terminal(i, timeout=10) for i in ids])
    assert all(j.state.value == "SUCCEEDED" for j in finals)


async def test_racing_same_key_with_different_bodies(service):
    """输入: 同一键、两种不同请求体并发。
    判定依据: 恰好一个作业建成，另一种请求体全部得到 IDEMPOTENCY_CONFLICT，绝不静默覆盖。
    """
    from workflow.models import WorkflowError, Reason

    spec_a = _spec("race-conflict")
    spec_b = JobSpec(
        idempotency_key="race-conflict", max_concurrency=1,
        steps=(StepSpec(id="a", kind="noop", params={"value": "DIFFERENT"}),),
    )

    async def call(sp):
        try:
            return await service.submit(sp)
        except WorkflowError as e:
            return e

    results = await asyncio.gather(*[call(spec_a) for _ in range(10)]
                                   + [call(spec_b) for _ in range(10)])
    jobs = [r for r in results if not isinstance(r, WorkflowError)]
    conflicts = [r for r in results if isinstance(r, WorkflowError)]
    job_ids = {j.id for j, _ in jobs}
    print(f"\n[输入] 同键两种请求体各并发 10 次；建成作业={len(job_ids)} 冲突拒绝={len(conflicts)}")
    assert len(job_ids) == 1
    assert len(conflicts) == 10
    assert all(r.reason is Reason.IDEMPOTENCY_CONFLICT for r in conflicts)
