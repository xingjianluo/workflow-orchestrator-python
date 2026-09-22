"""幂等提交：同键同体 -> 同一作业；同键异体 -> 409 可区分原因。"""
from __future__ import annotations

import pytest

from workflow.models import JobSpec, Reason, StepSpec, WorkflowError
from workflow.service import canonical_request_hash

from .conftest import print_trace


def make_spec(key: str = "idem-1", *, value=1, conc: int = 2) -> JobSpec:
    return JobSpec(
        idempotency_key=key,
        max_concurrency=conc,
        steps=(
            StepSpec(id="a", kind="noop", params={"value": value}),
            StepSpec(id="b", kind="noop", params={"value": value + 1},
                     depends_on=("a",)),
        ),
    )


async def test_duplicate_submit_returns_same_job(service):
    """输入: 完全相同的请求提交两次。判定: 幂等键+哈希均相同。预期: 同一 job_id, created 仅首次为 True。"""
    spec = make_spec()
    job1, created1 = await service.submit(spec)
    job2, created2 = await service.submit(spec)
    print(f"\n[输入] 同一请求体提交两次，key={spec.idempotency_key!r}")
    print(f"[判定] created1={created1} created2={created2} id 相同={job1.id == job2.id}")
    assert created1 is True
    assert created2 is False
    assert job1.id == job2.id

    final = await service.wait_for_terminal(job1.id, timeout=10)
    print(f"[状态跃迁] 最终作业状态={final.state.value}")
    print_trace(service.store, job1.id, "幂等-重复提交")
    assert final.state.value == "SUCCEEDED"
    # 关键不变量：步骤只执行一次（没有因重复提交而多跑）
    steps = {s.id: s for s in service.store.list_steps(job1.id)}
    assert steps["a"].attempt == 1 and steps["b"].attempt == 1


async def test_conflicting_submit_rejected_with_reason(service):
    """输入: 同幂等键但步骤参数不同。判定: request_hash 不同。预期: 抛 IDEMPOTENCY_CONFLICT。"""
    job1, _ = await service.submit(make_spec(value=1))
    conflicting = make_spec(value=999)
    h1 = canonical_request_hash(make_spec().steps, 2)
    h2 = canonical_request_hash(conflicting.steps, 2)
    print(f"\n[输入] 同键不同体；hash1={h1[:12]} hash2={h2[:12]} 相等={h1 == h2}")
    assert h1 != h2
    with pytest.raises(WorkflowError) as exc:
        await service.submit(conflicting)
    print(f"[判定] 拒绝原因码={exc.value.reason.value}（要求 IDEMPOTENCY_CONFLICT）")
    assert exc.value.reason is Reason.IDEMPOTENCY_CONFLICT
    # 原作业不受影响，仍然存在且可查询
    detail = service.get_job_detail(job1.id)
    assert detail["job"]["id"] == job1.id
    print_trace(service.store, job1.id, "幂等-冲突被拒")


async def test_concurrency_limit_change_is_conflict(service):
    """输入: 同键同步骤但并发上限不同。预期: 同样被识别为请求体不一致。"""
    await service.submit(make_spec(conc=2))
    with pytest.raises(WorkflowError) as exc:
        await service.submit(make_spec(conc=3))
    assert exc.value.reason is Reason.IDEMPOTENCY_CONFLICT


async def test_different_keys_are_different_jobs(service):
    """输入: 两个不同幂等键、相同步骤。预期: 两个独立作业。"""
    j1, c1 = await service.submit(make_spec("key-A"))
    j2, c2 = await service.submit(make_spec("key-B"))
    print(f"\n[输入] 不同幂等键 -> job1={j1.id[:8]} job2={j2.id[:8]}")
    assert c1 and c2
    assert j1.id != j2.id
    await asyncio_gather(service, [j1.id, j2.id])
    assert service.store.get_job(j1.id).state.value == "SUCCEEDED"
    assert service.store.get_job(j2.id).state.value == "SUCCEEDED"


async def asyncio_gather(service, ids):
    import asyncio
    await asyncio.gather(*[service.wait_for_terminal(i, timeout=10) for i in ids])
