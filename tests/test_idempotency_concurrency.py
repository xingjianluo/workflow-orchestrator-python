"""场景：并发提交 + 幂等。

- 无幂等键的并发提交产生不同作业，独立执行；
- 相同幂等键并发提交只创建一个作业、只执行一次，返回同一 job_id；
- 幂等键复用但请求体不一致 -> 409 / idempotency_conflict，且原因码可区分；
- 相同幂等键 + 相同请求体 -> 同一作业，不重复执行。
"""

from __future__ import annotations

import asyncio

from app.engine import IdempotencyConflict
from app.models import JobStatus, StepSpec, StepStatus

from tests.conftest import wait_for_status


def _specs(*, fail: bool = False) -> list[StepSpec]:
    return [
        StepSpec(
            key="a",
            action="fail" if fail else "succeed",
            max_attempts=1,
            backoff_base=0.001,
        ),
        StepSpec(
            key="b", action="succeed", depends_on=["a"], backoff_base=0.001
        ),
    ]


async def test_concurrent_submits_without_idempotency_key_run_independently(engine):
    print("\n[input] 4 concurrent submissions, no idempotency key")
    jobs = await asyncio.gather(
        *[engine.submit(_specs(), None, 2) for _ in range(4)]
    )
    ids = {j.id for j in jobs}
    print(f"[judge] distinct job ids: {sorted(ids)}")
    assert len(ids) == 4

    results = await asyncio.gather(*[wait_for_status(engine, jid) for jid in ids])
    print(f"[state] final job statuses: {results}")
    assert set(results) == {JobStatus.SUCCEEDED.value}

    # 每个作业的 a/b 都真实执行成功一次
    for jid in ids:
        steps = {s.key: s for s in engine.get_steps_state(jid)}
        assert steps["a"].status is StepStatus.SUCCEEDED
        assert steps["b"].status is StepStatus.SUCCEEDED
        assert steps["a"].attempt == 1 and steps["b"].attempt == 1


async def test_concurrent_same_idempotency_key_deduplicates(engine):
    print("\n[input] 8 concurrent submissions with same idempotency_key=K-1")

    async def submit():
        return await engine.submit(_specs(), "K-1", 2)

    jobs = await asyncio.gather(*[submit() for _ in range(8)])
    ids = {j.id for j in jobs}
    print(f"[judge] distinct job ids: {ids} (expect exactly 1)")
    assert len(ids) == 1

    job_id = next(iter(ids))
    final = await wait_for_status(engine, job_id)
    steps = engine.get_steps_state(job_id)
    attempts = {s.key: s.attempt for s in steps}
    print(f"[state] job={final} attempts={attempts} (each step executes once)")
    assert final == JobStatus.SUCCEEDED.value
    assert attempts == {"a": 1, "b": 1}


async def test_idempotent_resubmit_returns_same_job_after_completion(engine):
    print("\n[input] submit key=K-2 then resubmit identical payload after finish")
    j1 = await engine.submit(_specs(), "K-2", 2)
    await wait_for_status(engine, j1.id)
    j2 = await engine.submit(_specs(), "K-2", 2)
    print(f"[judge] first={j1.id} second={j2.id}")
    assert j1.id == j2.id
    # 步骤未被重复执行
    steps = engine.get_steps_state(j1.id)
    assert all(s.attempt == 1 for s in steps)


async def test_idempotency_conflict_on_different_payload(engine):
    print("\n[input] key=K-3 reused with different step content")
    j1 = await engine.submit(_specs(fail=False), "K-3", 2)
    await wait_for_status(engine, j1.id)
    try:
        await engine.submit(_specs(fail=True), "K-3", 2)
    except IdempotencyConflict as exc:
        print(f"[judge] rejected reason={exc.reason.value} detail={exc.message}")
        assert exc.reason.value == "idempotency_conflict"
    else:
        raise AssertionError("expected IdempotencyConflict")

    # 原作业不受影响
    assert engine.get_job_state(j1.id).status is JobStatus.SUCCEEDED


async def test_idempotency_conflict_distinguishes_concurrency_change(engine):
    print("\n[input] same steps but max_concurrency differs under same key")
    j1 = await engine.submit(_specs(), "K-4", 2)
    try:
        await engine.submit(_specs(), "K-4", 3)
    except IdempotencyConflict as exc:
        print(f"[judge] rejected reason={exc.reason.value}")
        assert exc.reason.value == "idempotency_conflict"
    else:
        raise AssertionError("expected IdempotencyConflict")
    await wait_for_status(engine, j1.id)


async def test_http_conflict_response_has_distinguishable_reason(engine, client):
    print("\n[input] HTTP POST same key with two payloads")
    body1 = {"idempotency_key": "HK", "max_concurrency": 2,
             "steps": [{"key": "a", "action": "succeed"}]}
    body2 = {"idempotency_key": "HK", "max_concurrency": 2,
             "steps": [{"key": "a", "action": "fail", "max_attempts": 1}]}
    r1 = await client.post("/jobs", json=body1)
    assert r1.status_code == 201
    job_id = r1.json()["job_id"]
    await wait_for_status(engine, job_id)  # 让首个作业先抵达终态
    r2 = await client.post("/jobs", json=body2)
    print(f"[judge] status={r2.status_code} body={r2.json()}")
    assert r2.status_code == 409
    detail = r2.json()["detail"]
    assert detail["reason"] == "idempotency_conflict"

    r3 = await client.get(f"/jobs/{job_id}")
    assert r3.json()["status"] == "succeeded"
