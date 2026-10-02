"""场景：取消传播。

- 长步骤执行中取消：作业 CANCELLED，运行中步骤与未启动下游全部 CANCELLED；
- 已成功步骤保持 SUCCEEDED（取消不篡改已完成结果）；
- 对终态作业取消 -> 409 / job_already_terminal；
- 对不存在作业取消 -> 404 / job_not_found。
"""

from __future__ import annotations

from app.engine import WorkflowError
from app.models import JobStatus, RejectReason, StepSpec, StepStatus
from tests.conftest import wait_for_status


async def test_cancel_propagates_to_running_and_unfinished_steps(engine):
    print("\n[input] long(a=0.5s) -> b, plus independent c(0.5s); cancel immediately")
    specs = [
        StepSpec(key="a", action="sleep", params={"seconds": 0.5}, max_attempts=1),
        StepSpec(key="b", action="sleep", params={"seconds": 0.2},
                 depends_on=["a"], max_attempts=1),
        StepSpec(key="c", action="sleep", params={"seconds": 0.5}, max_attempts=1),
        StepSpec(key="d", action="sleep", params={"seconds": 0.2},
                 depends_on=["c"], max_attempts=1),
    ]
    job = await engine.submit(specs, None, max_concurrency=2)
    await engine.cancel(job.id)

    final = await wait_for_status(engine, job.id, timeout=3)
    states = {s.key: s for s in engine.get_steps_state(job.id)}
    print(f"[judge] job={final}")
    for k in "abcd":
        s = states[k]
        print(f"[state] {k}: status={s.status.value} fail_reason={s.fail_reason} "
              f"attempts={s.attempt}")
    assert final == JobStatus.CANCELLED.value
    for k in "abcd":
        assert states[k].status is StepStatus.CANCELLED, k
        assert states[k].fail_reason == "cancelled"
    # 下游 b/d 从未真正开始执行
    assert states["b"].attempt == 0 and states["d"].attempt == 0


async def test_succeeded_steps_remain_after_cancel(engine):
    print("\n[input] a succeeds fast; b long-running; cancel while b runs")
    specs = [
        StepSpec(key="a", action="succeed", max_attempts=1),
        StepSpec(key="b", action="sleep", params={"seconds": 0.5},
                 depends_on=["a"], max_attempts=1),
    ]
    job = await engine.submit(specs, None, max_concurrency=1)
    # 等 a 成功、b 开始运行
    import asyncio
    for _ in range(100):
        st = {s.key: s for s in engine.get_steps_state(job.id)}
        if st["b"].status is StepStatus.RUNNING:
            break
        await asyncio.sleep(0.01)
    await engine.cancel(job.id)
    final = await wait_for_status(engine, job.id)
    states = {s.key: s for s in engine.get_steps_state(job.id)}
    print(f"[state] job={final} a={states['a'].status.value} "
          f"b={states['b'].status.value} b.reason={states['b'].fail_reason}")
    assert final == JobStatus.CANCELLED.value
    assert states["a"].status is StepStatus.SUCCEEDED  # 结果保留
    assert states["a"].result == {"ok": True}
    assert states["b"].status is StepStatus.CANCELLED


async def test_cancel_terminal_job_is_rejected(engine):
    print("\n[input] cancel an already succeeded job")
    job = await engine.submit([StepSpec(key="a", action="succeed")], None, 1)
    await wait_for_status(engine, job.id)
    try:
        await engine.cancel(job.id)
    except WorkflowError as exc:
        print(f"[judge] reason={exc.reason.value}")
        assert exc.reason is RejectReason.JOB_ALREADY_TERMINAL
    else:
        raise AssertionError("expected job_already_terminal")


async def test_cancel_unknown_job_404(client):
    print("\n[input] HTTP cancel unknown job")
    r = await client.post("/jobs/nope/cancel")
    print(f"[judge] status={r.status_code} body={r.json()}")
    assert r.status_code == 404
    assert r.json()["detail"]["reason"] == "job_not_found"


async def test_http_cancel_terminal_conflict(client, engine):
    print("\n[input] HTTP cancel terminal job -> 409 job_already_terminal")
    r = await client.post("/jobs", json={"steps": [{"key": "a", "action": "succeed"}]})
    jid = r.json()["job_id"]
    await wait_for_status(engine, jid)
    r2 = await client.post(f"/jobs/{jid}/cancel")
    print(f"[judge] status={r2.status_code} body={r2.json()}")
    assert r2.status_code == 409
    assert r2.json()["detail"]["reason"] == "job_already_terminal"
