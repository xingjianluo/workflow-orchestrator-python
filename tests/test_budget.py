"""场景：作业整体时间预算（墙钟）。

判定要点：
- 预算从作业被接受时刻起按墙钟扣减，与单步 timeout 无关；
- 到期后所有未终态步骤 -> TIMED_OUT(fail_reason=budget_exhausted)，
  已成功步骤结果原样保留，作业 -> TIMED_OUT（与 CANCELLED/FAILED 区分）；
- 退避等待中的步骤不再到点重跑，排队等并发槽位的步骤不再放行；
- 预算跨重启连续：停机时间计入；重启时已过期的作业直接落 TIMED_OUT；
- 取消与预算到点撞车时按“事件发生先后”仲裁，结果确定：
  cancel_requested_at <= deadline_at -> CANCELLED，否则 TIMED_OUT；
  对终态作业再取消一律 409 job_already_terminal。
"""

from __future__ import annotations

import asyncio
import time

from app.engine import WorkflowEngine, WorkflowError
from app.executor import StepExecutor
from app.models import JobStatus, RejectReason, StepSpec, StepStatus
from app.store import JobStore
from tests.conftest import wait_for_status


async def _new_engine(db_path: str) -> WorkflowEngine:
    store = JobStore(db_path)
    eng = WorkflowEngine(store, StepExecutor())
    await eng.start()
    return eng


async def test_budget_cutoff_running_step_and_preserves_succeeded(engine):
    print("\n[input] c succeeds fast; a sleeps 5s; b depends on a; budget=0.25s")
    specs = [
        StepSpec(key="c", action="succeed", params={"result": {"kept": 1}},
                 max_attempts=1),
        StepSpec(key="a", action="sleep", params={"seconds": 5.0}, max_attempts=1),
        StepSpec(key="b", action="succeed", depends_on=["a"], max_attempts=1),
    ]
    t0 = time.time()
    job = await engine.submit(specs, None, max_concurrency=2, budget_seconds=0.25)
    final = await wait_for_status(engine, job.id, timeout=5)
    elapsed = time.time() - t0
    states = {s.key: s for s in engine.get_steps_state(job.id)}
    print(f"[judge] job={final} elapsed={elapsed:.2f}s (budget=0.25s)")
    for k in "abc":
        s = states[k]
        print(f"[state] {k}: status={s.status.value} fail_reason={s.fail_reason} "
              f"attempts={s.attempt} result={s.result}")
    assert final == JobStatus.TIMED_OUT.value
    assert elapsed < 2.0  # 被预算掐断，而非等 a 睡完 5s
    # 运行中的 a 被掐断；未启动的下游 b 进入明确的超预算收尾（非静默跳过）
    assert states["a"].status is StepStatus.TIMED_OUT
    assert states["a"].fail_reason == "budget_exhausted"
    assert states["a"].attempt == 1
    assert states["b"].status is StepStatus.TIMED_OUT
    assert states["b"].fail_reason == "budget_exhausted"
    assert states["b"].attempt == 0
    # 已成功步骤结果原样保留
    assert states["c"].status is StepStatus.SUCCEEDED
    assert states["c"].result == {"kept": 1}
    job_state = engine.get_job_state(job.id)
    print(f"[state] job.error={job_state.error}")
    assert "budget_exhausted" in (job_state.error or "")


async def test_budget_exhausts_during_backoff_no_rerun(engine):
    print("\n[input] flaky always fails, backoff 0.3s; budget=0.2s expires in backoff")
    specs = [
        StepSpec(key="f", action="flaky", params={"fail_times": 5},
                 max_attempts=5, backoff_base=0.3, backoff_cap=0.3),
    ]
    job = await engine.submit(specs, None, 1, budget_seconds=0.2)
    # 等第一次失败进入 RETRYING（退避 0.3s，晚于预算到点 0.2s）
    for _ in range(100):
        s = engine.get_steps_state(job.id)[0]
        if s.status is StepStatus.RETRYING:
            break
        await asyncio.sleep(0.01)
    s = engine.get_steps_state(job.id)[0]
    print(f"[state] before deadline: status={s.status.value} attempt={s.attempt} "
          f"run_at-now={s.run_at - time.time():.2f}s")
    assert s.status is StepStatus.RETRYING

    final = await wait_for_status(engine, job.id, timeout=5)
    # 再过一段退避时间，确认退避到点后没有又跑一次
    await asyncio.sleep(0.4)
    s2 = engine.get_steps_state(job.id)[0]
    print(f"[judge] job={final} step={s2.status.value} attempt={s2.attempt} "
          f"reason={s2.fail_reason}")
    assert final == JobStatus.TIMED_OUT.value
    assert s2.status is StepStatus.TIMED_OUT
    assert s2.fail_reason == "budget_exhausted"
    assert s2.attempt == 1  # 退避等待期间预算耗尽，到点后未再执行


async def test_queued_step_not_released_after_budget(engine):
    print("\n[input] max_concurrency=1; a sleeps 5s holds slot; b queued; budget=0.2s")
    specs = [
        StepSpec(key="a", action="sleep", params={"seconds": 5.0}, max_attempts=1),
        StepSpec(key="b", action="sleep", params={"seconds": 0.05}, max_attempts=1),
    ]
    job = await engine.submit(specs, None, max_concurrency=1, budget_seconds=0.2)
    final = await wait_for_status(engine, job.id, timeout=5)
    states = {s.key: s for s in engine.get_steps_state(job.id)}
    print(f"[judge] job={final} a={states['a'].status.value} "
          f"b={states['b'].status.value} b.attempt={states['b'].attempt}")
    assert final == JobStatus.TIMED_OUT.value
    assert states["a"].status is StepStatus.TIMED_OUT
    # b 排队等并发额度期间预算耗尽：不再被放行，从未执行
    assert states["b"].status is StepStatus.TIMED_OUT
    assert states["b"].fail_reason == "budget_exhausted"
    assert states["b"].attempt == 0


async def test_budget_expired_during_downtime(db_path):
    print("\n[input] budget=0.3s, step sleeps 5s; stop at ~0.1s; downtime 0.5s; restart")
    eng1 = await _new_engine(db_path)
    specs = [StepSpec(key="a", action="sleep", params={"seconds": 5.0},
                      max_attempts=1)]
    job = await eng1.submit(specs, None, 1, budget_seconds=0.3)
    for _ in range(100):
        s = eng1.get_steps_state(job.id)[0]
        if s.status is StepStatus.RUNNING:
            break
        await asyncio.sleep(0.01)
    print(f"[state before kill] status={s.status.value} attempt={s.attempt}")
    await eng1.aclose()

    await asyncio.sleep(0.5)  # 停机期间墙钟继续走，预算早已用完
    eng2 = await _new_engine(db_path)
    try:
        j = eng2.get_job_state(job.id)
        s2 = eng2.get_steps_state(job.id)[0]
        print(f"[state after restart] job={j.status.value} "
              f"step={s2.status.value} attempt={s2.attempt} reason={s2.fail_reason}")
        # 恢复后直接落到超预算终态，而不是接着往下跑
        assert j.status is JobStatus.TIMED_OUT
        assert s2.status is StepStatus.TIMED_OUT
        assert s2.fail_reason == "budget_exhausted"
        assert s2.attempt == 1  # 崩溃前那一次，恢复后未再执行
        assert "budget_exhausted" in (j.error or "")
        # 保持终态：不会再被调度
        await asyncio.sleep(0.2)
        assert eng2.get_job_state(job.id).status is JobStatus.TIMED_OUT
    finally:
        await eng2.aclose()


async def test_budget_partial_elapsed_across_restart(db_path):
    print("\n[input] budget=0.5s; run 0.3s; restart; remaining ~0.2s then TIMED_OUT")
    eng1 = await _new_engine(db_path)
    specs = [StepSpec(key="a", action="sleep", params={"seconds": 5.0},
                      max_attempts=1)]
    t0 = time.time()
    job = await eng1.submit(specs, None, 1, budget_seconds=0.5)
    await asyncio.sleep(0.3)
    await eng1.aclose()  # 已用掉约 0.3s 预算

    eng2 = await _new_engine(db_path)
    try:
        j = eng2.get_job_state(job.id)
        print(f"[state after restart] job={j.status.value} "
              f"deadline_at-now={j.deadline_at - time.time():.2f}s")
        assert j.status is not JobStatus.TIMED_OUT  # 预算未用完，继续跑
        final = await wait_for_status(eng2, job.id, timeout=5)
        elapsed = time.time() - t0
        s = eng2.get_steps_state(job.id)[0]
        print(f"[judge] final={final} elapsed_since_submit={elapsed:.2f}s "
              f"(budget=0.5s; 若重启后重新计时应 ~0.8s)")
        assert final == JobStatus.TIMED_OUT.value
        # 已用掉的时间接着扣：到期点在提交后 ~0.5s，而非重启后重新计 0.5s
        assert elapsed < 0.7, elapsed
        assert s.status is StepStatus.TIMED_OUT
        assert s.fail_reason == "budget_exhausted"
    finally:
        await eng2.aclose()


async def test_cancel_before_budget_deadline_wins(engine):
    print("\n[input] budget=100s then forced deadline=now+0.3s; cancel at t0 < deadline")
    specs = [StepSpec(key="a", action="sleep", params={"seconds": 5.0},
                      max_attempts=1)]
    job = await engine.submit(specs, None, 1, budget_seconds=100.0)
    # 把预算到点拉近到取消之后：取消事件在先，仲裁应判 CANCELLED
    stored = engine._store.get_job(job.id)
    stored.deadline_at = time.time() + 0.3
    engine._store.update_job(stored)
    await engine.cancel(job.id)

    final = await wait_for_status(engine, job.id, timeout=5)
    await asyncio.sleep(0.4)  # 越过被拉近的到点，确认状态不被预算搅乱
    j = engine.get_job_state(job.id)
    s = engine.get_steps_state(job.id)[0]
    print(f"[judge] final={final} step={s.status.value} reason={s.fail_reason} "
          f"(cancel_requested_at <= deadline_at -> CANCELLED)")
    assert final == JobStatus.CANCELLED.value
    assert j.status is JobStatus.CANCELLED
    assert s.status is StepStatus.CANCELLED
    assert s.fail_reason == "cancelled"


async def test_budget_expired_before_cancel_wins_then_cancel_409(engine):
    print("\n[input] deadline forced to the past; cancel after expiry -> TIMED_OUT")
    specs = [StepSpec(key="a", action="sleep", params={"seconds": 5.0},
                      max_attempts=1)]
    job = await engine.submit(specs, None, 1, budget_seconds=100.0)
    # 预算到点先于取消请求（直接把到点改到过去）
    stored = engine._store.get_job(job.id)
    stored.deadline_at = time.time() - 0.5
    engine._store.update_job(stored)
    try:
        await engine.cancel(job.id)
        print("[judge] cancel accepted, but deadline earlier -> budget wins")
    except WorkflowError as exc:
        # 清扫可能已先完成：对终态作业取消被拒，同样判定预算在先
        print(f"[judge] cancel rejected: reason={exc.reason.value}")
        assert exc.reason is RejectReason.JOB_ALREADY_TERMINAL

    final = await wait_for_status(engine, job.id, timeout=5)
    s = engine.get_steps_state(job.id)[0]
    print(f"[judge] final={final} step={s.status.value} reason={s.fail_reason} "
          f"(deadline_at < cancel_requested_at -> TIMED_OUT)")
    assert final == JobStatus.TIMED_OUT.value
    assert s.status is StepStatus.TIMED_OUT
    assert s.fail_reason == "budget_exhausted"

    # 对已经结束的作业再调取消：稳定 409，不改变状态
    try:
        await engine.cancel(job.id)
    except WorkflowError as exc:
        print(f"[judge] cancel on terminal timed_out job -> {exc.reason.value}")
        assert exc.reason is RejectReason.JOB_ALREADY_TERMINAL
    else:
        raise AssertionError("expected job_already_terminal")
    assert engine.get_job_state(job.id).status is JobStatus.TIMED_OUT


async def test_budget_seconds_part_of_idempotency_payload(engine):
    print("\n[input] same idempotency key, different budget_seconds -> 409 conflict")
    specs = [StepSpec(key="a", action="succeed", max_attempts=1)]
    job = await engine.submit(specs, "IDEM-BUDGET", 1, budget_seconds=10.0)
    same = await engine.submit(specs, "IDEM-BUDGET", 1, budget_seconds=10.0)
    print(f"[judge] identical payload dedup: same job? {same.id == job.id}")
    assert same.id == job.id
    try:
        await engine.submit(specs, "IDEM-BUDGET", 1, budget_seconds=20.0)
    except WorkflowError as exc:
        print(f"[judge] different budget -> reason={exc.reason.value}")
        assert exc.reason is RejectReason.IDEMPOTENCY_CONFLICT
    else:
        raise AssertionError("expected idempotency_conflict")
    final = await wait_for_status(engine, job.id)
    assert final == JobStatus.SUCCEEDED.value  # 预算充足，正常成功


async def test_http_submit_with_budget_and_query(client, engine):
    print("\n[input] HTTP submit budget_seconds=0.2 with 5s sleep step; poll GET")
    r = await client.post("/jobs", json={
        "budget_seconds": 0.2,
        "steps": [{"key": "a", "action": "sleep",
                   "params": {"seconds": 5.0}, "max_attempts": 1}],
    })
    print(f"[state] submit -> {r.status_code} body={r.json()}")
    assert r.status_code == 201
    body = r.json()
    assert body["budget_seconds"] == 0.2
    assert body["deadline_at"] is not None
    jid = body["job_id"]

    final = await wait_for_status(engine, jid, timeout=5)
    r2 = await client.get(f"/jobs/{jid}")
    out = r2.json()
    print(f"[judge] GET status={out['status']} "
          f"step0={out['steps'][0]['status']}/{out['steps'][0]['fail_reason']}")
    assert final == JobStatus.TIMED_OUT.value
    assert out["status"] == "timed_out"
    assert out["steps"][0]["status"] == "timed_out"
    assert out["steps"][0]["fail_reason"] == "budget_exhausted"

    # 兼容范围：不带预算的作业行为与以前完全一致
    r3 = await client.post("/jobs", json={
        "steps": [{"key": "x", "action": "succeed"}],
    })
    jid3 = r3.json()["job_id"]
    assert r3.json()["budget_seconds"] is None
    assert r3.json()["deadline_at"] is None
    final3 = await wait_for_status(engine, jid3)
    print(f"[judge] no-budget job final={final3} (unchanged legacy behavior)")
    assert final3 == JobStatus.SUCCEEDED.value
