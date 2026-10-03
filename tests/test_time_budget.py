"""场景：作业整体时间预算（墙钟）超时。

覆盖：
- 运行中步骤被预算掐断：作业 TIMED_OUT，运行中/排队/下游步骤显式 TIMED_OUT，
  已成功步骤与结果原样保留；
- 退避等待期间预算耗尽：退避到点也不再重试（attempt 不再增长）；
- 停机期间跨过预算：重启后直接落 TIMED_OUT，不重新调度任何步骤；
- 预算未耗尽时正常完成；不带预算的作业行为与历史完全一致；
- 超时与取消撞车：先取消后到点=CANCELLED，先到点后取消=TIMED_OUT 且取消 409，
  对已结束作业再取消=409，状态互不搅乱；
- 预算到点与收尾扫描之间的窗口（运行期调度被打断 / 停机跨过截止点）内到达的
  取消同样拒绝（job_budget_deadline_reached），终态稳定 TIMED_OUT，跨重启一致；
- 取消与预算谁先谁后只按真实墙钟时刻判定（cancel_requested_at vs deadline）；
- 步骤 fail_reason=budget_exhausted 与 cancelled / upstream_failed /
  retry_exhausted 明确可区分；
- HTTP 提交透传 time_budget_seconds，查询可见预算与截止点。
"""

from __future__ import annotations

import asyncio
import time

from app.engine import WorkflowEngine, WorkflowError
from app.executor import StepExecutor
from app.models import (
    FAIL_REASON_BUDGET_EXHAUSTED,
    JobState,
    JobStatus,
    RejectReason,
    StepSpec,
    StepStatus,
)
from app.store import JobStore
from tests.conftest import wait_for_status


async def _new_engine(db_path: str) -> WorkflowEngine:
    eng = WorkflowEngine(JobStore(db_path), StepExecutor())
    await eng.start()
    return eng


async def _wait_job(engine: WorkflowEngine, job_id: str, status: JobStatus,
                   timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        j = engine.get_job_state(job_id)
        if j is not None and j.status is status:
            return
        if j is not None and j.status in (
            JobStatus.SUCCEEDED, JobStatus.FAILED,
            JobStatus.CANCELLED, JobStatus.TIMED_OUT,
        ) and j.status is not status:
            got = {s.key: (s.status.value, s.fail_reason)
                   for s in engine.get_steps_state(job_id)}
            raise AssertionError(
                f"expected {status.value}, got {j.status.value}; steps={got}"
            )
        await asyncio.sleep(0.005)
    raise AssertionError(f"job {job_id} did not reach {status.value}")


async def test_running_steps_cut_off_when_budget_exhausted(engine):
    print("\n[input] a(succeed) + b(sleep 2s) + c(succeed,depends b); "
          "time_budget=0.3s, concurrency=2")
    specs = [
        StepSpec(key="a", action="succeed", max_attempts=1),
        StepSpec(key="b", action="sleep", params={"seconds": 2.0}, max_attempts=1),
        StepSpec(key="c", action="succeed", depends_on=["b"], max_attempts=1),
    ]
    job = await engine.submit(specs, None, max_concurrency=2,
                              time_budget_seconds=0.3)
    t0 = time.monotonic()
    final = await wait_for_status(engine, job.id, timeout=3)
    elapsed = time.monotonic() - t0
    states = {s.key: s for s in engine.get_steps_state(job.id)}
    print(f"[judge] job={final} elapsed={elapsed:.2f}s (expect ~0.3s, not 2s)")
    for k in "abc":
        s = states[k]
        print(f"[state] {k}: status={s.status.value} fail_reason={s.fail_reason} "
              f"attempts={s.attempt} result={s.result}")
    assert final == JobStatus.TIMED_OUT.value
    assert 0.25 < elapsed < 0.9, elapsed
    # 已成功步骤与结果原样保留
    assert states["a"].status is StepStatus.SUCCEEDED
    assert states["a"].result == {"ok": True}
    # 运行中步骤被掐断，归因是预算而非取消
    assert states["b"].status is StepStatus.TIMED_OUT
    assert states["b"].fail_reason == FAIL_REASON_BUDGET_EXHAUSTED
    # 下游进入明确受阻终态，未被静默跳过、从未执行
    assert states["c"].status is StepStatus.TIMED_OUT
    assert states["c"].fail_reason == FAIL_REASON_BUDGET_EXHAUSTED
    assert states["c"].attempt == 0
    job_state = engine.get_job_state(job.id)
    assert "budget" in (job_state.error or "")


async def test_retrying_step_not_released_after_budget_dies(engine):
    print("\n[input] f(fail always, max_attempts=5, backoff=0.5s); "
          "time_budget=0.3s => budget dies during first backoff")
    specs = [
        StepSpec(key="f", action="fail", max_attempts=5,
                 backoff_base=0.5, backoff_cap=1.0),
    ]
    job = await engine.submit(specs, None, max_concurrency=1,
                              time_budget_seconds=0.3)
    final = await wait_for_status(engine, job.id, timeout=3)
    f = engine.get_steps_state(job.id)[0]
    print(f"[judge] job={final} step={f.status.value} attempts={f.attempt} "
          f"reason={f.fail_reason}")
    assert final == JobStatus.TIMED_OUT.value
    assert f.status is StepStatus.TIMED_OUT
    assert f.fail_reason == FAIL_REASON_BUDGET_EXHAUSTED
    assert f.attempt == 1  # 退避期间预算耗尽，到点也没有再跑
    # 再等过原本的退避点，确认不会复活重跑
    await asyncio.sleep(0.7)
    f2 = engine.get_steps_state(job.id)[0]
    print(f"[state] after backoff deadline passed: attempts={f2.attempt} "
          f"status={f2.status.value}")
    assert f2.attempt == 1 and f2.status is StepStatus.TIMED_OUT


async def test_queued_step_not_admitted_after_budget_dies(engine):
    print("\n[input] h1/h2(sleep 2s) occupy 2 slots; q(ready) queues; "
          "time_budget=0.2s => q never admitted")
    specs = [
        StepSpec(key="h1", action="sleep", params={"seconds": 2.0}, max_attempts=1),
        StepSpec(key="h2", action="sleep", params={"seconds": 2.0}, max_attempts=1),
        StepSpec(key="q", action="succeed", max_attempts=1),
    ]
    job = await engine.submit(specs, None, max_concurrency=2,
                              time_budget_seconds=0.2)
    final = await wait_for_status(engine, job.id, timeout=3)
    states = {s.key: s for s in engine.get_steps_state(job.id)}
    print(f"[judge] job={final}")
    for k in ("h1", "h2", "q"):
        print(f"[state] {k}: status={states[k].status.value} "
              f"attempts={states[k].attempt}")
    assert final == JobStatus.TIMED_OUT.value
    assert states["q"].status is StepStatus.TIMED_OUT
    assert states["q"].attempt == 0  # 排队等额度的步骤未被放行
    assert states["h1"].status is StepStatus.TIMED_OUT
    assert states["h2"].status is StepStatus.TIMED_OUT


async def test_budget_crossed_during_downtime_times_out_on_recovery(db_path):
    print("\n[input] long(sleep 5s), time_budget=0.2s; kill 0.05s after submit, "
          "stay down 0.45s (past deadline), restart => must be TIMED_OUT directly")
    eng1 = await _new_engine(db_path)
    specs = [
        StepSpec(key="done", action="succeed", max_attempts=1),
        StepSpec(key="long", action="sleep", params={"seconds": 5.0},
                 max_attempts=1),
    ]
    job = await eng1.submit(specs, None, max_concurrency=2,
                            time_budget_seconds=0.2)
    await asyncio.sleep(0.05)
    mid = {s.key: s for s in eng1.get_steps_state(job.id)}
    j_on_disk = eng1.get_job_state(job.id)
    print(f"[state before kill] done={mid['done'].status.value} "
          f"long={mid['long'].status.value} job={j_on_disk.status.value} "
          f"deadline={j_on_disk.budget_deadline}")
    assert mid["long"].status is StepStatus.RUNNING
    await eng1.aclose()

    await asyncio.sleep(0.45)  # 停机期间墙钟走过截止点

    eng2 = await _new_engine(db_path)
    try:
        # start() 返回时作业必须已是终态，无需再等
        j2 = eng2.get_job_state(job.id)
        states = {s.key: s for s in eng2.get_steps_state(job.id)}
        print(f"[state after restart] job={j2.status.value} "
              f"done={states['done'].status.value}(result={states['done'].result}) "
              f"long={states['long'].status.value} reason={states['long'].fail_reason}")
        assert j2.status is JobStatus.TIMED_OUT
        assert states["done"].status is StepStatus.SUCCEEDED
        assert states["done"].result == {"ok": True}
        assert states["long"].status is StepStatus.TIMED_OUT
        assert states["long"].fail_reason == FAIL_REASON_BUDGET_EXHAUSTED
        # 恢复后再等一拍，确认不会“又接着跑”
        before = states["long"].attempt
        await asyncio.sleep(0.2)
        after = eng2.get_steps_state(job.id)[1].attempt
        print(f"[judge] long.attempt stable: {before} -> {after}")
        assert before == after
    finally:
        await eng2.aclose()


async def test_budget_keeps_counting_after_restart_then_finishes(db_path):
    print("\n[input] sleep 0.15s steps, time_budget=1s; brief restart at 0.05s; "
          "used wall-clock must keep counting; job still succeeds within budget")
    eng1 = await _new_engine(db_path)
    specs = [StepSpec(key="s", action="sleep", params={"seconds": 0.15},
                      max_attempts=1)]
    job = await eng1.submit(specs, None, max_concurrency=1,
                            time_budget_seconds=1.0)
    await asyncio.sleep(0.05)
    j1 = eng1.get_job_state(job.id)
    print(f"[state before kill] job={j1.status.value} "
          f"budget={j1.time_budget_seconds}s deadline={j1.budget_deadline}")
    await eng1.aclose()

    eng2 = await _new_engine(db_path)
    try:
        final = await wait_for_status(eng2, job.id, timeout=3)
        s = eng2.get_steps_state(job.id)[0]
        print(f"[judge] final={final} status={s.status.value} attempts={s.attempt}")
        assert final == JobStatus.SUCCEEDED.value
        assert s.status is StepStatus.SUCCEEDED
    finally:
        await eng2.aclose()


async def test_job_completing_within_budget_succeeds(engine):
    print("\n[input] a,b(succeed), time_budget=5s generously large")
    specs = [
        StepSpec(key="a", action="succeed", max_attempts=1),
        StepSpec(key="b", action="sleep", params={"seconds": 0.02},
                 depends_on=["a"], max_attempts=1),
    ]
    job = await engine.submit(specs, None, max_concurrency=2,
                              time_budget_seconds=5.0)
    final = await wait_for_status(engine, job.id)
    print(f"[judge] final={final} (must be succeeded, budget irrelevant)")
    assert final == JobStatus.SUCCEEDED.value


async def test_job_without_budget_behaves_exactly_as_before(engine):
    print("\n[input] a(succeed)->b(echo); no time_budget field at all")
    specs = [
        StepSpec(key="a", action="succeed", max_attempts=1),
        StepSpec(key="b", action="echo", params={"v": 1}, depends_on=["a"]),
    ]
    job = await engine.submit(specs, None, max_concurrency=2)  # 不传预算
    j = engine.get_job_state(job.id)
    print(f"[state] budget fields: time_budget_seconds={j.time_budget_seconds} "
          f"budget_deadline={j.budget_deadline}")
    assert j.time_budget_seconds is None and j.budget_deadline is None
    final = await wait_for_status(engine, job.id)
    states = {s.key: s for s in engine.get_steps_state(job.id)}
    print(f"[judge] final={final}; a={states['a'].status.value} "
          f"b={states['b'].status.value} result={states['b'].result}")
    assert final == JobStatus.SUCCEEDED.value
    assert states["b"].result == {"v": 1}


async def test_cancel_before_budget_deadline_wins(engine):
    print("\n[input] sleep 2s, time_budget=0.5s; cancel at ~0.05s; wait past deadline")
    specs = [StepSpec(key="b", action="sleep", params={"seconds": 2.0},
                      max_attempts=1)]
    job = await engine.submit(specs, None, max_concurrency=1,
                              time_budget_seconds=0.5)
    await asyncio.sleep(0.05)
    await engine.cancel(job.id)  # 先取消
    await _wait_job(engine, job.id, JobStatus.CANCELLED)
    await asyncio.sleep(0.6)  # 再跨过预算截止点
    j = engine.get_job_state(job.id)
    s = engine.get_steps_state(job.id)[0]
    print(f"[judge] after deadline passed: job={j.status.value} "
          f"step={s.status.value} reason={s.fail_reason} "
          f"cancel_requested={j.cancel_requested}")
    assert j.status is JobStatus.CANCELLED  # 没有被超时搅成 TIMED_OUT
    assert s.status is StepStatus.CANCELLED
    assert s.fail_reason == "cancelled"


async def test_cancel_after_budget_deadline_is_rejected(engine):
    print("\n[input] sleep 2s, time_budget=0.15s; wait TIMED_OUT, then cancel")
    specs = [StepSpec(key="b", action="sleep", params={"seconds": 2.0},
                      max_attempts=1)]
    job = await engine.submit(specs, None, max_concurrency=1,
                              time_budget_seconds=0.15)
    await _wait_job(engine, job.id, JobStatus.TIMED_OUT, timeout=3)
    try:
        await engine.cancel(job.id)
    except WorkflowError as exc:
        print(f"[judge] cancel rejected reason={exc.reason.value} msg={exc.message}")
        assert exc.reason is RejectReason.JOB_ALREADY_TERMINAL
    else:
        raise AssertionError("expected job_already_terminal")
    j = engine.get_job_state(job.id)
    s = engine.get_steps_state(job.id)[0]
    print(f"[state] job stays {j.status.value}; step={s.status.value} "
          f"reason={s.fail_reason}; cancel_requested={j.cancel_requested}")
    assert j.status is JobStatus.TIMED_OUT
    assert j.cancel_requested is False
    assert s.fail_reason == FAIL_REASON_BUDGET_EXHAUSTED


async def test_budget_reason_distinguishable_from_other_terminals(engine):
    print("\n[input] 4 steps: ok / fail(2 attempts) / dep of fail / budget-victim")
    specs = [
        StepSpec(key="ok", action="succeed", max_attempts=1),
        StepSpec(key="bad", action="fail", max_attempts=2, backoff_base=0.001),
        StepSpec(key="down", action="succeed", depends_on=["bad"]),
    ]
    job = await engine.submit(specs, None, max_concurrency=4,
                              time_budget_seconds=5.0)
    final = await wait_for_status(engine, job.id)
    states = {s.key: s for s in engine.get_steps_state(job.id)}
    print(f"[judge] job={final} (budget large -> failure path, not timeout)")
    assert final == JobStatus.FAILED.value
    assert states["bad"].fail_reason == "retry_exhausted"
    assert states["down"].status is StepStatus.BLOCKED
    assert states["down"].fail_reason == "upstream_failed"
    assert engine.get_job_state(job.id).status is not JobStatus.TIMED_OUT


async def test_http_submit_budget_and_query_fields(client, engine):
    print("\n[input] HTTP POST job with time_budget_seconds=0.2, sleep step 2s")
    body = {
        "max_concurrency": 1,
        "time_budget_seconds": 0.2,
        "steps": [{"key": "b", "action": "sleep", "params": {"seconds": 2.0},
                   "max_attempts": 1}],
    }
    r = await client.post("/jobs", json=body)
    print(f"[judge] submit status={r.status_code} body={r.json()}")
    assert r.status_code == 201
    data = r.json()
    assert data["time_budget_seconds"] == 0.2
    assert isinstance(data["budget_deadline"], float)
    job_id = data["job_id"]
    await _wait_job(engine, job_id, JobStatus.TIMED_OUT, timeout=3)
    r2 = await client.get(f"/jobs/{job_id}")
    body2 = r2.json()
    print(f"[state] GET status={body2['status']} step={body2['steps'][0]['status']} "
          f"reason={body2['steps'][0]['fail_reason']}")
    assert body2["status"] == JobStatus.TIMED_OUT.value
    assert body2["steps"][0]["fail_reason"] == FAIL_REASON_BUDGET_EXHAUSTED

    # 非法预算（<=0）应被请求校验拒绝
    r3 = await client.post("/jobs", json={
        "time_budget_seconds": 0,
        "steps": [{"key": "x", "action": "succeed"}],
    })
    print(f"[judge] non-positive budget -> HTTP {r3.status_code}")
    assert r3.status_code == 422


async def test_budget_participates_in_idempotency_conflict(engine):
    print("\n[input] same idempotency key=BK reused with different time_budget")
    specs = [StepSpec(key="a", action="sleep", params={"seconds": 0.6},
                      max_attempts=1)]
    j1 = await engine.submit(specs, "BK", max_concurrency=1,
                             time_budget_seconds=0.3)
    print("[state] first submit budget=0.3 -> expect TIMED_OUT")
    await _wait_job(engine, j1.id, JobStatus.TIMED_OUT, timeout=3)
    try:
        await engine.submit(specs, "BK", max_concurrency=1,
                            time_budget_seconds=5.0)
    except WorkflowError as exc:
        print(f"[judge] resubmit with budget=5.0 rejected: {exc.reason.value}")
        assert exc.reason is RejectReason.IDEMPOTENCY_CONFLICT
    else:
        raise AssertionError("expected idempotency_conflict")
    # 同键同预算重放 -> 同一作业
    same = await engine.submit(specs, "BK", max_concurrency=1,
                               time_budget_seconds=0.3)
    print(f"[judge] identical-budget resubmit returns same job: "
          f"{same.id == j1.id}, status stays {same.status.value}")
    assert same.id == j1.id
    assert same.status is JobStatus.TIMED_OUT
    assert engine.get_job_state(j1.id).status is JobStatus.TIMED_OUT


async def test_cancel_before_downtime_budget_deadline_wins_after_restart(db_path):
    print("\n[input] sleep 5s, time_budget=0.5s; cancel at ~0.05s then kill; "
          "stay down 0.6s (past deadline); restart => cancel must still win")
    eng1 = await _new_engine(db_path)
    specs = [
        StepSpec(key="done", action="succeed", max_attempts=1),
        StepSpec(key="long", action="sleep", params={"seconds": 5.0},
                 max_attempts=1),
    ]
    job = await eng1.submit(specs, None, max_concurrency=2,
                            time_budget_seconds=0.5)
    await asyncio.sleep(0.05)
    await eng1.cancel(job.id)  # 取消先落盘
    # 不给运行中的调度循环留时间完成传播，立即“杀死”进程
    await eng1.aclose()
    on_disk = JobStore(db_path).get_job(job.id)
    print(f"[state on disk after kill] status={on_disk.status.value} "
          f"cancel_requested={on_disk.cancel_requested}")
    await asyncio.sleep(0.6)  # 停机期间跨过预算截止点

    eng2 = await _new_engine(db_path)
    try:
        j2 = eng2.get_job_state(job.id)
        states = {s.key: s for s in eng2.get_steps_state(job.id)}
        print(f"[state after restart] job={j2.status.value} "
              f"done={states['done'].status.value} long={states['long'].status.value} "
              f"long.reason={states['long'].fail_reason}")
        assert j2.status is JobStatus.CANCELLED  # 取消意图跨重启压过预算
        assert states["done"].status is StepStatus.SUCCEEDED
        assert states["long"].status is StepStatus.CANCELLED
        assert states["long"].fail_reason == "cancelled"
    finally:
        await eng2.aclose()


async def test_cancel_after_budget_deadline_but_before_sweep_is_rejected(db_path):
    """预算先到点、收尾扫描被打断的窗口内到达的取消：拒绝且终态仍为 TIMED_OUT。

    用 tick_gate 把“截止点之后的第一个调度 tick”确定性挂住，模拟题目描述的
    “那一刻调度被打断 / 进程正好在收尾前被停掉”：截止点已过但作业仍 RUNNING，
    此时发取消必须按冲突拒绝（job_budget_deadline_reached），不放行取消、
    不改写终态；放行收尾后作业稳定落 TIMED_OUT。
    """
    print("\n[input] sleep 5s, time_budget=0.1s; scheduler tick after deadline is "
          "gated; cancel arrives inside the deadline<->sweep window")
    gated = asyncio.Event()
    release = asyncio.Event()

    async def tick_gate(job_id: str) -> None:
        if not gated.is_set():
            gated.set()  # 首个 tick 立即放行（提交/起调度）
            return
        await release.wait()  # 之后的 tick 全部挂住，直到测试显式放行

    eng = WorkflowEngine(JobStore(db_path), StepExecutor(), tick_gate=tick_gate)
    await eng.start()
    try:
        specs = [
            StepSpec(key="ok", action="succeed", max_attempts=1),
            StepSpec(key="long", action="sleep", params={"seconds": 5.0},
                     max_attempts=1),
        ]
        job = await eng.submit(specs, None, max_concurrency=2,
                               time_budget_seconds=0.1)
        await gated.wait()
        await asyncio.sleep(0.25)  # 确定走过截止点；收尾 tick 仍被挂在门口
        j = eng.get_job_state(job.id)
        print(f"[state] deadline passed, sweep gated: job={j.status.value} "
              f"deadline={j.budget_deadline:.4f}")
        assert j.status is JobStatus.RUNNING
        try:
            await eng.cancel(job.id)
        except WorkflowError as exc:
            print(f"[judge] cancel in window rejected: reason={exc.reason.value} "
                  f"msg={exc.message}")
            assert exc.reason is RejectReason.JOB_BUDGET_DEADLINE_REACHED
        else:
            raise AssertionError("expected job_budget_deadline_reached rejection")
        j = eng.get_job_state(job.id)
        print(f"[state] rejection leaves job={j.status.value} "
              f"cancel_requested={j.cancel_requested} "
              f"cancel_requested_at={j.cancel_requested_at}")
        assert j.status is JobStatus.RUNNING          # 取消没有推动任何状态
        assert j.cancel_requested is False             # 迟到取消不落标记
        release.set()                                  # 放行被挂住的收尾扫描
        final = await wait_for_status(eng, job.id, timeout=3)
        states = {s.key: s for s in eng.get_steps_state(job.id)}
        print(f"[state after sweep] job={final} "
              f"ok={states['ok'].status.value} long={states['long'].status.value} "
              f"long.reason={states['long'].fail_reason}")
        assert final == JobStatus.TIMED_OUT.value
        assert states["ok"].status is StepStatus.SUCCEEDED
        assert states["ok"].result == {"ok": True}
        assert states["long"].status is StepStatus.TIMED_OUT
        assert states["long"].fail_reason == FAIL_REASON_BUDGET_EXHAUSTED
        assert eng.get_job_state(job.id).cancel_requested is False
    finally:
        release.set()
        await eng.aclose()


async def test_late_cancel_persisted_before_crash_times_out_after_restart(db_path):
    """取消在截止点之后才落盘、收尾前停机：同一份数据重启后必须落 TIMED_OUT。

    构造：截止点已过但收尾扫描尚未执行（模拟进程停在收尾前），取消标记带着
    晚于 deadline 的 cancel_requested_at 落盘后“杀死”进程；停机跨过截止点，
    用同一份数据重启 -> 拒绝兑现迟到取消，作业 TIMED_OUT，成功结果保留，
    cancel_requested 被清回 False。
    """
    print("\n[input] ok(succeed)+long(sleep 5s), time_budget=0.1s; gate the sweep; "
          "persist late cancel (timestamp after deadline), kill, restart")
    gated = asyncio.Event()
    release = asyncio.Event()

    async def tick_gate(job_id: str) -> None:
        if not gated.is_set():
            gated.set()
            return
        await release.wait()

    store = JobStore(db_path)
    eng1 = WorkflowEngine(store, StepExecutor(), tick_gate=tick_gate)
    await eng1.start()
    specs = [
        StepSpec(key="ok", action="succeed", max_attempts=1),
        StepSpec(key="long", action="sleep", params={"seconds": 5.0},
                 max_attempts=1),
    ]
    job = await eng1.submit(specs, None, max_concurrency=2,
                            time_budget_seconds=0.1)
    await gated.wait()
    await asyncio.sleep(0.25)  # 截止点已过，收尾被挂住
    on_disk = store.get_job(job.id)
    on_disk.cancel_requested = True
    on_disk.cancel_requested_at = time.time()  # 明确晚于 deadline
    store.update_job(on_disk)
    print(f"[state on disk at kill] status={on_disk.status.value} "
          f"cancel_requested={on_disk.cancel_requested} "
          f"cancel_at={on_disk.cancel_requested_at:.4f} "
          f"deadline={on_disk.budget_deadline:.4f}")
    # 注意：这里绝不能 release 挂住的收尾 tick——必须保证 TIMED_OUT 收尾
    # 完全没有在旧进程内发生，TIMED_OUT 只能由重启恢复路径写定。
    await eng1.aclose()  # “杀死”：撤销在途任务，不等待任何收尾
    await asyncio.sleep(0.2)  # 停机跨过截止点

    eng2 = WorkflowEngine(JobStore(db_path), StepExecutor())
    await eng2.start()
    try:
        j2 = eng2.get_job_state(job.id)
        states = {s.key: s for s in eng2.get_steps_state(job.id)}
        print(f"[state after restart] job={j2.status.value} "
              f"ok={states['ok'].status.value}(result={states['ok'].result}) "
              f"long={states['long'].status.value} reason={states['long'].fail_reason} "
              f"cancel_requested={j2.cancel_requested}")
        assert j2.status is JobStatus.TIMED_OUT  # 迟到取消不能改写预算终态
        assert states["ok"].status is StepStatus.SUCCEEDED
        assert states["ok"].result == {"ok": True}
        assert states["long"].status is StepStatus.TIMED_OUT
        assert states["long"].fail_reason == FAIL_REASON_BUDGET_EXHAUSTED
        assert j2.cancel_requested is False
        # 再等一拍确认不会复活重跑
        before = states["long"].attempt
        await asyncio.sleep(0.2)
        after = eng2.get_steps_state(job.id)[1].attempt
        print(f"[judge] long.attempt stable across restart: {before} -> {after}")
        assert before == after
    finally:
        await eng2.aclose()


def test_cancel_vs_budget_precedence_decided_by_real_timestamps():
    """先后判定的纯规则单测：只看真实墙钟时刻，与处理顺序、重启无关。"""
    def mk(cancel_at, deadline=100.0, has_budget=True, requested=True):
        return JobState(
            id="j",
            idempotency_key=None,
            cancel_requested=requested,
            cancel_requested_at=cancel_at,
            budget_deadline=deadline if has_budget else None,
        )

    print("\n[input] precedence truth table over (cancel_requested_at vs deadline)")
    cases = [
        ("cancel strictly before deadline", mk(99.9), True),
        ("cancel far before deadline", mk(1.0), True),
        ("cancel exactly at deadline -> timeout wins", mk(100.0), False),
        ("cancel after deadline -> timeout wins", mk(100.1), False),
        ("no budget: cancel always wins", mk(None, has_budget=False), True),
        ("no cancel requested", mk(None, requested=False), False),
        ("legacy row: flag set, timestamp missing -> cancel honored",
         mk(None), True),
    ]
    from app.engine import cancel_outranks_budget
    for name, job, expected in cases:
        got = cancel_outranks_budget(job)
        print(f"[judge] {name}: cancel_outranks_budget={got} (expect {expected})")
        assert got is expected
    print("[state] precedence is a pure timestamp comparison: no scheduler or "
          "restart involved")


async def test_concurrent_cancel_race_around_deadline_is_stable(engine):
    """收尾与取消几乎同时到达的并发压测：终态必须与“真实先后”证据永远自洽。

    一组带短预算、长步骤的作业，取消时刻在截止点两侧随机抖动并与调度器
    收尾真正并发；对每个作业只断言可复现的不变量：
      - 终态只有 CANCELLED 或 TIMED_OUT；
      - 若取消在截止点前被接受（cancel_requested_at<deadline），必为 CANCELLED；
      - 否则必为 TIMED_OUT，且迟到取消不落标记、不把终态改写成 CANCELLED。
    """
    import random

    n = 12
    budget = 0.2
    print(f"\n[input] {n} jobs, each sleep 5s, time_budget={budget}s; cancel at "
          f"random offset around deadline, truly concurrent with the sweep")
    submitted: list[tuple[str, float]] = []
    for i in range(n):
        specs = [StepSpec(key="s", action="sleep", params={"seconds": 5.0},
                          max_attempts=1)]
        job = await engine.submit(specs, None, max_concurrency=1,
                                  time_budget_seconds=budget)
        submitted.append((job.id, job.budget_deadline))

    async def cancel_later(job_id: str, deadline: float, rng: random.Random) -> None:
        # 截止点前后 ±0.12s 抖动，制造“几乎同时到达”
        offset = rng.uniform(-0.12, 0.12)
        await asyncio.sleep(max(0.0, budget + offset))
        try:
            await engine.cancel(job_id)
        except WorkflowError:
            pass  # 到点后/终态拒绝均属预期，最终只按不变量校验

    rng = random.Random(20261003)
    await asyncio.gather(
        *(cancel_later(jid, dl, rng) for jid, dl in submitted),
        *(wait_for_status(engine, jid, timeout=3) for jid, _ in submitted),
    )

    cancelled_jobs = timed_out_jobs = 0
    for jid, deadline in submitted:
        job = engine.get_job_state(jid)
        step = engine.get_steps_state(jid)[0]
        accepted_before = (
            job.cancel_requested_at is not None
            and job.cancel_requested_at < deadline
        )
        if job.status is JobStatus.CANCELLED:
            cancelled_jobs += 1
            cond = accepted_before and step.status is StepStatus.CANCELLED
            print(f"[state] {jid}: CANCELLED cancel_at<deadline={accepted_before} "
                  f"step={step.status.value}")
            assert cond, (jid, job.status, step.status, job.cancel_requested_at)
        elif job.status is JobStatus.TIMED_OUT:
            timed_out_jobs += 1
            print(f"[state] {jid}: TIMED_OUT cancel_at<deadline={accepted_before} "
                  f"step={step.status.value} cancel_requested={job.cancel_requested}")
            # 迟到取消绝不能把终态/步骤改成 cancelled，也不应残留标记
            assert step.status is StepStatus.TIMED_OUT
            assert step.fail_reason == FAIL_REASON_BUDGET_EXHAUSTED
            assert not accepted_before
        else:
            raise AssertionError(f"{jid} unexpected {job.status}")
    print(f"[judge] cancelled={cancelled_jobs} timed_out={timed_out_jobs} "
          f"(total {n}); every terminal matches the real timestamp order")
    assert cancelled_jobs + timed_out_jobs == n


async def test_http_cancel_in_deadline_window_returns_409_budget_reason(db_path):
    """HTTP 侧：截止点已过、收尾扫描被挂住时的取消 -> 409 job_budget_deadline_reached。"""
    print("\n[input] HTTP-equivalent: sleep 5s, time_budget=0.1s; sweep gated past "
          "deadline; cancel() inside the window")
    gated = asyncio.Event()
    release = asyncio.Event()

    async def tick_gate(job_id: str) -> None:
        if not gated.is_set():
            gated.set()
            return
        await release.wait()

    eng = WorkflowEngine(JobStore(db_path), StepExecutor(), tick_gate=tick_gate)
    await eng.start()
    try:
        specs = [StepSpec(key="b", action="sleep", params={"seconds": 5.0},
                          max_attempts=1)]
        job = await eng.submit(specs, None, max_concurrency=1,
                               time_budget_seconds=0.1)
        await gated.wait()
        await asyncio.sleep(0.25)
        try:
            await eng.cancel(job.id)
        except WorkflowError as exc:
            print(f"[judge] in-window cancel rejection reason={exc.reason.value}")
            assert exc.reason is RejectReason.JOB_BUDGET_DEADLINE_REACHED
        else:
            raise AssertionError("expected job_budget_deadline_reached")
        release.set()
        final = await wait_for_status(eng, job.id, timeout=3)
        assert final == JobStatus.TIMED_OUT.value
    finally:
        release.set()
        await eng.aclose()


async def test_http_cancel_after_timeout_returns_409_terminal(client, engine):
    """HTTP 侧：已落 TIMED_OUT 终态后再取消 -> 409 job_already_terminal，终态不改。"""
    print("\n[input] HTTP: sleep 5s, time_budget=0.1s; wait TIMED_OUT, POST cancel")
    body = {
        "max_concurrency": 1,
        "time_budget_seconds": 0.1,
        "steps": [{"key": "b", "action": "sleep", "params": {"seconds": 5.0},
                   "max_attempts": 1}],
    }
    r = await client.post("/jobs", json=body)
    job_id = r.json()["job_id"]
    await _wait_job(engine, job_id, JobStatus.TIMED_OUT, timeout=3)
    r2 = await client.post(f"/jobs/{job_id}/cancel")
    print(f"[judge] cancel after terminal -> HTTP {r2.status_code} "
          f"reason={r2.json()['detail']['reason']}")
    assert r2.status_code == 409
    assert r2.json()["detail"]["reason"] == "job_already_terminal"
    r3 = await client.get(f"/jobs/{job_id}")
    print(f"[state] GET status={r3.json()['status']} "
          f"step={r3.json()['steps'][0]['status']} "
          f"cancel_requested={r3.json()['cancel_requested']}")
    assert r3.json()["status"] == JobStatus.TIMED_OUT.value
    assert r3.json()["cancel_requested"] is False


async def test_all_succeeded_exactly_at_deadline_still_succeeds(engine):
    print("\n[input] step finishing just as the deadline passes; budget sweep "
          "must not overwrite SUCCEEDED")
    # 预算 0.25s，步骤 0.15s 完成：所有步骤在预算检查前已终态，
    # 作业应稳定 SUCCEEDED，即使完成时刻非常接近截止点。
    specs = [
        StepSpec(key="a", action="sleep", params={"seconds": 0.15},
                 max_attempts=1),
        StepSpec(key="b", action="sleep", params={"seconds": 0.15},
                 max_attempts=1),
    ]
    job = await engine.submit(specs, None, max_concurrency=2,
                              time_budget_seconds=0.25)
    final = await wait_for_status(engine, job.id, timeout=3)
    states = {s.key: s for s in engine.get_steps_state(job.id)}
    print(f"[judge] job={final} a={states['a'].status.value} "
          f"b={states['b'].status.value}")
    assert final == JobStatus.SUCCEEDED.value
    assert states["a"].status is StepStatus.SUCCEEDED
    assert states["b"].status is StepStatus.SUCCEEDED
