"""重启恢复：新进程从 SQLite 接管未完成作业，不重复执行已成功步骤。"""
from __future__ import annotations

import asyncio

from workflow.executors import (
    ExecutorRegistry,
    default_registry,
    get_run_counter,
    reset_run_counters,
)
from workflow.models import (
    JobRecord,
    JobSpec,
    JobState,
    Reason,
    StepRecord,
    StepSpec,
    StepState,
)
from workflow.service import JobService
from workflow.store import Store

from .conftest import print_trace


async def test_resume_after_restart_skips_succeeded_steps(db_path):
    """场景:
      作业 a(counted) -> b(counted, 慢)。
      进程1：a 已 SUCCEEDED，b 正在运行时“崩溃”（直接丢弃 service，不等待）。
      进程2：用同一 DB 文件新建 Store+Service 并 recover_all。
    判定依据:
      a 的进程内执行计数仍为 1（SUCCEEDED 不重跑）；b 最终 SUCCEEDED；
      b 崩溃时 RUNNING，恢复后回到 READY 重跑（at-least-once）。
    """
    reset_run_counters()
    store1 = Store(db_path); store1.init_schema()
    svc1 = JobService(store1, default_registry(), global_max_concurrency=4)
    spec = JobSpec(
        idempotency_key="resume-1", max_concurrency=2,
        steps=(
            StepSpec(id="a", kind="counted", params={"counter": "rec-a"}),
            StepSpec(id="b", kind="counted",
                     params={"counter": "rec-b", "sleep": 0.0},
                     # counted 默认不睡眠；这里自定义一个“慢 counted”
                     ),
        ),
    )
    # 用一个能在中途模拟崩溃的慢执行器替换 b 的运行
    gate = asyncio.Event()

    async def slow_counted(step_id, params, cancel):
        name = params["counter"]
        # 复用 counted 的计数逻辑
        from workflow.executors import _RUN_COUNTERS
        n = _RUN_COUNTERS.get(name, 0) + 1
        _RUN_COUNTERS[name] = n
        try:
            await asyncio.wait_for(gate.wait(), timeout=5.0)  # 进程1中永远不放开
        except (TimeoutError, asyncio.CancelledError):
            pass
        # 进程2里 gate 未 set 同样会等到 timeout——为测试稳定性，第二次直接返回
        return {"step_id": step_id, "runs": n, "phase": "finished"}

    reg1 = default_registry(); reg1.register("slow_counted", slow_counted)
    svc1.executors = reg1
    spec = JobSpec(
        idempotency_key="resume-1", max_concurrency=2,
        steps=(
            StepSpec(id="a", kind="counted", params={"counter": "rec-a"}),
            StepSpec(id="b", kind="slow_counted", params={"counter": "rec-b"}),
        ),
    )
    job, _ = await svc1.submit(spec)
    # 等 a 成功、b 进入 RUNNING
    for _ in range(200):
        a = store1.get_step(job.id, "a")
        b = store1.get_step(job.id, "b")
        if a.state.value == "SUCCEEDED" and b.state.value == "RUNNING":
            break
        await asyncio.sleep(0.01)
    print(f"\n[输入] 崩溃前: a={a.state.value} b={b.state.value}")
    assert a.state.value == "SUCCEEDED" and b.state.value == "RUNNING"
    a_attempt_before = a.attempt
    # 模拟进程崩溃：直接关闭 service（取消其任务）与数据库连接
    for t in svc1._tasks.values():
        t.cancel()
    await asyncio.sleep(0.05)
    store1.close()

    # ---- 进程 2 ----
    store2 = Store(db_path)
    reg2 = default_registry()

    async def fast_counted(step_id, params, cancel):
        from workflow.executors import _RUN_COUNTERS
        name = params["counter"]
        n = _RUN_COUNTERS.get(name, 0) + 1
        _RUN_COUNTERS[name] = n
        return {"step_id": step_id, "runs": n}

    reg2.register("slow_counted", fast_counted)  # 新进程里该步骤很快完成
    svc2 = JobService(store2, reg2, global_max_concurrency=4)
    recovered = await svc2.recover_all()
    print(f"[恢复] recover_all 接管作业数={recovered}（要求 1）")
    assert recovered == 1
    final = await svc2.wait_for_terminal(job.id, timeout=10)
    a2 = store2.get_step(job.id, "a")
    b2 = store2.get_step(job.id, "b")
    print(f"[判定] 最终 job={final.state.value} a={a2.state.value}(attempt={a2.attempt}) "
          f"b={b2.state.value}(attempt={b2.attempt})")
    print(f"[判定] 进程内执行计数: rec-a={get_run_counter('rec-a')}（要求 1，成功步骤不重跑）, "
          f"rec-b={get_run_counter('rec-b')}（在途步骤重跑）")
    assert final.state.value == "SUCCEEDED"
    assert a2.state.value == "SUCCEEDED"
    assert a2.attempt == a_attempt_before
    assert get_run_counter("rec-a") == 1
    assert b2.state.value == "SUCCEEDED"
    print_trace(store2, job.id, "重启恢复")
    await svc2.aclose(); store2.close()


async def test_recover_cancelling_job(db_path):
    """场景: 作业处于 CANCELLING 时进程重启；新进程继续把未完成步骤收割为 CANCELLED。"""
    reset_run_counters()
    store = Store(db_path); store.init_schema()
    svc = JobService(store, default_registry(), global_max_concurrency=4)
    # 先不启动 runner：直接构造持久化的 RUNNING 作业 + RUNNING 步骤，
    # 再把作业置为 CANCELLING，模拟“取消后、收割前进程死亡”。
    import time
    import uuid
    job_id = uuid.uuid4().hex
    now = time.time()
    job = JobRecord(
        id=job_id, idempotency_key="cancel-resume", request_hash="x",
        spec_json="{}", state=JobState.CANCELLING, max_concurrency=2,
        reason=Reason.CANCELLED.value, error="", created_ts=now, updated_ts=now,
    )
    step = StepRecord(
        id="s1", job_id=job_id, kind="noop", params_json="{}",
        depends_on_json="[]", state=StepState.RUNNING,
        attempt=1, max_retries=0, retry_backoff_seconds=0.0, timeout_seconds=None,
        reason="", error="", result_json="", run_after_ts=0.0,
    )
    store.insert_job_with_steps(job, [step])
    store.close()

    store2 = Store(db_path)
    svc2 = JobService(store2, default_registry(), global_max_concurrency=4)
    n = await svc2.recover_all()
    assert n == 1
    final = await svc2.wait_for_terminal(job_id, timeout=10)
    s1 = store2.get_step(job_id, "s1")
    print(f"\n[判定] 恢复 CANCELLING 作业: job={final.state.value} s1={s1.state.value}/{s1.reason}")
    assert final.state.value == "CANCELLED"
    assert s1.state.value == "CANCELLED"
    assert s1.reason == Reason.CANCELLED.value
    await svc2.aclose(); store2.close()
