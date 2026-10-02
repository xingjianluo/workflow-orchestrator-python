"""场景：进程重启后的恢复语义。

- 作业执行到“部分步骤已成功、其余在运行/待调度”时停止引擎，
  用同一 db 文件启动新引擎：作业继续直到终态；
- 已 SUCCEEDED 的步骤 attempt 不增加、结果保留（绝不重复执行）；
- 崩溃时停在 RETRYING/RUNNING 的步骤被重新调度（step_recovered）；
- 已终态作业在重启后保持终态、不产生重复执行。
"""

from __future__ import annotations

import asyncio

from app.engine import WorkflowEngine
from app.executor import StepExecutor
from app.models import JobStatus, StepSpec, StepStatus
from app.store import JobStore
from tests.conftest import wait_for_status


async def _new_engine(db_path: str) -> WorkflowEngine:
    store = JobStore(db_path)
    eng = WorkflowEngine(store, StepExecutor())
    await eng.start()
    return eng


async def test_restore_after_partial_progress(db_path):
    print("\n[input] a(0.05s) -> b(succeed); c long(2s); stop mid-flight, restart")
    eng1 = await _new_engine(db_path)
    specs = [
        StepSpec(key="a", action="sleep", params={"seconds": 0.05}),
        StepSpec(key="b", action="echo", params={"v": "from-b"},
                 depends_on=["a"]),
        StepSpec(key="c", action="sleep", params={"seconds": 2.0},
                 max_attempts=1),
    ]
    job = await eng1.submit(specs, None, max_concurrency=3)

    # 等 a/b 完成、c 仍在运行（模拟进程在作业中途被杀死）
    for _ in range(200):
        st = {s.key: s for s in eng1.get_steps_state(job.id)}
        if st["b"].status is StepStatus.SUCCEEDED and st["c"].status is StepStatus.RUNNING:
            break
        await asyncio.sleep(0.01)
    st = {s.key: s for s in eng1.get_steps_state(job.id)}
    print(f"[state before kill] a={st['a'].status.value} b={st['b'].status.value} "
          f"c={st['c'].status.value} attempts={{k: v.attempt for k, v in st.items()}}")
    assert st["a"].status is StepStatus.SUCCEEDED
    assert st["b"].status is StepStatus.SUCCEEDED
    assert st["c"].status is StepStatus.RUNNING
    await eng1.aclose()
    # store 在引擎内部，单独关闭：
    # （eng1 不暴露 store，重新连接同一文件即可）

    # ---- 进程重启：c 改为会成功的动作无法改 spec；这里直接新引擎继续，
    #      c 仍是 2s sleep，我们改为等待其完成 ----
    eng2 = await _new_engine(db_path)
    try:
        recovered_job = eng2.get_job_state(job.id)
        print(f"[state after restart] job={recovered_job.status.value} "
              f"cancel_requested={recovered_job.cancel_requested}")
        steps1 = {s.key: s for s in eng2.get_steps_state(job.id)}
        print(f"[state] recovered step statuses: "
              f"{ {k: s.status.value for k, s in steps1.items()} }")

        final = await wait_for_status(eng2, job.id, timeout=4)
        steps2 = {s.key: s for s in eng2.get_steps_state(job.id)}
        print(f"[judge] final={final}")
        for k in "abc":
            print(f"[state] {k}: status={steps2[k].status.value} "
                  f"attempts={steps2[k].attempt} result={steps2[k].result}")
        assert final == JobStatus.SUCCEEDED.value
        # a/b 成功结果保留且未重复执行
        assert steps2["a"].attempt == 1
        assert steps2["b"].attempt == 1
        assert steps2["b"].result == {"v": "from-b"}
        # c 在崩溃时未完成：被重新调度后成功（可能 attempt=2：中断一次 + 恢复后一次）
        assert steps2["c"].status is StepStatus.SUCCEEDED
        assert 1 <= steps2["c"].attempt <= 2
    finally:
        await eng2.aclose()


async def test_terminal_job_stays_terminal_after_restart(db_path):
    print("\n[input] finished job must not re-run on restart")
    eng1 = await _new_engine(db_path)
    job = await eng1.submit([StepSpec(key="a", action="succeed")], "IDEM-1", 1)
    await wait_for_status(eng1, job.id)
    await eng1.aclose()

    eng2 = await _new_engine(db_path)
    try:
        j = eng2.get_job_state(job.id)
        s = eng2.get_steps_state(job.id)[0]
        print(f"[state after restart] job={j.status.value} step={s.status.value} "
              f"attempts={s.attempt}")
        assert j.status is JobStatus.SUCCEEDED
        assert s.attempt == 1
        # 幂等键在新进程仍然有效
        dedup = await eng2.submit([StepSpec(key="a", action="succeed")], "IDEM-1", 1)
        print(f"[judge] dedup job id same? {dedup.id == job.id}")
        assert dedup.id == job.id
    finally:
        await eng2.aclose()


async def test_retry_state_resumes_after_restart(db_path):
    print("\n[input] flaky step in RETRYING when process killed; resumes and succeeds")
    eng1 = await _new_engine(db_path)
    specs = [
        StepSpec(key="f", action="flaky", params={"fail_times": 3},
                 max_attempts=5, backoff_base=0.5, backoff_cap=1.0),
    ]
    job = await eng1.submit(specs, None, 1)
    # 等到第一次失败进入 RETRYING（退避 0.5s）
    for _ in range(100):
        s = eng1.get_steps_state(job.id)[0]
        if s.status is StepStatus.RETRYING:
            break
        await asyncio.sleep(0.01)
    s = eng1.get_steps_state(job.id)[0]
    print(f"[state before kill] status={s.status.value} attempt={s.attempt} "
          f"run_at>now? {s.run_at}")
    assert s.status is StepStatus.RETRYING
    assert s.attempt == 1
    await eng1.aclose()

    eng2 = await _new_engine(db_path)
    try:
        final = await wait_for_status(eng2, job.id, timeout=5)
        s2 = eng2.get_steps_state(job.id)[0]
        print(f"[judge] final={final} attempts={s2.attempt} status={s2.status.value}")
        assert final == JobStatus.FAILED.value or final == JobStatus.SUCCEEDED.value
        # flaky 需 4 次尝试才成功；崩溃前已用掉 1 次，恢复后继续计数
        assert s2.status is StepStatus.SUCCEEDED
        assert s2.attempt == 4
    finally:
        await eng2.aclose()


async def test_graceful_shutdown_mid_run_then_resume(db_path):
    print("\n[input] graceful aclose while step RUNNING; state must stay recoverable")
    eng1 = await _new_engine(db_path)
    specs = [
        StepSpec(key="fast", action="succeed"),
        StepSpec(key="long", action="sleep", params={"seconds": 5.0},
                 max_attempts=1),
    ]
    job = await eng1.submit(specs, None, max_concurrency=2)
    for _ in range(200):
        st = {s.key: s for s in eng1.get_steps_state(job.id)}
        if st["long"].status is StepStatus.RUNNING:
            break
        await asyncio.sleep(0.01)
    await eng1.aclose()  # 优雅关闭：在途任务被中断但不落 CANCELLED

    # 直接读库确认落盘状态（没有新引擎）
    store = JobStore(db_path)
    persisted = {s.key: s.status for s in store.list_steps(job.id)}
    print(f"[state on disk after shutdown] { {k: v.value for k, v in persisted.items()} }")
    assert persisted["fast"] is StepStatus.SUCCEEDED
    assert persisted["long"] is StepStatus.RUNNING  # 未被误标 CANCELLED
    store.close()

    eng2 = await _new_engine(db_path)
    try:
        final = await wait_for_status(eng2, job.id, timeout=8)
        st = {s.key: s for s in eng2.get_steps_state(job.id)}
        print(f"[judge] final={final} fast.attempt={st['fast'].attempt} "
              f"long.status={st['long'].status.value} long.attempt={st['long'].attempt}")
        assert final == JobStatus.SUCCEEDED.value
        assert st["fast"].attempt == 1  # 已成功不重跑
        assert st["long"].status is StepStatus.SUCCEEDED
    finally:
        await eng2.aclose()
