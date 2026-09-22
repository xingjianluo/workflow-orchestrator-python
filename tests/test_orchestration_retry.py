"""场景：依赖编排、并发上限、阻断传播、重试耗尽。

判定要点（状态与失败原因必须可区分且稳定）：
- 无依赖步骤在 max_concurrency 内并行（用时间窗验证上限）；
- 下游必须等上游 SUCCEEDED 后才开始；
- 上游 FAILED -> 下游 BLOCKED(fail_reason=upstream_failed)，不静默跳过；
- 重试中的步骤为 RETRYING；耗尽后 FAILED(fail_reason=retry_exhausted)，
  且尝试次数恰好等于 max_attempts。
"""

from __future__ import annotations

import time

from app.models import JobStatus, StepSpec, StepStatus

from tests.conftest import wait_for_status


async def test_independent_steps_parallel_within_concurrency_limit(engine):
    print("\n[input] 3 sleep steps(0.2s), max_concurrency=2 => expect ~0.4s not 0.6s")
    specs = [
        StepSpec(key=f"s{i}", action="sleep", params={"seconds": 0.2},
                 max_attempts=1)
        for i in range(3)
    ]
    job = await engine.submit(specs, None, max_concurrency=2)
    t0 = time.monotonic()
    final = await wait_for_status(engine, job.id)
    elapsed = time.monotonic() - t0
    print(f"[judge] final={final} elapsed={elapsed:.2f}s (2 parallel waves expected)")
    assert final == JobStatus.SUCCEEDED.value
    assert 0.30 < elapsed < 0.55, elapsed

    steps = {s.key: s for s in engine.get_steps_state(job.id)}
    starts = sorted(s.started_at for s in steps.values())
    print(f"[state] sorted start times: {[round(x - starts[0], 3) for x in starts]}")
    # 第 3 个步骤不可能与前两个同时启动（信号量=2）
    assert starts[2] - starts[0] >= 0.15


async def test_dependency_waits_for_upstream_success(engine):
    print("\n[input] a(flaky x2) -> b(sleep) ; b must start only after a succeeded")
    specs = [
        StepSpec(key="a", action="flaky", params={"fail_times": 2},
                 max_attempts=3, backoff_base=0.01, backoff_cap=0.02),
        StepSpec(key="b", action="sleep", params={"seconds": 0.05},
                 depends_on=["a"]),
    ]
    job = await engine.submit(specs, None, max_concurrency=2)
    final = await wait_for_status(engine, job.id)
    states = {s.key: s for s in engine.get_steps_state(job.id)}
    print(f"[state] a={states['a'].status.value} attempts={states['a'].attempt} "
          f"finished={states['a'].finished_at}; b.started={states['b'].started_at}")
    assert final == JobStatus.SUCCEEDED.value
    assert states["a"].attempt == 3
    assert states["a"].status is StepStatus.SUCCEEDED
    assert states["b"].status is StepStatus.SUCCEEDED
    assert states["b"].started_at >= (states["a"].finished_at or 0) - 1e-6


async def test_downstream_blocked_when_upstream_fails(engine):
    print("\n[input] c fails permanently; d depends on c; e independent succeeds")
    specs = [
        StepSpec(key="c", action="fail", params={"message": "boom"},
                 max_attempts=2, backoff_base=0.001),
        StepSpec(key="d", action="succeed", depends_on=["c"]),
        StepSpec(key="e", action="succeed"),
        StepSpec(key="f", action="succeed", depends_on=["d"]),
    ]
    job = await engine.submit(specs, None, max_concurrency=4)
    final = await wait_for_status(engine, job.id)
    states = {s.key: s for s in engine.get_steps_state(job.id)}
    print(f"[judge] job={final}")
    for k in "cdef":
        s = states[k]
        print(f"[state] {k}: status={s.status.value} fail_reason={s.fail_reason} "
              f"attempts={s.attempt} error={s.error}")
    assert final == JobStatus.FAILED.value
    assert states["c"].status is StepStatus.FAILED
    assert states["c"].fail_reason == "retry_exhausted"
    assert states["c"].attempt == 2
    assert states["d"].status is StepStatus.BLOCKED
    assert states["d"].fail_reason == "upstream_failed"
    assert states["d"].attempt == 0  # 从未执行，而非静默成功
    assert states["f"].status is StepStatus.BLOCKED  # 阻断沿依赖链传播
    assert states["e"].status is StepStatus.SUCCEEDED  # 不受牵连
    job_state = engine.get_job_state(job.id)
    assert "retry_exhausted" in (job_state.error or "")
    assert "upstream_failed" in (job_state.error or "")


async def test_retry_exhaustion_marks_deterministic_terminal(engine):
    print("\n[input] step fails 5/5 with max_attempts=3 backoff=10ms")
    specs = [
        StepSpec(key="x", action="fail", max_attempts=3,
                 backoff_base=0.01, backoff_cap=0.02),
    ]
    job = await engine.submit(specs, None, max_concurrency=1)
    final = await wait_for_status(engine, job.id)
    x = engine.get_steps_state(job.id)[0]
    print(f"[state] job={final} step={x.status.value} attempts={x.attempt} "
          f"reason={x.fail_reason} error={x.error}")
    assert final == JobStatus.FAILED.value
    assert x.status is StepStatus.FAILED
    assert x.fail_reason == "retry_exhausted"
    assert x.attempt == 3
    assert x.error and x.error.startswith("forced_failure:")


async def test_no_retry_when_max_attempts_one(engine):
    print("\n[input] max_attempts=1 => single attempt, no RETRYING state")
    specs = [StepSpec(key="x", action="fail", max_attempts=1, backoff_base=0.001)]
    job = await engine.submit(specs, None, 1)
    final = await wait_for_status(engine, job.id)
    x = engine.get_steps_state(job.id)[0]
    print(f"[state] step={x.status.value} attempts={x.attempt} reason={x.fail_reason}")
    assert final == JobStatus.FAILED.value
    assert x.attempt == 1 and x.fail_reason == "retry_exhausted"


async def test_invalid_dependency_is_rejected(engine):
    print("\n[input] step depends on unknown key / self / cycle")
    from app.engine import WorkflowError
    bad_unknown = [
        StepSpec(key="a", action="succeed", depends_on=["ghost"]),
    ]
    try:
        await engine.submit(bad_unknown, None, 1)
    except WorkflowError as exc:
        print(f"[judge] unknown dep -> reason={exc.reason.value}")
        assert exc.reason.value == "invalid_dependency"
    else:
        raise AssertionError("expected rejection")

    cycle = [
        StepSpec(key="a", action="succeed", depends_on=["b"]),
        StepSpec(key="b", action="succeed", depends_on=["a"]),
    ]
    try:
        await engine.submit(cycle, None, 1)
    except WorkflowError as exc:
        print(f"[judge] cycle -> reason={exc.reason.value}")
        assert exc.reason.value == "invalid_dependency"
    else:
        raise AssertionError("expected rejection")
