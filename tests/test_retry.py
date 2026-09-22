"""失败重试：指数退避、重试耗尽 -> FAILED(RETRIES_EXHAUSTED)、下游受阻。"""
from __future__ import annotations

from workflow.models import JobSpec, Reason, StepSpec
from workflow.executors import get_run_counter

from .conftest import print_trace


async def test_retry_succeeds_within_budget(service):
    """输入: flaky 计划失败 2 次，max_retries=3（共 4 次机会）。
    判定依据: 第 3 次尝试成功，attempt=3，作业 SUCCEEDED。
    """
    spec = JobSpec(
        idempotency_key="retry-ok", max_concurrency=1,
        steps=(
            StepSpec(id="b", kind="flaky",
                     params={"fail_times": 2, "counter": "retry-ok-b"},
                     max_retries=3, retry_backoff_seconds=0.01),
        ),
    )
    job, _ = await service.submit(spec)
    final = await service.wait_for_terminal(job.id, timeout=10)
    step = service.store.get_step(job.id, "b")
    print(f"\n[输入] fail_times=2 max_retries=3 backoff=0.01s")
    print(f"[状态跃迁] b: {step.state.value} attempt={step.attempt}")
    assert final.state.value == "SUCCEEDED"
    assert step.state.value == "SUCCEEDED"
    assert step.attempt == 3
    print_trace(service.store, job.id, "重试成功")


async def test_retries_exhausted_enters_terminal_failed(service):
    """输入: flaky 永远失败，max_retries=2（共 3 次尝试）。
    判定依据: attempt=3，步骤 FAILED/RETRIES_EXHAUSTED，下游 BLOCKED/UPSTREAM_FAILED。
    """
    spec = JobSpec(
        idempotency_key="retry-ex", max_concurrency=1,
        steps=(
            StepSpec(id="x", kind="flaky",
                     params={"fail_times": 99, "counter": "retry-ex-x"},
                     max_retries=2, retry_backoff_seconds=0.01),
            StepSpec(id="y", kind="noop", params={}, depends_on=("x",)),
        ),
    )
    job, _ = await service.submit(spec)
    final = await service.wait_for_terminal(job.id, timeout=15)
    x = service.store.get_step(job.id, "x")
    y = service.store.get_step(job.id, "y")
    print(f"\n[输入] x 永远失败 max_retries=2；y 依赖 x")
    print(f"[判定] x: state={x.state.value} reason={x.reason} attempt={x.attempt}")
    print(f"[判定] y: state={y.state.value} reason={y.reason} attempt={y.attempt}")
    assert final.state.value == "FAILED"
    assert x.state.value == "FAILED"
    assert x.reason == Reason.RETRIES_EXHAUSTED.value
    assert x.attempt == 3  # 初次 + 2 次重试
    assert y.state.value == "BLOCKED"
    assert y.reason == Reason.UPSTREAM_FAILED.value
    assert get_run_counter("retry-ex-x") == 3
    print_trace(service.store, job.id, "重试耗尽")


async def test_backoff_timing_respected(service):
    """输入: max_retries=1, backoff=0.2s。判定依据: 两次尝试开始时间差 >= 0.2s。"""
    spec = JobSpec(
        idempotency_key="retry-bf", max_concurrency=1,
        steps=(
            StepSpec(id="z", kind="flaky",
                     params={"fail_times": 99, "counter": "retry-bf-z"},
                     max_retries=1, retry_backoff_seconds=0.2),
        ),
    )
    job, _ = await service.submit(spec)
    await service.wait_for_terminal(job.id, timeout=15)
    starts = [
        e["ts"] for e in service.store.list_events(job.id)
        if e["step_id"] == "z" and e["to_state"] == "RUNNING"
    ]
    print(f"\n[输入] backoff=0.2s；两次 RUNNING 时间戳={starts}")
    print(f"[判定] 间隔={starts[1] - starts[0]:.3f}s >= 0.2s")
    assert len(starts) == 2
    assert starts[1] - starts[0] >= 0.2


async def test_permanent_failure_not_retried(service):
    """输入: fail_permanent, max_retries=5。判定依据: 只执行 1 次，reason=EXECUTOR_ERROR。"""
    spec = JobSpec(
        idempotency_key="perm", max_concurrency=1,
        steps=(
            StepSpec(id="p", kind="fail_permanent",
                     params={"msg": "不可恢复"}, max_retries=5),
        ),
    )
    job, _ = await service.submit(spec)
    final = await service.wait_for_terminal(job.id, timeout=10)
    p = service.store.get_step(job.id, "p")
    print(f"\n[输入] 永久失败 max_retries=5；实际 attempt={p.attempt} reason={p.reason}")
    assert final.state.value == "FAILED"
    assert p.state.value == "FAILED"
    assert p.reason == Reason.EXECUTOR_ERROR.value
    assert p.attempt == 1
