"""DAG 依赖编排、并发上限、非法 DAG 拒绝、上游失败 -> BLOCKED。"""
from __future__ import annotations

import asyncio
import json

import pytest

from workflow.executors import ExecutorRegistry, default_registry
from workflow.models import JobSpec, Reason, StepSpec, WorkflowError
from workflow.service import JobService

from .conftest import print_trace


def slow_spec(sid, *, sleep=0.15, deps=(), counter=None):
    return StepSpec(
        id=sid, kind="slow", params={"sleep": sleep, **({"counter": counter} if counter else {})},
        depends_on=deps,
    )


async def test_independent_steps_run_within_concurrency_limit(service):
    """输入: 3 个无依赖 slow 步骤，作业并发上限=2。
    判定依据: 任意时刻 RUNNING 数不超过 2；3 个步骤总耗时应 < 3*sleep（有并行）。
    """
    spec = JobSpec(
        idempotency_key="conc-1", max_concurrency=2,
        steps=tuple(slow_spec(f"s{i}", sleep=0.15) for i in range(3)),
    )
    loop = asyncio.get_running_loop()
    job, _ = await service.submit(spec)
    samples: list[int] = []

    async def sampler():
        while True:
            steps = service.store.list_steps(job.id)
            samples.append(sum(1 for s in steps if s.state.value == "RUNNING"))
            await asyncio.sleep(0.02)

    t = asyncio.create_task(sampler())
    final = await service.wait_for_terminal(job.id, timeout=10)
    t.cancel()
    print(f"\n[输入] 3 个独立 slow 步骤，并发上限 2；RUNNING 采样序列={samples}")
    print(f"[判定] 采样最大并发={max(samples)}（要求 <=2 且出现过 2）")
    assert final.state.value == "SUCCEEDED"
    assert max(samples) == 2


async def test_dependent_step_starts_after_upstream_succeeds(service):
    """输入: c 依赖 a,b。判定: c 的 RUNNING 事件晚于 a,b 的 SUCCEEDED 事件。"""
    spec = JobSpec(
        idempotency_key="dep-1", max_concurrency=4,
        steps=(
            slow_spec("a", sleep=0.05),
            slow_spec("b", sleep=0.2),
            slow_spec("c", sleep=0.02, deps=("a", "b")),
        ),
    )
    job, _ = await service.submit(spec)
    await service.wait_for_terminal(job.id, timeout=10)
    events = service.store.list_events(job.id)

    def time_of(step, to_state):
        return [e["ts"] for e in events
                if e["step_id"] == step and e["to_state"] == to_state][0]

    c_start = time_of("c", "RUNNING")
    a_done = time_of("a", "SUCCEEDED")
    b_done = time_of("b", "SUCCEEDED")
    print(f"\n[输入] c 依赖 a,b；a 完成={a_done:.3f} b 完成={b_done:.3f} c 开始={c_start:.3f}")
    print(f"[判定] c_start > max(a,b) = {c_start > max(a_done, b_done)}")
    assert c_start > a_done and c_start > b_done
    states = {s.id: s.state.value for s in service.store.list_steps(job.id)}
    assert states == {"a": "SUCCEEDED", "b": "SUCCEEDED", "c": "SUCCEEDED"}
    print_trace(service.store, job.id, "依赖顺序")


async def test_downstream_blocked_when_upstream_fails(service):
    """输入: d 永久失败，e 依赖 d；f 依赖 e。
    判定: d=FAILED(EXECUTOR_ERROR)，e/f=BLOCKED(UPSTREAM_FAILED)，作业 FAILED。
    """
    spec = JobSpec(
        idempotency_key="block-1", max_concurrency=4,
        steps=(
            StepSpec(id="d", kind="fail_permanent", params={"msg": "炸了"}),
            StepSpec(id="e", kind="noop", params={}, depends_on=("d",)),
            StepSpec(id="f", kind="noop", params={}, depends_on=("e",)),
        ),
    )
    job, _ = await service.submit(spec)
    final = await service.wait_for_terminal(job.id, timeout=10)
    steps = {s.id: s for s in service.store.list_steps(job.id)}
    print("\n[输入] d 永久失败，e->d, f->e")
    for sid in ("d", "e", "f"):
        print(f"[状态] {sid}: {steps[sid].state.value} reason={steps[sid].reason}")
    assert final.state.value == "FAILED"
    assert steps["d"].state.value == "FAILED"
    assert steps["d"].reason == Reason.EXECUTOR_ERROR.value
    assert steps["e"].state.value == "BLOCKED"
    assert steps["e"].reason == Reason.UPSTREAM_FAILED.value
    assert steps["f"].state.value == "BLOCKED"
    assert steps["f"].reason == Reason.UPSTREAM_FAILED.value
    # 受阻步骤从未被执行
    assert steps["e"].attempt == 0 and steps["f"].attempt == 0
    print_trace(service.store, job.id, "上游失败受阻")


async def test_cycle_is_rejected(service):
    spec = JobSpec(
        idempotency_key="cyc", steps=(
            StepSpec(id="a", kind="noop", depends_on=("b",)),
            StepSpec(id="b", kind="noop", depends_on=("a",)),
        ),
    )
    with pytest.raises(WorkflowError) as exc:
        await service.submit(spec)
    assert exc.value.reason is Reason.DAG_CYCLE


async def test_unknown_dependency_is_rejected(service):
    spec = JobSpec(
        idempotency_key="unk", steps=(
            StepSpec(id="a", kind="noop", depends_on=("ghost",)),
        ),
    )
    with pytest.raises(WorkflowError) as exc:
        await service.submit(spec)
    assert exc.value.reason is Reason.DAG_UNKNOWN_DEP


async def test_global_semaphore_caps_across_jobs(store):
    """输入: 两个作业各 3 个 slow 步骤，作业上限 3，全局上限 2。
    判定: 跨作业采样的 RUNNING 总数不超过全局上限 2。
    """
    svc = JobService(store, default_registry(), global_max_concurrency=2)
    specs = [
        JobSpec(idempotency_key=f"g{k}", max_concurrency=3,
                steps=tuple(slow_spec(f"s{i}", sleep=0.12) for i in range(3)))
        for k in range(2)
    ]
    jobs = []
    for sp in specs:
        j, _ = await svc.submit(sp)
        jobs.append(j)
    samples: list[int] = []

    async def sampler():
        while True:
            n = sum(
                sum(1 for s in store.list_steps(j.id) if s.state.value == "RUNNING")
                for j in jobs
            )
            samples.append(n)
            await asyncio.sleep(0.015)

    t = asyncio.create_task(sampler())
    await asyncio.gather(*[svc.wait_for_terminal(j.id, timeout=15) for j in jobs])
    t.cancel()
    print(f"\n[输入] 两作业共 6 个 slow，全局并发=2；跨作业 RUNNING 采样={samples}")
    assert max(samples) <= 2
    assert 2 in samples
