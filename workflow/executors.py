"""步骤执行器注册表。

执行器签名::

    async def executor(step_id: str, params: dict, cancel: asyncio.Event) -> Any

- 抛出 :class:`RetryableError` 表示“可重试失败”，由调度器按退避策略重试；
- 抛出其它异常表示“永久失败”，步骤直接进入 FAILED 终态；
- 执行器应当协作式地检查 ``cancel``（事件被 set 时尽快返回/抛错），
  取消传播对不响应取消的执行器最多等到其当前一次尝试结束。
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from .models import Reason, WorkflowError

Executor = Callable[..., Awaitable[Any]]


class RetryableError(Exception):
    """可按退避策略重试的执行失败。"""


@dataclass
class ExecutorRegistry:
    _registry: dict[str, Executor] = field(default_factory=dict)

    def register(self, kind: str, fn: Executor) -> None:
        if not kind:
            raise ValueError("步骤类型不能为空")
        self._registry[kind] = fn

    def get(self, kind: str) -> Executor:
        try:
            return self._registry[kind]
        except KeyError:
            raise WorkflowError(
                Reason.UNKNOWN_STEP_KIND, f"未注册的步骤类型: {kind}"
            ) from None

    def kinds(self) -> list[str]:
        return sorted(self._registry)


# --------------------------------------------------------------------------
# 内置执行器：供本地运行与测试使用。所有行为由步骤 params 决定：
#   {"value": ...}              noop —— 成功并返回 value
#   {"fail_times": N, ...}      flaky —— 前 N 次抛 RetryableError，之后成功
#   {"permanent": True, "msg"}  fail_permanent —— 立即永久失败
#   {"sleep": S}                slow —— 睡眠 S 秒（响应取消）
#   {"counter": "c"}            counted —— 每次执行把进程内计数器 c 加 1
# counted 用于重启恢复测试：成功的步骤绝不能被第二次执行。
_RUN_COUNTERS: dict[str, int] = {}


def reset_run_counters() -> None:
    _RUN_COUNTERS.clear()


def get_run_counter(name: str) -> int:
    return _RUN_COUNTERS.get(name, 0)


async def noop_executor(step_id: str, params: dict, cancel: asyncio.Event) -> Any:
    return {"step_id": step_id, "value": params.get("value")}


async def slow_executor(step_id: str, params: dict, cancel: asyncio.Event) -> dict:
    delay = float(params.get("sleep", 0.05))
    try:
        await asyncio.wait_for(cancel.wait(), timeout=delay)
    except TimeoutError:
        return {"step_id": step_id, "slept": delay}
    raise asyncio.CancelledError(f"{step_id} 在睡眠中收到取消")


async def flaky_executor(step_id: str, params: dict, cancel: asyncio.Event) -> dict:
    name = str(params.get("counter", f"flaky:{step_id}"))
    attempt_index = _RUN_COUNTERS.get(name, 0)
    _RUN_COUNTERS[name] = attempt_index + 1
    fail_times = int(params.get("fail_times", 1))
    if attempt_index < fail_times:
        raise RetryableError(
            f"{step_id} 第 {attempt_index + 1} 次执行失败（计划失败 {fail_times} 次）"
        )
    return {"step_id": step_id, "succeeded_on_attempt": attempt_index + 1}


async def fail_permanent_executor(
    step_id: str, params: dict, cancel: asyncio.Event
) -> dict:
    raise RuntimeError(str(params.get("msg", f"{step_id} 永久失败")))


async def counted_executor(
    step_id: str, params: dict, cancel: asyncio.Event
) -> dict:
    name = str(params.get("counter", step_id))
    n = _RUN_COUNTERS.get(name, 0) + 1
    _RUN_COUNTERS[name] = n
    return {"step_id": step_id, "runs": n}


def default_registry() -> ExecutorRegistry:
    """注册全部内置执行器类型。"""
    reg = ExecutorRegistry()
    reg.register("noop", noop_executor)
    reg.register("slow", slow_executor)
    reg.register("flaky", flaky_executor)
    reg.register("fail_permanent", fail_permanent_executor)
    reg.register("counted", counted_executor)
    return reg
