"""步骤执行器：把声明式 action 执行为结果/异常。

内置 action：
  succeed -> 立即成功，result 取自 params.result（默认 {"ok": true}）
  fail    -> 每次尝试都失败，错误信息取自 params.message（默认 forced failure）
  flaky   -> 前 fail_times 次失败，之后成功
             params: {"fail_times": 1, "message": "..."}
  sleep   -> 休眠 params.seconds 秒；取消事件置位时立即抛 CancellationError
  echo    -> 直接把 params 作为 result 返回（用于断言编排结果）

所有非取消类失败统一抛 StepExecutionError，附带稳定 reason，
便于引擎区分 retry_exhausted 等终态归因。
"""

from __future__ import annotations

import asyncio
from typing import Any

from app.models import CancellationError


class StepExecutionError(Exception):
    """步骤业务失败（可按重试策略重试）。"""

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message


class StepExecutor:
    def __init__(self, cancel_event: asyncio.Event | None = None) -> None:
        # 进程级取消钩子；步骤级别的取消由引擎通过 asyncio.Task.cancel 传播
        self._cancel_event = cancel_event

    def _check_cancelled(self) -> None:
        if self._cancel_event is not None and self._cancel_event.is_set():
            raise CancellationError("cancelled before execution")

    async def run(self, action: str, params: dict[str, Any]) -> Any:
        self._check_cancelled()
        handler = {
            "succeed": self._succeed,
            "fail": self._fail,
            "flaky": None,  # 需要 attempt 上下文，由 run_with_attempt 处理
            "sleep": self._sleep,
            "echo": self._echo,
        }.get(action)
        if handler is None:
            raise StepExecutionError(
                "unknown_action", f"unknown action: {action!r}"
            )
        return await handler(params)

    async def run_with_attempt(
        self, action: str, params: dict[str, Any], attempt: int
    ) -> Any:
        """执行一次尝试；attempt 从 1 开始，flaky 依赖该序号。"""
        self._check_cancelled()
        if action == "flaky":
            fail_times = int(params.get("fail_times", 1))
            message = str(params.get("message", "flaky failure"))
            if attempt <= fail_times:
                raise StepExecutionError(
                    "transient_failure",
                    f"{message} (attempt {attempt}/{fail_times + 1})",
                )
            return {"ok": True, "attempt": attempt}
        return await self.run(action, params)

    async def _succeed(self, params: dict[str, Any]) -> Any:
        return params.get("result", {"ok": True})

    async def _fail(self, params: dict[str, Any]) -> Any:
        message = str(params.get("message", "forced failure"))
        reason = str(params.get("reason", "forced_failure"))
        raise StepExecutionError(reason, message)

    async def _echo(self, params: dict[str, Any]) -> Any:
        return dict(params)

    async def _sleep(self, params: dict[str, Any]) -> Any:
        seconds = float(params.get("seconds", 0.1))
        deadline = asyncio.get_running_loop().time() + seconds
        while True:
            self._check_cancelled()
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return {"slept": seconds}
            await asyncio.sleep(min(0.02, remaining))
