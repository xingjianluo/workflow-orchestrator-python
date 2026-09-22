"""领域模型：作业、步骤的状态机与数据结构。

状态模型（均为持久化状态，重启后可见）：

作业状态 JobStatus:
    PENDING   -> RUNNING -> SUCCEEDED / FAILED / CANCELLED

步骤状态 StepStatus:
    PENDING    已创建，等待依赖完成
    READY      依赖全部成功，等待调度槽位
    RUNNING    正在执行（含退避等待中的 RETRYING 视图见 attempt 字段）
    RETRYING   一次尝试失败，退避后将重试
    SUCCEEDED  终态：执行成功
    FAILED     终态：重试耗尽
    BLOCKED    终态：上游最终失败（FAILED/BLOCKED/CANCELLED），本步骤不再执行
    CANCELLED  终态：作业被取消，步骤尚未完成

终态集合：SUCCEEDED / FAILED / BLOCKED / CANCELLED
"""

from __future__ import annotations

import enum
import time
from dataclasses import dataclass, field
from typing import Any


class JobStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StepStatus(str, enum.Enum):
    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    RETRYING = "retrying"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    BLOCKED = "blocked"
    CANCELLED = "cancelled"


STEP_TERMINAL: frozenset[StepStatus] = frozenset(
    {
        StepStatus.SUCCEEDED,
        StepStatus.FAILED,
        StepStatus.BLOCKED,
        StepStatus.CANCELLED,
    }
)

# 上游的这些状态会把下游步骤推入 BLOCKED
STEP_FAILING: frozenset[StepStatus] = frozenset(
    {
        StepStatus.FAILED,
        StepStatus.BLOCKED,
        StepStatus.CANCELLED,
    }
)

JOB_TERMINAL: frozenset[JobStatus] = frozenset(
    {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED}
)


# 幂等冲突等可区分的失败原因码
class RejectReason(str, enum.Enum):
    IDEMPOTENCY_CONFLICT = "idempotency_conflict"  # 幂等键相同但请求体不一致
    JOB_NOT_FOUND = "job_not_found"
    JOB_ALREADY_TERMINAL = "job_already_terminal"  # 终态作业不可取消
    INVALID_DEPENDENCY = "invalid_dependency"  # 依赖不存在或成环


class CancellationError(Exception):
    """步骤执行期间收到取消信号。"""


@dataclass
class StepSpec:
    """步骤声明（来自提交请求）。"""

    key: str
    action: str  # succeed | fail | flaky | sleep
    params: dict[str, Any] = field(default_factory=dict)
    depends_on: list[str] = field(default_factory=list)
    max_attempts: int = 3  # 含首次执行，<=1 表示不重试
    backoff_base: float = 0.05  # 指数退避基数（秒），实际等待 base * 2**(attempt-1)，上限 backoff_cap
    backoff_cap: float = 1.0
    timeout: float | None = None  # 单次尝试超时（秒）

    def request_payload(self) -> dict[str, Any]:
        """用于幂等一致性比较的规范化请求内容（不含幂等键）。"""
        return {
            "key": self.key,
            "action": self.action,
            "params": self.params,
            "depends_on": list(self.depends_on),
            "max_attempts": self.max_attempts,
            "backoff_base": self.backoff_base,
            "backoff_cap": self.backoff_cap,
            "timeout": self.timeout,
        }


@dataclass
class StepState:
    """步骤的持久化运行时状态。"""

    id: int | None
    job_id: str
    key: str
    spec: StepSpec
    status: StepStatus = StepStatus.PENDING
    attempt: int = 0  # 已开始的尝试次数
    run_at: float = 0.0  # RETRYING 时最早可再次执行的时间戳（单调时钟基准见 engine）
    result: Any = None
    error: str | None = None  # 最近一次失败原因
    fail_reason: str | None = None  # 终态归因（区分 retry_exhausted / upstream_failed / cancelled）
    started_at: float | None = None
    finished_at: float | None = None


@dataclass
class JobState:
    """作业的持久化运行时状态。"""

    id: str
    idempotency_key: str | None
    status: JobStatus = JobStatus.PENDING
    max_concurrency: int = 2
    cancel_requested: bool = False
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    error: str | None = None
    request_payload: dict[str, Any] | None = None  # 规范化请求体，用于幂等冲突检测
