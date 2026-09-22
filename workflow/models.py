"""状态模型、领域对象与错误原因码（纯数据，无行为）。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field


class StepState(str, enum.Enum):

    r"""步骤状态机。

    PENDING -> READY -> RUNNING -> SUCCEEDED
                     \-> FAILED (可重试时回 READY) -> 终态 FAILED
    任意非终态在作业取消时 -> CANCELLED；前置最终失败 -> BLOCKED。
    """

    PENDING = "PENDING"
    READY = "READY"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    BLOCKED = "BLOCKED"
    CANCELLED = "CANCELLED"


# 非终态 / 终态集合
STEP_NON_TERMINAL = frozenset(
    {StepState.PENDING, StepState.READY, StepState.RUNNING}
)
STEP_TERMINAL = frozenset(
    {
        StepState.SUCCEEDED,
        StepState.FAILED,
        StepState.BLOCKED,
        StepState.CANCELLED,
    }
)


class JobState(str, enum.Enum):
    """作业状态机。"""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"


JOB_TERMINAL = frozenset(
    {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELLED}
)


# ---- 可区分的失败 / 拒绝原因码 -------------------------------------------
class Reason(str, enum.Enum):
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"  # 幂等键复用但请求体不一致
    DAG_CYCLE = "DAG_CYCLE"                        # 依赖图存在环
    DAG_UNKNOWN_DEP = "DAG_UNKNOWN_DEP"            # 依赖了不存在的步骤
    RETRIES_EXHAUSTED = "RETRIES_EXHAUSTED"        # 重试次数耗尽
    UPSTREAM_FAILED = "UPSTREAM_FAILED"            # 前置步骤最终失败
    CANCELLED = "CANCELLED"                        # 作业被取消
    EXECUTOR_ERROR = "EXECUTOR_ERROR"              # 执行器业务失败
    UNKNOWN_STEP_KIND = "UNKNOWN_STEP_KIND"        # 未注册的步骤类型
    JOB_NOT_FOUND = "JOB_NOT_FOUND"
    JOB_ALREADY_TERMINAL = "JOB_ALREADY_TERMINAL"


@dataclass
class StepSpec:
    """提交时的步骤定义。"""

    id: str
    kind: str
    params: dict = field(default_factory=dict)
    depends_on: tuple[str, ...] = ()
    max_retries: int = 0
    retry_backoff_seconds: float = 0.0
    timeout_seconds: float | None = None


@dataclass
class JobSpec:
    """提交时的作业定义。"""

    idempotency_key: str
    steps: tuple[StepSpec, ...]
    max_concurrency: int = 4
    request_hash: str = ""


@dataclass
class StepRecord:
    """steps 表的一行（运行态）。"""

    id: str
    job_id: str
    kind: str
    params_json: str
    depends_on_json: str
    state: StepState
    attempt: int
    max_retries: int
    retry_backoff_seconds: float
    timeout_seconds: float | None
    reason: str
    error: str
    result_json: str
    run_after_ts: float


@dataclass
class JobRecord:
    """jobs 表的一行。"""

    id: str
    idempotency_key: str
    request_hash: str
    spec_json: str
    state: JobState
    max_concurrency: int
    reason: str
    error: str
    created_ts: float
    updated_ts: float


class WorkflowError(Exception):
    """带可区分原因码的业务异常。"""

    def __init__(self, reason: Reason, message: str):
        self.reason = reason
        super().__init__(message)
