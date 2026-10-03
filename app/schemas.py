"""HTTP 请求/响应 schema（pydantic）。"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from app.models import JobState, JobStatus, StepSpec, StepState, StepStatus


class StepIn(BaseModel):
    key: str = Field(min_length=1)
    action: str = Field(min_length=1)
    params: dict[str, Any] = Field(default_factory=dict)
    depends_on: list[str] = Field(default_factory=list)
    max_attempts: int = Field(default=3, ge=1)
    backoff_base: float = Field(default=0.05, ge=0)
    backoff_cap: float = Field(default=1.0, ge=0)
    timeout: float | None = Field(default=None, ge=0)

    def to_spec(self) -> StepSpec:
        return StepSpec(
            key=self.key,
            action=self.action,
            params=self.params,
            depends_on=list(self.depends_on),
            max_attempts=self.max_attempts,
            backoff_base=self.backoff_base,
            backoff_cap=self.backoff_cap,
            timeout=self.timeout,
        )


class JobSubmitIn(BaseModel):
    idempotency_key: str | None = None
    max_concurrency: int = Field(default=2, ge=1)
    # 作业整体时间预算（秒，墙钟）；缺省/None 表示不设预算，行为与历史完全一致。
    time_budget_seconds: float | None = Field(default=None, gt=0)
    steps: list[StepIn] = Field(min_length=1)


class StepOut(BaseModel):
    key: str
    status: StepStatus
    attempt: int
    result: Any = None
    error: str | None = None
    fail_reason: str | None = None
    depends_on: list[str]


class JobOut(BaseModel):
    job_id: str
    idempotency_key: str | None = None
    status: JobStatus
    cancel_requested: bool
    cancel_requested_at: float | None = None
    max_concurrency: int
    error: str | None = None
    time_budget_seconds: float | None = None
    budget_deadline: float | None = None
    steps: list[StepOut]

    @classmethod
    def from_state(cls, job: JobState, steps: list[StepState]) -> "JobOut":
        return cls(
            job_id=job.id,
            idempotency_key=job.idempotency_key,
            status=job.status,
            cancel_requested=job.cancel_requested,
            cancel_requested_at=job.cancel_requested_at,
            max_concurrency=job.max_concurrency,
            error=job.error,
            time_budget_seconds=job.time_budget_seconds,
            budget_deadline=job.budget_deadline,
            steps=[
                StepOut(
                    key=s.key,
                    status=s.status,
                    attempt=s.attempt,
                    result=s.result,
                    error=s.error,
                    fail_reason=s.fail_reason,
                    depends_on=list(s.spec.depends_on),
                )
                for s in steps
            ],
        )


class RejectOut(BaseModel):
    detail: str
    reason: str
