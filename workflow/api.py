"""FastAPI 路由与请求/响应模型。

端点::

    POST   /jobs                  提交作业（body.idempotency_key，或 Idempotency-Key 头）
    GET    /jobs/{job_id}         查询作业详情（状态 + 步骤 + 事件轨迹）
    POST   /jobs/{job_id}/cancel  取消作业
    GET    /healthz               健康检查
"""
from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field

from .models import JobSpec, Reason, StepSpec, WorkflowError
from .service import JobService


# ---- 请求 / 响应模型 ------------------------------------------------------
class StepIn(BaseModel):
    id: str = Field(..., min_length=1, description="作业内唯一的步骤 ID")
    kind: str = Field(..., min_length=1, description="执行器类型，如 noop/flaky")
    params: dict[str, Any] = Field(default_factory=dict)
    depends_on: list[str] = Field(default_factory=list)
    max_retries: int = Field(0, ge=0, le=100)
    retry_backoff_seconds: float = Field(0.0, ge=0.0, le=3600.0)
    timeout_seconds: float | None = Field(None, gt=0.0, le=86400.0)


class JobIn(BaseModel):
    idempotency_key: str = Field("", min_length=0)
    max_concurrency: int = Field(4, ge=1, le=256)
    steps: list[StepIn] = Field(..., min_length=1)


def _to_spec(body: JobIn) -> JobSpec:
    return JobSpec(
        idempotency_key=body.idempotency_key,
        max_concurrency=body.max_concurrency,
        steps=tuple(
            StepSpec(
                id=s.id,
                kind=s.kind,
                params=s.params,
                depends_on=tuple(s.depends_on),
                max_retries=s.max_retries,
                retry_backoff_seconds=s.retry_backoff_seconds,
                timeout_seconds=s.timeout_seconds,
            )
            for s in body.steps
        ),
    )


def _http_status(reason: Reason) -> int:
    return {
        Reason.IDEMPOTENCY_CONFLICT: 409,
        Reason.DAG_CYCLE: 422,
        Reason.DAG_UNKNOWN_DEP: 422,
        Reason.UNKNOWN_STEP_KIND: 422,
        Reason.JOB_NOT_FOUND: 404,
        Reason.JOB_ALREADY_TERMINAL: 409,
    }.get(reason, 400)


def create_app(service: JobService) -> FastAPI:
    app = FastAPI(
        title="本地异步工作流编排服务",
        version="0.1.0",
        description="支持幂等提交、DAG 依赖编排、并发上限、退避重试、取消传播与重启恢复。",
    )
    app.state.service = service

    def svc(request: Request) -> JobService:
        # lifespan 启动后会用真实 service 覆盖 app.state.service。
        return request.app.state.service

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/jobs", status_code=202)
    async def submit_job(
        request: Request,
        body: JobIn,
        idempotency_key: str | None = Header(default=None),
    ) -> dict[str, Any]:
        # 允许用 Idempotency-Key 头提供/覆盖幂等键；头与 body 同时给出时必须一致。
        if idempotency_key:
            if body.idempotency_key and body.idempotency_key != idempotency_key:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "reason": Reason.IDEMPOTENCY_CONFLICT.value,
                        "message": (
                            f"body.idempotency_key={body.idempotency_key!r} 与 "
                            f"Idempotency-Key 头={idempotency_key!r} 不一致"
                        ),
                    },
                )
            body.idempotency_key = idempotency_key
        if not body.idempotency_key:
            raise HTTPException(
                status_code=422,
                detail={
                    "reason": "MISSING_IDEMPOTENCY_KEY",
                    "message": "必须通过 body.idempotency_key 或 Idempotency-Key 头提供幂等键",
                },
            )
        try:
            job, created = await svc(request).submit(_to_spec(body))
        except WorkflowError as exc:
            raise HTTPException(
                status_code=_http_status(exc.reason),
                detail={"reason": exc.reason.value, "message": str(exc)},
            ) from None
        return {
            "job_id": job.id,
            "state": job.state.value,
            "created": created,
            "idempotency_key": job.idempotency_key,
        }

    @app.get("/jobs/{job_id}")
    async def get_job(job_id: str, request: Request) -> dict[str, Any]:
        try:
            return svc(request).get_job_detail(job_id)
        except WorkflowError as exc:
            raise HTTPException(
                status_code=_http_status(exc.reason),
                detail={"reason": exc.reason.value, "message": str(exc)},
            ) from None

    @app.post("/jobs/{job_id}/cancel", status_code=202)
    async def cancel_job(job_id: str, request: Request) -> dict[str, Any]:
        try:
            job = await svc(request).cancel(job_id)
        except WorkflowError as exc:
            raise HTTPException(
                status_code=_http_status(exc.reason),
                detail={"reason": exc.reason.value, "message": str(exc)},
            ) from None
        return {"job_id": job.id, "state": job.state.value, "reason": job.reason}

    return app
