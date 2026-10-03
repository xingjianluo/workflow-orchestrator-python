"""FastAPI 路由：提交 / 查询 / 取消。

错误响应体统一为 {"detail": {...,"reason": <可区分原因码>}}，HTTP 状态：
  409 idempotency_conflict / job_already_terminal
  404 job_not_found
  400 invalid_dependency
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from app.engine import WorkflowError
from app.schemas import JobOut, JobSubmitIn


def build_router(get_engine) -> APIRouter:
    """get_engine: 零参可调用，返回当前 WorkflowEngine（由 lifespan 注入）。"""
    router = APIRouter()

    def _http_for(exc: WorkflowError) -> HTTPException:
        status = {
            "idempotency_conflict": 409,
            "job_already_terminal": 409,
            "job_budget_deadline_reached": 409,
            "job_not_found": 404,
            "invalid_dependency": 400,
        }[exc.reason.value]
        return HTTPException(
            status_code=status,
            detail={"detail": exc.message, "reason": exc.reason.value},
        )

    @router.post("/jobs", response_model=JobOut, status_code=201)
    async def submit_job(payload: JobSubmitIn) -> JobOut:
        engine = get_engine()
        specs = [s.to_spec() for s in payload.steps]
        try:
            job = await engine.submit(
                specs=specs,
                idempotency_key=payload.idempotency_key,
                max_concurrency=payload.max_concurrency,
                time_budget_seconds=payload.time_budget_seconds,
            )
        except WorkflowError as exc:
            raise _http_for(exc) from exc
        return JobOut.from_state(job, engine.get_steps_state(job.id))

    @router.get("/jobs/{job_id}", response_model=JobOut)
    async def get_job(job_id: str) -> JobOut:
        engine = get_engine()
        job = engine.get_job_state(job_id)
        if job is None:
            raise HTTPException(
                status_code=404,
                detail={
                    "detail": f"unknown job: {job_id}",
                    "reason": "job_not_found",
                },
            )
        return JobOut.from_state(job, engine.get_steps_state(job_id))

    @router.post("/jobs/{job_id}/cancel", response_model=JobOut)
    async def cancel_job(job_id: str) -> JobOut:
        engine = get_engine()
        try:
            job = await engine.cancel(job_id)
        except WorkflowError as exc:
            raise _http_for(exc) from exc
        return JobOut.from_state(job, engine.get_steps_state(job_id))

    return router
