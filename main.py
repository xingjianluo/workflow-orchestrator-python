"""本地启动入口：uvicorn main:app

持久化文件默认 workflow.db（可用环境变量 WORKFLOW_DB 覆盖）。
本地运行：
    uv run uvicorn main:app
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api import build_router
from app.engine import WorkflowEngine
from app.executor import StepExecutor
from app.store import JobStore

DB_PATH = os.environ.get("WORKFLOW_DB", "workflow.db")

_engine: WorkflowEngine | None = None


def get_engine() -> WorkflowEngine:
    assert _engine is not None, "engine not started"
    return _engine


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _engine
    store = JobStore(DB_PATH)
    _engine = WorkflowEngine(store, StepExecutor())
    await _engine.start()  # 重启恢复在此发生
    try:
        yield
    finally:
        await _engine.aclose()


app = FastAPI(title="async-workflow-orchestrator", lifespan=lifespan)
app.include_router(build_router(get_engine))


@app.get("/")
def read_root() -> dict[str, str]:
    return {"service": "workflow-orchestrator"}
