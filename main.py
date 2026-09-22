"""服务入口。

本地运行::

    uv run uvicorn main:app --reload
    # 或
    uv run fastapi dev main.py

环境变量::

    WORKFLOW_DB_PATH            SQLite 文件路径（默认 ./workflow.db）
    WORKFLOW_GLOBAL_CONCURRENCY 全局跨作业并发槽（默认 8）
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from workflow.api import create_app
from workflow.executors import default_registry
from workflow.service import JobService
from workflow.store import Store

DB_PATH = os.environ.get("WORKFLOW_DB_PATH", "./workflow.db")
GLOBAL_MAX_CONCURRENCY = int(os.environ.get("WORKFLOW_GLOBAL_CONCURRENCY", "8"))

logging.basicConfig(
    level=os.environ.get("WORKFLOW_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    store = Store(DB_PATH)
    store.init_schema()
    service = JobService(
        store,
        default_registry(),
        global_max_concurrency=GLOBAL_MAX_CONCURRENCY,
    )
    # 重启恢复：接管所有未终态作业。崩溃时停在 RUNNING 的步骤回到 READY
    # 重跑（at-least-once），已 SUCCEEDED 的步骤绝不会被再次执行。
    recovered = await service.recover_all()
    if recovered:
        logging.getLogger("workflow.main").info(
            "重启后恢复了 %d 个未完成作业", recovered
        )
    app.state.service = service
    try:
        yield
    finally:
        await service.aclose()
        store.close()


def create_main_app() -> FastAPI:
    # lifespan 内部才构造 service；create_app 需要一个 service 实例来挂载路由，
    # 因此先建一个轻量“占位 service”，启动时由 lifespan 替换 app.state.service。
    from workflow.executors import ExecutorRegistry

    placeholder = JobService(
        Store(":memory:"), ExecutorRegistry(), global_max_concurrency=1
    )
    app = create_app(placeholder)
    app.router.lifespan_context = lifespan
    return app


app = create_main_app()
