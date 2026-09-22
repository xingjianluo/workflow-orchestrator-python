"""公共测试夹具。

每个测试独立 SQLite 文件；引擎的 INFO 日志（输入 / 状态跃迁 / 判定依据）
在 -s 或失败时可见；同时提供 ``print_trace`` 把持久化事件流打印出来，
满足“状态流转与失败原因可区分且稳定、日志可解释”的验证要求。
"""
from __future__ import annotations

import logging

import pytest

from workflow.executors import default_registry, reset_run_counters
from workflow.service import JobService
from workflow.store import Store

# 让所有用例都能看到引擎的判定日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    force=True,
)


@pytest.fixture
def db_path(tmp_path: pytest.TempPathFactory) -> str:
    return str(tmp_path / "workflow.db")


@pytest.fixture
def store(db_path: str) -> Store:
    s = Store(db_path)
    s.init_schema()
    yield s
    s.close()


@pytest.fixture
def service(store: Store) -> JobService:
    reset_run_counters()
    return JobService(store, default_registry(), global_max_concurrency=4)


def print_trace(store: Store, job_id: str, title: str = "事件轨迹") -> None:
    """以稳定可读的形式打印作业的状态跃迁轨迹。"""
    print(f"\n===== {title} job={job_id[:8]} =====")
    for e in store.list_events(job_id):
        arrow = f"{e['from_state']} -> {e['to_state']}" if e["to_state"] else e["kind"]
        print(
            f"#{e['seq']:>3} {e['kind']:<16} "
            f"step={e['step_id'] or '-':<6} {arrow:<26} "
            f"reason={e['reason'] or '-':<20} {e['detail']}"
        )
    print("=" * 60)
