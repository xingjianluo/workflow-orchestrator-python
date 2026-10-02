"""SQLite 持久化层（stdlib sqlite3，WAL 模式）。

所有写操作在同一把锁下串行化，状态跃迁与事件落盘原子完成。
服务重启后据此恢复：SUCCEEDED 步骤不会再次执行，
RETRYING/RUNNING（上次进程中断）的步骤会被重新调度。
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from typing import Any

from app.models import JobState, JobStatus, StepSpec, StepState, StepStatus

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id               TEXT PRIMARY KEY,
    idempotency_key  TEXT UNIQUE,
    status           TEXT NOT NULL,
    max_concurrency  INTEGER NOT NULL,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL,
    error            TEXT,
    request_payload  TEXT
);
CREATE TABLE IF NOT EXISTS steps (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      TEXT NOT NULL,
    key         TEXT NOT NULL,
    status      TEXT NOT NULL,
    attempt     INTEGER NOT NULL DEFAULT 0,
    run_at      REAL NOT NULL DEFAULT 0,
    result      TEXT,
    error       TEXT,
    fail_reason TEXT,
    started_at  REAL,
    finished_at REAL,
    spec        TEXT NOT NULL,
    UNIQUE(job_id, key)
);
CREATE INDEX IF NOT EXISTS idx_steps_job ON steps(job_id);
"""

# 既有库的增量列（CREATE TABLE IF NOT EXISTS 不会修改已存在的表）
_JOB_EXTRA_COLUMNS = {
    "budget_seconds": "REAL",        # 整体时间预算（秒），NULL 表示不启用
    "deadline_at": "REAL",           # 预算到期的墙钟时间戳
    "cancel_requested_at": "REAL",   # 取消请求落盘的墙钟时刻
}


def _spec_to_json(spec: StepSpec) -> str:
    return json.dumps(
        {
            "key": spec.key,
            "action": spec.action,
            "params": spec.params,
            "depends_on": list(spec.depends_on),
            "max_attempts": spec.max_attempts,
            "backoff_base": spec.backoff_base,
            "backoff_cap": spec.backoff_cap,
            "timeout": spec.timeout,
        },
        sort_keys=True,
    )


def _spec_from_json(raw: str) -> StepSpec:
    d = json.loads(raw)
    return StepSpec(
        key=d["key"],
        action=d["action"],
        params=d.get("params", {}),
        depends_on=list(d.get("depends_on", [])),
        max_attempts=d.get("max_attempts", 3),
        backoff_base=d.get("backoff_base", 0.05),
        backoff_cap=d.get("backoff_cap", 1.0),
        timeout=d.get("timeout"),
    )


def _dumps(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, default=str)


def _loads(raw: str | None) -> Any:
    if raw is None:
        return None
    return json.loads(raw)


class JobStore:
    """单连接 + 互斥锁的简单可靠存储；SQLite 以文件形式持久化。"""

    def __init__(self, path: str = "workflow.db") -> None:
        self._path = path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            path,
            check_same_thread=False,
            isolation_level=None,  # autocommit；写操作显式 BEGIN IMMEDIATE
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self.init_schema()

    def init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(_SCHEMA)
            existing = {
                r["name"] for r in self._conn.execute("PRAGMA table_info(jobs)")
            }
            for col, ddl in _JOB_EXTRA_COLUMNS.items():
                if col not in existing:
                    self._conn.execute(f"ALTER TABLE jobs ADD COLUMN {col} {ddl}")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---------------- 作业 ----------------
    def create_job(self, job: JobState, specs: list[StepSpec]) -> JobState:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    """INSERT INTO jobs(id, idempotency_key, status, max_concurrency,
                       cancel_requested, created_at, updated_at, error, request_payload,
                       budget_seconds, deadline_at, cancel_requested_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        job.id,
                        job.idempotency_key,
                        job.status.value,
                        int(job.max_concurrency),
                        1 if job.cancel_requested else 0,
                        job.created_at,
                        job.updated_at,
                        job.error,
                        _dumps(job.request_payload),
                        job.budget_seconds,
                        job.deadline_at,
                        job.cancel_requested_at,
                    ),
                )
                for spec in specs:
                    self._conn.execute(
                        """INSERT INTO steps(job_id, key, status, attempt, run_at,
                           result, error, fail_reason, started_at, finished_at, spec)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                        (
                            job.id,
                            spec.key,
                            StepStatus.PENDING.value,
                            0,
                            0.0,
                            None,
                            None,
                            None,
                            None,
                            None,
                            _spec_to_json(spec),
                        ),
                    )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return job

    def _row_to_job(self, row: sqlite3.Row) -> JobState:
        return JobState(
            id=row["id"],
            idempotency_key=row["idempotency_key"],
            status=JobStatus(row["status"]),
            max_concurrency=row["max_concurrency"],
            cancel_requested=bool(row["cancel_requested"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            error=row["error"],
            request_payload=_loads(row["request_payload"]),
            budget_seconds=row["budget_seconds"],
            deadline_at=row["deadline_at"],
            cancel_requested_at=row["cancel_requested_at"],
        )

    def get_job(self, job_id: str) -> JobState | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
        return self._row_to_job(row) if row else None

    def get_job_by_idempotency_key(self, key: str) -> JobState | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE idempotency_key = ?", (key,)
            ).fetchone()
        return self._row_to_job(row) if row else None

    def update_job(self, job: JobState) -> None:
        job.updated_at = time.time()
        with self._lock:
            self._conn.execute(
                """UPDATE jobs SET status=?, max_concurrency=?, cancel_requested=?,
                   updated_at=?, error=?, request_payload=?,
                   budget_seconds=?, deadline_at=?, cancel_requested_at=?
                   WHERE id=?""",
                (
                    job.status.value,
                    int(job.max_concurrency),
                    1 if job.cancel_requested else 0,
                    job.updated_at,
                    job.error,
                    _dumps(job.request_payload),
                    job.budget_seconds,
                    job.deadline_at,
                    job.cancel_requested_at,
                    job.id,
                ),
            )

    def try_set_cancel_requested(self, job_id: str, ts: float) -> JobState | None:
        """仅当作业非终态时置取消标记（定向 UPDATE，不覆盖状态列）。

        返回落盘后的最新作业行；作业不存在或已终态时返回 None。
        定向更新避免与调度器的状态写互相覆盖（取消只写标记列）。
        """
        from app.models import JOB_TERMINAL

        terminal = tuple(s.value for s in JOB_TERMINAL)
        placeholders = ",".join("?" for _ in terminal)
        with self._lock:
            cur = self._conn.execute(
                f"""UPDATE jobs SET cancel_requested=1, cancel_requested_at=?,
                    updated_at=? WHERE id=? AND status NOT IN ({placeholders})""",
                (ts, time.time(), job_id, *terminal),
            )
            if cur.rowcount == 0:
                return None
        return self.get_job(job_id)

    def list_all_jobs(self) -> list[JobState]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM jobs ORDER BY created_at").fetchall()
        return [self._row_to_job(r) for r in rows]

    # ---------------- 步骤 ----------------
    def _row_to_step(self, row: sqlite3.Row) -> StepState:
        return StepState(
            id=row["id"],
            job_id=row["job_id"],
            key=row["key"],
            spec=_spec_from_json(row["spec"]),
            status=StepStatus(row["status"]),
            attempt=row["attempt"],
            run_at=row["run_at"],
            result=_loads(row["result"]),
            error=row["error"],
            fail_reason=row["fail_reason"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
        )

    def list_steps(self, job_id: str) -> list[StepState]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM steps WHERE job_id=? ORDER BY id", (job_id,)
            ).fetchall()
        return [self._row_to_step(r) for r in rows]

    def get_step(self, job_id: str, key: str) -> StepState | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM steps WHERE job_id=? AND key=?", (job_id, key)
            ).fetchone()
        return self._row_to_step(row) if row else None

    def update_step(self, step: StepState) -> None:
        with self._lock:
            self._conn.execute(
                """UPDATE steps SET status=?, attempt=?, run_at=?, result=?,
                   error=?, fail_reason=?, started_at=?, finished_at=?
                   WHERE job_id=? AND key=?""",
                (
                    step.status.value,
                    step.attempt,
                    step.run_at,
                    _dumps(step.result),
                    step.error,
                    step.fail_reason,
                    step.started_at,
                    step.finished_at,
                    step.job_id,
                    step.key,
                ),
            )
