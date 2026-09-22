"""SQLite 持久化层。

只负责序列化存取与状态写入；调度规则在 scheduler/service 层。
所有写操作在短事务中完成，进程崩溃时最多丢失“正在进行中”的一次尝试，
不会丢失已提交的状态跃迁。
"""
from __future__ import annotations

import sqlite3
import threading
import time
from collections.abc import Iterator

from .models import JOB_TERMINAL, JobRecord, JobState, StepRecord, StepState

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    request_hash TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    state TEXT NOT NULL,
    max_concurrency INTEGER NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    error TEXT NOT NULL DEFAULT '',
    created_ts REAL NOT NULL,
    updated_ts REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS steps (
    id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    params_json TEXT NOT NULL,
    depends_on_json TEXT NOT NULL,
    state TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 0,
    retry_backoff_seconds REAL NOT NULL DEFAULT 0,
    timeout_seconds REAL,
    reason TEXT NOT NULL DEFAULT '',
    error TEXT NOT NULL DEFAULT '',
    result_json TEXT NOT NULL DEFAULT '',
    run_after_ts REAL NOT NULL DEFAULT 0,
    PRIMARY KEY (job_id, id)
);
CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    step_id TEXT NOT NULL DEFAULT '',
    ts REAL NOT NULL,
    kind TEXT NOT NULL,
    from_state TEXT NOT NULL DEFAULT '',
    to_state TEXT NOT NULL DEFAULT '',
    reason TEXT NOT NULL DEFAULT '',
    detail TEXT NOT NULL DEFAULT ''
);
"""

NON_TERMINAL_JOB_STATES = tuple(s for s in JobState if s not in JOB_TERMINAL)


def _now() -> float:
    return time.time()


class Store:
    """线程安全的 SQLite 封装（check_same_thread=False + 互斥锁）。

    FastAPI 的同步端点与 asyncio 调度器可能来自不同线程，因此用一把
    进程内互斥锁串行化所有数据库访问；本地单机场景足够。
    """

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA synchronous=FULL")

    # -- 生命周期 ---------------------------------------------------------
    def init_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # -- 行映射 -----------------------------------------------------------
    @staticmethod
    def _row_to_job(r: sqlite3.Row) -> JobRecord:
        return JobRecord(
            id=r["id"],
            idempotency_key=r["idempotency_key"],
            request_hash=r["request_hash"],
            spec_json=r["spec_json"],
            state=JobState(r["state"]),
            max_concurrency=r["max_concurrency"],
            reason=r["reason"],
            error=r["error"],
            created_ts=r["created_ts"],
            updated_ts=r["updated_ts"],
        )

    @staticmethod
    def _row_to_step(r: sqlite3.Row) -> StepRecord:
        return StepRecord(
            id=r["id"],
            job_id=r["job_id"],
            kind=r["kind"],
            params_json=r["params_json"],
            depends_on_json=r["depends_on_json"],
            state=StepState(r["state"]),
            attempt=r["attempt"],
            max_retries=r["max_retries"],
            retry_backoff_seconds=r["retry_backoff_seconds"],
            timeout_seconds=r["timeout_seconds"],
            reason=r["reason"],
            error=r["error"],
            result_json=r["result_json"],
            run_after_ts=r["run_after_ts"],
        )

    def _iter_jobs(self, sql: str, params: tuple = ()) -> Iterator[JobRecord]:
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return (self._row_to_job(r) for r in rows)

    # -- 读 ---------------------------------------------------------------
    def get_job_by_idempotency_key(self, key: str) -> JobRecord | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE idempotency_key = ?", (key,)
            ).fetchone()
        return self._row_to_job(row) if row is not None else None

    def get_job(self, job_id: str) -> JobRecord:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
        if row is None:
            raise KeyError(job_id)
        return self._row_to_job(row)

    def list_steps(self, job_id: str) -> list[StepRecord]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM steps WHERE job_id = ? ORDER BY rowid",
                (job_id,),
            ).fetchall()
        return [self._row_to_step(r) for r in rows]

    def get_step(self, job_id: str, step_id: str) -> StepRecord:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM steps WHERE job_id = ? AND id = ?",
                (job_id, step_id),
            ).fetchone()
        if row is None:
            raise KeyError(step_id)
        return self._row_to_step(row)

    def list_all_jobs(self) -> list[JobRecord]:
        return list(self._iter_jobs("SELECT * FROM jobs ORDER BY created_ts"))

    def list_non_terminal_jobs(self) -> list[JobRecord]:
        placeholders = ",".join("?" for _ in NON_TERMINAL_JOB_STATES)
        sql = "SELECT * FROM jobs WHERE state IN (" + placeholders + ") ORDER BY created_ts"
        return list(
            self._iter_jobs(
                sql, tuple(s.value for s in NON_TERMINAL_JOB_STATES)
            )
        )

    # -- 写 ---------------------------------------------------------------
    def insert_job_with_steps(
        self, job: JobRecord, steps: list[StepRecord]
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """INSERT INTO jobs (id, idempotency_key, request_hash, spec_json,
                       state, max_concurrency, reason, error, created_ts, updated_ts)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (
                    job.id,
                    job.idempotency_key,
                    job.request_hash,
                    job.spec_json,
                    job.state.value,
                    job.max_concurrency,
                    job.reason,
                    job.error,
                    job.created_ts,
                    job.updated_ts,
                ),
            )
            self._conn.executemany(
                """INSERT INTO steps (id, job_id, kind, params_json, depends_on_json,
                       state, attempt, max_retries, retry_backoff_seconds,
                       timeout_seconds, reason, error, result_json, run_after_ts)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                [
                    (
                        s.id,
                        s.job_id,
                        s.kind,
                        s.params_json,
                        s.depends_on_json,
                        s.state.value,
                        s.attempt,
                        s.max_retries,
                        s.retry_backoff_seconds,
                        s.timeout_seconds,
                        s.reason,
                        s.error,
                        s.result_json,
                        s.run_after_ts,
                    )
                    for s in steps
                ],
            )

    _STEP_COLUMNS = frozenset(
        {"state", "attempt", "reason", "error", "result_json", "run_after_ts", "kind"}
    )

    def update_step(self, job_id: str, step_id: str, **fields) -> None:
        if not fields:
            return
        for k in fields:
            if k not in self._STEP_COLUMNS:
                raise ValueError(f"非法字段: {k}")
        assignments = ", ".join(f"{k} = ?" for k in fields)
        values = [
            v.value if isinstance(v, StepState) else v for v in fields.values()
        ]
        with self._lock, self._conn:
            cur = self._conn.execute(
                f"UPDATE steps SET {assignments} WHERE job_id = ? AND id = ?",
                (*values, job_id, step_id),
            )
            if cur.rowcount != 1:
                raise KeyError(step_id)

    _JOB_COLUMNS = frozenset({"state", "reason", "error"})

    def update_job(self, job_id: str, **fields) -> None:
        if not fields:
            return
        for k in fields:
            if k not in self._JOB_COLUMNS:
                raise ValueError(f"非法字段: {k}")
        fields = {**fields, "updated_ts": _now()}
        assignments = ", ".join(f"{k} = ?" for k in fields)
        values = [
            v.value if isinstance(v, JobState) else v for v in fields.values()
        ]
        with self._lock, self._conn:
            cur = self._conn.execute(
                f"UPDATE jobs SET {assignments} WHERE id = ?",
                (*values, job_id),
            )
            if cur.rowcount != 1:
                raise KeyError(job_id)

    def append_event(
        self,
        job_id: str,
        kind: str,
        step_id: str = "",
        from_state: str = "",
        to_state: str = "",
        reason: str = "",
        detail: str = "",
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """INSERT INTO events (job_id, step_id, ts, kind,
                       from_state, to_state, reason, detail)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (
                    job_id,
                    step_id,
                    _now(),
                    kind,
                    from_state,
                    to_state,
                    reason,
                    detail,
                ),
            )

    def list_events(self, job_id: str) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                """SELECT seq, job_id, step_id, ts, kind, from_state,
                          to_state, reason, detail
                     FROM events WHERE job_id = ? ORDER BY seq""",
                (job_id,),
            ).fetchall()
        return [dict(r) for r in rows]
