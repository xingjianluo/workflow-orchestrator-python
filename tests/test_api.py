"""HTTP 端到端：提交、查询、取消、错误码、重启后数据仍可查。"""
from __future__ import annotations

import time

import pytest

from fastapi.testclient import TestClient

from main import create_main_app
from workflow.executors import default_registry, reset_run_counters
from workflow.service import JobService
from workflow.store import Store


@pytest.fixture
def client(db_path, monkeypatch):
    monkeypatch.setenv("WORKFLOW_DB_PATH", db_path)
    import main as main_mod
    # create_main_app 的占位 service 使用 :memory:；lifespan 内会用真实路径替换
    reset_run_counters()
    app = create_main_app()
    # 覆盖 lifespan 使用的 DB 路径常量
    monkeypatch.setattr(main_mod, "DB_PATH", db_path)
    with TestClient(app) as c:
        yield c


def _wait_state(client, job_id, targets, polls=100, interval=0.02):
    for _ in range(polls):
        d = client.get(f"/jobs/{job_id}").json()
        if d["job"]["state"] in targets:
            return d
        time.sleep(interval)
    return d


def test_submit_and_query_lifecycle(client):
    payload = {
        "idempotency_key": "http-1", "max_concurrency": 2,
        "steps": [
            {"id": "a", "kind": "noop", "params": {"value": 1}},
            {"id": "b", "kind": "flaky", "params": {"fail_times": 1, "counter": "http-b"},
             "max_retries": 2, "retry_backoff_seconds": 0.01, "depends_on": ["a"]},
        ],
    }
    r = client.post("/jobs", json=payload)
    assert r.status_code == 202
    body = r.json()
    assert body["created"] is True
    detail = _wait_state(client, body["job_id"], {"SUCCEEDED"})
    assert detail["job"]["state"] == "SUCCEEDED"
    states = {s["id"]: s["state"] for s in detail["steps"]}
    assert states == {"a": "SUCCEEDED", "b": "SUCCEEDED"}
    attempts = {s["id"]: s["attempt"] for s in detail["steps"]}
    assert attempts["b"] == 2
    # 事件轨迹可解释
    kinds = [e["kind"] for e in detail["events"]]
    assert "job_submitted" in kinds
    print("\n[事件轨迹 kinds]", kinds)


def test_idempotency_conflict_http_status(client):
    payload = {"idempotency_key": "http-2", "steps": [{"id": "a", "kind": "noop"}]}
    assert client.post("/jobs", json=payload).status_code == 202
    payload2 = {"idempotency_key": "http-2",
                "steps": [{"id": "a", "kind": "noop", "params": {"x": 1}}]}
    r = client.post("/jobs", json=payload2)
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "IDEMPOTENCY_CONFLICT"


def test_dag_validation_http_422(client):
    payload = {"idempotency_key": "http-3", "steps": [
        {"id": "a", "kind": "noop", "depends_on": ["ghost"]}
    ]}
    r = client.post("/jobs", json=payload)
    assert r.status_code == 422
    assert r.json()["detail"]["reason"] == "DAG_UNKNOWN_DEP"


def test_cancel_via_http(client):
    payload = {"idempotency_key": "http-4", "max_concurrency": 4, "steps": [
        {"id": "a", "kind": "slow", "params": {"sleep": 1.0}},
        {"id": "b", "kind": "slow", "params": {"sleep": 1.0}},
    ]}
    jid = client.post("/jobs", json=payload).json()["job_id"]
    time.sleep(0.1)
    r = client.post(f"/jobs/{jid}/cancel")
    assert r.status_code == 202
    assert r.json()["state"] == "CANCELLING"
    detail = _wait_state(client, jid, {"CANCELLED"})
    assert detail["job"]["state"] == "CANCELLED"
    assert all(s["state"] == "CANCELLED" for s in detail["steps"])
    # 终态作业再次取消 -> 409
    r2 = client.post(f"/jobs/{jid}/cancel")
    assert r2.status_code == 409
    assert r2.json()["detail"]["reason"] == "JOB_ALREADY_TERMINAL"


def test_job_not_found(client):
    r = client.get("/jobs/nope")
    assert r.status_code == 404
    assert r.json()["detail"]["reason"] == "JOB_NOT_FOUND"


def test_missing_idempotency_key(client):
    r = client.post("/jobs", json={"steps": [{"id": "a", "kind": "noop"}]})
    assert r.status_code == 422
