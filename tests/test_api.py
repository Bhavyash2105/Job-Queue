from fastapi.testclient import TestClient
import api, core

client = TestClient(api.app)


def test_create_job_returns_202_and_job_is_readable():
    res = client.post("/jobs", json={"type": "sleep", "payload": {"seconds": 1}})
    assert res.status_code == 202
    job_id = res.json()["id"]
    got = client.get(f"/jobs/{job_id}")
    assert got.status_code == 200
    assert got.json()["status"] == "pending"


def test_unknown_job_returns_404():
    assert client.get("/jobs/does-not-exist").status_code == 404


def test_duplicate_idempotency_key_is_flagged():
    body = {"type": "sleep", "idempotency_key": "order-42"}
    first = client.post("/jobs", json=body).json()
    second = client.post("/jobs", json=body).json()
    assert first["duplicate"] is False
    assert second["duplicate"] is True
    assert first["id"] == second["id"]


def test_stats_reports_queue_sizes():
    client.post("/jobs", json={"type": "sleep"})
    assert client.get("/stats").json() == {"pending": 1, "processing": 0, "delayed": 0, "dead": 0}


def test_retry_rejects_job_not_in_dead_letter_queue():
    job_id = client.post("/jobs", json={"type": "sleep"}).json()["id"]
    assert client.post(f"/jobs/{job_id}/retry").status_code == 404


def test_dead_job_can_be_retried():
    job_id, _ = core.enqueue("flaky", {}, max_retries=0)
    core.dequeue(timeout=1)
    core.fail(job_id, "boom")
    assert [j["id"] for j in client.get("/dead").json()] == [job_id]
    assert client.post(f"/jobs/{job_id}/retry").status_code == 200
    assert client.get(f"/jobs/{job_id}").json()["status"] == "pending"