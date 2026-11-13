from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from core import enqueue, get_job, stats, list_dead, requeue_dead, get_ledger

app = FastAPI(title="Job Queue")

class JobIn(BaseModel):
    type: str
    payload: dict = {}
    max_retries: int = 3
    idempotency_key: str | None = None

@app.post("/jobs", status_code=202)
def create_job(job: JobIn):
    job_id, created = enqueue(job.type, job.payload, job.max_retries, job.idempotency_key)
    return {"id": job_id, "status": "pending", "duplicate": not created}

@app.get("/jobs/{job_id}")
def read_job(job_id: str):
    job = get_job(job_id)
    if job is None:
        raise HTTPException(404, "job not found")
    return job

@app.get("/ledger")
def read_ledger():
    return get_ledger()

@app.get("/stats")
def read_stats():
    return stats()

@app.get("/dead")
def read_dead():
    return list_dead()

@app.post("/jobs/{job_id}/retry")
def retry_dead(job_id: str):
    if not requeue_dead(job_id):
        raise HTTPException(404, "job is not in the dead-letter queue")
    return {"id": job_id, "status": "pending"}