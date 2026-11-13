import os, time, random
from core import dequeue, ack, fail, get_job, promote_due, reap_stuck, apply_effect

def handle(job):
    t = job["type"]
    if t == "sleep":
        time.sleep(job["payload"].get("seconds", 1))
    elif t == "flaky":
        if random.random() < job["payload"].get("fail_rate", 0.7):
            raise RuntimeError("random failure")
    elif t == "crash":
        if job["attempts"] == 0:
            os._exit(1)     # simulates the worker dying mid-job, no cleanup
    elif t == "credit":
            time.sleep(job["payload"].get("seconds", 0))
            applied = apply_effect(job["id"], job["payload"]["amount"])
            print("effect applied" if applied else "duplicate run, effect skipped")
    else:
        raise ValueError(f"unknown job type: {t}")

print("worker started")
last_reap = 0
while True:
    promote_due()
    if time.time() - last_reap > 2:
        n = reap_stuck()
        if n:
            print(f"reclaimed {n} stuck job(s)")
        last_reap = time.time()
    job_id = dequeue(timeout=2)
    if job_id is None:
        continue
    job = get_job(job_id)
    try:
        handle(job)
        if ack(job_id):
            print("done", job_id)
        else:
            print("late finish, job was already reclaimed:", job_id)
    except Exception as e:
        print(fail(job_id, e), job_id, "-", e)