import json, os, time, uuid, random
import redis

r = redis.Redis(host=os.getenv("REDIS_HOST", "localhost"),
                port=int(os.getenv("REDIS_PORT", "6379")),
                db=int(os.getenv("REDIS_DB", "0")),
                decode_responses=True)
PENDING = "jobs:pending"
PROCESSING = "jobs:processing"
DELAYED = "jobs:delayed"
DEAD = "jobs:dead"

MAX_RETRIES = 3
BASE_DELAY = 2
VISIBILITY_TIMEOUT = int(os.getenv("VISIBILITY_TIMEOUT", "30"))  # seconds

IDEMPOTENCY_TTL = 24 * 3600
LEDGER = "ledger"

def enqueue(job_type, payload, max_retries=MAX_RETRIES):
    job_id = str(uuid.uuid4())
    r.hset(f"job:{job_id}", mapping={
        "id": job_id,
        "type": job_type,
        "payload": json.dumps(payload),
        "status": "pending",
        "attempts": 0,
        "max_retries": max_retries,
        "last_error": "",
        "lease_until": 0,
        "created_at": time.time(),
    })
    r.lpush(PENDING, job_id)
    return job_id

def get_job(job_id):
    job = r.hgetall(f"job:{job_id}")
    if not job:
        return None
    job["payload"] = json.loads(job["payload"])
    job["attempts"] = int(job["attempts"])
    job["max_retries"] = int(job["max_retries"])
    return job

def dequeue(timeout=5):
    job_id = r.blmove(PENDING, PROCESSING, timeout, src="RIGHT", dest="LEFT")
    if job_id:
        r.hset(f"job:{job_id}", mapping={
            "status": "processing",
            "lease_until": time.time() + VISIBILITY_TIMEOUT,
        })
    return job_id

def ack(job_id):
    # only the party that removes the job from "processing" gets to finish it
    if r.lrem(PROCESSING, 1, job_id) == 0:
        return False
    r.hset(f"job:{job_id}", "status", "done")
    return True

def fail(job_id, error):
    if r.lrem(PROCESSING, 1, job_id) == 0:
        return "ignored"            # someone else already handled it
    key = f"job:{job_id}"
    attempts = r.hincrby(key, "attempts", 1)
    max_retries = int(r.hget(key, "max_retries"))
    pipe = r.pipeline()
    pipe.hset(key, "last_error", str(error))
    if attempts > max_retries:
        pipe.hset(key, "status", "dead")
        pipe.lpush(DEAD, job_id)
        pipe.execute()
        return "dead"
    delay = BASE_DELAY * (2 ** (attempts - 1)) + random.uniform(0, 1)
    pipe.hset(key, "status", "retrying")
    pipe.zadd(DELAYED, {job_id: time.time() + delay})
    pipe.execute()
    return "retrying"

def promote_due():
    for job_id in r.zrangebyscore(DELAYED, 0, time.time()):
        if r.zrem(DELAYED, job_id):
            r.hset(f"job:{job_id}", "status", "pending")
            r.lpush(PENDING, job_id)

def reap_stuck():
    """Reclaim jobs whose worker stopped renewing the lease (crashed or hung)."""
    now = time.time()
    reclaimed = 0
    for job_id in r.lrange(PROCESSING, 0, -1):
        lease = float(r.hget(f"job:{job_id}", "lease_until") or 0)
        if lease < now and fail(job_id, "worker timed out") != "ignored":
            reclaimed += 1
    return reclaimed

def stats():
    return {
        "pending": r.llen(PENDING),
        "processing": r.llen(PROCESSING),
        "delayed": r.zcard(DELAYED),
        "dead": r.llen(DEAD),
    }

def list_dead():
    return [get_job(j) for j in r.lrange(DEAD, 0, -1)]

def requeue_dead(job_id):
    if r.lrem(DEAD, 1, job_id) == 0:
        return False
    r.hset(f"job:{job_id}", mapping={"attempts": 0, "status": "pending", "last_error": ""})
    r.lpush(PENDING, job_id)
    return True

def enqueue(job_type, payload, max_retries=MAX_RETRIES, idempotency_key=None):
    job_id = str(uuid.uuid4())
    if idempotency_key:
        # SET NX succeeds only for the first caller with this key
        if not r.set(f"idem:{idempotency_key}", job_id, nx=True, ex=IDEMPOTENCY_TTL):
            return r.get(f"idem:{idempotency_key}"), False
    r.hset(f"job:{job_id}", mapping={
        "id": job_id,
        "type": job_type,
        "payload": json.dumps(payload),
        "status": "pending",
        "attempts": 0,
        "max_retries": max_retries,
        "last_error": "",
        "lease_until": 0,
        "created_at": time.time(),
    })
    r.lpush(PENDING, job_id)
    return job_id, True

def apply_effect(job_id, amount):
    """Idempotent side effect: at most one ledger entry per job id."""
    return bool(r.hsetnx(LEDGER, job_id, amount))

def get_ledger():
    return r.hgetall(LEDGER)