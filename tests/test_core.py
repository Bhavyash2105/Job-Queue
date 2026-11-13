import threading, time
import core


def test_enqueue_creates_pending_job():
    job_id, created = core.enqueue("sleep", {"seconds": 1})
    job = core.get_job(job_id)
    assert created is True
    assert job["status"] == "pending"
    assert job["attempts"] == 0
    assert core.stats()["pending"] == 1


def test_dequeue_returns_none_when_empty():
    assert core.dequeue(timeout=1) is None


def test_dequeue_moves_job_to_processing_with_lease():
    job_id, _ = core.enqueue("sleep", {})
    assert core.dequeue(timeout=1) == job_id
    job = core.get_job(job_id)
    assert job["status"] == "processing"
    assert float(job["lease_until"]) > time.time()
    assert core.stats() == {"pending": 0, "processing": 1, "delayed": 0, "dead": 0}


def test_ack_marks_job_done():
    job_id, _ = core.enqueue("sleep", {})
    core.dequeue(timeout=1)
    assert core.ack(job_id) is True
    assert core.get_job(job_id)["status"] == "done"
    assert core.stats()["processing"] == 0


def test_fail_schedules_retry():
    job_id, _ = core.enqueue("flaky", {})
    core.dequeue(timeout=1)
    assert core.fail(job_id, "boom") == "retrying"
    job = core.get_job(job_id)
    assert job["status"] == "retrying"
    assert job["attempts"] == 1
    assert job["last_error"] == "boom"
    assert core.r.zscore(core.DELAYED, job_id) > time.time()
    assert core.stats()["processing"] == 0


def test_backoff_doubles_each_attempt():
    job_id, _ = core.enqueue("flaky", {}, max_retries=5)
    delays = []
    for _ in range(3):
        core.r.lpush(core.PROCESSING, job_id)
        start = time.time()
        core.fail(job_id, "boom")
        delays.append(core.r.zscore(core.DELAYED, job_id) - start)
        core.r.zrem(core.DELAYED, job_id)
    assert 2 <= delays[0] < 3.5
    assert 4 <= delays[1] < 5.5
    assert 8 <= delays[2] < 9.5


def test_job_goes_to_dead_letter_queue_after_max_retries():
    job_id, _ = core.enqueue("flaky", {}, max_retries=0)
    core.dequeue(timeout=1)
    assert core.fail(job_id, "boom") == "dead"
    assert core.get_job(job_id)["status"] == "dead"
    assert core.r.lrange(core.DEAD, 0, -1) == [job_id]


def test_promote_due_moves_ready_retries_to_pending():
    job_id, _ = core.enqueue("flaky", {})
    core.dequeue(timeout=1)
    core.fail(job_id, "boom")
    core.r.zadd(core.DELAYED, {job_id: time.time() - 1})   # pretend the wait is over
    core.promote_due()
    assert core.get_job(job_id)["status"] == "pending"
    assert core.stats()["pending"] == 1
    assert core.stats()["delayed"] == 0


def test_reaper_reclaims_job_from_crashed_worker():
    job_id, _ = core.enqueue("crash", {})
    core.dequeue(timeout=1)                                   # a worker takes it, then dies
    core.r.hset(f"job:{job_id}", "lease_until", time.time() - 1)   # its lease expires
    assert core.reap_stuck() == 1
    job = core.get_job(job_id)
    assert job["status"] == "retrying"
    assert job["attempts"] == 1
    assert job["last_error"] == "worker timed out"


def test_reaper_leaves_jobs_with_live_lease_alone():
    job_id, _ = core.enqueue("sleep", {})
    core.dequeue(timeout=1)
    assert core.reap_stuck() == 0
    assert core.get_job(job_id)["status"] == "processing"


def test_late_ack_after_reclaim_is_rejected():
    job_id, _ = core.enqueue("sleep", {})
    core.dequeue(timeout=1)
    core.r.hset(f"job:{job_id}", "lease_until", time.time() - 1)
    core.reap_stuck()
    assert core.ack(job_id) is False
    assert core.get_job(job_id)["status"] == "retrying"


def test_only_one_of_two_racing_failures_wins():
    job_id, _ = core.enqueue("flaky", {})
    core.dequeue(timeout=1)
    results = []
    barrier = threading.Barrier(2)

    def attempt():
        barrier.wait()
        results.append(core.fail(job_id, "boom"))

    threads = [threading.Thread(target=attempt) for _ in range(2)]
    for t in threads: t.start()
    for t in threads: t.join()
    assert sorted(results) == ["ignored", "retrying"]
    assert core.get_job(job_id)["attempts"] == 1


def test_idempotency_key_prevents_duplicate_jobs():
    first, created1 = core.enqueue("sleep", {}, idempotency_key="order-42")
    second, created2 = core.enqueue("sleep", {}, idempotency_key="order-42")
    assert created1 is True and created2 is False
    assert first == second
    assert core.stats()["pending"] == 1


def test_apply_effect_is_idempotent():
    assert core.apply_effect("job-1", 100) is True
    assert core.apply_effect("job-1", 100) is False
    assert core.get_ledger() == {"job-1": "100"}


def test_requeue_dead_sends_job_back_to_pending():
    job_id, _ = core.enqueue("flaky", {}, max_retries=0)
    core.dequeue(timeout=1)
    core.fail(job_id, "boom")
    assert core.requeue_dead(job_id) is True
    job = core.get_job(job_id)
    assert job["status"] == "pending"
    assert job["attempts"] == 0
    assert core.stats()["dead"] == 0
    assert core.requeue_dead(job_id) is False