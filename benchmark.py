import sys, time
import httpx

API = "http://localhost:8000"
N = int(sys.argv[1]) if len(sys.argv) > 1 else 100

with httpx.Client(base_url=API, timeout=10) as c:
    start = time.time()
    for _ in range(N):
        c.post("/jobs", json={"type": "sleep", "payload": {"seconds": 1}})
    enqueued = time.time() - start

    while True:
        s = c.get("/stats").json()
        if s["pending"] == 0 and s["processing"] == 0 and s["delayed"] == 0:
            break
        time.sleep(0.5)
    total = time.time() - start

print(f"jobs={N}  enqueue={enqueued:.1f}s  total={total:.1f}s  throughput={N/total:.2f} jobs/s  dead={s['dead']}")