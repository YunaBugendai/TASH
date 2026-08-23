"""
Headless smoke test: runs a real Master server and a real Worker client
over actual TCP sockets on localhost, auto-approves the Worker (as a
test stand-in for the human clicking "Authorize"), distributes a small
job, and asserts it completes with the right combined result.

This is *not* part of the pytest suite (it opens real sockets and runs
for a couple of seconds) -- it's a manual end-to-end check.
Run with: python3 e2e_check.py
"""
import asyncio
import sys
import time

sys.path.insert(0, ".")

from master.config import MasterConfig
from master.master_core import MasterServer
from master.task_manager import TaskManager
from shared import crypto
from worker.worker_core import WorkerClient


async def main():
    config = MasterConfig(host="127.0.0.1", port=18765, discovery_port=18766, task_timeout_sec=10)
    pairing_code = crypto.generate_pairing_code()
    tm = TaskManager(task_timeout_sec=config.task_timeout_sec)
    server = MasterServer(config, tm, pairing_code)
    server.set_auto_approve(True)  # stand-in for the human clicking "Authorize"

    events = []
    server.on_event = lambda ev, data: events.append((ev, data))

    server.start()
    await asyncio.sleep(0.3)  # let the TCP/UDP servers come up

    client = WorkerClient("test-worker-1")
    client_events = []
    client.on_event = lambda ev, data: client_events.append((ev, data))
    client.set_user_authorized(True)  # stand-in for the human clicking "Authorize"
    client.start("127.0.0.1", config.port, pairing_code)

    # Wait for authorization to complete.
    for _ in range(50):
        if any(ev == "worker_authorized" for ev, _ in events):
            break
        await asyncio.sleep(0.1)
    assert any(ev == "worker_authorized" for ev, _ in events), "worker never got authorized"
    print("[OK] worker authorized via pairing code + auto-approve")

    N = 50_000
    CHUNK = 5_000
    tm.start_job("job-e2e", "cpu_benchmark", N, CHUNK)
    server.dispatch_job()

    t0 = time.time()
    while not tm.is_job_finished() and time.time() - t0 < 15:
        await asyncio.sleep(0.1)

    snap = tm.progress_snapshot()
    print(f"[OK] job finished: {snap}")
    assert snap["done"] == snap["total"], "not all chunks completed"

    combined = tm.combine_results()
    assert combined["count"] == N, f"expected {N} items, got {combined['count']}"
    print(f"[OK] combined result count == {N}, sum={combined['sum']:.4f}")

    # Sanity-check against a direct, non-distributed computation.
    from tasks.cpu_benchmark_task import cpu_benchmark
    direct = cpu_benchmark({"start": 0, "end": N})
    assert abs(direct["sum"] - combined["sum"]) < 1e-6, "distributed sum doesn't match direct computation"
    print("[OK] distributed sum matches single-device computation exactly")

    # Also confirm an unregistered task type is refused end-to-end.
    from tasks.base_task import run_task
    try:
        run_task("shell_exec", {"cmd": "rm -rf /"})
        print("[FAIL] unregistered task type was NOT refused!")
        sys.exit(1)
    except ValueError:
        print("[OK] unregistered task type correctly refused")

    client.stop()
    server.stop()
    print("\nALL END-TO-END CHECKS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
