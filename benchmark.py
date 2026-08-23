"""
Benchmark comparison: Master-only vs Master+1 Worker vs Master+all
connected Workers, run back-to-back on the same workload so the
numbers are comparable.

Runs here are blocking (they poll until each scenario's job finishes).
Call this from a background thread, never from the Tkinter main
thread, or the GUI will freeze for the whole duration of the
benchmark. master/gui.py does this via threading.Thread.
"""
import time
from typing import List, Tuple

from master.master_core import WorkerStatus
from master.task_manager import TaskManager
from shared import system_info
from tasks.cpu_benchmark_task import cpu_benchmark


class BenchmarkRunner:
    def __init__(self, server, shared_task_manager: TaskManager):
        self.server = server
        self.shared_task_manager = shared_task_manager

    # ------------------------------------------------------------- scenarios

    def _run_master_only(self, n: int) -> dict:
        t0 = time.time()
        result = cpu_benchmark({"start": 0, "end": n})
        elapsed = time.time() - t0
        return {
            "total_s": elapsed, "compute_s": elapsed, "network_s": 0.0,
            "throughput": n / elapsed if elapsed > 0 else 0.0,
            "master_util": system_info.get_cpu_info().get("usage_percent"),
            "worker_util": None,
        }

    def _avg_worker_util(self, worker_ids: List[str]):
        vals = []
        for wid in worker_ids:
            conn = self.server.workers.get(wid)
            if conn:
                v = conn.sysinfo.get("cpu", {}).get("usage_percent")
                if v is not None:
                    vals.append(v)
        return sum(vals) / len(vals) if vals else None

    def _run_distributed(self, n: int, chunk_size: int, worker_ids: List[str]) -> dict:
        # Use a scratch TaskManager for the duration of this measurement so
        # it never collides with a job the operator has queued in the Job tab.
        tm = TaskManager(task_timeout_sec=self.shared_task_manager.task_timeout_sec)
        original_tm = self.server.task_manager
        self.server.task_manager = tm

        paused_others = []
        for wid, conn in self.server.workers.items():
            if conn.status == WorkerStatus.AUTHORIZED and wid not in worker_ids:
                self.server.set_worker_paused(wid, True)
                paused_others.append(wid)

        t0 = time.time()
        tm.start_job(f"bench-{int(t0)}", "cpu_benchmark", n, chunk_size)
        self.server.dispatch_job()
        while not tm.is_job_finished():
            time.sleep(0.1)
        elapsed = time.time() - t0

        combined = tm.combine_results()
        done_chunks = [c for c in tm.chunks if c.result]
        avg_compute = (sum(c.result.get("compute_seconds", 0) for c in done_chunks) / len(done_chunks)
                       if done_chunks else 0.0)
        network_s = max(0.0, elapsed - avg_compute)
        master_util = system_info.get_cpu_info().get("usage_percent")
        worker_util = self._avg_worker_util(worker_ids)

        for wid in paused_others:
            self.server.set_worker_paused(wid, False)
        self.server.task_manager = original_tm

        return {
            "total_s": elapsed, "compute_s": avg_compute, "network_s": network_s,
            "throughput": combined.get("count", 0) / elapsed if elapsed > 0 else 0.0,
            "master_util": master_util, "worker_util": worker_util,
        }

    # ---------------------------------------------------------------- report

    @staticmethod
    def _fmt_pct(v):
        return f"{v:.0f}%" if v is not None else "n/a"

    def run_all(self, n: int, chunk_size: int) -> Tuple[list, str]:
        rows = []
        master_only = self._run_master_only(n)
        rows.append((
            "Master only", f"{master_only['total_s']:.2f}", f"{master_only['compute_s']:.2f}",
            f"{master_only['network_s']:.2f}", f"{master_only['throughput']:.0f}/s",
            self._fmt_pct(master_only["master_util"]), self._fmt_pct(master_only["worker_util"]),
            "1.00x", "100%",
        ))

        authorized = [wid for wid, c in self.server.workers.items()
                      if c.status == WorkerStatus.AUTHORIZED]
        if not authorized:
            return rows, ("No authorized Workers connected -- only the Master-only "
                          "row could be measured. Connect a Worker to see the rest.")

        note = ""
        one = self._run_distributed(n, chunk_size, authorized[:1])
        speedup1 = master_only["total_s"] / one["total_s"] if one["total_s"] > 0 else 0
        rows.append((
            "Master + 1 Worker", f"{one['total_s']:.2f}", f"{one['compute_s']:.2f}",
            f"{one['network_s']:.2f}", f"{one['throughput']:.0f}/s",
            self._fmt_pct(one["master_util"]), self._fmt_pct(one["worker_util"]),
            f"{speedup1:.2f}x", f"{100 * speedup1:.0f}%",
        ))
        if one["total_s"] > master_only["total_s"]:
            note = ("Distributing to 1 Worker was SLOWER than Master-only for this "
                    "workload -- network/coordination overhead outweighed the extra "
                    "compute power. Try a larger workload (N) or a bigger chunk size.")

        if len(authorized) > 1:
            allw = self._run_distributed(n, chunk_size, authorized)
            speedup_all = master_only["total_s"] / allw["total_s"] if allw["total_s"] > 0 else 0
            efficiency = 100 * speedup_all / len(authorized)
            rows.append((
                f"Master + {len(authorized)} Workers", f"{allw['total_s']:.2f}",
                f"{allw['compute_s']:.2f}", f"{allw['network_s']:.2f}",
                f"{allw['throughput']:.0f}/s",
                self._fmt_pct(allw["master_util"]), self._fmt_pct(allw["worker_util"]),
                f"{speedup_all:.2f}x", f"{efficiency:.0f}%",
            ))
            if allw["total_s"] > master_only["total_s"]:
                note = ("Distributing made this workload SLOWER than Master-only -- "
                        "network/coordination overhead outweighed the extra compute "
                        "power for this N and chunk size. Try a larger N or a bigger "
                        "chunk size so each chunk does more work per round trip.")

        return rows, note
