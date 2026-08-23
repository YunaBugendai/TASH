"""
Splits a workload into chunks, hands them out to available Workers on
request, tracks progress, and reassigns chunks that fail or time out.

Scheduling policy: idle Workers pull the next unassigned chunk whenever
they become free (a classic work-queue / task-stealing design). This
gives dynamic load balancing "for free" -- a fast Worker naturally
finishes its chunk and grabs another sooner than a slow one, without
the Master needing to pre-guess throughput. The Master does keep a
rolling average of each Worker's items/second, but only uses it for
the ETA display and the benchmark screen, not to hand-tune assignment.
"""
import time
import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Dict, List, Optional


class ChunkStatus(str, Enum):
    PENDING = "pending"
    ASSIGNED = "assigned"
    DONE = "done"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class Chunk:
    chunk_id: int
    task_type: str
    params: dict
    status: ChunkStatus = ChunkStatus.PENDING
    worker_id: Optional[str] = None
    assigned_at: Optional[float] = None
    attempts: int = 0
    result: Optional[dict] = None


@dataclass
class JobStats:
    job_id: str
    task_type: str
    total_chunks: int
    started_at: float = field(default_factory=time.time)


class TaskManager:
    """
    One TaskManager instance runs one job at a time. Thread-safe: the
    asyncio networking thread and the GUI thread both touch this
    through the same re-entrant lock.
    """

    MAX_ATTEMPTS = 3

    def __init__(self, task_timeout_sec: int = 30):
        self._lock = threading.RLock()
        self.task_timeout_sec = task_timeout_sec
        self.job: Optional[JobStats] = None
        self.chunks: List[Chunk] = []
        self.on_progress: Optional[Callable[[], None]] = None
        self.worker_throughput: Dict[str, float] = {}  # worker_id -> rolling avg items/sec
        self._cancelled = False
        self.notified_finished = False

    # ------------------------------------------------------------- job control

    def start_job(self, job_id: str, task_type: str, total_items: int, chunk_size: int):
        with self._lock:
            self._cancelled = False
            self.notified_finished = False
            chunks = []
            cid = 0
            for start in range(0, total_items, chunk_size):
                end = min(start + chunk_size, total_items)
                chunks.append(Chunk(chunk_id=cid, task_type=task_type,
                                     params={"start": start, "end": end}))
                cid += 1
            self.chunks = chunks
            self.job = JobStats(job_id=job_id, task_type=task_type, total_chunks=len(chunks))

    def cancel(self):
        """Stop handing out new chunks and mark everything still in flight
        or waiting as cancelled, so progress/ETA reflect a stopped job
        instead of hanging forever."""
        with self._lock:
            self._cancelled = True
            for c in self.chunks:
                if c.status in (ChunkStatus.PENDING, ChunkStatus.ASSIGNED):
                    c.status = ChunkStatus.CANCELLED
                    c.worker_id = None
        self._notify()

    def is_cancelled(self) -> bool:
        with self._lock:
            return self._cancelled

    # --------------------------------------------------------- chunk lifecycle

    def next_chunk_for(self, worker_id: str) -> Optional[Chunk]:
        with self._lock:
            if self._cancelled:
                return None
            for c in self.chunks:
                if c.status == ChunkStatus.PENDING:
                    c.status = ChunkStatus.ASSIGNED
                    c.worker_id = worker_id
                    c.assigned_at = time.time()
                    c.attempts += 1
                    return c
        return None

    def mark_done(self, chunk_id: int, worker_id: str, result: dict):
        with self._lock:
            c = self._find(chunk_id)
            if c is None or c.worker_id != worker_id:
                return  # stale/duplicate completion from a reassigned or timed-out worker
            elapsed = max(1e-6, time.time() - (c.assigned_at or time.time()))
            items = result.get("count", 0)
            rate = items / elapsed
            prev = self.worker_throughput.get(worker_id, rate)
            self.worker_throughput[worker_id] = 0.6 * prev + 0.4 * rate
            c.status = ChunkStatus.DONE
            c.result = result
        self._notify()

    def mark_failed(self, chunk_id: int, worker_id: str, requeue: bool = True):
        with self._lock:
            c = self._find(chunk_id)
            if c is None:
                return
            if requeue and c.attempts < self.MAX_ATTEMPTS:
                c.status = ChunkStatus.PENDING
            else:
                c.status = ChunkStatus.FAILED
            c.worker_id = None
            c.assigned_at = None
        self._notify()

    def release_worker_chunks(self, worker_id: str):
        """Requeue any in-flight chunk belonging to a worker that disconnected."""
        changed = False
        with self._lock:
            for c in self.chunks:
                if c.worker_id == worker_id and c.status == ChunkStatus.ASSIGNED:
                    self._requeue_or_fail_locked(c)
                    changed = True
        if changed:
            self._notify()

    def check_timeouts(self) -> List[int]:
        """Call periodically; requeues stale chunks. Returns the chunk_ids
        that timed out this round."""
        timed_out = []
        now = time.time()
        with self._lock:
            for c in self.chunks:
                if c.status == ChunkStatus.ASSIGNED and c.assigned_at and \
                        (now - c.assigned_at) > self.task_timeout_sec:
                    timed_out.append(c.chunk_id)
                    self._requeue_or_fail_locked(c)
        if timed_out:
            self._notify()
        return timed_out

    def _requeue_or_fail_locked(self, c: Chunk):
        c.status = ChunkStatus.PENDING if c.attempts < self.MAX_ATTEMPTS else ChunkStatus.FAILED
        c.worker_id = None
        c.assigned_at = None

    def _find(self, chunk_id: int) -> Optional[Chunk]:
        for c in self.chunks:
            if c.chunk_id == chunk_id:
                return c
        return None

    def _notify(self):
        if self.on_progress:
            try:
                self.on_progress()
            except Exception:
                pass

    # ---------------------------------------------------------- progress / ETA

    def progress_snapshot(self) -> dict:
        with self._lock:
            if not self.job:
                return {"total": 0, "done": 0, "failed": 0, "cancelled": 0,
                        "pending": 0, "assigned": 0}
            total = len(self.chunks)
            done = sum(1 for c in self.chunks if c.status == ChunkStatus.DONE)
            failed = sum(1 for c in self.chunks if c.status == ChunkStatus.FAILED)
            cancelled = sum(1 for c in self.chunks if c.status == ChunkStatus.CANCELLED)
            pending = sum(1 for c in self.chunks if c.status == ChunkStatus.PENDING)
            assigned = sum(1 for c in self.chunks if c.status == ChunkStatus.ASSIGNED)
            return {"total": total, "done": done, "failed": failed, "cancelled": cancelled,
                    "pending": pending, "assigned": assigned}

    def per_worker_progress(self) -> Dict[str, int]:
        with self._lock:
            out: Dict[str, int] = {}
            for c in self.chunks:
                if c.status == ChunkStatus.DONE and c.worker_id:
                    out[c.worker_id] = out.get(c.worker_id, 0) + 1
            return out

    def is_job_finished(self) -> bool:
        with self._lock:
            if not self.chunks:
                return False
            return all(c.status in (ChunkStatus.DONE, ChunkStatus.FAILED, ChunkStatus.CANCELLED)
                       for c in self.chunks)

    def should_notify_finished(self) -> bool:
        """Returns True exactly once per job, the first time every chunk
        has reached a terminal state -- so the GUI shows one 'job
        finished' notice instead of one per worker that happens to go
        idle around the same time."""
        with self._lock:
            if self.notified_finished:
                return False
            if self.is_job_finished():
                self.notified_finished = True
                return True
            return False

    def combine_results(self) -> dict:
        """Task-specific combination step for the built-in cpu_benchmark task."""
        with self._lock:
            total_sum = 0.0
            total_count = 0
            for c in self.chunks:
                if c.status == ChunkStatus.DONE and c.result:
                    total_sum += c.result.get("sum", 0.0)
                    total_count += c.result.get("count", 0)
            return {"sum": total_sum, "count": total_count}

    def eta_seconds(self) -> Optional[float]:
        with self._lock:
            if not self.job or not self.chunks:
                return None
            remaining = sum(1 for c in self.chunks
                            if c.status in (ChunkStatus.PENDING, ChunkStatus.ASSIGNED))
            if remaining == 0:
                return 0.0
            avg_rate = sum(self.worker_throughput.values())
            if avg_rate <= 0:
                return None
            chunk_items = self.chunks[0].params["end"] - self.chunks[0].params["start"]
            return (remaining * chunk_items) / avg_rate
