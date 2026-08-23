import time

from master.task_manager import TaskManager, ChunkStatus


def test_chunk_splitting():
    tm = TaskManager()
    tm.start_job("job-1", "cpu_benchmark", total_items=25, chunk_size=10)
    assert len(tm.chunks) == 3
    assert tm.chunks[0].params == {"start": 0, "end": 10}
    assert tm.chunks[1].params == {"start": 10, "end": 20}
    assert tm.chunks[-1].params == {"start": 20, "end": 25}


def test_assignment_and_completion():
    tm = TaskManager()
    tm.start_job("job-1", "cpu_benchmark", total_items=10, chunk_size=10)
    chunk = tm.next_chunk_for("worker-a")
    assert chunk is not None
    assert chunk.status == ChunkStatus.ASSIGNED
    assert tm.next_chunk_for("worker-b") is None  # no more chunks to hand out

    tm.mark_done(chunk.chunk_id, "worker-a", {"sum": 1.0, "count": 10})
    assert tm.is_job_finished()
    combined = tm.combine_results()
    assert combined["count"] == 10


def test_worker_disconnect_requeues_chunk():
    tm = TaskManager()
    tm.start_job("job-1", "cpu_benchmark", total_items=10, chunk_size=10)
    chunk = tm.next_chunk_for("worker-a")
    tm.release_worker_chunks("worker-a")
    assert chunk.status == ChunkStatus.PENDING
    assert chunk.worker_id is None


def test_timeout_requeues_chunk():
    tm = TaskManager(task_timeout_sec=0)
    tm.start_job("job-1", "cpu_benchmark", total_items=10, chunk_size=10)
    chunk = tm.next_chunk_for("worker-a")
    time.sleep(0.05)
    timed_out = tm.check_timeouts()
    assert chunk.chunk_id in timed_out
    assert chunk.status == ChunkStatus.PENDING


def test_max_attempts_marks_failed():
    tm = TaskManager(task_timeout_sec=0)
    tm.start_job("job-1", "cpu_benchmark", total_items=10, chunk_size=10)
    chunk = None
    for _ in range(TaskManager.MAX_ATTEMPTS):
        chunk = tm.next_chunk_for("worker-a")
        tm.mark_failed(chunk.chunk_id, "worker-a")
    assert chunk.status == ChunkStatus.FAILED


def test_cancel_marks_remaining_chunks_cancelled():
    tm = TaskManager()
    tm.start_job("job-1", "cpu_benchmark", total_items=30, chunk_size=10)
    tm.next_chunk_for("worker-a")  # one chunk assigned, two still pending
    tm.cancel()
    statuses = {c.status for c in tm.chunks}
    assert ChunkStatus.PENDING not in statuses
    assert ChunkStatus.ASSIGNED not in statuses
    assert tm.next_chunk_for("worker-b") is None  # no new chunks after cancel


def test_should_notify_finished_fires_once():
    tm = TaskManager()
    tm.start_job("job-1", "cpu_benchmark", total_items=10, chunk_size=10)
    chunk = tm.next_chunk_for("worker-a")
    tm.mark_done(chunk.chunk_id, "worker-a", {"sum": 1.0, "count": 10})
    assert tm.should_notify_finished() is True
    assert tm.should_notify_finished() is False  # already notified once
