"""
Built-in CPU benchmark task: f(x) for x in [start, end).

f(x) is a deterministic, side-effect-free floating point calculation
chosen so that:

  - it is embarrassingly parallel (every x is independent), which is
    what makes it splittable across Workers at all;
  - it is expensive enough per element to be a meaningful CPU
    benchmark instead of finishing before the network round trip does;
  - identical inputs always produce identical outputs, which lets the
    Master cheaply verify a Worker's result (see `sample_digest`)
    without re-running the whole chunk itself.
"""
import hashlib
import math
import time

from tasks.base_task import register_task


def f(x: int) -> float:
    v = math.sin(x) * math.cos(x / 3.0 + 1.0)
    v += math.sqrt(abs(x) + 1.0)
    v += math.log(abs(x) + 2.0)
    return v


def sample_digest(start: int, end: int, sample_size: int = 8) -> str:
    """Deterministic checksum over a handful of evenly spaced points in
    [start, end). The Master recomputes this independently to spot-check
    that a Worker actually computed the assigned range instead of
    forging a result -- cheap because it only touches `sample_size`
    points, not the whole chunk."""
    if end <= start:
        return hashlib.sha256(b"").hexdigest()
    step = max(1, (end - start) // sample_size)
    h = hashlib.sha256()
    for x in range(start, end, step):
        h.update(f"{x}:{f(x):.10f}".encode("utf-8"))
    return h.hexdigest()


@register_task("cpu_benchmark")
def cpu_benchmark(params: dict) -> dict:
    start = int(params["start"])
    end = int(params["end"])
    cpu_limit_sleep = float(params.get("cpu_limit_sleep", 0.0))

    t0 = time.perf_counter()
    total = 0.0
    count = 0
    yield_every = 2000  # how often to honor the CPU throttle, if any
    for x in range(start, end):
        total += f(x)
        count += 1
        if cpu_limit_sleep > 0 and count % yield_every == 0:
            time.sleep(cpu_limit_sleep)
    elapsed = time.perf_counter() - t0

    return {
        "start": start,
        "end": end,
        "count": count,
        "sum": total,
        "digest": sample_digest(start, end),
        "compute_seconds": elapsed,
    }
