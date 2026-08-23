from tasks.cpu_benchmark_task import cpu_benchmark, sample_digest


def test_cpu_benchmark_basic():
    result = cpu_benchmark({"start": 0, "end": 1000})
    assert result["count"] == 1000
    assert result["start"] == 0
    assert result["end"] == 1000
    assert result["digest"] == sample_digest(0, 1000)


def test_cpu_benchmark_deterministic():
    r1 = cpu_benchmark({"start": 10, "end": 20})
    r2 = cpu_benchmark({"start": 10, "end": 20})
    assert r1["sum"] == r2["sum"]
    assert r1["digest"] == r2["digest"]


def test_digest_detects_wrong_range():
    assert sample_digest(0, 1000) != sample_digest(0, 999)
    assert sample_digest(0, 1000) != sample_digest(1, 1000)


def test_empty_range():
    result = cpu_benchmark({"start": 5, "end": 5})
    assert result["count"] == 0
    assert result["sum"] == 0.0


def test_cpu_limit_sleep_does_not_change_result():
    fast = cpu_benchmark({"start": 0, "end": 5000, "cpu_limit_sleep": 0.0})
    throttled = cpu_benchmark({"start": 0, "end": 5000, "cpu_limit_sleep": 0.001})
    assert fast["sum"] == throttled["sum"]
    assert fast["digest"] == throttled["digest"]
