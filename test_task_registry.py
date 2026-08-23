import pytest

from tasks.base_task import TASK_REGISTRY, register_task, run_task
import tasks.cpu_benchmark_task  # noqa: F401  registers "cpu_benchmark"


def test_cpu_benchmark_is_registered():
    assert "cpu_benchmark" in TASK_REGISTRY


def test_unknown_task_type_is_rejected():
    with pytest.raises(ValueError):
        run_task("rm -rf /", {})


def test_run_task_dispatches_to_registered_function():
    result = run_task("cpu_benchmark", {"start": 0, "end": 100})
    assert result["count"] == 100


def test_register_task_decorator_adds_entry():
    @register_task("_test_only_task")
    def _dummy(params):
        return {"ok": True}

    assert TASK_REGISTRY["_test_only_task"]({}) == {"ok": True}
    del TASK_REGISTRY["_test_only_task"]  # clean up so tests stay isolated
