"""
Task registry.

Only task types registered here can ever be executed by a Worker. The
Master sends a `task_type` string and a small JSON-serializable
`params` dict; a Worker looks the type up in TASK_REGISTRY and calls
the matching function. There is no code path anywhere in this project
that evals, execs, imports, or shells out based on a string coming
from the network. If `task_type` is not in this registry, the chunk is
rejected with TASK_FAILED and nothing runs.

To add a new distributable computation later, write a function that
takes a params dict and returns a JSON-serializable result dict,
decorate it with @register_task("your_task_name"), and import that
module once (e.g. from worker/worker_core.py) so the decorator runs.
Nothing about the networking layer needs to change.
"""
from typing import Any, Callable, Dict

TASK_REGISTRY: Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]] = {}


def register_task(name: str):
    def deco(fn):
        TASK_REGISTRY[name] = fn
        return fn
    return deco


def run_task(task_type: str, params: Dict[str, Any]) -> Dict[str, Any]:
    if task_type not in TASK_REGISTRY:
        raise ValueError(f"Unknown/unregistered task type: {task_type!r}")
    return TASK_REGISTRY[task_type](params)
