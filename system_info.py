"""
Best-effort local hardware / utilization info.

Every lookup here is wrapped so a missing optional dependency or an
unsupported platform degrades to "unknown" values instead of crashing
the Master or Worker app. Nothing here touches the network or executes
anything from outside this process.
"""
from typing import Any, Dict

try:
    import psutil
except ImportError:  # pragma: no cover - exercised only when psutil is missing
    psutil = None


def get_cpu_info() -> Dict[str, Any]:
    try:
        if psutil is None:
            raise RuntimeError("psutil not installed")
        return {
            "logical_cores": psutil.cpu_count(logical=True),
            "physical_cores": psutil.cpu_count(logical=False),
            "usage_percent": psutil.cpu_percent(interval=0.1),
        }
    except Exception:
        return {"logical_cores": None, "physical_cores": None, "usage_percent": None}


def get_ram_info() -> Dict[str, Any]:
    try:
        if psutil is None:
            raise RuntimeError("psutil not installed")
        vm = psutil.virtual_memory()
        return {
            "total_gb": round(vm.total / (1024 ** 3), 2),
            "used_percent": vm.percent,
        }
    except Exception:
        return {"total_gb": None, "used_percent": None}


def get_gpu_info() -> Dict[str, Any]:
    # Optional dependency: pip install GPUtil (NVIDIA only)
    try:
        import GPUtil  # type: ignore
        gpus = GPUtil.getGPUs()
        if gpus:
            g = gpus[0]
            return {
                "name": g.name,
                "load_percent": round(g.load * 100, 1),
                "mem_used_mb": g.memoryUsed,
                "mem_total_mb": g.memoryTotal,
            }
    except Exception:
        pass
    # Fall back to calling nvidia-smi directly if it's on PATH.
    try:
        import subprocess
        out = subprocess.run(
            ["nvidia-smi",
             "--query-gpu=name,utilization.gpu,memory.used,memory.total",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=2,
        )
        if out.returncode == 0 and out.stdout.strip():
            name, load, mem_used, mem_total = [p.strip() for p in out.stdout.splitlines()[0].split(",")]
            return {"name": name, "load_percent": float(load),
                    "mem_used_mb": float(mem_used), "mem_total_mb": float(mem_total)}
    except Exception:
        pass
    return {"name": "N/A", "load_percent": None, "mem_used_mb": None, "mem_total_mb": None}


def get_full_snapshot() -> Dict[str, Any]:
    return {
        "cpu": get_cpu_info(),
        "ram": get_ram_info(),
        "gpu": get_gpu_info(),
    }
