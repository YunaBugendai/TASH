"""Master-side configuration, with simple JSON persistence in the user's home dir."""
import json
import os
from dataclasses import dataclass, asdict

from shared.constants import (
    DEFAULT_TCP_PORT, DEFAULT_UDP_DISCOVERY_PORT, TASK_DEFAULT_TIMEOUT_SEC,
)

CONFIG_PATH = os.path.join(os.path.expanduser("~"), ".distcomp_master.json")


@dataclass
class MasterConfig:
    host: str = "0.0.0.0"
    port: int = DEFAULT_TCP_PORT
    discovery_port: int = DEFAULT_UDP_DISCOVERY_PORT
    task_timeout_sec: int = TASK_DEFAULT_TIMEOUT_SEC
    chunk_size: int = 5000

    def save(self):
        try:
            with open(CONFIG_PATH, "w") as f:
                json.dump(asdict(self), f, indent=2)
        except OSError:
            pass  # persistence is a convenience, never fatal

    @classmethod
    def load(cls) -> "MasterConfig":
        try:
            with open(CONFIG_PATH) as f:
                data = json.load(f)
            return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})
        except Exception:
            return cls()
