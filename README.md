# Distributed Compute (Master / Worker)

A consent-based local distributed computing app for devices you own or
have explicitly authorized. One **Master** splits a workload into
independent chunks and hands them to one or more **Worker** apps on the
same local network; Workers only ever run a small set of predefined,
built-in computations -- never arbitrary code sent over the network.

Built-in workload: a CPU benchmark, `f(x)` for `x = 1..N`, split into
chunks and distributed. See `docs/PROTOCOL.md` for the wire protocol and
`docs/TROUBLESHOOTING.md` if something doesn't connect.

## What this is (and isn't)

- Every Worker must be started by hand on a machine you control, and its
  operator must explicitly authorize the Master before anything runs.
- The Master requires a pairing code (shown on its own screen) plus a
  manual "Authorize this Worker?" click before a Worker joins.
- A Worker only ever executes functions already registered in
  `tasks/base_task.py` -- there is no code path that evals, execs, or
  shells out based on data received from the network.
- This is LAN tooling, not a hardened, internet-facing service. Don't
  expose the Master's ports to the public internet.
- **Android:** true Android support isn't practical with this project's
  stack (Tkinter has no Android runtime). Building a native Android
  Worker would require a separate app (e.g. Kotlin, or a Python-on-Android
  toolchain like Kivy/Chaquopy) and was out of scope here -- documented
  rather than faked. The protocol in `docs/PROTOCOL.md` is designed so a
  native Android Worker could speak it without touching the Master or
  the Windows/Linux Worker at all.

## Requirements

- Python 3.9+
- Tkinter (usually bundled with Python; see Troubleshooting if it's missing)
- Windows or Linux, for both Master and Worker

## Installation

```bash
git clone <this-repo-url> distributed-compute
cd distributed-compute
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

Optional, for NVIDIA GPU stats in the dashboard:

```bash
pip install GPUtil
```

## Running the Master

```bash
python -m master.master_app
# or: scripts/run_master.sh   (Linux/macOS)
# or: scripts\run_master.bat  (Windows)
```

On launch, the Master:
1. Starts listening for Workers on TCP port `8765` and LAN discovery on
   UDP port `8766` (both configurable via `~/.distcomp_master.json` after
   the first run).
2. Generates and displays a **pairing code** at the top of the Dashboard
   tab -- give this to whoever is starting a Worker.

To run a job:
1. Go to the **Job** tab, set workload size `N` and chunk size.
2. Click **Run on Master only** for a local baseline, or **Distribute to
   Workers** once at least one Worker shows "authorized" on the Dashboard.
3. Watch overall and per-worker progress; **Stop all computation** cancels
   everything in flight.
4. Use the **Benchmark** tab to compare Master-only vs +1 Worker vs +all
   Workers on the same workload, including a warning if distributing
   turned out slower than running locally.

## Running a Worker

```bash
python -m worker.worker_app
# or: scripts/run_worker.sh   (Linux/macOS)
# or: scripts\run_worker.bat  (Windows)
```

1. Enter the Master's IP and port (or click **Find Master on LAN**).
2. Enter the pairing code the Master operator gave you.
3. Click **Connect** -- a popup will ask you to explicitly authorize this
   Master. Nothing runs until you click yes.
4. Optionally lower the **CPU usage limit** slider before work arrives.
5. **Pause** or **Disconnect** at any time; the Worker automatically
   reconnects (with backoff) after a temporary network drop, without
   needing you to re-authorize.

## Running the tests

```bash
pip install -r requirements-dev.txt
pytest
```

Tests cover the wire protocol, pairing/token auth, chunk splitting and
reassignment (including timeouts, disconnects, and cancellation), the
CPU benchmark task's determinism and result-verification digest, the
task registry's refusal to run unregistered task types, and the
never-raises contract of the hardware-info helpers.

There's also `e2e_check.py` at the repo root -- a manual, non-pytest
smoke test that spins up a real Master and a real Worker over actual
TCP sockets on localhost, distributes a real job, and checks the
distributed result exactly matches a single-device computation:

```bash
python3 e2e_check.py
```

## Adding a new distributable task

1. Write a function `def my_task(params: dict) -> dict` in a new module
   under `tasks/`.
2. Decorate it with `@register_task("my_task_name")`.
3. Import that module once from `worker/worker_core.py` (next to the
   existing `import tasks.cpu_benchmark_task`) so the decorator runs.
4. Have the Master's job-creation code build chunks with
   `task_type="my_task_name"` and whatever `params` your function needs.

Nothing about `shared/protocol.py`, `master/master_core.py`, or
`worker/worker_core.py` needs to change -- the networking layer is
generic over task type.

## Project layout

```
distributed-compute/
├── README.md
├── requirements.txt
├── requirements-dev.txt
├── pytest.ini
├── .gitignore
├── docs/
│   ├── PROTOCOL.md            # full wire-protocol reference + sequence diagrams
│   └── TROUBLESHOOTING.md
├── scripts/
│   ├── run_master.sh / .bat
│   └── run_worker.sh / .bat
├── shared/                    # code used by both Master and Worker
│   ├── constants.py           # ports, timeouts, protocol version
│   ├── protocol.py            # message format + framing
│   ├── crypto.py              # pairing code + session token helpers
│   └── system_info.py         # best-effort CPU/RAM/GPU snapshot
├── tasks/                     # the generic task registry + built-in tasks
│   ├── base_task.py           # TASK_REGISTRY; the only way anything executes
│   └── cpu_benchmark_task.py  # f(x) for x in [start, end), plus verification digest
├── master/
│   ├── config.py              # persisted settings (~/.distcomp_master.json)
│   ├── task_manager.py        # chunk splitting, assignment, timeouts, reassignment
│   ├── master_core.py         # asyncio TCP server + UDP discovery + auth handshake
│   ├── benchmark.py           # Master-only vs +1 Worker vs +N Workers comparison
│   ├── gui.py                 # Tkinter dashboard / job / benchmark / logs / settings tabs
│   └── master_app.py          # entry point: `python -m master.master_app`
├── worker/
│   ├── worker_core.py         # asyncio client: connect, auth, heartbeat, execute, reconnect
│   ├── gui.py                 # Tkinter connection/status/controls/logs UI
│   └── worker_app.py          # entry point: `python -m worker.worker_app`
└── tests/
    ├── test_protocol.py
    ├── test_crypto.py
    ├── test_cpu_benchmark_task.py
    ├── test_task_manager.py
    ├── test_task_registry.py
    └── test_system_info.py
```

## Security summary

- Explicit pairing (numeric code) + explicit operator approval before any
  Worker is trusted.
- Per-worker session tokens for silent reconnect after network blips,
  without weakening the initial explicit-approval requirement (a token
  that doesn't match a previously-approved `worker_id` is rejected).
- Protocol version is checked on every registration; mismatches are
  rejected rather than guessed at.
- Task execution is table-driven (`TASK_REGISTRY`) -- an unrecognized
  `task_type` is refused, and there is no eval/exec/shell path anywhere
  from network input to code execution.
- Workers never receive or run shell commands, binaries, or scripts from
  the Master -- only `task_type` + a small JSON `params` dict.
