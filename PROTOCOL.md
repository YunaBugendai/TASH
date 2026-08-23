# Communication Protocol

Version: `1.0` (see `shared/constants.py::PROTOCOL_VERSION`)

## Transport

- **Control channel:** one TCP connection per Worker, opened by the Worker
  to the Master. Messages are JSON objects, one per line
  (newline-delimited), see `shared/protocol.py`.
- **Discovery:** a UDP broadcast on port `8766` (configurable). A Worker
  broadcasts `{"magic": "DISTCOMP_DISCOVER_V1"}`; any Master on the LAN
  replies directly to the sender with
  `{"magic": "DISTCOMP_MASTER_V1", "host": ..., "port": ...}`.
  This never grants access by itself -- it only tells a Worker where a
  Master is; the Worker still needs the pairing code to register.

## Message envelope

Every message is one line of JSON:

```json
{"type": "HEARTBEAT", "msg_id": "a1b2c3...", "ts": 1719000000.123, "version": "1.0", "payload": {}}
```

| Field     | Meaning                                             |
|-----------|------------------------------------------------------|
| `type`    | One of the message types below                       |
| `msg_id`  | Random hex id, set by the sender                      |
| `ts`      | Unix timestamp when the sender created the message    |
| `version` | Protocol version of the sender                        |
| `payload` | Message-specific data (see below)                     |

## Message types

### Handshake / lifecycle

| Type          | Direction        | Payload                                                                 | Purpose |
|---------------|------------------|--------------------------------------------------------------------------|---------|
| `REGISTER`    | worker -> master | `version, worker_id, name, pairing_code, sysinfo`                        | First-time join request. |
| `RECONNECT`   | worker -> master | `version, worker_id, name, token, sysinfo`                               | Resume a previously-authorized session after a dropped connection, without asking the operator to approve again. |
| `AUTHORIZED`  | master -> worker | `token, worker_id, heartbeat_timeout`                                    | Registration/reconnect accepted. `token` must be echoed back on the next `RECONNECT`. |
| `REJECTED`    | master -> worker | `reason`                                                                 | Registration/reconnect refused (bad pairing code, version mismatch, operator declined, or stale reconnect credentials). |

`REGISTER` always requires: (1) a pairing code that matches the one shown
on the Master's screen, and (2) the Master operator clicking "Authorize"
in a popup naming the Worker's address. `RECONNECT` skips the popup **only**
if `worker_id` was previously authorized in this Master run **and** the
supplied `token` matches the one that Master issued it -- a stranger who
doesn't have that token cannot use `RECONNECT` to get in.

### Liveness

| Type             | Direction        | Payload                | Purpose |
|------------------|------------------|--------------------------|---------|
| `HEARTBEAT`      | worker -> master | `client_ts`              | Sent every 5s. Master uses the round trip to estimate latency and to detect a dead connection (15s timeout). |
| `HEARTBEAT_ACK`  | master -> worker | `{}`                     | Acknowledges a heartbeat. |
| `STATUS_UPDATE`  | worker -> master | `sysinfo` (cpu/ram/gpu)  | Sent alongside each heartbeat so the dashboard stays current. |

### Task lifecycle

| Type             | Direction        | Payload                                | Purpose |
|------------------|------------------|-------------------------------------------|---------|
| `TASK_ASSIGN`    | master -> worker | `chunk_id, task_type, params`              | Assigns one chunk of work. |
| `TASK_ACK`       | worker -> master | `chunk_id`                                 | Confirms the chunk was received and execution is starting. |
| `TASK_COMPLETE`  | worker -> master | `chunk_id, result`                         | Chunk finished; `result` is task-specific (for `cpu_benchmark`: `sum, count, digest, compute_seconds`). |
| `TASK_FAILED`    | worker -> master | `chunk_id, error`                          | Execution raised an exception, or the worker was paused/asked to skip. |
| `TASK_TIMEOUT`   | master -> worker | `chunk_id`                                 | Informational: the Master gave up waiting and reassigned this chunk elsewhere. |
| `TASK_CANCEL`    | master -> worker | `{}`                                       | "Stop all computation" -- worker should discard in-flight work for the current job. |

### Connection control

| Type          | Direction                | Payload | Purpose |
|---------------|---------------------------|---------|---------|
| `PAUSE`       | master -> worker           | `{}`    | Worker stops accepting new chunks (finishes the current one). |
| `RESUME`      | master -> worker           | `{}`    | Worker resumes accepting chunks. |
| `DISCONNECT`  | either direction           | `{}`    | Graceful goodbye, sent before closing the socket on purpose. |
| `ERROR`       | either direction           | `reason`| Protocol-level error unrelated to a specific task. |

## Result verification

For the built-in `cpu_benchmark` task, the Master never trusts a Worker's
`sum` blindly. `tasks/cpu_benchmark_task.py::sample_digest(start, end)` is a
cheap, deterministic checksum over a handful of points in the chunk's
range. The Worker includes its own `digest` in `TASK_COMPLETE`; the Master
recomputes the expected digest locally (this takes microseconds, unlike
redoing the whole chunk) and rejects the result if they don't match. A
mismatched chunk is requeued exactly like a failure.

## Sequence: normal job

```
Worker                          Master
  | --- REGISTER -------------> |
  |                              |  (pairing code checked, operator approves)
  | <--- AUTHORIZED ------------ |
  | --- HEARTBEAT (every 5s) --> |
  | <--- TASK_ASSIGN ----------- |
  | --- TASK_ACK --------------> |
  |   (computes chunk)           |
  | --- TASK_COMPLETE ---------> |  (digest verified)
  | <--- TASK_ASSIGN (next) ---- |
  |            ...               |
  | --- DISCONNECT ------------> |
```

## Sequence: dropped connection and reconnect

```
Worker                          Master
  |  (network drops mid-task)    |
  |                              |  heartbeat timeout after 15s -> chunk requeued
  |  (worker retries with        |
  |   exponential backoff:       |
  |   2s, 4s, 8s ... capped 30s) |
  | --- RECONNECT (with token) -> |
  | <--- AUTHORIZED ------------ |  (no operator prompt -- token proves identity)
  | <--- TASK_ASSIGN ----------- |
```

## Version compatibility

Every `REGISTER`/`RECONNECT` includes `version`. If it doesn't match the
Master's `PROTOCOL_VERSION`, the Master replies `REJECTED` with
`reason: "protocol version mismatch"` and its own version, and closes the
connection -- it never tries to guess compatibility.
