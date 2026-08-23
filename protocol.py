"""
Communication protocol for the distributed compute system.

Messages are JSON objects, one per line (newline-delimited), sent over
a TCP stream. Every message has:

    type      - str, one of MessageType.*
    msg_id    - str, unique id (uuid4 hex) set by the sender
    ts        - float, unix timestamp when the message was created
    version   - str, protocol version of the sender
    payload   - dict, message-specific data

See docs/PROTOCOL.md for the full message reference and sequence
diagrams.
"""
import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


class MessageType:
    # Handshake / lifecycle
    REGISTER = "REGISTER"                 # worker -> master: first-time join request
    RECONNECT = "RECONNECT"               # worker -> master: resume a previously authorized session
    AUTHORIZED = "AUTHORIZED"             # master -> worker: registration/reconnect accepted
    REJECTED = "REJECTED"                 # master -> worker: registration/reconnect refused
    VERSION_CHECK = "VERSION_CHECK"       # either direction: explicit protocol version probe

    # Liveness
    HEARTBEAT = "HEARTBEAT"               # worker -> master
    HEARTBEAT_ACK = "HEARTBEAT_ACK"       # master -> worker
    STATUS_UPDATE = "STATUS_UPDATE"       # worker -> master: sysinfo / utilization refresh

    # Task lifecycle
    TASK_ASSIGN = "TASK_ASSIGN"           # master -> worker
    TASK_ACK = "TASK_ACK"                 # worker -> master: chunk received, starting work
    TASK_COMPLETE = "TASK_COMPLETE"       # worker -> master: result attached
    TASK_FAILED = "TASK_FAILED"           # worker -> master: execution error
    TASK_TIMEOUT = "TASK_TIMEOUT"         # master -> worker: informational, chunk was reassigned
    TASK_CANCEL = "TASK_CANCEL"           # master -> worker: abort in-flight / pending work

    # Connection control
    PAUSE = "PAUSE"                       # master -> worker: stop pulling new chunks
    RESUME = "RESUME"                     # master -> worker: resume pulling chunks
    DISCONNECT = "DISCONNECT"             # either direction: graceful goodbye

    ERROR = "ERROR"                       # either direction: protocol-level error


@dataclass
class Message:
    type: str
    payload: Dict[str, Any] = field(default_factory=dict)
    msg_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    ts: float = field(default_factory=time.time)
    version: str = "1.0"

    def to_line(self) -> bytes:
        data = {
            "type": self.type,
            "msg_id": self.msg_id,
            "ts": self.ts,
            "version": self.version,
            "payload": self.payload,
        }
        return (json.dumps(data, separators=(",", ":")) + "\n").encode("utf-8")

    @staticmethod
    def from_line(line: bytes) -> "Message":
        data = json.loads(line.decode("utf-8"))
        return Message(
            type=data["type"],
            payload=data.get("payload", {}),
            msg_id=data.get("msg_id", uuid.uuid4().hex),
            ts=data.get("ts", time.time()),
            version=data.get("version", "1.0"),
        )


class ProtocolError(Exception):
    """Raised when a peer sends something that isn't a well-formed Message."""


async def read_message(reader) -> Optional[Message]:
    """Read one newline-delimited message from an asyncio StreamReader.
    Returns None on clean EOF (peer closed the connection)."""
    line = await reader.readline()
    if not line:
        return None
    try:
        return Message.from_line(line)
    except Exception as exc:  # malformed JSON / missing fields
        raise ProtocolError(f"Malformed message: {exc}")


async def write_message(writer, message: Message) -> None:
    writer.write(message.to_line())
    await writer.drain()
