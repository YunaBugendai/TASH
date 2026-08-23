"""
Master-side networking: accepts Worker connections, runs the
authorization handshake, relays heartbeats/tasks, and answers UDP
discovery broadcasts from Workers on the same LAN.

Runs its own asyncio event loop in a background thread so the Tkinter
GUI's mainloop is never blocked. Every public method here is safe to
call from the GUI thread; they marshal into the event loop with
call_soon_threadsafe / run_coroutine_threadsafe. State changes that the
GUI needs to know about go out through `on_event(name, data)`, which
the GUI turns into a thread-safe queue.Queue -- nothing in this module
ever touches a Tkinter widget directly.
"""
import asyncio
import json
import logging
import socket
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional

from shared import crypto
from shared.constants import DISCOVERY_MAGIC, DISCOVERY_REPLY_MAGIC, HEARTBEAT_TIMEOUT_SEC
from shared.protocol import Message, MessageType, ProtocolError, read_message, write_message
from master.task_manager import TaskManager

logger = logging.getLogger("master.core")


class WorkerStatus:
    PENDING_AUTH = "pending_auth"
    AUTHORIZED = "authorized"
    PAUSED = "paused"
    DISCONNECTED = "disconnected"


@dataclass
class WorkerConn:
    worker_id: str
    address: str
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    token: Optional[str] = None
    status: str = WorkerStatus.PENDING_AUTH
    sysinfo: dict = field(default_factory=dict)
    last_heartbeat: float = field(default_factory=time.time)
    latency_ms: Optional[float] = None
    name: str = ""


class MasterServer:
    def __init__(self, config, task_manager: TaskManager, pairing_code: str):
        self.config = config
        self.task_manager = task_manager
        self.pairing_code = pairing_code
        self.workers: Dict[str, WorkerConn] = {}
        self.on_event: Optional[Callable[[str, dict], None]] = None  # -> GUI

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._server = None
        self._discovery_transport = None

        # Explicit-authorization bookkeeping.
        self._awaiting_approval: Dict[str, "asyncio.Future"] = {}
        self._approved_worker_ids: set = set()
        self._known_tokens: Dict[str, str] = {}
        self._auto_approve = False

    # ---------------------------------------------------------------- lifecycle

    def start(self):
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def _run_loop(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.create_task(self._start_tcp_server())
        self._loop.create_task(self._start_udp_discovery())
        self._loop.create_task(self._timeout_monitor())
        self._loop.create_task(self._heartbeat_monitor())
        self._loop.run_forever()

    def stop(self):
        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread and self._thread.is_alive() and threading.current_thread() is not self._thread:
            self._thread.join(timeout=2)

    async def _start_tcp_server(self):
        self._server = await asyncio.start_server(
            self._handle_client, self.config.host, self.config.port)
        logger.info("Master listening on %s:%s", self.config.host, self.config.port)

    async def _start_udp_discovery(self):
        loop = asyncio.get_event_loop()
        transport, _protocol = await loop.create_datagram_endpoint(
            lambda: _DiscoveryProtocol(self),
            local_addr=("0.0.0.0", self.config.discovery_port),
            allow_broadcast=True,
        )
        self._discovery_transport = transport

    def _emit(self, event: str, data: dict):
        if self.on_event:
            try:
                self.on_event(event, data)
            except Exception:
                logger.exception("GUI event callback raised")

    # ------------------------------------------------------------ TCP handling

    async def _handle_client(self, reader, writer):
        addr = writer.get_extra_info("peername")
        addr_str = f"{addr[0]}:{addr[1]}" if addr else "unknown"
        worker_id = None
        try:
            msg = await read_message(reader)
            if msg is None or msg.type not in (MessageType.REGISTER, MessageType.RECONNECT):
                await write_message(writer, Message(MessageType.REJECTED,
                                    {"reason": "expected REGISTER or RECONNECT"}))
                writer.close()
                return

            payload = msg.payload
            from shared.constants import PROTOCOL_VERSION
            if payload.get("version") != PROTOCOL_VERSION:
                await write_message(writer, Message(MessageType.REJECTED,
                                    {"reason": "protocol version mismatch",
                                     "master_version": PROTOCOL_VERSION}))
                writer.close()
                return

            worker_id = payload.get("worker_id") or addr_str
            name = payload.get("name", worker_id)
            conn = WorkerConn(worker_id=worker_id, address=addr_str, reader=reader,
                              writer=writer, name=name, sysinfo=payload.get("sysinfo", {}))
            self.workers[worker_id] = conn

            is_reconnect = msg.type == MessageType.RECONNECT
            if is_reconnect:
                approved = (worker_id in self._approved_worker_ids and
                           self._known_tokens.get(worker_id) == payload.get("token"))
                if not approved:
                    await write_message(writer, Message(MessageType.REJECTED,
                                        {"reason": "unrecognized reconnect credentials; "
                                                    "please register again"}))
                    writer.close()
                    del self.workers[worker_id]
                    return
            else:
                if payload.get("pairing_code") != self.pairing_code:
                    await write_message(writer, Message(MessageType.REJECTED,
                                        {"reason": "invalid pairing code"}))
                    writer.close()
                    del self.workers[worker_id]
                    return
                approved = await self._await_approval(conn)
                if not approved:
                    await write_message(writer, Message(MessageType.REJECTED,
                                        {"reason": "rejected by master operator"}))
                    writer.close()
                    del self.workers[worker_id]
                    return
                self._approved_worker_ids.add(worker_id)

            token = crypto.generate_session_token()
            self._known_tokens[worker_id] = token
            conn.token = token
            conn.status = WorkerStatus.AUTHORIZED
            await write_message(writer, Message(MessageType.AUTHORIZED,
                                {"token": token, "worker_id": worker_id,
                                 "heartbeat_timeout": HEARTBEAT_TIMEOUT_SEC}))
            self._emit("worker_authorized", {"worker_id": worker_id})

            await self._client_loop(conn)

        except ProtocolError as exc:
            logger.warning("Protocol error from %s: %s", addr_str, exc)
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        finally:
            if worker_id and worker_id in self.workers:
                self.workers[worker_id].status = WorkerStatus.DISCONNECTED
                self.task_manager.release_worker_chunks(worker_id)
                self._emit("worker_disconnected", {"worker_id": worker_id})
            try:
                writer.close()
            except Exception:
                pass

    async def _await_approval(self, conn: WorkerConn) -> bool:
        if self._auto_approve:
            return True
        loop = asyncio.get_event_loop()
        fut = loop.create_future()
        self._awaiting_approval[conn.worker_id] = fut
        self._emit("approval_requested", {"worker_id": conn.worker_id,
                                           "address": conn.address, "name": conn.name})
        try:
            return await fut
        finally:
            self._awaiting_approval.pop(conn.worker_id, None)

    def approve_worker(self, worker_id: str, approved: bool):
        """Called from the GUI thread once the operator answers the prompt."""
        if self._loop is None:
            return

        def _resolve():
            fut = self._awaiting_approval.get(worker_id)
            if fut and not fut.done():
                fut.set_result(approved)

        self._loop.call_soon_threadsafe(_resolve)

    def set_auto_approve(self, value: bool):
        self._auto_approve = value

    async def _client_loop(self, conn: WorkerConn):
        while True:
            msg = await read_message(conn.reader)
            if msg is None:
                break
            await self._dispatch(conn, msg)

    async def _dispatch(self, conn: WorkerConn, msg: Message):
        t = msg.type
        if t == MessageType.HEARTBEAT:
            conn.last_heartbeat = time.time()
            sent_ts = msg.payload.get("client_ts")
            if sent_ts:
                conn.latency_ms = round((time.time() - sent_ts) * 1000, 1)
            await write_message(conn.writer, Message(MessageType.HEARTBEAT_ACK, {}))

        elif t == MessageType.STATUS_UPDATE:
            conn.sysinfo = msg.payload.get("sysinfo", conn.sysinfo)
            self._emit("worker_status", {"worker_id": conn.worker_id, "sysinfo": conn.sysinfo})

        elif t == MessageType.TASK_ACK:
            self._emit("task_ack", {"worker_id": conn.worker_id,
                                     "chunk_id": msg.payload.get("chunk_id")})

        elif t == MessageType.TASK_COMPLETE:
            chunk_id = msg.payload["chunk_id"]
            result = msg.payload["result"]
            if self._validate_result(result):
                self.task_manager.mark_done(chunk_id, conn.worker_id, result)
                self._emit("chunk_done", {"worker_id": conn.worker_id, "chunk_id": chunk_id})
            else:
                self.task_manager.mark_failed(chunk_id, conn.worker_id)
                self._emit("chunk_invalid", {"worker_id": conn.worker_id, "chunk_id": chunk_id})
            await self._maybe_send_next(conn)

        elif t == MessageType.TASK_FAILED:
            chunk_id = msg.payload.get("chunk_id")
            self.task_manager.mark_failed(chunk_id, conn.worker_id)
            self._emit("chunk_failed", {"worker_id": conn.worker_id, "chunk_id": chunk_id,
                                         "error": msg.payload.get("error")})
            await self._maybe_send_next(conn)

        elif t == MessageType.DISCONNECT:
            self.task_manager.release_worker_chunks(conn.worker_id)
            conn.status = WorkerStatus.DISCONNECTED

        elif t == MessageType.PAUSE:
            conn.status = WorkerStatus.PAUSED

        elif t == MessageType.RESUME:
            conn.status = WorkerStatus.AUTHORIZED

    def _validate_result(self, result: dict) -> bool:
        """Spot-check the CPU-benchmark digest so a worker can't get credit
        for a forged or garbage result."""
        try:
            from tasks.cpu_benchmark_task import sample_digest
            expected = sample_digest(result["start"], result["end"])
            return expected == result.get("digest")
        except Exception:
            return False

    async def _maybe_send_next(self, conn: WorkerConn):
        if conn.status != WorkerStatus.AUTHORIZED:
            return
        chunk = self.task_manager.next_chunk_for(conn.worker_id)
        if chunk:
            await write_message(conn.writer, Message(MessageType.TASK_ASSIGN, {
                "chunk_id": chunk.chunk_id, "task_type": chunk.task_type,
                "params": chunk.params,
            }))
            self._emit("chunk_assigned", {"worker_id": conn.worker_id, "chunk_id": chunk.chunk_id})
        elif self.task_manager.should_notify_finished():
            self._emit("job_finished", {})

    # -------------------------------------------------------- job dispatch API

    def dispatch_job(self):
        """Push a first chunk to every idle, authorized worker. Call this
        from the GUI thread right after task_manager.start_job()."""
        if self._loop is None:
            return
        for conn in list(self.workers.values()):
            if conn.status == WorkerStatus.AUTHORIZED:
                asyncio.run_coroutine_threadsafe(self._maybe_send_next(conn), self._loop)

    def set_worker_paused(self, worker_id: str, paused: bool):
        conn = self.workers.get(worker_id)
        if not conn or self._loop is None:
            return
        conn.status = WorkerStatus.PAUSED if paused else WorkerStatus.AUTHORIZED
        msg = Message(MessageType.PAUSE if paused else MessageType.RESUME, {})
        asyncio.run_coroutine_threadsafe(write_message(conn.writer, msg), self._loop)
        if not paused:
            asyncio.run_coroutine_threadsafe(self._maybe_send_next(conn), self._loop)

    def disconnect_worker(self, worker_id: str):
        conn = self.workers.get(worker_id)
        if not conn or self._loop is None:
            return

        async def _close():
            try:
                await write_message(conn.writer, Message(MessageType.DISCONNECT, {}))
            except Exception:
                pass
            conn.writer.close()

        asyncio.run_coroutine_threadsafe(_close(), self._loop)
        self.task_manager.release_worker_chunks(worker_id)

    def cancel_job(self):
        self.task_manager.cancel()
        if self._loop is None:
            return
        for conn in list(self.workers.values()):
            asyncio.run_coroutine_threadsafe(
                write_message(conn.writer, Message(MessageType.TASK_CANCEL, {})), self._loop)

    # ----------------------------------------------------------------- monitors

    async def _timeout_monitor(self):
        while True:
            await asyncio.sleep(2)
            timed_out = self.task_manager.check_timeouts()
            for chunk_id in timed_out:
                self._emit("chunk_timeout", {"chunk_id": chunk_id})
            for conn in list(self.workers.values()):
                if conn.status == WorkerStatus.AUTHORIZED:
                    await self._maybe_send_next(conn)

    async def _heartbeat_monitor(self):
        while True:
            await asyncio.sleep(3)
            now = time.time()
            for worker_id, conn in list(self.workers.items()):
                if conn.status in (WorkerStatus.AUTHORIZED, WorkerStatus.PAUSED) and \
                        (now - conn.last_heartbeat) > HEARTBEAT_TIMEOUT_SEC:
                    conn.status = WorkerStatus.DISCONNECTED
                    self.task_manager.release_worker_chunks(worker_id)
                    self._emit("worker_timed_out", {"worker_id": worker_id})


class _DiscoveryProtocol(asyncio.DatagramProtocol):
    """Answers LAN broadcast probes from Workers looking for a Master."""

    def __init__(self, server: MasterServer):
        self.server = server
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        try:
            msg = json.loads(data.decode("utf-8"))
        except Exception:
            return
        if msg.get("magic") != DISCOVERY_MAGIC:
            return
        reply = json.dumps({
            "magic": DISCOVERY_REPLY_MAGIC,
            "host": socket.gethostbyname(socket.gethostname()),
            "port": self.server.config.port,
            "name": "Master",
        }).encode("utf-8")
        self.transport.sendto(reply, addr)
