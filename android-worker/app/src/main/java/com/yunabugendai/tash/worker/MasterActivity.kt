package com.yunabugendai.tash.worker

import android.app.Activity
import android.os.Bundle
import android.widget.*
import org.json.JSONObject
import java.net.*
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors

/** Minimal Android Master compatible with TASH's newline-delimited JSON protocol. */
class MasterActivity : Activity() {
    private lateinit var status: TextView
    private lateinit var codeView: TextView
    private lateinit var workersView: TextView
    private lateinit var start: Button
    private val master = AndroidMaster()

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val box = LinearLayout(this).apply { orientation = LinearLayout.VERTICAL; setPadding(32,32,32,32) }
        codeView = TextView(this).apply { textSize = 20f }
        status = TextView(this).apply { text = "Starting Master..." }
        workersView = TextView(this).apply { text = "Workers: 0"; setPadding(0,20,0,20) }
        start = Button(this).apply { text = "Run CPU Benchmark"; isEnabled = false }
        box.addView(TextView(this).apply { text = "TASH Android Master"; textSize = 24f })
        box.addView(codeView); box.addView(status); box.addView(workersView); box.addView(start)
        setContentView(box)

        master.onEvent = { text -> runOnUiThread { status.text = text; workersView.text = "Workers: ${master.workerCount()}"; start.isEnabled = master.workerCount() > 0 } }
        master.start()
        codeView.text = "Pairing code: ${master.pairingCode}  |  TCP: 8765"
        start.setOnClickListener { master.startBenchmark { done -> runOnUiThread { status.text = done } } }
    }

    override fun onDestroy() { master.stop(); super.onDestroy() }
}

private class AndroidMaster {
    companion object { const val TCP_PORT = 8765; const val DISCOVERY_PORT = 8766; const val MAGIC = "TASH_DISCOVERY_V1"; const val REPLY = "TASH_MASTER_V1" }
    data class Worker(val id: String, val name: String, val socket: Socket, val out: java.io.BufferedWriter)
    private val executor = Executors.newCachedThreadPool()
    private val workers = ConcurrentHashMap<String, Worker>()
    private val pending = ConcurrentHashMap<Int, String>()
    private var server: ServerSocket? = null
    private var udp: DatagramSocket? = null
    private var running = false
    private var chunkCounter = 0
    val pairingCode: String = (100000..999999).random().toString()
    var onEvent: ((String) -> Unit)? = null

    fun start() {
        running = true
        executor.execute { tcpLoop() }
        executor.execute { discoveryLoop() }
        onEvent?.invoke("Master ready on LAN")
    }

    fun stop() {
        running = false
        try { server?.close() } catch (_: Exception) {}
        try { udp?.close() } catch (_: Exception) {}
        workers.values.forEach { try { it.socket.close() } catch (_: Exception) {} }
        executor.shutdownNow()
    }

    fun workerCount() = workers.size

    private fun tcpLoop() {
        try {
            server = ServerSocket(TCP_PORT)
            while (running) {
                val socket = server!!.accept()
                executor.execute { handleWorker(socket) }
            }
        } catch (_: Exception) { if (running) onEvent?.invoke("TCP server stopped") }
    }

    private fun handleWorker(socket: Socket) {
        try {
            socket.soTimeout = 0
            val input = socket.getInputStream().bufferedReader(Charsets.UTF_8)
            val output = socket.getOutputStream().bufferedWriter(Charsets.UTF_8)
            val first = input.readLine() ?: return
            val msg = JSONObject(first)
            if (msg.optString("type") != "REGISTER") { socket.close(); return }
            val p = msg.getJSONObject("payload")
            if (p.optString("version") != "1.0" || p.optString("pairing_code") != pairingCode) { socket.close(); return }
            val id = p.optString("worker_id", UUID.randomUUID().toString())
            val worker = Worker(id, p.optString("name", id), socket, output)
            workers[id] = worker
            write(output, envelope("AUTHORIZED", JSONObject().put("token", UUID.randomUUID().toString()).put("worker_id", id).put("heartbeat_timeout", 30)))
            onEvent?.invoke("Authorized ${worker.name} (${socket.inetAddress.hostAddress})")
            while (running && !socket.isClosed) {
                val line = input.readLine() ?: break
                handle(worker, JSONObject(line))
            }
        } catch (e: Exception) {
            if (running) onEvent?.invoke("Worker connection error: ${e.message}")
        } finally {
            try { socket.close() } catch (_: Exception) {}
            workers.entries.removeIf { it.value.socket == socket }
            onEvent?.invoke("Workers: ${workers.size}")
        }
    }

    private fun handle(worker: Worker, msg: JSONObject) {
        when (msg.optString("type")) {
            "HEARTBEAT" -> write(worker.out, envelope("HEARTBEAT_ACK", JSONObject()))
            "STATUS_UPDATE" -> Unit
            "TASK_COMPLETE" -> {
                val id = msg.optInt("chunk_id", -1)
                pending.remove(id)
                onEvent?.invoke("${worker.name} completed chunk $id")
            }
            "TASK_FAILED" -> { pending.remove(msg.optInt("chunk_id", -1)); onEvent?.invoke("${worker.name} failed a task") }
        }
    }

    fun startBenchmark(done: (String) -> Unit) {
        if (workers.isEmpty()) { done("No workers connected"); return }
        executor.execute {
            val total = 20_000_000L
            val chunk = 1_000_000L
            var start = 0L
            while (start < total && running) {
                val worker = workers.values.toList().getOrNull(((start / chunk) % workers.size).toInt()) ?: break
                val end = minOf(total, start + chunk)
                val id = ++chunkCounter
                pending[id] = worker.id
                val params = JSONObject().put("start", start).put("end", end)
                write(worker.out, envelope("TASK_ASSIGN", JSONObject().put("chunk_id", id).put("task_type", "cpu_benchmark").put("params", params)))
                start = end
            }
            done("Benchmark dispatched: ${chunkCounter} chunks")
        }
    }

    private fun discoveryLoop() {
        try {
            udp = DatagramSocket(DISCOVERY_PORT).apply { reuseAddress = true }
            val buf = ByteArray(4096)
            while (running) {
                val packet = DatagramPacket(buf, buf.size)
                udp!!.receive(packet)
                val text = String(packet.data, packet.offset, packet.length, Charsets.UTF_8)
                val msg = JSONObject(text)
                if (msg.optString("magic") == MAGIC) {
                    val reply = JSONObject().put("magic", REPLY).put("host", localIp()).put("port", TCP_PORT).put("name", "Android Master")
                    val bytes = reply.toString().toByteArray(Charsets.UTF_8)
                    udp!!.send(DatagramPacket(bytes, bytes.size, packet.address, packet.port))
                }
            }
        } catch (_: Exception) { }
    }

    private fun localIp(): String = try { NetworkInterface.getNetworkInterfaces().toList().flatMap { it.inetAddresses.toList() }.firstOrNull { !it.isLoopbackAddress && it is Inet4Address }?.hostAddress ?: "0.0.0.0" } catch (_: Exception) { "0.0.0.0" }
    private fun envelope(type: String, payload: JSONObject) = JSONObject().apply { put("type", type); put("msg_id", UUID.randomUUID().toString().replace("-", "")); put("ts", System.currentTimeMillis()/1000.0); put("version", "1.0"); put("payload", payload) }
    private fun write(out: java.io.BufferedWriter, obj: JSONObject) { synchronized(out) { out.write(obj.toString()); out.newLine(); out.flush() } }
}
