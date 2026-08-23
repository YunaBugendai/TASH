package com.yunabugendai.tash.worker

import android.app.*
import android.os.Bundle
import android.os.Build
import android.view.Gravity
import android.widget.*
import org.json.JSONObject
import java.util.UUID
import java.util.concurrent.Executors

class MainActivity : Activity() {
    private lateinit var host: EditText
    private lateinit var port: EditText
    private lateinit var code: EditText
    private lateinit var name: EditText
    private lateinit var status: TextView
    private lateinit var connect: Button
    private var client: AndroidWorkerClient? = null

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val box = LinearLayout(this).apply { orientation = LinearLayout.VERTICAL; setPadding(32, 32, 32, 32) }
        fun field(hint: String, value: String = ""): EditText = EditText(this).apply { this.hint = hint; setText(value) }
        host = field("Master IP")
        port = field("Master port", "8765")
        code = field("Pairing code")
        name = field("Worker name", "Android-${Build.MODEL}")
        status = TextView(this).apply { text = "Disconnected"; textSize = 16f; setPadding(0, 24, 0, 24) }
        connect = Button(this).apply { text = "Connect" }
        box.addView(host); box.addView(port); box.addView(code); box.addView(name); box.addView(status); box.addView(connect)
        setContentView(box)

        connect.setOnClickListener {
            if (client?.isConnected() == true) { client?.disconnect(); return@setOnClickListener }
            AlertDialog.Builder(this)
                .setTitle("Authorize Master")
                .setMessage("Allow this Android device to connect to ${host.text}:${port.text} and perform authorized TASH computations?")
                .setNegativeButton("Cancel", null)
                .setPositiveButton("Authorize") { _, _ -> startWorker() }
                .show()
        }
    }

    private fun startWorker() {
        connect.isEnabled = false
        status.text = "Connecting..."
        client = AndroidWorkerClient(name.text.toString(), object : AndroidWorkerClient.Listener {
            override fun onStatus(text: String) = runOnUiThread { status.text = text }
            override fun onConnected() = runOnUiThread { connect.isEnabled = true; connect.text = "Disconnect" }
            override fun onDisconnected() = runOnUiThread { connect.isEnabled = true; connect.text = "Connect" }
        }).also {
            it.start(host.text.toString().trim(), port.text.toString().toIntOrNull() ?: 8765, code.text.toString().trim())
        }
    }

    override fun onDestroy() { client?.disconnect(); super.onDestroy() }
}

private class AndroidWorkerClient(private val workerName: String, private val listener: Listener) {
    interface Listener { fun onStatus(text: String); fun onConnected(); fun onDisconnected() }
    private val executor = Executors.newSingleThreadExecutor()
    private var socket: java.net.Socket? = null
    private var running = false
    private var workerId = UUID.randomUUID().toString().replace("-", "").take(12)
    private var token: String? = null
    private var reconnect = false
    private var masterHost = ""
    private var masterPort = 8765
    private var pairingCode = ""
    private var currentJob: Thread? = null
    @Volatile private var cancelled = false

    fun isConnected() = running && socket?.isConnected == true

    fun start(host: String, port: Int, code: String) {
        masterHost = host; masterPort = port; pairingCode = code; running = true
        executor.execute { connectionLoop() }
    }

    fun disconnect() {
        running = false; cancelled = true
        try { send("DISCONNECT", JSONObject()) } catch (_: Exception) {}
        try { socket?.close() } catch (_: Exception) {}
        listener.onDisconnected()
    }

    private fun connectionLoop() {
        var backoff = 2000L
        while (running) {
            try {
                listener.onStatus("Connecting to $masterHost:$masterPort...")
                socket = java.net.Socket()
                socket!!.connect(java.net.InetSocketAddress(masterHost, masterPort), 5000)
                val input = socket!!.getInputStream().bufferedReader(Charsets.UTF_8)
                val output = socket!!.getOutputStream().bufferedWriter(Charsets.UTF_8)
                val register = if (reconnect && token != null) "RECONNECT" else "REGISTER"
                val payload = JSONObject().apply {
                    put("version", "1.0")
                    put("worker_id", workerId)
                    put("name", workerName)
                    put("sysinfo", sysInfo())
                    if (register == "REGISTER") put("pairing_code", pairingCode) else put("token", token)
                }
                write(output, envelope(register, payload))
                val reply = input.readLine() ?: throw java.io.IOException("Master closed connection")
                val r = JSONObject(reply)
                if (r.optString("type") != "AUTHORIZED") {
                    token = null; reconnect = false
                    throw java.io.IOException("Rejected: ${r.optJSONObject("payload")?.optString("reason") ?: "unknown reason"}")
                }
                token = r.getJSONObject("payload").getString("token")
                reconnect = true
                listener.onStatus("Authorized and connected")
                listener.onConnected()
                backoff = 2000L
                startHeartbeat(output)
                while (running) {
                    val line = input.readLine() ?: break
                    handle(JSONObject(line), output)
                }
            } catch (e: Exception) {
                if (running) listener.onStatus("Disconnected: ${e.message ?: "connection error"}")
            } finally {
                try { socket?.close() } catch (_: Exception) {}
                socket = null
                if (running) listener.onDisconnected()
            }
            if (running) {
                Thread.sleep(backoff)
                backoff = minOf(30000L, backoff * 2)
            }
        }
    }

    private fun startHeartbeat(output: java.io.BufferedWriter) {
        Thread {
            while (running && socket?.isConnected == true) {
                try {
                    Thread.sleep(5000)
                    write(output, envelope("HEARTBEAT", JSONObject().put("client_ts", System.currentTimeMillis() / 1000.0)))
                    write(output, envelope("STATUS_UPDATE", JSONObject().put("sysinfo", sysInfo())))
                } catch (_: Exception) { break }
            }
        }.start()
    }

    private fun handle(msg: JSONObject, output: java.io.BufferedWriter) {
        when (msg.optString("type")) {
            "TASK_ASSIGN" -> {
                val p = msg.getJSONObject("payload"); val id = p.getInt("chunk_id")
                write(output, envelope("TASK_ACK", JSONObject().put("chunk_id", id)))
                cancelled = false
                currentJob = Thread { executeTask(id, p, output) }.also { it.start() }
            }
            "TASK_CANCEL" -> cancelled = true
            "PAUSE" -> listener.onStatus("Paused")
            "RESUME" -> listener.onStatus("Authorized and connected")
            "DISCONNECT" -> { running = false; try { socket?.close() } catch (_: Exception) {} }
            "HEARTBEAT_ACK" -> Unit
        }
    }

    private fun executeTask(chunkId: Int, p: JSONObject, output: java.io.BufferedWriter) {
        try {
            if (p.getString("task_type") != "cpu_benchmark") throw IllegalArgumentException("Unsupported task type")
            val params = p.getJSONObject("params")
            val start = params.getLong("start"); val end = params.getLong("end")
            val t0 = System.nanoTime(); var sum = 0.0; var count = 0L
            var x = start
            while (x < end) {
                if (cancelled) { write(output, envelope("TASK_FAILED", JSONObject().put("chunk_id", chunkId).put("error", "cancelled"))); return }
                sum += f(x); count++; x++
            }
            val elapsed = (System.nanoTime() - t0) / 1_000_000_000.0
            val result = JSONObject().apply {
                put("start", start); put("end", end); put("count", count); put("sum", sum)
                put("digest", sampleDigest(start, end)); put("compute_seconds", elapsed)
            }
            write(output, envelope("TASK_COMPLETE", JSONObject().put("chunk_id", chunkId).put("result", result)))
            listener.onStatus("Completed chunk $chunkId in %.3fs".format(elapsed))
        } catch (e: Exception) {
            write(output, envelope("TASK_FAILED", JSONObject().put("chunk_id", chunkId).put("error", e.toString())))
        }
    }

    private fun f(x: Long): Double {
        val d = x.toDouble()
        var v = kotlin.math.sin(d) * kotlin.math.cos(d / 3.0 + 1.0)
        v += kotlin.math.sqrt(kotlin.math.abs(d) + 1.0)
        v += kotlin.math.ln(kotlin.math.abs(d) + 2.0)
        return v
    }

    private fun sampleDigest(start: Long, end: Long, sampleSize: Int = 8): String {
        if (end <= start) return sha256("")
        val step = maxOf(1L, (end - start) / sampleSize)
        val sb = StringBuilder(); var x = start
        while (x < end) { sb.append(x).append(':').append("%.10f".format(java.util.Locale.US, f(x))); x += step }
        return sha256(sb.toString())
    }

    private fun sha256(s: String): String = java.security.MessageDigest.getInstance("SHA-256").digest(s.toByteArray()).joinToString("") { "%02x".format(it) }

    private fun sysInfo(): JSONObject = JSONObject().apply {
        put("cpu", JSONObject().apply { put("logical_cores", Runtime.getRuntime().availableProcessors()); put("physical_cores", Runtime.getRuntime().availableProcessors()); put("usage_percent", JSONObject.NULL) })
        put("ram", JSONObject().apply { val am = MainActivityHolder.memoryInfo; put("total_gb", am.totalMem / 1073741824.0); put("used_percent", JSONObject.NULL) })
        put("gpu", JSONObject().apply { put("name", "Android GPU"); put("load_percent", JSONObject.NULL); put("mem_used_mb", JSONObject.NULL); put("mem_total_mb", JSONObject.NULL) })
    }

    private fun envelope(type: String, payload: JSONObject) = JSONObject().apply { put("type", type); put("msg_id", UUID.randomUUID().toString().replace("-", "")); put("ts", System.currentTimeMillis() / 1000.0); put("version", "1.0"); put("payload", payload) }
    private fun write(out: java.io.BufferedWriter, obj: JSONObject) { synchronized(out) { out.write(obj.toString()); out.newLine(); out.flush() } }

    object MainActivityHolder { lateinit var memoryInfo: android.app.ActivityManager.MemoryInfo }
}
