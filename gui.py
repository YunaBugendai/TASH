"""
Tkinter GUI for the Master application.

All state changes coming from the networking thread arrive as
(event, data) tuples on a thread-safe queue.Queue; `_poll_events`
(driven by Tkinter's own `root.after`, so it always runs on the main
thread) is the only place that ever touches widgets. Nothing in
master_core.py, task_manager.py, or benchmark.py imports tkinter.
"""
import queue
import threading
import time
import tkinter as tk
from tkinter import ttk, messagebox

from master.benchmark import BenchmarkRunner
from master.master_core import MasterServer
from master.task_manager import TaskManager


class MasterApp:
    def __init__(self, root, config, pairing_code):
        self.root = root
        self.config = config
        self.pairing_code = pairing_code
        self.events = queue.Queue()

        self.task_manager = TaskManager(task_timeout_sec=config.task_timeout_sec)
        self.task_manager.on_progress = lambda: self.events.put(("progress", {}))

        self.server = MasterServer(config, self.task_manager, pairing_code)
        self.server.on_event = lambda ev, data: self.events.put((ev, data))
        self.benchmark = BenchmarkRunner(self.server, self.task_manager)

        self.job_id_counter = 0

        root.title("Distributed Compute -- Master")
        root.geometry("1000x680")
        self._build_ui()
        self.server.start()
        self._poll_events()

    # ------------------------------------------------------------------- UI

    def _build_ui(self):
        nb = ttk.Notebook(self.root)
        nb.pack(fill="both", expand=True)

        self.tab_dashboard = ttk.Frame(nb)
        self.tab_job = ttk.Frame(nb)
        self.tab_benchmark = ttk.Frame(nb)
        self.tab_logs = ttk.Frame(nb)
        self.tab_settings = ttk.Frame(nb)

        nb.add(self.tab_dashboard, text="Dashboard / Workers")
        nb.add(self.tab_job, text="Job")
        nb.add(self.tab_benchmark, text="Benchmark")
        nb.add(self.tab_logs, text="Logs")
        nb.add(self.tab_settings, text="Settings")

        self._build_dashboard(self.tab_dashboard)
        self._build_job_tab(self.tab_job)
        self._build_benchmark_tab(self.tab_benchmark)
        self._build_logs_tab(self.tab_logs)
        self._build_settings_tab(self.tab_settings)

    def _build_dashboard(self, parent):
        top = ttk.Frame(parent)
        top.pack(fill="x", padx=8, pady=6)
        ttk.Label(top, text=f"Pairing code for Workers:  {self.pairing_code}",
                  font=("TkDefaultFont", 12, "bold")).pack(side="left")
        ttk.Label(top, text="  (Workers need this once, plus your IP + port, to join)"
                  ).pack(side="left")

        columns = ("name", "address", "status", "cpu", "ram", "gpu", "latency")
        self.tree = ttk.Treeview(parent, columns=columns, show="headings", height=14)
        headers = {"name": "Worker", "address": "Address", "status": "Status",
                   "cpu": "CPU %", "ram": "RAM %", "gpu": "GPU",
                   "latency": "Latency (ms)"}
        widths = {"name": 130, "address": 140, "status": 100, "cpu": 70,
                  "ram": 70, "gpu": 160, "latency": 100}
        for c in columns:
            self.tree.heading(c, text=headers[c])
            self.tree.column(c, width=widths[c], anchor="center")
        self.tree.pack(fill="both", expand=True, padx=8, pady=4)

        btns = ttk.Frame(parent)
        btns.pack(fill="x", padx=8, pady=6)
        ttk.Button(btns, text="Pause", command=self._pause_selected).pack(side="left", padx=2)
        ttk.Button(btns, text="Resume", command=self._resume_selected).pack(side="left", padx=2)
        ttk.Button(btns, text="Disconnect", command=self._disconnect_selected).pack(side="left", padx=2)
        ttk.Button(btns, text="How do I add a Worker?", command=self._manual_add_info
                   ).pack(side="right", padx=2)

    def _build_job_tab(self, parent):
        form = ttk.LabelFrame(parent, text="Create job -- CPU benchmark: f(x) for x = 1..N")
        form.pack(fill="x", padx=8, pady=8)

        ttk.Label(form, text="Workload size (N):").grid(row=0, column=0, sticky="w", padx=4, pady=4)
        self.var_n = tk.IntVar(value=2_000_000)
        ttk.Entry(form, textvariable=self.var_n, width=15).grid(row=0, column=1, padx=4)

        ttk.Label(form, text="Chunk size:").grid(row=0, column=2, sticky="w", padx=4)
        self.var_chunk = tk.IntVar(value=self.config.chunk_size)
        ttk.Entry(form, textvariable=self.var_chunk, width=10).grid(row=0, column=3, padx=4)

        ttk.Button(form, text="Run on Master only", command=self._run_master_only).grid(
            row=1, column=0, padx=4, pady=6)
        ttk.Button(form, text="Distribute to Workers", command=self._run_distributed).grid(
            row=1, column=1, padx=4, pady=6)
        ttk.Button(form, text="Stop all computation", command=self._stop_all).grid(
            row=1, column=3, padx=4, pady=6)

        prog = ttk.LabelFrame(parent, text="Progress")
        prog.pack(fill="both", expand=True, padx=8, pady=8)
        self.overall_bar = ttk.Progressbar(prog, mode="determinate", maximum=100)
        self.overall_bar.pack(fill="x", padx=8, pady=8)
        self.lbl_progress = ttk.Label(prog, text="No job running.")
        self.lbl_progress.pack(anchor="w", padx=8)
        self.lbl_eta = ttk.Label(prog, text="")
        self.lbl_eta.pack(anchor="w", padx=8)

        columns = ("worker", "chunks_done")
        self.per_worker_tree = ttk.Treeview(prog, columns=columns, show="headings", height=8)
        self.per_worker_tree.heading("worker", text="Worker")
        self.per_worker_tree.heading("chunks_done", text="Chunks completed")
        self.per_worker_tree.pack(fill="both", expand=True, padx=8, pady=8)

    def _build_benchmark_tab(self, parent):
        ttk.Label(parent, text=("Compares Master-only vs Master + 1 Worker vs Master + all "
                                 "connected Workers on the same workload, back-to-back.")
                  ).pack(anchor="w", padx=8, pady=8)
        ttk.Button(parent, text="Run comparison benchmark",
                   command=self._run_benchmark_suite).pack(padx=8, pady=4, anchor="w")

        columns = ("scenario", "total_s", "compute_s", "network_s", "throughput",
                   "master_util", "worker_util", "speedup", "efficiency")
        self.bench_tree = ttk.Treeview(parent, columns=columns, show="headings", height=8)
        headers = {"scenario": "Scenario", "total_s": "Total (s)", "compute_s": "Compute (s)",
                   "network_s": "Network (s)", "throughput": "Throughput",
                   "master_util": "Master CPU", "worker_util": "Worker CPU (avg)",
                   "speedup": "Speedup", "efficiency": "Efficiency"}
        for c in columns:
            self.bench_tree.heading(c, text=headers[c])
            self.bench_tree.column(c, width=100, anchor="center")
        self.bench_tree.pack(fill="both", expand=True, padx=8, pady=8)
        self.bench_note = ttk.Label(parent, text="", foreground="#a04000", wraplength=900,
                                     justify="left")
        self.bench_note.pack(anchor="w", padx=8, pady=4)

    def _build_logs_tab(self, parent):
        self.log_text = tk.Text(parent, state="disabled", wrap="word")
        self.log_text.pack(fill="both", expand=True, padx=8, pady=8)

    def _build_settings_tab(self, parent):
        frm = ttk.Frame(parent)
        frm.pack(fill="x", padx=8, pady=8)
        ttk.Label(frm, text=f"TCP port: {self.config.port}").grid(row=0, column=0, sticky="w", pady=2)
        ttk.Label(frm, text=f"Discovery (UDP) port: {self.config.discovery_port}").grid(
            row=1, column=0, sticky="w", pady=2)
        ttk.Label(frm, text=f"Task timeout (s): {self.config.task_timeout_sec}").grid(
            row=2, column=0, sticky="w", pady=2)

        self.var_auto_approve = tk.BooleanVar(value=False)
        ttk.Checkbutton(frm, text="Auto-approve new Workers (skip the manual confirmation dialog)",
                        variable=self.var_auto_approve,
                        command=lambda: self.server.set_auto_approve(self.var_auto_approve.get())
                        ).grid(row=3, column=0, sticky="w", pady=8)
        ttk.Label(frm, text=("Note: this only affects first-time REGISTER requests. "
                              "A Worker that reconnects after a network drop is always "
                              "let back in automatically using its existing session token, "
                              "with no re-prompt needed."), wraplength=700, justify="left"
                  ).grid(row=4, column=0, sticky="w", pady=4)

    # --------------------------------------------------------------- actions

    def _selected_worker_id(self):
        sel = self.tree.selection()
        return sel[0] if sel else None

    def _pause_selected(self):
        wid = self._selected_worker_id()
        if wid:
            self.server.set_worker_paused(wid, True)

    def _resume_selected(self):
        wid = self._selected_worker_id()
        if wid:
            self.server.set_worker_paused(wid, False)

    def _disconnect_selected(self):
        wid = self._selected_worker_id()
        if wid:
            self.server.disconnect_worker(wid)

    def _manual_add_info(self):
        messagebox.showinfo(
            "Add a Worker",
            f"On the Worker app, enter this Master's IP address, port {self.config.port}, "
            f"and pairing code {self.pairing_code}. Workers on the same local network can "
            f"also find this Master automatically via the 'Find Master on LAN' button.")

    def _run_master_only(self):
        n = self.var_n.get()
        self._log(f"Running master-only benchmark for N={n}...")
        threading.Thread(target=self._master_only_thread, args=(n,), daemon=True).start()

    def _master_only_thread(self, n):
        import time as _t
        from tasks.cpu_benchmark_task import cpu_benchmark
        t0 = _t.time()
        result = cpu_benchmark({"start": 0, "end": n})
        elapsed = _t.time() - t0
        self.events.put(("master_only_done", {"n": n, "elapsed": elapsed, "sum": result["sum"]}))

    def _run_distributed(self):
        n = self.var_n.get()
        chunk = self.var_chunk.get()
        self.job_id_counter += 1
        job_id = f"job-{self.job_id_counter}"
        self.task_manager.start_job(job_id, "cpu_benchmark", n, chunk)
        self.server.dispatch_job()
        self._log(f"Distributed job {job_id} started: N={n}, chunk_size={chunk}")

    def _stop_all(self):
        self.server.cancel_job()
        self._log("Stop requested: cancelling all pending and in-flight chunks.")

    def _run_benchmark_suite(self):
        n = self.var_n.get()
        chunk = self.var_chunk.get()
        self._log("Running comparison benchmark (this can take a while)...")
        threading.Thread(target=self._benchmark_thread, args=(n, chunk), daemon=True).start()

    def _benchmark_thread(self, n, chunk):
        rows, note = self.benchmark.run_all(n, chunk)
        self.events.put(("benchmark_result", {"rows": rows, "note": note}))

    # ---------------------------------------------------------------- events

    def _poll_events(self):
        try:
            while True:
                ev, data = self.events.get_nowait()
                self._handle_event(ev, data)
        except queue.Empty:
            pass
        self._refresh_progress()
        self.root.after(200, self._poll_events)

    def _handle_event(self, ev, data):
        if ev == "approval_requested":
            self._prompt_approval(data)
        elif ev in ("worker_authorized", "worker_disconnected", "worker_timed_out"):
            self._refresh_worker_tree()
            self._log(f"{ev}: {data}")
        elif ev == "worker_status":
            self._refresh_worker_tree()
        elif ev in ("chunk_done", "chunk_failed", "chunk_timeout", "chunk_invalid",
                    "chunk_assigned", "task_ack", "progress"):
            pass  # picked up by the periodic _refresh_progress() call
        elif ev == "job_finished":
            self._log("Job finished.")
            messagebox.showinfo("Job finished", "All chunks completed, failed, or were cancelled.")
        elif ev == "master_only_done":
            self._log(f"Master-only done: {data['n']} items in {data['elapsed']:.2f}s "
                      f"({data['n']/data['elapsed']:.0f} items/s), sum={data['sum']:.4f}")
            messagebox.showinfo("Master-only run complete",
                                f"{data['n']} items in {data['elapsed']:.2f}s "
                                f"({data['n']/data['elapsed']:.0f} items/s)")
        elif ev == "benchmark_result":
            for row in self.bench_tree.get_children():
                self.bench_tree.delete(row)
            for r in data["rows"]:
                self.bench_tree.insert("", "end", values=r)
            self.bench_note.config(text=data["note"] or "")
            self._log("Benchmark comparison finished.")
        else:
            self._log(f"{ev}: {data}")

    def _prompt_approval(self, data):
        answer = messagebox.askyesno(
            "New Worker requesting authorization",
            f"Worker '{data['name']}' at {data['address']} wants to join.\n\n"
            f"Authorize this Worker?")
        self.server.approve_worker(data["worker_id"], answer)

    def _refresh_worker_tree(self):
        for row in self.tree.get_children():
            self.tree.delete(row)
        for wid, conn in self.server.workers.items():
            cpu = conn.sysinfo.get("cpu", {}).get("usage_percent", "?")
            ram = conn.sysinfo.get("ram", {}).get("used_percent", "?")
            gpu = conn.sysinfo.get("gpu", {}).get("name", "?")
            self.tree.insert("", "end", iid=wid, values=(
                conn.name, conn.address, conn.status, cpu, ram, gpu,
                conn.latency_ms if conn.latency_ms is not None else "?"))

    def _refresh_progress(self):
        snap = self.task_manager.progress_snapshot()
        if snap["total"] == 0:
            return
        pct = 100.0 * snap["done"] / snap["total"]
        self.overall_bar["value"] = pct
        self.lbl_progress.config(text=(
            f"{snap['done']}/{snap['total']} chunks done  "
            f"({snap['pending']} pending, {snap['assigned']} in flight, "
            f"{snap['failed']} failed, {snap['cancelled']} cancelled)"))
        eta = self.task_manager.eta_seconds()
        if eta is None:
            self.lbl_eta.config(text="ETA: calculating...")
        else:
            self.lbl_eta.config(text=f"ETA: {eta:.0f}s")

        per_worker = self.task_manager.per_worker_progress()
        for row in self.per_worker_tree.get_children():
            self.per_worker_tree.delete(row)
        for wid, count in per_worker.items():
            conn = self.server.workers.get(wid)
            name = conn.name if conn else wid
            self.per_worker_tree.insert("", "end", values=(name, count))

    def _log(self, text):
        self.log_text.config(state="normal")
        self.log_text.insert("end", f"[{time.strftime('%H:%M:%S')}] {text}\n")
        self.log_text.see("end")
        self.log_text.config(state="disabled")
