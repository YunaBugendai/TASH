"""
Entry point for the Worker application.

Run with:
    python -m worker.worker_app
"""
import logging
import socket
import tkinter as tk

from worker.gui import WorkerApp


def main():
    logging.basicConfig(level=logging.INFO,
                         format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    worker_name = socket.gethostname()
    root = tk.Tk()
    WorkerApp(root, worker_name)
    root.mainloop()


if __name__ == "__main__":
    main()
