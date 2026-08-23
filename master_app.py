"""
Entry point for the Master application.

Run with:
    python -m master.master_app
"""
import logging
import tkinter as tk

from master.config import MasterConfig
from master.gui import MasterApp
from shared import crypto


def main():
    logging.basicConfig(level=logging.INFO,
                         format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = MasterConfig.load()
    pairing_code = crypto.generate_pairing_code()

    root = tk.Tk()
    app = MasterApp(root, config, pairing_code)
    try:
        root.mainloop()
    finally:
        app.server.stop()
        config.save()


if __name__ == "__main__":
    main()
