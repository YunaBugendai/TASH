# TASH Android Worker

Native Android Worker for TASH protocol v1.0. It connects to an existing TASH Master over TCP, performs explicit pairing, sends heartbeats/status, accepts only the built-in `cpu_benchmark` task, verifies work using the same deterministic digest algorithm as the Python Worker, and reconnects using the issued session token.

## Build

Open `android-worker/` in Android Studio and build the `app` module, or run:

```bash
./gradlew :app:assembleDebug
```

Install the resulting debug APK on an Android device on the same LAN as the Master.

## Connect

1. Start the normal TASH Master on Windows/Linux.
2. Read its pairing code and local IP address.
3. Open TASH Android Worker.
4. Enter the Master IP, port `8765`, pairing code, and a worker name.
5. Press **Connect** and explicitly authorize the Master in the Android confirmation dialog.
6. Approve the Worker on the Master dashboard.

The Android Worker does not expose a shell, execute arbitrary received code, or grant access to the device. It only implements the TASH protocol and the built-in CPU benchmark task.

## Current limitations

- LAN discovery is not yet implemented in the Android UI; enter the Master IP manually.
- GPU utilization is reported as unavailable rather than guessed.
- Android CPU utilization is not reported because a portable per-process CPU percentage is not available from the same simple API used by the desktop Worker.
- The current Android Worker is intended for foreground use; it does not claim a background service/keep-alive exemption.
