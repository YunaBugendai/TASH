# Troubleshooting

## Worker can't find the Master ("Find Master on LAN" finds nothing)

- UDP discovery uses port `8766` and TCP uses `8765` by default -- make
  sure both are allowed through the Master machine's firewall.
  - Linux (ufw): `sudo ufw allow 8765/tcp` and `sudo ufw allow 8766/udp`
  - Windows: Windows Defender Firewall -> Advanced settings -> Inbound
    Rules -> New Rule -> allow TCP 8765 and UDP 8766 for python.exe.
- Some routers/APs (common on guest Wi-Fi or "client isolation" networks)
  block broadcast traffic between devices even on the same SSID. If
  discovery fails, just enter the Master's IP and port manually -- it's
  shown in the Master's "Add a Worker" dialog.
- Discovery only works on IPv4 broadcast; it won't cross VPNs or subnets.

## Worker gets "invalid pairing code"

The pairing code is regenerated every time the Master app starts (it's
shown at the top of the Dashboard tab). If the Master was restarted, get
the new code before retrying.

## Worker gets "protocol version mismatch"

Both apps ship from the same repository and share `shared/constants.py`,
so this should only happen if you're running mismatched versions of the
Master and Worker (e.g. one updated, one not). Make sure both machines
are running the same checkout/release.

## Worker shows "awaiting_consent" forever

That's expected until you click "Authorize this Master?" in the popup on
the Worker machine -- this is a required, explicit local confirmation and
is never skipped automatically, even if the pairing code was correct.

## Worker keeps flipping to "disconnected" / reconnecting

- Check the Master operator actually clicked "Yes" on the authorization
  popup when the Worker first registered.
- If it was previously authorized and is now failing `RECONNECT`, the
  Master process was probably restarted (its in-memory token list resets).
  The Worker will automatically fall back to a fresh `REGISTER`, which
  needs the pairing code and a new approval click.
- A heartbeat timeout (15s of silence) marks a Worker disconnected and
  requeues its in-flight chunk. Flaky Wi-Fi will do this repeatedly --
  the Worker's exponential backoff (2s, 4s, 8s... capped at 30s) is
  intentional so it doesn't hammer the network.

## GPU shows "N/A" on a machine that has a GPU

GPU info is best-effort only:
1. It tries the optional `GPUtil` package (`pip install GPUtil`) --
   NVIDIA only.
2. It falls back to calling `nvidia-smi` if that binary is on `PATH`.
3. If neither works (AMD/Intel GPUs, no drivers, or nothing installed),
   it reports `N/A` rather than guessing. This never blocks anything
   else in the app.

## `ModuleNotFoundError: No module named 'tkinter'` on Linux

Some minimal Linux distros ship Python without Tk bindings.
- Debian/Ubuntu: `sudo apt install python3-tk`
- Fedora: `sudo dnf install python3-tkinter`
- Arch: `sudo pacman -S tk`

## Distributing made things slower, not faster

This is expected for small workloads or tiny chunk sizes -- network round
trips and per-chunk overhead can outweigh the parallel compute time. Use
the Benchmark tab: if "Master + N Workers" shows a total time worse than
"Master only", the tab will say so explicitly. Try:
- A larger workload (`N`)
- A bigger chunk size, so each round trip carries more work
- Fewer, faster Workers instead of many slow/high-latency ones

## Port already in use when starting the Master

Another process (maybe a previous Master instance that didn't shut down
cleanly) is holding port 8765 or 8766.
- Linux: `lsof -i :8765` / `lsof -i :8766` to find the PID, then `kill` it.
- Windows: `netstat -ano | findstr 8765`, then `taskkill /PID <pid> /F`.
- Or edit `~/.distcomp_master.json` (created after the first run) to pick
  different ports.

## Tests fail with `ImportError` for `shared`, `master`, `worker`, or `tasks`

Run `pytest` from the repository root, not from inside `tests/`. The
included `pytest.ini` and `tests/__init__.py` are set up so this works
out of the box from the root.
