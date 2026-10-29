# lanferry — fast, resumable LAN file transfer

Move tens of gigabytes between a Mac and a Linux box over plain HTTP.
No cloud, no accounts, no dependencies (Python 3.9+ only).

Built for one job: **50 GB from this Mac to a Linux machine, on Wi-Fi, without
losing progress when the link hiccups.**

---

## Quickstart (2 commands)

**On the Mac (the machine that has the files):**

```bash
cd /Users/asiamatrix/Documents/allow_file_share
python3 main.py --root "/path/to/the/50gb"
```

It prints something like:

```
lanferry 2.0.0  -  serving /path/to/the/50gb
lan      http://192.168.1.3:8000
python3 pull.py http://192.168.1.3:8000 /destination/folder
```

**On the Linux box (the machine receiving them):**

```bash
python3 pull.py http://192.168.1.3:8000 ~/incoming
```

That's it. `pull.py` is a single file — fetch it straight from the Mac:

```bash
# on the Linux box
curl -fsSO http://192.168.1.3:8000/pull.py
python3 pull.py http://192.168.1.3:8000 ~/incoming
```

---

## How fast is it?

At 585 Mbps Wi-Fi (802.11ac, 5 GHz, what this Mac negotiates), expect
**35–50 MB/s**, so:

| Link | Realistic speed | 50 GB takes |
|---|---|---|
| Gigabit Ethernet | ~110 MB/s | ~8 min |
| Wi-Fi 5 GHz (this Mac) | ~35–50 MB/s | ~20–25 min |
| Wi-Fi 2.4 GHz | ~8–12 MB/s | ~1.5 h |

The server itself does **~700 MB/s** on loopback, so the network is the limit,
not the code.

Before committing 50 GB, click **speed test** in the web UI (or run
`curl -o /dev/null http://192.168.1.3:8000/api/speedtest?mb=128`) to see the real
rate. If it is under ~20 MB/s, see *Make it faster* below.

---

## Why this does not waste your 4 hours

* **Resume at 64 MiB granularity.** Kill it, lose Wi-Fi, reboot — rerun the same
  command and it continues from the last completed chunk.
* **12 parallel connections** by default (`--connections 16` if you like).
* **No re-encoding.** Raw bytes over HTTP; no base64, no JSON framing, no
  WebSocket relay through the server.
* **Size + SHA-256 verification** (`--verify`) proves the copy is intact.
* **Partial files never look complete.** Data lands in `*.lanferry.part` and is
  renamed only when every chunk is present and the length is exact.

If a run is interrupted, just do the same thing again:

```bash
python3 pull.py http://192.168.1.3:8000 ~/incoming
```

It prints `resuming 31.2 GiB already present, 18.8 GiB to go`.

---

## Useful options

```bash
# one sub-folder only, 16 connections, verify at the end
python3 pull.py http://192.168.1.3:8000 ~/incoming --only "videos/2026" --connections 16 --verify

# see what would be transferred (no writes)
python3 pull.py http://192.168.1.3:8000 ~/incoming --dry-run

# list the remote tree
python3 pull.py http://192.168.1.3:8000 ~/incoming --list

# glob filters
python3 pull.py http://192.168.1.3:8000 ~/incoming --include '*.mp4' --exclude '*cache*'

# server with a shared secret
python3 main.py --token s3cret
python3 pull.py http://192.168.1.3:8000 ~/incoming --token s3cret
```

Other `main.py` options: `--root DIR`, `--port N`, `--bind ADDR`, `--verbose`.
On macOS it automatically runs `caffeinate` so the machine and Wi-Fi do not
sleep mid-transfer.

---

## Browser UI (no CLI needed)

Open `http://192.168.1.3:8000/` on either machine.

* **Get files** — browse the share, download individual files, or stream a whole
  folder as `.tar`. Single-stream, so use `pull.py` for the big stuff.
* **Send files** — drag files *or folders* to upload to the Mac. 8 MiB chunks,
  4 files in parallel, resumable: re-drop the same files and it continues.
* **speed test** — measures the real link throughput.

---

## Verify 50 GB actually arrived intact

```bash
# on the Linux box, after the pull
python3 pull.py http://192.168.1.3:8000 ~/incoming --verify
```

`--verify` re-reads every file and compares SHA-256 with the source. It is I/O
bound (~1–2 GB/s on SSD), so budget ~1 minute per 50 GB. Run it once at the end,
not during the transfer.

Spot-check a single file instead:

```bash
curl "http://192.168.1.3:8000/api/hash?path=big.zip"     # server's sha256
sha256sum ~/incoming/big.zip                             # local
```

---

## Make it faster

1. **Use Ethernet if you can.** A $15 USB-C gigabit adapter turns 25 minutes
   into 8. This Mac only has Wi-Fi active right now (`en0`, 5 GHz, 585 Mbps).
2. **Put both machines on the same 5 GHz SSID.** 2.4 GHz is 4–8× slower.
3. **Sit closer / line of sight.** -57 dBm is fine; below -70 dBm throughput
   collapses.
4. **Turn off VPNs and "Wi-Fi Assist".** They route LAN traffic the long way.
5. **Do not run the transfer through a browser tab** for the full 50 GB; use
   `pull.py`.
6. Bump connections: `--connections 16` (more streams help on lossy Wi-Fi).

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `cannot reach http://…` | Wrong IP. The server prints the correct one at startup. Check both machines are on the same subnet (`192.168.1.x`). |
| Hangs at 0 bytes | macOS firewall. System Settings → Network → Firewall → allow incoming for Python, or turn it off temporarily. |
| `403 path escapes share root` | You asked for a path outside `--root`. Point `--root` at the right folder. |
| `address already in use` | Another copy is running: `lsof -i :8000`, or use `--port 8001`. |
| Transfer dies when the Mac sleeps | Should not happen (caffeinate). Check `pmset -g` / keep it plugged in. |
| Wi-Fi client isolation | Some guest/office networks block device-to-device traffic. Use a phone hotspot or a normal SSID. |
| Linux says "No space left" | `pull.py` warns up front; free space or pull a subset with `--only`. |
| Want to cancel | `Ctrl+C`. Progress is saved. |

---

## How it works (30 seconds)

```
Mac                                                      Linux
┌──────────────────────────┐                    ┌───────────────────────┐
│ main.py :8000            │  GET /api/tree     │ pull.py               │
│  GET  /api/tree          │ ─────────────────► │  plans files          │
│  GET  /api/download      │                    │  12 worker threads    │
│    Range: bytes=a-b      │ ◄───────────────── │  pwrite at offsets    │
│  PUT  /api/upload        │  206 + bytes       │  records done chunks  │
│    ?offset=…             │                    │  rename .part → final │
└──────────────────────────┘                    └───────────────────────┘
```

Every chunk is a normal HTTP range request, so any proxy/tool can also read the
files. Uploads use `PUT` with an explicit `offset`, and the server rejects a
mismatched offset with `409` plus the real offset — that is what makes resume
byte-exact.

API: `/api/status`, `/api/browse`, `/api/tree`, `/api/download`, `/api/tar`,
`/api/hash`, `/api/upload`, `/api/upload/status`, `/api/speedtest`.

---

## Security

No authentication by default — anyone on your LAN can read/write the share.
That is the point (zero friction), but do not run it on public Wi-Fi.
Use `--token` when you want a shared secret, and remember to stop the server
(`Ctrl+C`) when you are done.

---

## Files

| File | Purpose |
|---|---|
| `main.py` | Server + web UI host. Zero dependencies, stdlib only. |
| `pull.py` | Parallel, resumable client. Copy this to the Linux box. |
| `index.html` | Browser UI (served by `main.py`). |
| `context.md` | Change log / design notes. |
