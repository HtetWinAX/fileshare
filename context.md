# LAN File Transfer — Design & Change Log

## v2.0 (current) — HTTP byte-stream transfer

The v1 system relayed every file chunk through the Python server as base64
inside JSON over a WebSocket. That caps throughput at a few MB/s and buffers
everything twice. For a 50 GB Mac→Linux job it was the wrong design.

v2 replaces it with plain HTTP byte streams and makes resume the core feature.

### Server — `main.py` (stdlib only, no FastAPI/uvicorn)

* `ThreadingHTTPServer`, one thread per connection, 1 MiB buffers.
* `GET /api/download` — streams files with `Range` support (resume/parallel).
* `PUT /api/upload?path=&offset=` — raw body append; a mismatched offset returns
  `409` with the true offset, which makes resume byte-exact.
* `GET /api/upload/status` — bytes already on disk for a path.
* `GET /api/tree` — recursive listing used by the client.
* `GET /api/tar` — streams a folder as a tar (chunked encoding, no temp file).
* `GET /api/hash` — SHA-256 of a file for verification.
* `GET /api/speedtest` — zeros, for measuring the real link.
* Path traversal blocked via `resolve()` + `relative_to(root)`.
* Optional `--token` (`X-Token` header or `?token=`).
* macOS: spawns `caffeinate` so the machine/Wi-Fi does not sleep mid-transfer.
* Serves the UI and `pull.py` itself, so the client can bootstrap with `curl`.

### Client — `pull.py` (stdlib only)

* Multi-connection (default 12), 64 MiB chunk granularity.
* Writes with `pwrite` at absolute offsets, records completed chunks in a
  `.lanferry.state` sidecar, and renames `.lanferry.part` → final only after all
  chunks are present and the length is exact.
* Retries with backoff; a dead link costs one chunk, not the file.
* Skips files whose size already matches; `--verify` compares SHA-256.
* Progress line: percent, bytes, MB/s, ETA, active file.

### UI — `index.html`

* Two modes: **Get files** (browse/download/`.tar`) and **Send files**
  (drag-drop files *or* folders, 8 MiB chunks, 4 parallel, resumable).
* Copy-paste commands with the real LAN IP filled in.
* Throughput test and live server counters.

### Measured on this machine (loopback, Python 3.13)

| Path | Throughput |
|---|---|
| Download (single stream) | ~720 MB/s |
| Pull client, 8 connections | ~357 MB/s |
| Tar stream + extract + hash | ~960 MB/s |
| Speedtest endpoint | ~4.7 GB/s |

Wi-Fi (802.11ac, 5 GHz, 585 Mbps PHY) is the limit at ~35–50 MB/s, not the code.

### Verified

* Full download hash matches source; `Range` returns exact bytes.
* Upload aborted mid-chunk resumes at the exact byte and hashes correctly.
* `pull.py` killed with SIGKILL, then re-run: all 6 files hash correctly.
* Tar stream extracts to correct hashes.
* Token auth (URL + header), 401 on missing token.
* Path traversal (`../`, `%2e%2e`) → 403.
* Filenames with `%`, spaces and unicode round-trip correctly.
* Browser upload request shape (`Blob` body) verified against the server.

### Migration note

v1 (`uvicorn main:app`) and the v1 WebSocket UI were replaced in place. The v1
UI file was not recoverable. `main.py` is now zero-dependency: run it with
`python3 main.py`, not uvicorn.

### Usage

```bash
# Mac
python3 main.py --root /path/to/data

# Linux
curl -fsSO http://<mac-ip>:8000/pull.py
python3 pull.py http://<mac-ip>:8000 /destination
```
