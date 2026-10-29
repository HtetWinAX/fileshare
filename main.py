#!/usr/bin/env python3
"""
lanferry - a fast, resumable LAN file transfer server.

Zero third-party dependencies. Built for moving tens of gigabytes over a
local network where the link may drop and you cannot afford to start over.

    python3 main.py                          # share the current folder on :8000
    python3 main.py --root ~/data --port 8000
    python3 main.py --token s3cret           # require ?token=s3cret / X-Token

Why it is fast:
  * raw HTTP bodies, no base64 / JSON / WebSocket framing overhead
  * 1 MiB read/write buffers, one thread per connection
  * HTTP Range on download, byte-offset PUT on upload for parallel + resume
  * no compression, no checksumming unless you ask for it

Endpoints
  GET  /                            web UI
  GET  /api/status                  server info + live counters
  GET  /api/browse?path=            one directory
  GET  /api/tree?path=              recursive listing (used by pull.py)
  GET  /api/download?path=          stream a file (Range/resume supported)
  GET  /api/tar?path=               stream a directory as a .tar
  GET  /api/hash?path=              sha256 of a file
  GET  /api/upload/status?path=     bytes already on disk for a path
  PUT  /api/upload?path=&offset=    resumable raw-body upload
  GET  /api/speedtest?mb=           stream zeros (throughput test)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import signal
import socket
import socketserver
import subprocess
import sys
import tarfile
import threading
import time
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

VERSION = "2.0.0"
BUF = 1024 * 1024  # 1 MiB

# Junk that should never be transferred from a mac/Windows box.
IGNORED_NAMES = {
    ".DS_Store",
    ".localized",
    ".Spotlight-V100",
    ".Trashes",
    ".fseventsd",
    ".TemporaryItems",
    "Thumbs.db",
    "desktop.ini",
    "__pycache__",
}

MIME_OVERRIDES = {
    ".html": "text/html; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".json": "application/json",
}


# --------------------------------------------------------------------------- #
# Shared application state
# --------------------------------------------------------------------------- #
class App:
    def __init__(self, root: Path, token: str | None, verbose: bool, ui: Path, port: int = 8000):
        self.root = root.resolve()
        self.token = token or None
        self.verbose = verbose
        self.ui = ui
        self.port = port
        self.started = time.time()
        self.lock = threading.Lock()
        self.bytes_out = 0
        self.bytes_in = 0
        self.files_out = 0
        self.files_in = 0
        # active transfers: key -> dict, used only for display
        self.active: dict[str, dict] = {}

    def add_out(self, n: int, file_done: bool = False) -> None:
        with self.lock:
            self.bytes_out += n
            if file_done:
                self.files_out += 1

    def add_in(self, n: int, file_done: bool = False) -> None:
        with self.lock:
            self.bytes_in += n
            if file_done:
                self.files_in += 1

    def journey(self, key: str, **kw) -> None:
        with self.lock:
            entry = self.active.setdefault(key, {})
            entry.update(kw)
            entry["updated"] = time.time()

    def drop_journey(self, key: str) -> None:
        with self.lock:
            self.active.pop(key, None)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def lan_ips() -> list[str]:
    """Best-effort list of non-loopback IPv4 addresses."""
    ips: list[str] = []
    try:
        # No packets are sent; this just asks the kernel which source it'd use.
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.2)
        try:
            s.connect(("8.8.8.8", 80))
            ips.append(s.getsockname()[0])
        finally:
            s.close()
    except OSError:
        pass
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ip = info[4][0]
            if ip not in ips and not ip.startswith("127."):
                ips.append(ip)
    except OSError:
        pass
    return [ip for ip in ips if not ip.startswith("127.")]


def human(n: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024 or unit == "TiB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024
    return f"{n:.1f} TiB"


def safe_join(root: Path, rel: str) -> Path:
    """Resolve `rel` inside `root`, refusing traversal and absolute paths."""
    rel = (rel or "").replace("\\", "/").strip("/")
    if "\x00" in rel:
        raise PermissionError("invalid path")
    target = (root / rel).resolve() if rel else root
    try:
        target.relative_to(root)
    except ValueError:
        raise PermissionError("path escapes share root")
    return target


def parse_range(header: str, size: int) -> tuple[int, int] | None:
    """Parse a single-range `Range: bytes=...` header. Returns (start, end_exclusive)."""
    if not header:
        return None
    m = re.match(r"^\s*bytes\s*=\s*(\d*)\s*-\s*(\d*)\s*$", header)
    if not m:
        return None
    first, last = m.group(1), m.group(2)
    if not first and not last:
        return None
    if not first:  # bytes=-N  (last N bytes)
        n = int(last)
        if n <= 0:
            return None
        return max(0, size - n), size
    start = int(first)
    end = int(last) + 1 if last else size
    if start >= size or end <= start:
        return None
    return start, min(end, size)


def iter_files(base: Path):
    """Yield (path, relpath, is_dir) under `base`, skipping junk and symlinks."""
    stack = [base]
    while stack:
        cur = stack.pop()
        try:
            entries = list(os.scandir(cur))
        except OSError:
            continue
        for e in entries:
            if e.name in IGNORED_NAMES:
                continue
            try:
                is_dir = e.is_dir(follow_symlinks=False)
                is_file = e.is_file(follow_symlinks=False)
            except OSError:
                continue
            if not (is_dir or is_file):
                continue  # symlinks, sockets, devices: skip on purpose
            p = Path(e.path)
            yield p, p.relative_to(base), is_dir
            if is_dir:
                stack.append(p)


# --------------------------------------------------------------------------- #
# Request handler
# --------------------------------------------------------------------------- #
class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = f"lanferry/{VERSION}"
    timeout = 600  # seconds of zero progress before a stalled peer is dropped

    # -- plumbing ---------------------------------------------------------- #
    @property
    def app(self) -> App:
        return self.server.app  # type: ignore[attr-defined]

    def log_message(self, fmt: str, *args) -> None:  # noqa: A003
        if not self.app.verbose:
            return
        msg = fmt % args
        # never print tokens from query strings
        msg = re.sub(r"(token=)[^&\s]+", r"\1***", msg)
        sys.stderr.write(f"[{time.strftime('%H:%M:%S')}] {self.client_address[0]} {msg}\n")

    def _send_common(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "X-Token, Content-Type, Range")
        self.send_header("Access-Control-Allow-Methods", "GET, HEAD, PUT, OPTIONS")
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Cache-Control", "no-store")

    def end_headers(self) -> None:
        super().end_headers()
        # From here on the response is committed; errors must close the socket
        # instead of trying to emit a second HTTP response.
        self._headers_sent = True

    def _json(self, status: int, obj: dict, close: bool = False) -> None:
        body = json.dumps(obj, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self._send_common()
        if close or status >= 400:
            self.close_connection = True
            self.send_header("Connection", "close")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _error(self, status: int, message: str) -> None:
        self._json(status, {"error": message}, close=True)

    def _authorized(self, query: dict) -> bool:
        token = self.app.token
        if not token:
            return True
        supplied = self.headers.get("X-Token") or (query.get("token", [""])[0])
        return supplied == token

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(HTTPStatus.NO_CONTENT)
        self._send_common()
        self.send_header("Content-Length", "0")
        self.end_headers()

    # -- routing ----------------------------------------------------------- #
    def do_GET(self) -> None:  # noqa: N802
        self._route("GET")

    def do_HEAD(self) -> None:  # noqa: N802
        self._route("HEAD")

    def do_PUT(self) -> None:  # noqa: N802
        self._route("PUT")

    def _route(self, method: str) -> None:
        self._headers_sent = False
        try:
            parts = urllib.parse.urlsplit(self.path)
            path = parts.path
            query = urllib.parse.parse_qs(parts.query, keep_blank_values=True)

            if method == "PUT":
                if path == "/api/upload":
                    if not self._authorized(query):
                        return self._error(401, "bad or missing token")
                    return self.handle_upload(query)
                return self._error(404, "not found")

            if path in ("/", "/index.html"):
                return self.serve_ui()
            if path == "/pull.py":
                if not self._authorized(query):
                    return self._error(401, "bad or missing token")
                return self.serve_static(self.app.ui.parent / "pull.py", "text/x-python")
            if path == "/favicon.ico":
                self.send_response(204)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            if path == "/api/status":
                return self.handle_status(query)
            if not self._authorized(query):
                return self._error(401, "bad or missing token")

            if path == "/api/browse":
                return self.handle_browse(query)
            if path == "/api/tree":
                return self.handle_tree(query)
            if path == "/api/download":
                return self.handle_download(query)
            if path == "/api/tar":
                return self.handle_tar(query)
            if path == "/api/hash":
                return self.handle_hash(query)
            if path == "/api/upload/status":
                return self.handle_upload_status(query)
            if path == "/api/speedtest":
                return self.handle_speedtest(query)
            return self._error(404, "not found")
        except PermissionError as e:
            self._error(403, str(e))
        except BrokenPipeError:
            self.close_connection = True
        except ConnectionResetError:
            self.close_connection = True
        except Exception as e:  # keep the server alive, report the problem
            if self.app.verbose:
                import traceback

                traceback.print_exc()
            if self._headers_sent:
                # Too late for an error response: dropping the socket is the
                # only way to tell the client the body is incomplete.
                self.close_connection = True
                return
            try:
                self._error(500, f"{type(e).__name__}: {e}")
            except Exception:
                self.close_connection = True

    # -- UI ---------------------------------------------------------------- #
    def serve_static(self, p: Path, ctype: str) -> None:
        if not p.is_file():
            return self._error(404, "not found")
        data = p.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self._send_common()
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(data)

    def serve_ui(self) -> None:
        self.serve_static(self.app.ui, "text/html; charset=utf-8")

    # -- read-only API ----------------------------------------------------- #
    def handle_status(self, query: dict) -> None:
        authorized = self._authorized(query)
        with self.app.lock:
            out, inn = self.app.bytes_out, self.app.bytes_in
            fout, fin = self.app.files_out, self.app.files_in
            active = list(self.app.active.values())
        self._json(
            200,
            {
                "app": "lanferry",
                "version": VERSION,
                "root": str(self.app.root) if authorized else None,
                "uptime": round(time.time() - self.app.started, 1),
                "bytes_out": out,
                "bytes_in": inn,
                "files_out": fout,
                "files_in": fin,
                "token_required": bool(self.app.token),
                "authorized": authorized,
                "urls": [f"http://{ip}:{self.app.port}" for ip in lan_ips()],
                "active": active,
            },
        )

    def handle_browse(self, query: dict) -> None:
        rel = query.get("path", [""])[0]
        folder = safe_join(self.app.root, rel)
        if not folder.exists():
            return self._error(404, "no such folder")
        if not folder.is_dir():
            return self._error(400, "not a folder")
        items = []
        try:
            with os.scandir(folder) as it:
                for e in it:
                    if e.name in IGNORED_NAMES:
                        continue
                    try:
                        st = e.stat(follow_symlinks=False)
                        is_dir = e.is_dir(follow_symlinks=False)
                        is_file = e.is_file(follow_symlinks=False)
                    except OSError:
                        continue
                    if not (is_dir or is_file):
                        continue
                    items.append(
                        {
                            "name": e.name,
                            "path": (Path(rel) / e.name).as_posix() if rel else e.name,
                            "type": "dir" if is_dir else "file",
                            "size": 0 if is_dir else st.st_size,
                            "mtime": st.st_mtime,
                        }
                    )
        except PermissionError:
            return self._error(403, "permission denied")
        items.sort(key=lambda i: (i["type"] != "dir", i["name"].lower()))
        self._json(200, {"path": rel, "parent": str(Path(rel).parent) if rel else None, "items": items})

    def handle_tree(self, query: dict) -> None:
        rel = query.get("path", [""])[0]
        base = safe_join(self.app.root, rel)
        if not base.exists():
            return self._error(404, "no such path")
        if base.is_dir():
            entries = []
            for p, r, is_dir in iter_files(base):
                st = p.stat(follow_symlinks=False)
                entries.append(
                    {
                        "path": p.relative_to(self.app.root).as_posix(),
                        "type": "dir" if is_dir else "file",
                        "size": 0 if is_dir else st.st_size,
                        "mtime": st.st_mtime,
                    }
                )
            entries.sort(key=lambda e: e["path"])
        else:
            st = base.stat()
            entries = [
                {
                    "path": base.relative_to(self.app.root).as_posix(),
                    "type": "file",
                    "size": st.st_size,
                    "mtime": st.st_mtime,
                }
            ]
        total = sum(e["size"] for e in entries if e["type"] == "file")
        self._json(200, {"root": str(base), "path": rel, "total": total, "entries": entries})

    def handle_hash(self, query: dict) -> None:
        rel = query.get("path", [""])[0]
        target = safe_join(self.app.root, rel)
        if not target.is_file():
            return self._error(404, "no such file")
        h = hashlib.sha256()
        with target.open("rb") as f:
            while True:
                chunk = f.read(4 * BUF)
                if not chunk:
                    break
                h.update(chunk)
        self._json(200, {"path": rel, "size": target.stat().st_size, "sha256": h.hexdigest()})

    def handle_upload_status(self, query: dict) -> None:
        rel = query.get("path", [""])[0]
        if not rel:
            return self._error(400, "path required")
        target = safe_join(self.app.root, rel)
        if target.exists() and target.is_dir():
            return self._json(200, {"path": rel, "exists": True, "size": 0, "is_dir": True})
        size = target.stat().st_size if target.is_file() else 0
        self._json(200, {"path": rel, "exists": target.is_file(), "size": size, "is_dir": False})

    # -- download ---------------------------------------------------------- #
    def handle_download(self, query: dict) -> None:
        rel = query.get("path", [""])[0]
        if not rel:
            return self._error(400, "path required")
        target = safe_join(self.app.root, rel)
        if not target.exists():
            return self._error(404, "no such file")
        if target.is_dir():
            return self._error(400, "is a directory - use /api/tar")
        size = target.stat().st_size
        mtime = target.stat().st_mtime
        etag = f'"{size:x}-{int(mtime):x}"'

        start, end = 0, size
        partial = False
        rng = self.headers.get("Range")
        if rng:
            parsed = parse_range(rng, size)
            if parsed is None:
                self.send_response(416)
                self.send_header("Content-Range", f"bytes */{size}")
                self.send_header("Content-Length", "0")
                self._send_common()
                self.end_headers()
                return
            start, end = parsed
            partial = True

        length = max(0, end - start)
        self.send_response(206 if partial else 200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(length))
        self.send_header("Content-Disposition", f'attachment; filename*=UTF-8\'\'{urllib.parse.quote(target.name)}')
        self.send_header("ETag", etag)
        self.send_header("Last-Modified", self.date_time_string(mtime))
        if partial:
            self.send_header("Content-Range", f"bytes {start}-{end - 1}/{size}")
        self._send_common()
        self.end_headers()

        if self.command == "HEAD" or length == 0:
            return

        key = f"out:{rel}"
        self.app.journey(key, kind="download", path=rel, size=size, started=time.time(), client=self.client_address[0])
        try:
            with target.open("rb") as f:
                f.seek(start)
                remaining = length
                while remaining > 0:
                    chunk = f.read(min(BUF, remaining))
                    if not chunk:
                        break
                    self.connection.sendall(chunk)
                    remaining -= len(chunk)
                    self.app.add_out(len(chunk))
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True
        finally:
            self.app.add_out(0, file_done=(remaining == 0))
            self.app.drop_journey(key)

    def handle_tar(self, query: dict) -> None:
        rel = query.get("path", [""])[0]
        base = safe_join(self.app.root, rel)
        if not base.is_dir():
            return self._error(400, "not a directory")

        self.send_response(200)
        self.send_header("Content-Type", "application/x-tar")
        self.send_header("Content-Disposition", f'attachment; filename="{base.name}.tar"')
        self.send_header("Transfer-Encoding", "chunked")
        self._send_common()
        self.end_headers()

        class ChunkWriter:
            """Minimal chunked-transfer writer that tarfile can stream into."""

            def __init__(self, wfile, app):
                self.wfile = wfile
                self.app = app
                self.buf = bytearray()
                self.closed = False

            def write(self, data) -> int:
                self.buf += data
                if len(self.buf) >= 256 * 1024:
                    self.flush()
                return len(data)

            def flush(self) -> None:
                if self.buf:
                    self.wfile.write(f"{len(self.buf):X}\r\n".encode())
                    self.wfile.write(self.buf)
                    self.wfile.write(b"\r\n")
                    self.app.add_out(len(self.buf))
                    self.buf.clear()

            def tell(self) -> int:
                return 0  # stream mode never needs a real offset

            def close(self) -> None:
                if not self.closed:
                    self.flush()
                    self.wfile.write(b"0\r\n\r\n")
                    self.closed = True

        writer = ChunkWriter(self.wfile, self.app)
        key = f"tar:{rel}"
        self.app.journey(key, kind="tar", path=rel, size=0, started=time.time(), client=self.client_address[0])
        try:
            with tarfile.open(fileobj=writer, mode="w|") as tar:  # type: ignore[arg-type]
                for p, r, is_dir in iter_files(base):
                    arcname = (Path(base.name) / r).as_posix()
                    tar.add(str(p), arcname=arcname, recursive=False)
            writer.close()
            self.app.add_out(0, file_done=True)
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True
        finally:
            self.app.drop_journey(key)

    def handle_speedtest(self, query: dict) -> None:
        try:
            mb = max(1, min(1024, int(query.get("mb", ["32"])[0])))
        except ValueError:
            mb = 32
        length = mb * BUF
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(length))
        self._send_common()
        self.end_headers()
        if self.command == "HEAD":
            return
        block = b"\0" * BUF
        try:
            for _ in range(mb):
                self.connection.sendall(block)
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True

    # -- upload ------------------------------------------------------------ #
    def handle_upload(self, query: dict) -> None:
        rel = query.get("path", [""])[0]
        if not rel:
            return self._error(400, "path required")
        target = safe_join(self.app.root, rel)
        if target.exists() and target.is_dir():
            return self._error(400, "target is a directory")

        try:
            length = int(self.headers.get("Content-Length", ""))
        except ValueError:
            return self._error(411, "Content-Length required")
        if length < 0:
            return self._error(400, "bad Content-Length")

        try:
            offset = int(query.get("offset", ["0"])[0])
        except ValueError:
            return self._error(400, "bad offset")
        try:
            expected = int(query.get("size", ["-1"])[0])
        except ValueError:
            expected = -1
        try:
            mtime = float(query.get("mtime", ["0"])[0])
        except ValueError:
            mtime = 0.0

        target.parent.mkdir(parents=True, exist_ok=True)
        existing = target.stat().st_size if target.is_file() else 0

        # `restart=1` throws away a partial/oversized file and starts clean.
        if query.get("restart", ["0"])[0] == "1":
            try:
                target.unlink(missing_ok=True)
            except OSError:
                return self._error(500, "cannot replace existing file")
            existing = 0

        # Resume contract: the client must continue at exactly the current EOF.
        if offset != existing:
            return self._json(409, {"error": "offset mismatch", "offset": existing}, close=True)

        key = f"in:{rel}"
        self.app.journey(
            key, kind="upload", path=rel, size=max(expected, existing + length),
            started=time.time(), client=self.client_address[0],
        )
        written = 0
        complete = False
        new_size = offset
        try:
            mode = "r+b" if target.is_file() else "wb"
            with target.open(mode) as f:
                f.seek(offset)
                f.truncate(offset)
                remaining = length
                while remaining > 0:
                    chunk = self.rfile.read(min(BUF, remaining))
                    if not chunk:
                        break  # peer went away; keep what we have for resume
                    f.write(chunk)
                    written += len(chunk)
                    remaining -= len(chunk)
                    self.app.add_in(len(chunk))
                f.flush()
                new_size = offset + written
                complete = expected >= 0 and new_size >= expected
            if complete and mtime > 0:
                try:
                    os.utime(target, (mtime, mtime))
                except OSError:
                    pass
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True
        finally:
            self.app.add_in(0, file_done=complete)
            self.app.drop_journey(key)

        if self.close_connection:
            return
        self._json(
            200,
            {
                "path": rel,
                "offset": offset,
                "written": written,
                "size": new_size,
                "complete": complete,
            },
        )


class Server(socketserver.ThreadingMixIn, HTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 256

    def __init__(self, addr, handler, app: App):
        self.app = app
        super().__init__(addr, handler)

    def server_bind(self) -> None:
        # BaseServer.server_bind does the hostname lookup; keep it, the
        # ThreadingMixIn handles the rest.
        super().server_bind()


# --------------------------------------------------------------------------- #
# Startup
# --------------------------------------------------------------------------- #
def main() -> int:
    here = Path(__file__).resolve().parent
    p = argparse.ArgumentParser(
        description="Fast resumable LAN file transfer server (zero dependencies).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--root", default=os.getcwd(), help="folder to share")
    p.add_argument("--port", type=int, default=8000, help="TCP port")
    p.add_argument("--bind", default="0.0.0.0", help="address to bind")
    p.add_argument("--token", default="", help="optional shared secret")
    p.add_argument("--verbose", action="store_true", help="log every request")
    args = p.parse_args()

    root = Path(args.root).expanduser().resolve()
    if not root.is_dir():
        print(f"error: --root is not a directory: {root}", file=sys.stderr)
        return 2

    app = App(root=root, token=args.token, verbose=args.verbose, ui=here / "index.html", port=args.port)
    try:
        httpd = Server((args.bind, args.port), Handler, app)
    except OSError as e:
        print(f"error: cannot bind {args.bind}:{args.port} - {e}", file=sys.stderr)
        print("hint: is another copy already running? try --port 8001", file=sys.stderr)
        return 1

    shown = "127.0.0.1" if args.bind in ("0.0.0.0", "::", "") else args.bind
    urls = [f"http://{ip}:{args.port}" for ip in lan_ips()] or [f"http://{shown}:{args.port}"]

    print()
    print(f"  lanferry {VERSION}  -  serving {root}")
    print(f"  local    http://{shown}:{args.port}")
    print(f"  lan      {urls[0]}")
    if args.token:
        print(f"  token    {args.token}  (append ?token=... or send X-Token)")
    print()
    print(f"  fastest way to pull 50 GB on the other machine:")
    print(f"    python3 pull.py {urls[0]} /destination/folder")
    print(f"  or from a browser:")
    print(f"    open {urls[0]}")
    print()
    print("  Ctrl+C to stop.")
    print(flush=True)

    # Keep the machine (and its Wi-Fi radio) awake for the duration of the run.
    if sys.platform == "darwin" and shutil.which("caffeinate"):
        try:
            subprocess.Popen(
                ["caffeinate", "-i", "-m", "-s", "-w", str(os.getpid())],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            print("  sleep disabled while this server runs (caffeinate)")
            print(flush=True)
        except OSError:
            pass

    def _stop(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _stop)
    try:
        httpd.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        print("\n  stopping...")
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
