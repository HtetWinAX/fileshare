#!/usr/bin/env python3
"""
lanferry pull - fast, resumable client for main.py. Stdlib only.

Runs on the receiving machine (Linux/Windows/macOS) and pulls a folder over
plain HTTP using many parallel connections. Every completed 64 MiB chunk is
recorded, so a dropped Wi-Fi link costs you seconds, not gigabytes.

    python3 pull.py http://192.168.1.3:8000 ~/incoming
    python3 pull.py http://192.168.1.3:8000 ~/incoming --only "files"
    python3 pull.py http://192.168.1.3:8000 ~/incoming --connections 16
    python3 pull.py http://192.168.1.3:8000 ~/incoming --verify
"""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import os
import shutil
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

CHUNK = 64 * 1024 * 1024          # granularity of resume
IO = 1024 * 1024                  # socket read size
MAX_RETRIES = 6
TIMEOUT = 120


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def human(n: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024 or unit == "TiB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TiB"


def duration(seconds: float) -> str:
    if seconds <= 0 or seconds > 86400 * 7:
        return "--"
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h{m:02d}m"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


class Ctx:
    def __init__(self, args):
        self.base = args.url.rstrip("/")
        self.dest = Path(args.dest).expanduser().resolve()
        self.token = args.token
        self.connections = args.connections
        self.overwrite = args.overwrite
        self.verify = args.verify
        self.include = args.include
        self.exclude = args.exclude
        self.quiet = args.quiet

    def url(self, endpoint: str, **params) -> str:
        if self.token:
            params["token"] = self.token
        q = urllib.parse.urlencode(params)
        return f"{self.base}{endpoint}" + (f"?{q}" if q else "")

    def open(self, endpoint: str, headers: dict | None = None, **params):
        req = urllib.request.Request(self.url(endpoint, **params))
        req.add_header("User-Agent", "lanferry-pull/2.0")
        if self.token:
            req.add_header("X-Token", self.token)
        for k, v in (headers or {}).items():
            req.add_header(k, v)
        return urllib.request.urlopen(req, timeout=TIMEOUT)


class Progress:
    def __init__(self, total_bytes: int, total_files: int, quiet: bool = False):
        self.lock = threading.Lock()
        self.total_bytes = total_bytes
        self.quiet = quiet
        self.done_bytes = 0
        self.total_files = total_files
        self.done_files = 0
        self.skipped_files = 0
        self.failed: list[str] = []
        self.active: dict[str, int] = {}
        self.start = time.time()
        self.last_t = self.start
        self.last_bytes = 0
        self.speed = 0.0
        self.stop = False
        self.rows_written = 0

    def add(self, n: int) -> None:
        with self.lock:
            self.done_bytes += n

    def begin(self, rel: str) -> None:
        with self.lock:
            self.active[rel] = 0

    def file_add(self, rel: str, n: int) -> None:
        with self.lock:
            self.active[rel] = self.active.get(rel, 0) + n
            self.done_bytes += n

    def file_sub(self, rel: str, n: int) -> None:
        """Undo progress for bytes that will be re-sent after a retry."""
        with self.lock:
            self.active[rel] = max(0, self.active.get(rel, 0) - n)
            self.done_bytes = max(0, self.done_bytes - n)

    def end(self, rel: str, ok: bool) -> None:
        with self.lock:
            self.active.pop(rel, None)
            if ok:
                self.done_files += 1

    def fail(self, rel: str) -> None:
        with self.lock:
            self.active.pop(rel, None)
            self.failed.append(rel)

    def skip(self) -> None:
        with self.lock:
            self.skipped_files += 1

    def _snapshot(self):
        now = time.time()
        dt = now - self.last_t
        if dt >= 0.4:
            self.speed = (self.done_bytes - self.last_bytes) / dt
            self.last_t = now
            self.last_bytes = self.done_bytes
        pct = (self.done_bytes / self.total_bytes * 100) if self.total_bytes else 100.0
        remain = self.total_bytes - self.done_bytes
        eta = remain / self.speed if self.speed > 1024 else 0
        with self.lock:
            active = sorted(self.active.items(), key=lambda kv: -kv[1])[:2]
        return pct, eta, active

    def render(self, final: bool = False) -> None:
        pct, eta, active = self._snapshot()
        current = ""
        if active:
            rel, got = active[0]
            name = Path(rel).name
            if len(name) > 34:
                name = name[:31] + "..."
            current = f"  {name}"
        line = (
            f"\r{pct:5.1f}%  {human(self.done_bytes)}/{human(self.total_bytes)}"
            f"  {human(self.speed)}/s  eta {duration(eta)}"
            f"  [{self.done_files + self.skipped_files}/{self.total_files} files]"
            f"{current}"
        )
        if self.quiet:
            return
        if sys.stderr.isatty():
            sys.stderr.write(line + " " * 8)
            if final:
                sys.stderr.write("\n")
            sys.stderr.flush()
        else:
            # Redirected to a file: one line every ~5 s, no carriage returns.
            now = time.time()
            if final or now - getattr(self, "_last_print", 0) >= 5:
                self._last_print = now
                sys.stderr.write(line.lstrip("\r").rstrip() + "\n")
                sys.stderr.flush()

    def run(self) -> None:
        while not self.stop:
            self.render()
            time.sleep(0.5)


# --------------------------------------------------------------------------- #
# remote metadata
# --------------------------------------------------------------------------- #
def fetch_tree(ctx: Ctx, remote: str) -> list[dict]:
    with ctx.open("/api/tree", path=remote) as r:
        data = json.loads(r.read())
    return data["entries"]


def fetch_hash(ctx: Ctx, rel: str) -> str:
    with ctx.open("/api/hash", path=rel) as r:
        return json.loads(r.read())["sha256"]


# --------------------------------------------------------------------------- #
# transfer
# --------------------------------------------------------------------------- #
class FileJob:
    def __init__(self, rel: str, size: int, mtime: float, ctx: Ctx):
        self.rel = rel
        self.size = size
        self.mtime = mtime
        self.final = ctx.dest / rel
        self.part = Path(str(self.final) + ".lanferry.part")
        self.state = Path(str(self.final) + ".lanferry.state")
        self.nchunks = max(1, (size + CHUNK - 1) // CHUNK)
        self.done: set[int] = set()
        self.lock = threading.Lock()
        self.finished = False

    # -- resume bookkeeping ------------------------------------------------ #
    def load_state(self) -> int:
        """Return number of bytes already fetched."""
        if not self.state.is_file() or not self.part.is_file():
            self.reset()
            return 0
        try:
            data = json.loads(self.state.read_text())
            if data.get("size") != self.size or data.get("chunk") != CHUNK:
                self.reset()
                return 0
            self.done = {int(i) for i in data.get("done", [])}
        except (OSError, ValueError, TypeError):
            self.reset()
            return 0
        have = min(self.part.stat().st_size, self.size)
        return min(self.size, len(self.done) * CHUNK)

    def reset(self) -> None:
        self.done = set()
        try:
            self.part.unlink(missing_ok=True)
            self.state.unlink(missing_ok=True)
        except OSError:
            pass

    def save_state(self) -> None:
        tmp = Path(str(self.state) + ".tmp")
        payload = json.dumps({"size": self.size, "chunk": CHUNK, "done": sorted(self.done)})
        try:
            tmp.write_text(payload)
            os.replace(tmp, self.state)
        except OSError:
            pass

    def finish(self) -> None:
        with self.lock:
            if self.finished:
                return
            self.finished = True
        if not self.part.is_file():
            # State says done but the data file is gone: start this one over.
            self.done = set()
            self.reset()
            self.finished = False
            return
        # Make sure the file has its exact final length, then publish it.
        with self.part.open("r+b") as f:
            f.truncate(self.size)
            f.flush()
            os.fsync(f.fileno())
        os.replace(self.part, self.final)
        if self.mtime > 0:
            try:
                os.utime(self.final, (self.mtime, self.mtime))
            except OSError:
                pass
        self.state.unlink(missing_ok=True)


def pwrite_all(fd: int, data: bytes, offset: int) -> None:
    view = memoryview(data)
    written = 0
    while written < len(view):
        n = os.pwrite(fd, view[written:], offset + written)
        if n <= 0:
            raise IOError("short write")
        written += n


def transfer_chunk(ctx: Ctx, job: FileJob, index: int, progress: Progress) -> None:
    start = index * CHUNK
    end = min(start + CHUNK, job.size)
    expected = end - start
    url = ctx.url("/api/download", path=job.rel)

    for attempt in range(MAX_RETRIES):
        got = 0
        try:
            req = urllib.request.Request(url, headers={"Range": f"bytes={start}-{end - 1}"})
            if ctx.token:
                req.add_header("X-Token", ctx.token)
            with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
                if r.status == 200:
                    # Server ignored Range: skip forward to our chunk.
                    to_skip = start
                    while to_skip > 0:
                        block = r.read(min(IO, to_skip))
                        if not block:
                            raise IOError("short read while skipping")
                        to_skip -= len(block)
                elif r.status != 206:
                    raise IOError(f"unexpected HTTP {r.status}")

                fd = os.open(job.part, os.O_WRONLY | os.O_CREAT, 0o644)
                try:
                    pos = start
                    while pos - start < expected:
                        block = r.read(min(IO, expected - (pos - start)))
                        if not block:
                            break
                        pwrite_all(fd, block, pos)
                        pos += len(block)
                        got += len(block)
                        progress.file_add(job.rel, len(block))
                finally:
                    os.close(fd)

            if got < expected:
                raise IOError(f"incomplete chunk ({got}/{expected} bytes)")
            with job.lock:
                job.done.add(index)
                job.save_state()
                remaining = job.nchunks - len(job.done)
            if remaining == 0:
                job.finish()
                progress.end(job.rel, True)
            return
        except urllib.error.HTTPError as e:
            if e.code in (401, 403, 404):
                progress.file_sub(job.rel, got)
                progress.fail(job.rel)
                if not ctx.quiet:
                    sys.stderr.write(f"\n  ! {job.rel}: HTTP {e.code}\n")
                return
            progress.file_sub(job.rel, got)
            if attempt == MAX_RETRIES - 1:
                progress.fail(job.rel)
                if not ctx.quiet:
                    sys.stderr.write(f"\n  ! {job.rel}: HTTP {e.code}\n")
                return
            time.sleep(min(10.0, 0.5 * (2**attempt)))
        except (urllib.error.URLError, OSError, IOError) as e:
            progress.file_sub(job.rel, got)
            if attempt == MAX_RETRIES - 1:
                progress.fail(job.rel)
                if not ctx.quiet:
                    sys.stderr.write(f"\n  ! {job.rel}: {e}\n")
                return
            time.sleep(min(10.0, 0.5 * (2**attempt)))


def prepare(ctx: Ctx, entries: list[dict]) -> tuple[list[FileJob], list[str]]:
    jobs: list[FileJob] = []
    empty_dirs: list[str] = []
    for e in entries:
        rel = e["path"]
        if e["type"] == "dir":
            empty_dirs.append(rel)
            continue
        if not matches(ctx, rel):
            continue
        jobs.append(FileJob(rel, int(e["size"]), float(e.get("mtime", 0)), ctx))
    return jobs, empty_dirs


def matches(ctx: Ctx, rel: str) -> bool:
    if ctx.include and not any(fnmatch.fnmatch(rel, pat) for pat in ctx.include):
        return False
    if ctx.exclude and any(fnmatch.fnmatch(rel, pat) for pat in ctx.exclude):
        return False
    return True


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #
def main() -> int:
    p = argparse.ArgumentParser(
        description="Fast resumable pull client for lanferry (stdlib only).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("url", help="http://server-ip:port")
    p.add_argument("dest", help="local destination folder")
    p.add_argument("--only", default="", help="remote sub-folder to pull (default: whole share)")
    p.add_argument("--connections", type=int, default=12, help="parallel HTTP connections")
    p.add_argument("--include", action="append", default=[], help="glob to include (repeatable)")
    p.add_argument("--exclude", action="append", default=[], help="glob to exclude (repeatable)")
    p.add_argument("--token", default="", help="server token, if enabled")
    p.add_argument("--overwrite", action="store_true", help="re-download files whose size differs")
    p.add_argument("--verify", action="store_true", help="sha256 every file after download (slow)")
    p.add_argument("--dry-run", action="store_true", help="list what would be transferred")
    p.add_argument("--list", action="store_true", help="print the remote tree and exit")
    p.add_argument("--quiet", action="store_true", help="no progress line")
    args = p.parse_args()

    ctx = Ctx(args)
    try:
        entries = fetch_tree(ctx, args.only)
    except urllib.error.HTTPError as e:
        print(f"error: server returned HTTP {e.code} for {args.url}", file=sys.stderr)
        if e.code == 401:
            print("hint: this server needs --token", file=sys.stderr)
        return 2
    except (urllib.error.URLError, OSError) as e:
        print(f"error: cannot reach {args.url} ({e})", file=sys.stderr)
        print("hint: same Wi-Fi/LAN? check the IP and that main.py is running", file=sys.stderr)
        return 2

    jobs, dirs = prepare(ctx, entries)

    if args.list:
        for j in jobs:
            print(f"{human(j.size):>10}  {j.rel}")
        print(f"\n{len(jobs)} files, {human(sum(j.size for j in jobs))}")
        return 0

    if not jobs:
        print("nothing to transfer (check --include / --exclude)")
        return 0

    total = sum(j.size for j in jobs)
    if not args.quiet:
        probe = ctx.dest
        while not probe.exists() and probe != probe.parent:
            probe = probe.parent
        free = shutil.disk_usage(probe).free
        print(f"remote  {args.url}  ({human(total)} in {len(jobs)} files)")
        print(f"dest    {ctx.dest}  ({human(free)} free)")
        if free < total:
            print(f"WARNING: destination needs {human(total - free)} more space", file=sys.stderr)

    if args.dry_run:
        for j in jobs:
            print(f"{human(j.size):>10}  {j.rel}")
        print(f"\n{len(jobs)} files, {human(total)} (dry run)")
        return 0

    ctx.dest.mkdir(parents=True, exist_ok=True)
    for d in dirs:
        (ctx.dest / d).mkdir(parents=True, exist_ok=True)

    # Work out what is already there before counting, so the ETA is honest.
    todo: list[tuple[FileJob, list[int]]] = []
    skipped_bytes = 0
    for job in jobs:
        job.final.parent.mkdir(parents=True, exist_ok=True)
        if job.final.is_file() and not job.final.is_symlink():
            have = job.final.stat().st_size
            if have == job.size:
                skipped_bytes += job.size
                continue
            if not args.overwrite:
                print(f"  skip (size differs, use --overwrite): {job.rel}", file=sys.stderr)
                skipped_bytes += job.size
                continue
            job.final.unlink(missing_ok=True)

        if job.size == 0:
            job.final.write_bytes(b"")
            continue

        already = job.load_state()
        missing = [i for i in range(job.nchunks) if i not in job.done]
        if not missing:
            job.finish()
            if job.final.is_file():
                skipped_bytes += job.size
                continue
            missing = list(range(job.nchunks))
        skipped_bytes += min(already, job.size)
        todo.append((job, missing))

    done_files_already = len(jobs) - len(todo)
    progress = Progress(total, len(jobs), args.quiet)
    progress.done_bytes = skipped_bytes
    progress.done_files = done_files_already
    progress.last_bytes = skipped_bytes

    if not todo:
        if not args.quiet:
            print(f"already up to date: {len(jobs)} files, {human(total)}")
        return 0

    if not args.quiet:
        remaining = total - skipped_bytes
        print(f"resuming {human(skipped_bytes)} already present, {human(remaining)} to go\n")

    stop = threading.Event()

    def printer():
        while not stop.is_set():
            progress.render()
            time.sleep(0.5)

    t = threading.Thread(target=printer, daemon=True)
    t.start()

    workers = max(1, min(args.connections, max(1, len(todo))))

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = []
        for job, missing in todo:
            progress.begin(job.rel)
            for idx in missing:
                futures.append(pool.submit(transfer_chunk, ctx, job, idx, progress))
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as e:  # never kill the whole run
                if not args.quiet:
                    sys.stderr.write(f"\n  ! worker error: {e}\n")

    stop.set()
    t.join(timeout=1)
    progress.render(final=True)

    if args.verify and not progress.failed:
        if not args.quiet:
            print("verifying sha256...")
        for job in jobs:
            if not job.final.is_file():
                continue
            h = hashlib.sha256()
            with job.final.open("rb") as f:
                while True:
                    b = f.read(4 * IO)
                    if not b:
                        break
                    h.update(b)
            try:
                remote = fetch_hash(ctx, job.rel)
            except Exception as e:
                print(f"  ? {job.rel}: cannot verify ({e})")
                continue
            if remote != h.hexdigest():
                print(f"  MISMATCH {job.rel}")
                progress.failed.append(job.rel)
            elif not args.quiet:
                print(f"  ok {job.rel}")

    transfer_failed = [job.rel for job, _missing in todo if not job.final.is_file()]
    bad = sorted(set(progress.failed) | set(transfer_failed))
    print()
    if bad:
        print(f"failed: {len(bad)} file(s). Re-run the same command to resume.")
        for rel in bad[:15]:
            print(f"  - {rel}")
        return 1
    got = sum(j.size for j in jobs)
    elapsed = time.time() - progress.start
    rate = got / elapsed if elapsed > 0 else 0
    print(f"done: {len(jobs)} files, {human(got)} in {duration(elapsed)} ({human(rate)}/s)")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.stderr.write("\ninterrupted - progress is saved, re-run to resume\n")
        sys.exit(130)
