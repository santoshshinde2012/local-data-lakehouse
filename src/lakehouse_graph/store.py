"""Ladybug projection of the Parquet graph, plus build housekeeping (lock, promote, gc).

Parquet is canonical; ``graph.lbdb`` is a rebuildable projection (LadybugDB 0.21.1,
embedded, MIT). Rules applied to every open, enforced by tests/graph/test_queries_lint.py:

  * always cap ``buffer_pool_size`` (loader 256 MB, server 128 MB; the engine default is
    ~80% of RAM) and ``max_num_threads``;
  * never install or load an engine extension (no network, no native add-ons);
  * rebuild on every engine bump, never migrate a .lbdb file;
  * one read-only Database per serving process; a new build is picked up by restart;
  * a Connection keeps every distinct parameterised statement prepared (1.5 - 2.2 MB of the
    buffer pool each, never released while it lives): the fixed templates fit the serving
    Connection; ad-hoc statement texts run on a fresh ``connect(db)`` that is closed afterwards.

Housekeeping is portable (macOS has no flock(1); ``ln -sfn`` is not atomic):
  BuildLock      fcntl.flock on $GRAPH_ROOT/.lock; build, promote and gc all take it, and the
                 contract check takes it to write contract.json, so none of them ever sees
                 another one half done (it is not re-entrant: never nest them)
  update_link    temp symlink + os.replace (atomic swap) for <profile>/latest and current
  replace_dir    swap a finished temp build over an existing build directory in one atomic
                 exchange (renamex_np / renameat2), so a link into it never dangles. Where the
                 filesystem has no exchange it falls back to two renames, undoes the first if
                 the second fails, and restore_orphans() (run by build, promote and gc) puts
                 the old build back if the process died in between
  promote(profile, build_dir)   $GRAPH_ROOT/current -> a default-profile build with a strict
                 contract pass (contract.json); by default the newest one that is still fresh
  gc(profile, keep=3)           keep the newest N builds per profile; never delete a build that
                 current or latest points at, or that a live process holds (pidfile + os.kill(pid, 0))

Loader CLI (used by build.py in a child process so the loader's RSS is measured alone):
  python -m lakehouse_graph.store load --parquet-dir <dir> --db <file>
"""
from __future__ import annotations

import argparse
import errno
import fcntl
import json
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Self

import pyarrow as pa

from . import spec

DB_FILE = "graph.lbdb"
LOAD_BUFFER_POOL_MB = 256
SERVE_BUFFER_POOL_MB = 128
THREADS = 2
QUERY_TIMEOUT_MS = 5000
PID_DIR = ".pids"
CONTRACT_FILE = "contract.json"
TRASH_PREFIX = ".trash-"  # .trash-<build id>-<pid>: the old build, between the two renames of a rename-replace
_MB = 1024 * 1024


# --------------------------------------------------------------------------- DDL from the spec
def _lb_type(t: pa.DataType) -> str:
    if pa.types.is_string(t):
        return "STRING"
    if pa.types.is_int64(t):
        return "INT64"
    if pa.types.is_float64(t):
        return "DOUBLE"
    if pa.types.is_boolean(t):
        return "BOOLEAN"
    if pa.types.is_date32(t):
        return "DATE"
    raise ValueError(f"no Ladybug type for arrow type {t}")


def ddl_statements() -> list[str]:
    """CREATE NODE/REL TABLE statements generated from spec.NODE_SCHEMA / EDGE_SCHEMA."""
    out = []
    for n in spec.NODE_SCHEMA.values():
        cols = ", ".join(f"{c} {_lb_type(t)}" for c, t in n.columns)
        out.append(f"CREATE NODE TABLE {n.label}({cols}, PRIMARY KEY({n.key}))")
    for e in spec.EDGE_SCHEMA.values():
        props = "".join(f", {c} {_lb_type(t)}" for c, t in e.columns)
        out.append(f"CREATE REL TABLE {e.rel}(FROM {e.src} TO {e.dst}{props})")
    return out


def rows(result) -> list[list]:
    out = []
    while result.has_next():
        out.append(result.get_next())
    return out


def _quoted(path: Path) -> str:
    s = str(path)
    if "'" in s or "\\" in s:
        raise ValueError(f"unsupported character in path for COPY: {s}")
    return f"'{s}'"


def count_all(conn) -> dict[str, int]:
    counts = {}
    for label in spec.NODE_SCHEMA:
        counts[label] = int(rows(conn.execute(f"MATCH (n:{label}) RETURN count(n)"))[0][0])
    for rel in spec.EDGE_SCHEMA:
        counts[rel] = int(rows(conn.execute(f"MATCH ()-[e:{rel}]->() RETURN count(e)"))[0][0])
    return counts


def load_ladybug(parquet_dir: str | os.PathLike, db_path: str | os.PathLike,
                 buffer_pool_mb: int = LOAD_BUFFER_POOL_MB, threads: int = THREADS) -> dict:
    """COPY every node then edge table FROM Parquet into a fresh database, then CHECKPOINT."""
    import ladybug as lb

    parquet_dir, db_path = Path(parquet_dir), Path(db_path)
    if db_path.exists():
        raise FileExistsError(f"{db_path} exists: the loader only writes a fresh database (rebuild, never migrate)")
    t0 = time.perf_counter()
    db = lb.Database(str(db_path), buffer_pool_size=buffer_pool_mb * _MB, max_num_threads=threads)
    conn = lb.Connection(db, num_threads=threads)
    try:
        for stmt in ddl_statements():
            conn.execute(stmt)
        per_table = {}
        for name, file in [(n.label, n.file) for n in spec.NODE_SCHEMA.values()] + \
                          [(e.rel, e.file) for e in spec.EDGE_SCHEMA.values()]:
            t = time.perf_counter()
            conn.execute(f"COPY {name} FROM {_quoted(parquet_dir / file)}")
            per_table[name] = round(time.perf_counter() - t, 3)
        conn.execute("CHECKPOINT")
        load_s = time.perf_counter() - t0
        counts = count_all(conn)
    finally:
        conn.close()
        db.close()
    size = sum(p.stat().st_size for p in db_path.parent.glob(db_path.name + "*") if p.is_file())
    return {"db": db_path.name, "load_s": round(load_s, 2), "per_table_s": per_table, "counts": counts,
            "db_bytes": int(size), "buffer_pool_mb": buffer_pool_mb, "threads": threads,
            "max_rss_bytes": _max_rss_bytes()}


def open_readonly(db_path: str | os.PathLike, buffer_pool_mb: int = SERVE_BUFFER_POOL_MB, threads: int = THREADS,
                  timeout_ms: int = QUERY_TIMEOUT_MS):
    """(db, conn) on an existing graph.lbdb: read-only, capped pool and threads, query timeout.

    read_only blocks graph mutations only (not file or extension I/O); never expose a raw
    query surface on this connection without the OS sandbox.
    """
    import ladybug as lb

    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"{db_path} not found: run make graph-build")
    db = lb.Database(str(db_path), read_only=True, buffer_pool_size=buffer_pool_mb * _MB, max_num_threads=threads)
    try:
        conn = connect(db, threads, timeout_ms)
    except BaseException:  # never leak the database handle (and its file lock) on a failed open
        db.close()
        raise
    return db, conn


def connect(db, threads: int = THREADS, timeout_ms: int = QUERY_TIMEOUT_MS):
    """A new Connection on an open Database: capped threads, query timeout.

    Why a caller may want a second one: the engine (0.21.1) keeps every distinct statement
    that was executed WITH parameters prepared for the life of its Connection, and each holds
    1.5 - 2.2 MB of the Database's buffer pool (measured on the tiny build: a 128 MB pool is
    full after 58 - 86 distinct parameterised statements, 256 MB after 122 - 170; the same text
    with other parameter values, and statements without parameters, cost nothing). Afterwards
    every statement on that Connection fails with "buffer pool is full". The named templates of
    queries.py are a fixed, small set (8 parameterised tool templates, 20 with the contract's),
    so one serving Connection holds them all with room for ~60 more
    (tests/graph/test_queries_lint.py runs the whole catalog on one). Anything that executes
    ad-hoc statement texts (a guarded raw-query surface, a test that plants many templates) must
    take a fresh Connection per statement, or per small batch, and close the old one: that
    releases the memory; the Database stays open.
    """
    import ladybug as lb

    conn = lb.Connection(db, num_threads=threads)
    try:
        conn.set_query_timeout(timeout_ms)
    except BaseException:
        conn.close()
        raise
    return conn


def _max_rss_bytes() -> int:
    import resource
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(r if sys.platform == "darwin" else r * 1024)


# --------------------------------------------------------------------------- build lock
class BuildLock:
    """Exclusive build lock: fcntl.flock on $GRAPH_ROOT/.lock (released on close or exit)."""

    def __init__(self, root: str | os.PathLike | None = None, timeout: float = 600.0, poll: float = 0.25, log=None):
        self.path = spec.lock_path(root)
        self.timeout, self.poll, self.log = timeout, poll, log
        self._fd: int | None = None

    def __enter__(self) -> Self:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
        deadline = time.monotonic() + self.timeout
        announced = False
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError as e:
                if e.errno not in (errno.EAGAIN, errno.EACCES):
                    os.close(fd)
                    raise
                if time.monotonic() >= deadline:
                    os.close(fd)
                    raise TimeoutError(f"another graph build holds {self.path} (waited {self.timeout:.0f} s)") from e
                if self.log and not announced:
                    self.log(f"    waiting for the build lock {self.path} ...")
                    announced = True
                time.sleep(self.poll)
        os.ftruncate(fd, 0)
        os.write(fd, f"{os.getpid()}\n".encode())
        self._fd = fd
        return self

    def __exit__(self, *exc) -> None:
        if self._fd is not None:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            os.close(self._fd)
            self._fd = None


# --------------------------------------------------------------------------- links, promote
def update_link(link: Path, target: Path) -> Path:
    """Atomically point ``link`` at ``target``: temp symlink + os.replace (rename(2))."""
    link, target = Path(link), Path(target)
    if link.exists() and not link.is_symlink():
        raise RuntimeError(f"{link} exists and is not a symlink; refusing to replace it")
    link.parent.mkdir(parents=True, exist_ok=True)
    tmp = link.with_name(f".{link.name}.tmp-{os.getpid()}")
    if tmp.is_symlink() or tmp.exists():
        tmp.unlink()
    os.symlink(os.path.relpath(target, link.parent), tmp)
    os.replace(tmp, link)
    return link


def link_target(link: Path) -> Path | None:
    link = Path(link)
    return link.resolve() if link.is_symlink() and link.exists() else None


def exchange_paths(a: str | os.PathLike, b: str | os.PathLike) -> None:
    """Atomically exchange two existing paths: afterwards ``a`` holds what ``b`` held and vice versa.

    One system call, so no observer ever sees either name missing: renamex_np(RENAME_SWAP) on
    macOS, renameat2(RENAME_EXCHANGE) on Linux (libc via ctypes, standard library only).
    Raises OSError when the platform, libc or filesystem has no such call.
    """
    import ctypes

    rename_exchange = 2  # RENAME_SWAP (macOS <sys/stdio.h>) and RENAME_EXCHANGE (Linux <linux/fs.h>) are both 2
    src, dst = os.fsencode(a), os.fsencode(b)
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        if sys.platform == "darwin":
            fn = libc.renamex_np
            fn.argtypes, fn.restype = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint], ctypes.c_int
            rc = fn(src, dst, rename_exchange)
        elif sys.platform.startswith("linux"):
            at_fdcwd = -100
            fn = libc.renameat2
            fn.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
            fn.restype = ctypes.c_int
            rc = fn(at_fdcwd, src, at_fdcwd, dst, rename_exchange)
        else:
            raise OSError(errno.ENOTSUP, f"no atomic exchange on {sys.platform}")
    except AttributeError as e:  # a libc without the call
        raise OSError(errno.ENOSYS, f"this libc has no atomic exchange call ({e})") from e
    if rc != 0:
        err = ctypes.get_errno()
        raise OSError(err, f"atomic exchange of {a} and {b} failed: {os.strerror(err)}")


def replace_dir(new: str | os.PathLike, final: str | os.PathLike) -> str:
    """Put the finished directory ``new`` at ``final``, replacing the directory that is there.

    "exchange": one atomic exchange, then the old tree (now at ``new``) is removed; ``final``
    exists at every instant, so ``current`` / ``latest`` links into it never dangle and a crash
    leaves either the old or the new build, never half of one.
    "rename": the fallback where the filesystem has no exchange call: old -> ``.trash-*``, new ->
    ``final`` (two renames; ``final`` is missing only between them), then the trash is removed.
    If the second rename fails the old build is put back before the error is raised; if the
    process dies between the two, gc restores the orphaned ``.trash-*`` (``restore_orphans``).
    Returns the method used (the builder logs it). Other leftovers of a crash are dot-directories
    that gc removes. The Linux renameat2 branch is validated by CI: with GRAPH_REQUIRE_EXCHANGE=1
    tests/graph/test_build.py fails, instead of skipping or accepting "rename", when the exchange
    call is missing, so a silent fallback cannot pass the graph job.
    """
    new, final = Path(new), Path(final)
    try:
        exchange_paths(new, final)
    except OSError:
        trash = final.with_name(f"{TRASH_PREFIX}{final.name}-{os.getpid()}")
        if trash.exists():
            shutil.rmtree(trash)
        os.replace(final, trash)
        try:
            os.replace(new, final)
        except BaseException:
            os.replace(trash, final)  # the old build (and its contract.json) is back; links into it are valid again
            raise
        shutil.rmtree(trash, ignore_errors=True)
        return "rename"
    shutil.rmtree(new, ignore_errors=True)
    return "exchange"


def restore_orphans(builds: str | os.PathLike) -> list[str]:
    """Put back builds that a crashed rename-replace left in ``.trash-<id>-<pid>``.

    Only when the writer is gone (pid not alive) and ``<id>`` is missing: that trash directory is
    then the only copy of the build (the crash fell between replace_dir's two renames). Call it
    under the BuildLock. Returns the ids restored; any other trash is left for gc to remove.
    """
    restored = []
    builds = Path(builds)
    for d in sorted(builds.glob(f"{TRASH_PREFIX}*")) if builds.is_dir() else []:
        name, _, pid = d.name[len(TRASH_PREFIX):].rpartition("-")
        if not d.is_dir() or not name or not pid.isdigit() or pid_alive(int(pid)) or (builds / name).exists():
            continue
        os.replace(d, builds / name)
        restored.append(name)
    return restored


def read_contract(build_dir: Path) -> dict | None:
    try:
        return json.loads((Path(build_dir) / CONTRACT_FILE).read_text())
    except (OSError, ValueError):
        return None


def is_strict_pass(contract: dict | None, man: dict) -> bool:
    """True when ``contract`` (a build's contract.json) records a strict pass of the build as
    its manifest describes it now: same id, same Parquet bytes, same pinned exports."""
    if not contract or contract.get("status") != "pass" or not contract.get("strict"):
        return False
    return (contract.get("business_build_id") == man.get("business_build_id")
            and contract.get("files_sha256") == {k: v["sha256"] for k, v in man.get("files", {}).items()}
            and contract.get("exports_sha256") == man.get("exports", {}).get("sha256"))


def passing_builds(profile: str = "default", root: str | os.PathLike | None = None) -> list[tuple[str, Path]]:
    """(built_at, build dir) of the builds with a valid strict contract pass, oldest build first."""
    from . import manifest as mf

    out = []
    bdir = spec.builds_dir(profile, root)
    for d in sorted(p for p in bdir.iterdir() if p.is_dir() and not p.name.startswith(".")) if bdir.is_dir() else []:
        try:
            man = mf.read_manifest(d)
        except (OSError, ValueError):
            continue
        c = read_contract(d)
        if is_strict_pass(c, man):
            out.append((man.get("built_at", ""), c.get("checked_at", ""), d))
    return [(built, d) for built, _checked, d in sorted(out)]


def promote(profile: str = "default", build_dir: str | os.PathLike | None = None, *,
            root: str | os.PathLike | None = None, lock_timeout: float = 600.0, log=None) -> Path:
    """Atomically point $GRAPH_ROOT/current at a default-profile build (temp symlink + os.replace).

    ``build_dir`` None: the most recently built build that has a strict contract pass and
    is still fresh (its id matches its sources and the code on disk now: the bronze, or for an
    Iceberg-sourced build its recorded input pins; manifest.is_fresh), so a pass that predates
    a code or data change is never promoted by default. ``build_dir`` given (a build
    directory or a bare business_build_id): that build, which needs its strict pass but may
    be stale; naming it is how an older build is rolled back to.

    Selection and the link swap happen under the BuildLock: gc and a replacing build take the
    same lock, so the chosen build cannot be deleted or swapped between the two steps.
    """
    from . import manifest as mf

    if profile != "default":
        raise RuntimeError(f"only the default profile can be promoted (got {profile!r}): $GRAPH_ROOT/current "
                           f"is the serving build of data/sample/churn")
    with BuildLock(root, timeout=lock_timeout, log=log):
        restore_orphans(spec.builds_dir(profile, root))
        passing = passing_builds(profile, root)
        if build_dir is not None:
            raw = Path(build_dir)
            want = (spec.builds_dir(profile, root) / raw if len(raw.parts) == 1 else raw.absolute()).resolve()
            chosen = [d for _, d in passing if d.resolve() == want]
            if not chosen:
                raise RuntimeError(f"no strict contract pass recorded for build {build_dir}: run make graph-check "
                                   f"on it first (only the default profile can be promoted)")
            target = chosen[0]
        else:
            if not passing:
                raise RuntimeError("no strict contract pass recorded for any default-profile build: run make "
                                   "graph-local first (only the default profile can be promoted)")
            fresh = [d for _, d in passing if mf.is_fresh(mf.read_manifest(d))]
            if not fresh:
                stale = ", ".join(d.name for _, d in passing)
                raise RuntimeError(f"every build with a strict contract pass is stale ({stale}): its sources (the "
                                   f"bronze, or the Iceberg pins) or the build code changed since. Run make "
                                   f"graph-local, or name a build to roll back "
                                   f"to (build_graph_local.py promote --build <id>)")
            target = fresh[-1]
        update_link(spec.current_link(root), target)
    return target


# --------------------------------------------------------------------------- pidfiles, gc
def write_pidfile(build_dir: str | os.PathLike, pid: int | None = None) -> Path:
    """Mark ``build_dir`` as held by a serving process (the MCP launcher calls this)."""
    d = Path(build_dir) / PID_DIR
    d.mkdir(parents=True, exist_ok=True)
    pid = pid or os.getpid()
    p = d / f"{pid}.pid"
    p.write_text(f"{pid}\n")
    return p


def remove_pidfile(build_dir: str | os.PathLike, pid: int | None = None) -> None:
    p = Path(build_dir) / PID_DIR / f"{pid or os.getpid()}.pid"
    try:
        p.unlink()
    except FileNotFoundError:
        pass


def pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, owned by someone else
    return True


def live_pids(build_dir: str | os.PathLike) -> list[int]:
    """Pids of live processes holding the build; stale pidfiles are ignored."""
    d = Path(build_dir) / PID_DIR
    out = []
    for p in sorted(d.glob("*.pid")) if d.is_dir() else []:
        try:
            pid = int(p.read_text().strip() or p.stem)
        except (OSError, ValueError):
            continue
        if pid_alive(pid):
            out.append(pid)
    return out


def _built_key(d: Path) -> tuple[str, float]:
    try:
        built = json.loads((d / "manifest.json").read_text()).get("built_at", "")
    except (OSError, ValueError):
        built = ""
    return (built, d.stat().st_mtime)


def gc(profile: str | None = None, keep: int = 3, *, root: str | os.PathLike | None = None,
       lock_timeout: float = 600.0, log=None) -> dict:
    """Remove old builds, keeping the newest ``keep`` per profile and anything in use.

    ``profile`` None: every profile under the graph root. Runs under the BuildLock (as build and
    promote do): what ``current`` / ``latest`` point at is read and honoured inside the lock, so a
    build that a concurrent promote has just chosen is never deleted under it. A build that a
    crashed rename-replace left in ``.trash-*`` is restored first (``restored`` in the report);
    the other dot-directories of dead writers are removed.
    """
    groot = spec.graph_root(root)
    report: dict[str, dict] = {}
    if not groot.is_dir():
        return report
    profiles = [spec.check_profile(profile)] if profile else sorted(
        p.name for p in groot.iterdir() if not p.is_symlink() and (p / "builds").is_dir() and spec.is_profile(p.name))
    with BuildLock(groot, timeout=lock_timeout, log=log):
        current = link_target(spec.current_link(groot))
        for prof in profiles:
            bdir = spec.builds_dir(prof, groot)
            if not bdir.is_dir():
                continue
            restored = restore_orphans(bdir)  # first: a crashed replace's only copy of a build is not "stale"
            latest = link_target(spec.latest_link(prof, groot))
            builds = sorted((d for d in bdir.iterdir() if d.is_dir() and not d.name.startswith(".")),
                            key=_built_key, reverse=True)
            rep = {"kept": [], "removed": [], "held": [], "stale_tmp_removed": [], "restored": restored}
            for i, d in enumerate(builds):
                pids = live_pids(d)
                if i < keep:
                    rep["kept"].append(d.name)
                elif pids:
                    rep["held"].append({"build": d.name, "reason": f"live pid(s) {pids}"})
                elif d.resolve() in (current, latest):
                    rep["held"].append({"build": d.name, "reason": "current/latest points at it"})
                else:
                    shutil.rmtree(d)
                    rep["removed"].append(d.name)
            for d in sorted(p for p in bdir.iterdir() if p.is_dir() and p.name.startswith(".")):
                try:
                    pid = int(d.name.rsplit("-", 1)[-1])
                except ValueError:
                    pid = -1
                if not pid_alive(pid):  # a crashed build's temp dir (we hold the lock)
                    shutil.rmtree(d, ignore_errors=True)
                    rep["stale_tmp_removed"].append(d.name)
            report[prof] = rep
    return report


# --------------------------------------------------------------------------- CLI (loader)
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Load the renewal graph Parquet into a fresh Ladybug database.")
    sub = ap.add_subparsers(dest="cmd", required=True)
    ld = sub.add_parser("load")
    ld.add_argument("--parquet-dir", required=True)
    ld.add_argument("--db", required=True)
    ld.add_argument("--buffer-pool-mb", type=int, default=LOAD_BUFFER_POOL_MB)
    ld.add_argument("--threads", type=int, default=THREADS)
    a = ap.parse_args(argv)
    print(json.dumps(load_ladybug(a.parquet_dir, a.db, a.buffer_pool_mb, a.threads)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
