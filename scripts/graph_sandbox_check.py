#!/usr/bin/env python3
"""graph_sandbox_check.py -- macOS-only proof that the OS sandbox around the MCP servers holds while the tools answer.

    $(GRAPH_PY) scripts/graph_sandbox_check.py [--build <dir>] [--graph-root <dir>] [--control] [--log-check]
                                               [--control-network] [--allow-unchecked] [--toolsets ...] [-v]
                                               [--cypher | --cypher-only] [--json <file>]

Everything goes through scripts/graph_mcp.sh, i.e. the exact production launch path:

  1. static      sandbox-exec present, profile present and deny-default, launcher pins /usr/bin/sandbox-exec
  2. probe A     the debug probe under the sandbox with the real $HOME: the lakehouse tools still answer
                 (graph_describe, graph_find, metric_route_counts, lineage_pit) and the runtime basics work,
                 while every write outside the logs dir (Python, Cypher COPY TO /tmp, EXPORT DATABASE), every
                 sensitive read (Python, Cypher LOAD FROM on fake ~/.ssh, ~/.aws ... sentinels; real credential
                 paths are only opened), all network (TCP to 1.1.1.1, loopback, UDP, DNS, AF_UNIX, HTTPS),
                 fork / subprocess / exec, mach look-ups and POSIX shm are DENIED
  3. probe B     HOME pointed at a fake home INSIDE the readable graph root: proves the profile's explicit
                 ~/.ssh, ~/.aws ... re-denies (not only the allowlist)
  4. probe M     pandas-only variant (no ladybug import), as the metrics server runs
  5. mcp         an MCP client lists and calls tools through the sandboxed launcher, every toolset, in the
                 legacy `initialize` and the 2026-07-28 protocol; readOnlyHint on every tool
  6. fail closed sandbox-exec "missing" (simulated) -> exit 78, nothing on stdout, nothing started;
                 GRAPH_SANDBOX=0 is the only opt-out and prints its banner
  7. --control   the same probe UNSANDBOXED (a safe subset) succeeds at exactly what step 2 shows denied;
                 its network probes stay on this machine (loopback, a bind, a local AF_UNIX socket) unless
                 --control-network also lets it reach the internet (DNS, TCP 1.1.1.1, HTTPS example.com)
  8. --log-check the unified log has no reported sandbox denial for the normal MCP sessions
  C. --cypher    (needs the build's evidence graph, scripts/build_evidence_graph.py) the guarded raw-Cypher server:
                 C1 the probe inside its evidence-only profile opens evidence.lbdb DIRECTLY (the Cypher guard
                 bypassed) and every read of a label-bearing file (LOAD FROM the Renewal / BILLED Parquet, ATTACH
                 graph.lbdb, Python reads), other files, writes outside the logs dir, extensions, the network and
                 processes are denied by the OS while graph_cypher answers; C2 MCP sessions to graph_mcp.sh
                 --enable-cypher in both protocols (exactly graph_cypher, sandboxed=true, the deny list refused,
                 with --log-check no denial but the server's own start check); C3 graph_cypher is absent by default
                 and refused with GRAPH_SANDBOX=0; C4 (--control) the same bypass UNSANDBOXED reads the labels, so
                 the OS layer is what stops it. --cypher-only runs the static checks and C only.

Exit code: 0 = every expectation holds (or not macOS: SKIP), 1 = at least one failed.

Two roles in one file. Run as above it is the CHECK. The check copies this file to
$GRAPH_ROOT/.sandbox-check/graph_sandbox_probe.py and starts it with ``graph_mcp.sh --probe``, where it is
the PROBE: it attacks the sandbox from the inside (the engine statements a hostile query would try, plain
Python file I/O, links, dlopen from the writable dir, sockets, processes, mach services) and only records
what happened; the expectations live in the check. The attack statements live here, never under
src/lakehouse_graph (product code holds no extension or file statement, test_queries_lint.py). The probe
is not an MCP tool. Real credential paths are only opened and closed (or listdir-counted), never read;
content reads target sentinel fixtures; the unsandboxed runs (the GRAPH_SANDBOX=0 opt-out of step 6 and
--control) never touch the real home, the repo, the venv, the network-installing statement or, without
--control-network, any host but this one. Under the sandbox the external attempts are denied in-process,
before a packet leaves. The check removes every fixture and probe-* file it made.
"""
from __future__ import annotations

# The probe half of this file attacks the sandbox on purpose: fixed /tmp targets it must NOT be able to write,
# a chmod it must not be allowed, an exec it must be refused. Those are the test, not insecure code.
# ruff: noqa: S108, S103, S606
import argparse
import asyncio
import ctypes
import datetime as dt
import errno
import json
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

SENTINEL = "FAKE-SENTINEL-DO-NOT-LEAK"
PROBE_MODULE = "graph_sandbox_probe"
FIXTURE_DIR = ".sandbox-check"
# Relative paths of the sentinel files in a fake home (shared by name between check and probe).
CREDENTIAL_FILES = [
    ".ssh/id_ed25519", ".aws/credentials", ".config/gh/hosts.yml", ".netrc", ".docker/config.json",
    ".gnupg/private-keys-v1.d/key", ".kube/config", ".pgpass", ".claude/.credentials.json", ".claude.json",
    "Library/Keychains/login.keychain-db",
    "Library/Application Support/Google/Chrome/Default/Cookies",
    "Library/Application Support/Firefox/Profiles/x.default/cookies.sqlite",
    "Library/Cookies/Cookies.binarycookies",
]
NON_CREDENTIAL_FILE = "Documents/private.csv"     # not on the explicit deny list (the honest counter-example)
ENV_FILES = [".env", ".env.local", ".envrc", ".env-prod", "nested/.env/secret.txt"]   # repo-secret shapes
REAL_HOME_PATHS = [
    ".ssh", ".ssh/known_hosts", ".aws", ".aws/credentials", ".config", ".netrc", ".docker", ".docker/config.json",
    ".gnupg", ".kube", ".kube/config", ".pgpass", ".claude", ".claude.json", ".gitconfig", ".zsh_history",
    "Library/Keychains", "Library/Keychains/login.keychain-db", "Library/Cookies", "Library/Application Support",
    "Library/Safari", "Library/Messages", "Documents", "Desktop", "Downloads",
]
SYSTEM_PATHS = ["/etc/hosts", "/etc/passwd", "/private/etc/ssh/ssh_config", "/Library/Keychains/System.keychain",
                "/Volumes", "/Users", "/private/tmp", "/Applications", "/Library/Preferences", "/opt/homebrew/etc",
                "/opt/homebrew/var", "/System/Volumes/Data", "/System/Volumes/Preboot"]
MACH_SERVICES = ["com.apple.dnssd.service", "com.apple.pasteboard.1", "com.apple.coreservices.launchservicesd",
                 "com.apple.SecurityServer", "com.apple.securityd.xpc", "com.apple.windowserver.active",
                 "com.apple.cfprefsd.daemon", "com.apple.system.notification_center", "com.apple.logd",
                 "com.apple.system.opendirectoryd.libinfo", "com.apple.trustd"]
# toolset -> (tool, arguments) called in the MCP sessions
MCP_CALLS = {
    "graph": [("graph_describe", {}), ("graph_find", {"query": "maya", "limit": 2}),
              ("graph_exposure", {"entity_id": "inc-002", "response_format": "detailed"})],   # LAPACK (Accelerate)
    "metrics": [("metric_route_counts", {}), ("metric_feature_card", {"feature": "limit_hits_14d"})],
    "lineage": [("lineage_pit", {}), ("lineage_unused", {})],
    "cohorts": [("cohort_list", {})],
}
EXPECTED_TOOLS = {"graph": 5, "metrics": 3, "lineage": 4, "cohorts": 2}

# =================================================================================================== PROBE
RESULTS: list[dict] = []


def rec(name: str, group: str, outcome: str, detail: object = "", **extra) -> None:
    RESULTS.append({"probe": name, "group": group, "outcome": outcome, "detail": str(detail)[:200], **extra})


def attempt(name: str, group: str, fn, side_effect: str | None = None) -> None:
    """Run fn and classify: allowed | denied (EPERM/EACCES, engine 'not permitted', DNS failure) | absent | error.
    A created side effect is reported, then removed where the sandbox allows it."""
    try:
        val = fn()
        outcome, detail = "allowed", "" if val is None else val
    except PermissionError as e:
        outcome, detail = "denied", f"{type(e).__name__}: {e}"
    except FileNotFoundError as e:
        outcome, detail = "absent", f"{type(e).__name__}: {e}"
    except socket.gaierror as e:
        outcome, detail = "denied", f"gaierror: {e}"
    except OSError as e:
        low = str(e).lower()
        dl = "dlopen(" in low and ("not permitted" in low or "sandbox" in low)
        outcome = "denied" if (e.errno in (errno.EPERM, errno.EACCES) or dl) else "error"
        detail = f"{type(e).__name__}: {e}"
    except Exception as e:  # noqa: BLE001 - the probe records every outcome
        low = str(e).lower()
        if "operation not permitted" in low or "permission denied" in low:
            outcome = "denied"
        elif "no such file" in low or "does not exist" in low:
            outcome = "absent"
        else:
            outcome = "error"
        detail = f"{type(e).__name__}: {e}"
    extra = {}
    if side_effect is not None:
        present = os.path.lexists(side_effect)
        extra["side_effect_present"] = present
        if present:
            try:
                if os.path.isdir(side_effect) and not os.path.islink(side_effect):
                    shutil.rmtree(side_effect)
                else:
                    os.unlink(side_effect)
            except OSError:
                pass  # e.g. unlink is denied inside the logs dir: the check removes it
    rec(name, group, outcome, detail, **extra)


def open_only(path: str) -> str:
    fd = os.open(path, os.O_RDONLY)
    os.close(fd)
    return "opened (content not read)"


def list_count(path: str) -> str:
    return f"{len(os.listdir(path))} entries (names not recorded)"


def read_sentinel(path: str) -> str:
    with open(path, encoding="utf-8") as f:
        return "LEAK: sentinel read" if SENTINEL in f.read() else "read (no sentinel)"


def py_write(path: str) -> str:
    with open(path, "w", encoding="utf-8") as f:
        f.write("probe\n")
    return "written"


def append(path: str) -> str:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps({"t": time.time(), "probe": True}) + "\n")
    return "appended"


def mach_lookup(service: str) -> str:
    """bootstrap_look_up(): 0 = reachable; 1100 BOOTSTRAP_NOT_PRIVILEGED = denied by the sandbox."""
    libc = ctypes.CDLL(None)
    bootstrap_port = ctypes.c_uint.in_dll(libc, "bootstrap_port")
    port = ctypes.c_uint(0)
    libc.bootstrap_look_up.argtypes = [ctypes.c_uint, ctypes.c_char_p, ctypes.POINTER(ctypes.c_uint)]
    libc.bootstrap_look_up.restype = ctypes.c_int
    kr = libc.bootstrap_look_up(bootstrap_port, service.encode(), ctypes.byref(port))
    if kr == 0:
        return "kr=0 (send right obtained)"
    if kr == 1100:
        raise PermissionError(errno.EPERM, "bootstrap_look_up kr=1100 BOOTSTRAP_NOT_PRIVILEGED")
    raise OSError(errno.EIO, f"bootstrap_look_up kr={kr}")


def tcp(host: str, port: int) -> str:
    s = socket.create_connection((host, port), timeout=3)
    s.close()
    return "connected"


def udp() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        return f"sent {s.sendto(b'x', ('1.1.1.1', 9))} byte"
    finally:
        s.close()


def listen() -> str:
    s = socket.socket()
    try:
        s.bind(("127.0.0.1", 0))
        s.listen(1)
        return f"listening on {s.getsockname()[1]}"
    finally:
        s.close()


def unix_connect(path: str) -> str:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        s.settimeout(2)
        s.connect(path)
        return "connected"
    finally:
        s.close()


def unix_bind(directory: Path, name: str) -> str:
    """Relative path (sun_path is limited to 104 bytes); the cwd is restored afterwards."""
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    fd = os.open(".", os.O_RDONLY)
    try:
        os.chdir(directory)
        s.bind(name)
        return "bound"
    finally:
        s.close()
        os.fchdir(fd)
        os.close(fd)


def urllib_get() -> str:
    import urllib.error
    import urllib.request

    try:
        with urllib.request.urlopen("https://example.com", timeout=4) as r:
            return f"HTTP {r.status}"
    except urllib.error.URLError as e:
        if isinstance(e.reason, OSError):
            raise PermissionError(errno.EPERM, f"URLError: {e.reason}") from e
        raise


def fork() -> str:
    pid = os.fork()
    if pid == 0:
        os._exit(0)
    os.waitpid(pid, 0)
    return "forked"


def run_cmd(argv: list[str]) -> str:
    return f"rc={subprocess.run(argv, capture_output=True, check=True, timeout=8).returncode}"


def shm() -> str:
    from multiprocessing import shared_memory

    m = shared_memory.SharedMemory(create=True, size=1024)
    m.close()
    m.unlink()
    return "created"


def sem() -> str:
    import multiprocessing as mp

    mp.Semaphore(1)
    return "created"


def mkstemp() -> str:
    fd, p = tempfile.mkstemp(prefix="sbprobe-")
    os.close(fd)
    os.unlink(p)
    return f"created in {os.path.dirname(p)}"


def thread_ok() -> str:
    import threading

    out: list[int] = []
    th = threading.Thread(target=lambda: out.append(1))
    th.start()
    th.join()
    return "ok" if out else "no"


def lib_versions() -> str:
    import numpy
    import pandas
    import pyarrow

    return f"numpy {numpy.__version__} pandas {pandas.__version__} pyarrow {pyarrow.__version__}"


def arrow_tz() -> str:
    import pyarrow as pa
    import pyarrow.compute as pc

    arr = pa.array([dt.datetime(2026, 9, 30, 12, 0)], type=pa.timestamp("s"))
    return pc.assume_timezone(arr, "Asia/Kolkata")[0].as_py().isoformat()


def networkx_louvain() -> str:
    import networkx as nx

    communities = nx.community.louvain_communities(nx.karate_club_graph(), seed=1)
    return f"networkx {nx.__version__} communities={len(communities)}"


def lakehouse_tools(build: Path, logs: Path, with_ladybug: bool) -> str:
    """The real tools answer inside the same sandboxed process the attacks run in."""
    from lakehouse_graph import tools
    from lakehouse_graph.context import ToolContext

    ctx = ToolContext(build, logs_dir=logs, allow_unchecked=True)
    names = ["metric_route_counts", "metric_lapse_rate"]
    if with_ladybug:
        names += ["graph_describe", "graph_find", "graph_renewal_evidence"]
        if ctx.has_lineage():
            names.append("lineage_pit")
    args = {"graph_find": {"query": "maya"}, "graph_renewal_evidence": {"renewal_id": "sub_maya:2026-10-07"}}
    answered = []
    for n in names:
        env = tools.call(ctx, n, args.get(n, {}))
        if set(env) == {"data", "provenance", "caveats", "truncated", "note"}:
            answered.append(n)
    return f"{len(answered)}/{len(names)} tools answered: {', '.join(answered)}"


def write_then_read(path: Path) -> str:
    py_write(str(path))
    with open(path, encoding="utf-8") as f:
        return f"read back {len(f.read())} bytes"


def dlopen_copy(dst: Path) -> str:
    """Copy a (validly signed) extension module into the writable dir and dlopen the copy."""
    import sysconfig

    cands = sorted(Path(sysconfig.get_path("stdlib"), "lib-dynload").glob("*.so"), key=lambda p: p.stat().st_size)
    if not cands:
        cands = sorted(Path(sysconfig.get_path("purelib")).glob("*/*.so"), key=lambda p: p.stat().st_size)
    with open(cands[0], "rb") as fi, open(dst, "wb") as fo:
        fo.write(fi.read())
    ctypes.CDLL(str(dst))
    return "LOADED native code from the writable dir"


def create_then(path: Path, action) -> str:
    py_write(str(path))
    action(path)
    return "done"


def symlink_then(link: Path, target: str, mode: str) -> str:
    os.symlink(target, link)
    if mode == "w":
        return py_write(str(link)) + " through symlink"
    return read_sentinel(str(link))


def hardlink_then_read(src: str, dst: Path) -> str:
    os.link(src, dst)
    return read_sentinel(str(dst))


def rename_back(p: str) -> str:
    os.rename(p, p + ".moved")
    os.rename(p + ".moved", p)
    return "renamed (and restored)"


def cypher_probe(a: argparse.Namespace) -> int:
    """The guard-bypass probe (graph_mcp.sh --probe --enable-cypher): the evidence-only profile of a cypher server,
    with the engine opened DIRECTLY (no cypher_guard), runs what the guard refuses. The OS layer must still stop
    every read of a label-bearing file, every write outside the logs dir, the network and the extensions."""
    build, logs, repo = Path(a.build), Path(a.logs), Path(a.repo)
    tag = str(os.getpid())
    t0 = time.perf_counter()
    g = "works"
    if not a.control:
        def the_tool() -> str:
            from lakehouse_graph import cypher_guard

            ok, reasons = cypher_guard.sandbox_status(build)
            if not ok:
                raise PermissionError(errno.EPERM, "; ".join(reasons))
            ctx = cypher_guard.CypherContext(build, logs_dir=logs, sandboxed=True)
            try:
                env = cypher_guard.call(ctx, "graph_cypher", {"query": "MATCH (r:Renewal) RETURN count(r) AS n"})
            finally:
                ctx.close()
            return f"graph_cypher answered n={env['data']['rows'][0]['n']} (sandbox_status ok)"
        attempt("graph_cypher (guarded) answers in the evidence-only sandbox", g, the_tool)
    conn = None
    try:
        import ladybug as lb

        db = lb.Database(str(build / "evidence.lbdb"), read_only=True, buffer_pool_size=128 * 1024 * 1024,
                         max_num_threads=2, backend="pybind")
        conn = lb.Connection(db)
        conn.set_query_timeout(5000)
        rec("ladybug open evidence.lbdb read_only (guard bypassed)", g, "allowed", f"version={lb.version}")
    except Exception as e:  # noqa: BLE001 - the probe records every outcome
        rec("ladybug open evidence.lbdb read_only", g, "error", f"{type(e).__name__}: {e}")

    def cy(q: str) -> str:
        r = conn.execute(q)
        try:
            return str(r.get_all())[:120]
        except Exception as e:  # noqa: BLE001 - the probe records every outcome
            return f"<no rows: {str(e)[:60]}>"

    if conn is None:
        json.dump({"pid": os.getpid(), "sandboxed_env": os.environ.get("GRAPH_SANDBOXED", ""), "control": a.control,
                   "results": RESULTS}, sys.stdout, indent=1)
        return 1
    attempt("raw MATCH on the evidence graph", g, lambda: cy("MATCH (r:Renewal) RETURN count(r)"))
    renewal_parquet = str(build / "parquet" / "nodes_Renewal.parquet")
    g = "labels"
    attempt("LOAD FROM the build's Renewal Parquet (labels: churned, outcome, route)", g,
            lambda: cy(f"LOAD FROM '{renewal_parquet}' RETURN churned, count(*)"))
    attempt("ATTACH the build's full graph.lbdb (labels, SIMILAR_TO)", g,
            lambda: cy(f"ATTACH '{build / 'graph.lbdb'}' AS full (dbtype lbug)"))
    attempt("python read the build's Renewal Parquet", g, lambda: open_only(renewal_parquet))
    attempt("python read the build's graph.lbdb", g, lambda: open_only(str(build / "graph.lbdb")))
    attempt("python read the build's manifest.json", g, lambda: open_only(str(build / "manifest.json")))
    attempt("LOAD FROM the build's BILLED edges (outcome evidence)", g,
            lambda: cy(f"LOAD FROM '{build / 'parquet' / 'edges_BILLED.parquet'}' RETURN count(*)"))
    if not a.control:
        g = "files"
        attempt("LOAD FROM /etc/hosts", g, lambda: cy("LOAD FROM '/etc/hosts' (file_format='csv', header=false) "
                                                      "RETURN count(*)"))
        attempt("python listdir the build dir", g, lambda: list_count(str(build)))
        attempt("python listdir the graph root", g, lambda: list_count(str(build.parents[2])))
        attempt("python read GRAPH_ROOT/.audit_key", g, lambda: open_only(str(build.parents[2] / ".audit_key")))
        attempt("python listdir the repo root", g, lambda: list_count(str(repo)))
        if a.outer_home:
            for rel in CREDENTIAL_FILES[:4]:
                p = str(Path(a.outer_home) / rel)
                attempt(f"outer ~/{rel} cypher LOAD FROM", g,
                        lambda p=p: cy(f"LOAD FROM '{p}' (file_format='csv', header=true) RETURN *"))
    g = "write"
    q = f"/tmp/sbprobe-cy-{tag}.csv"
    attempt("COPY evidence TO /tmp", g, lambda: cy(f"COPY (MATCH (r:Renewal) RETURN r.renewal_id) TO '{q}'"),
            side_effect=q)
    if not a.control:
        exp = f"/tmp/sbprobe-cy-export-{tag}"
        attempt("EXPORT DATABASE /tmp", g, lambda: cy(f"EXPORT DATABASE '{exp}'"), side_effect=exp)
        attempt("COPY evidence TO the build dir", g, lambda: cy(f"COPY (MATCH (r:Renewal) RETURN r.renewal_id) TO "
                                                                 f"'{build / f'sbprobe-{tag}.csv'}'"),
                side_effect=str(build / f"sbprobe-{tag}.csv"))
        attempt("python write the build dir", g, lambda: py_write(str(build / f"sbprobe-py-{tag}.csv")),
                side_effect=str(build / f"sbprobe-py-{tag}.csv"))
        g = "engine"
        attempt("CREATE in the read-only evidence graph", g, lambda: cy("CREATE (:Plan {plan_tier: 'zz'})"))
        attempt("INSTALL json (network + ~/.lbdb)", g, lambda: cy("INSTALL json"))
        attempt("LOAD EXTENSION algo (native code from ~/.lbdb)", g, lambda: cy("LOAD EXTENSION algo"))
        attempt("LOAD FROM an https URL", g, lambda: cy("LOAD FROM 'https://example.com/x.csv' RETURN count(*)"))
        g = "net"
        attempt("tcp 1.1.1.1:53", g, lambda: tcp("1.1.1.1", 53))
        attempt("dns getaddrinfo example.com", g, lambda: socket.getaddrinfo("example.com", 443)[0][4][0])
        attempt("tcp loopback 127.0.0.1:11434 (ollama)", g, lambda: tcp("127.0.0.1", 11434))
        g = "proc"
        attempt("subprocess /bin/ls", g, lambda: run_cmd(["/bin/ls", "/"]))
        attempt("os.fork()", g, fork)
    g = "logs-info"
    lq = str(logs / f"probe-cy-{tag}.csv")
    attempt("COPY evidence TO the logs dir (write-only, regular files: allowed by design)", g,
            lambda: cy(f"COPY (MATCH (p:Plan) RETURN p.plan_tier) TO '{lq}'"))
    json.dump({"pid": os.getpid(), "sandboxed_env": os.environ.get("GRAPH_SANDBOXED", ""), "control": a.control,
               "elapsed_ms": round(1000 * (time.perf_counter() - t0), 1), "results": RESULTS}, sys.stdout, indent=1)
    sys.stdout.write("\n")
    sys.stdout.flush()
    return 0


def probe_main(argv: list[str]) -> int:
    """The probe (runs under the sandbox through graph_mcp.sh --probe)."""
    ap = argparse.ArgumentParser(prog="graph_sandbox_probe probe")
    ap.add_argument("--build", required=True)
    ap.add_argument("--logs", required=True)
    ap.add_argument("--repo", required=True)
    ap.add_argument("--cypher", action="store_true",
                    help="the guard-bypass probe of a cypher server (graph_mcp.sh --probe --enable-cypher)")
    ap.add_argument("--fixture", default="", help="dir INSIDE the readable graph root: home/, .env files, canary.txt")
    ap.add_argument("--outer-home", default="", help="fake home OUTSIDE every readable tree")
    ap.add_argument("--real-home", default="", help="the user's real home (open-only probes)")
    ap.add_argument("--skip-ladybug", action="store_true", help="pandas-only (metrics) variant")
    ap.add_argument("--control", action="store_true", help="UNSANDBOXED comparison run (safe subset)")
    ap.add_argument("--external-network", action="store_true",
                    help="with --control: also try DNS / TCP / UDP / HTTPS to the internet (opt-in)")
    ap.add_argument("--exec-test", action="store_true", help="finish with os.execv(/bin/echo)")
    a = ap.parse_args(argv)
    if a.cypher:
        return cypher_probe(a)
    # Sandboxed, the external attempts are the test (denied in-process, nothing leaves); unsandboxed only on request.
    external = not a.control or a.external_network
    build, logs, repo = Path(a.build), Path(a.logs), Path(a.repo)
    tag = str(os.getpid())
    t0 = time.perf_counter()

    g = "works"
    attempt("os.urandom(16)", g, lambda: f"{len(os.urandom(16))} bytes")
    attempt("time.tzname + localtime", g, lambda: f"{time.tzname} {time.strftime('%z')}")
    attempt("zoneinfo Asia/Kolkata", g, lambda: str(__import__("zoneinfo").ZoneInfo("Asia/Kolkata")))
    attempt("os.cpu_count (sysctl)", g, lambda: str(os.cpu_count()))
    attempt("hashlib.sha256", g, lambda: __import__("hashlib").sha256(b"x").hexdigest()[:12])
    attempt("os.getcwd()", g, os.getcwd)
    attempt("socket.socketpair (asyncio self-pipe)", g, lambda: [s.close() for s in socket.socketpair()] and "ok")
    attempt("threading (anyio worker threads)", g, thread_ok)
    attempt("import numpy/pandas/pyarrow", g, lib_versions)
    attempt("pandas read_parquet(build)", g,
            lambda: f"rows={len(__import__('pandas').read_parquet(next((build / 'parquet').glob('*.parquet'))))}")
    attempt("pyarrow compute tz (arrow tzdb)", g, arrow_tz)
    attempt("networkx louvain", g, networkx_louvain)
    attempt("append audit line in logs dir", g, lambda: append(str(logs / f"probe-audit-{tag}.jsonl")))
    if not a.control:  # the tools themselves are proven unsandboxed by the test suite
        attempt("lakehouse_graph tools answer", g, lambda: lakehouse_tools(build, logs, not a.skip_ladybug))

    conn = None
    if not a.skip_ladybug:
        try:
            import ladybug as lb

            db = lb.Database(str(build / "graph.lbdb"), read_only=True, buffer_pool_size=128 * 1024 * 1024,
                             max_num_threads=2, backend="pybind")
            conn = lb.Connection(db)
            conn.set_query_timeout(5000)
            rec("ladybug open read_only (backend=pybind)", g, "allowed", f"version={lb.version}")
        except Exception as e:  # noqa: BLE001 - the probe records every outcome
            rec("ladybug open read_only", g, "error", f"{type(e).__name__}: {e}")

    def cy(q: str) -> str:
        r = conn.execute(q)
        try:
            return str(r.get_all())[:120]
        except Exception as e:  # noqa: BLE001 - the probe records every outcome
            return f"<no rows: {str(e)[:60]}>"

    if conn is not None:
        attempt("cypher MATCH count", g, lambda: cy("MATCH (n) RETURN count(n)"))

    # ---------------------------------------------------------------- writes outside the logs dir
    g = "write"
    targets = {"/tmp": f"/tmp/sbprobe-{tag}.csv",
               "TMPDIR": os.path.join(os.environ.get("TMPDIR") or "/private/var/tmp", f"sbprobe-{tag}.csv")}
    if a.fixture:
        targets["graph root (fixture dir)"] = str(Path(a.fixture) / f"sbprobe-{tag}.csv")
    if not a.control:  # never litter the real home / repo / venv in the unsandboxed control
        if a.real_home:
            targets["real home"] = os.path.join(a.real_home, f".sbprobe-{tag}.csv")
        targets.update({"repo root": str(repo / f"sbprobe-{tag}.csv"),
                        "repo src": str(repo / "src" / f"sbprobe-{tag}.py"),
                        "build dir": str(build / f"sbprobe-{tag}.csv"),
                        "venv": os.path.join(sys.prefix, f"sbprobe-{tag}.pth")})
    for label, p in targets.items():
        attempt(f"python write {label}", g, lambda p=p: py_write(p), side_effect=p)
        if conn is not None:
            q = p + ".cy.csv"
            attempt(f"cypher COPY TO {label}", g, lambda q=q: cy(f"COPY (MATCH (n) RETURN count(n)) TO '{q}'"),
                    side_effect=q)
    if conn is not None:
        exp = f"/tmp/sbprobe-export-{tag}"
        attempt("cypher EXPORT DATABASE /tmp", g, lambda: cy(f"EXPORT DATABASE '{exp}'"), side_effect=exp)
    attempt("python mkdir /tmp", g, lambda: os.mkdir(f"/tmp/sbprobe-dir-{tag}"), side_effect=f"/tmp/sbprobe-dir-{tag}")
    attempt("python open graph.lbdb O_RDWR", g, lambda: os.close(os.open(build / "graph.lbdb", os.O_RDWR)))
    if a.fixture:
        canary = str(Path(a.fixture) / "canary.txt")
        attempt("chmod canary (read-only tree)", g, lambda: os.chmod(canary, 0o600))
        attempt("append canary (read-only tree)", g, lambda: append(canary))
        attempt("rename canary (read-only tree)", g, lambda: rename_back(canary))
        attempt("unlink canary (read-only tree)", g, lambda: os.unlink(canary))

    # ---------------------------------------------------------------- the logs dir: write-only, files only
    g = "logs"
    attempt("python create file in logs dir", "logs-allowed", lambda: py_write(str(logs / f"probe-w-{tag}.txt")))
    attempt("read back own file in logs dir", g, lambda: write_then_read(logs / f"probe-rb-{tag}.txt"))
    attempt("listdir logs dir", g, lambda: list_count(str(logs)))
    attempt("dlopen a .so copied into logs dir", g, lambda: dlopen_copy(logs / f"probe-so-{tag}.so"))
    attempt("mkdir inside logs dir", g, lambda: os.mkdir(logs / f"probe-dir-{tag}"))
    attempt("unlink own file in logs dir", g, lambda: create_then(logs / f"probe-ul-{tag}.txt", os.unlink))
    attempt("chmod +x own file in logs dir", g,
            lambda: create_then(logs / f"probe-ch-{tag}.txt", lambda p: os.chmod(p, 0o755)))
    attempt("symlink in logs dir -> /tmp, write through", g,
            lambda: symlink_then(logs / f"probe-wl-{tag}", f"/tmp/sbprobe-symlink-target-{tag}", "w"),
            side_effect=f"/tmp/sbprobe-symlink-target-{tag}")
    attempt("unix socket bind in logs dir", g, lambda: unix_bind(logs, f"probe-{tag}.sock"))

    # ---------------------------------------------------------------- engine statements
    g = "engine"
    if conn is not None:
        attempt("cypher CREATE node (read_only engine)", g, lambda: cy("CREATE (:ZZProbe {id: 1})"))
        if not a.control:  # unsandboxed, the install statement really downloads native code into ~/.lbdb
            attempt("cypher INSTALL json (network + ~/.lbdb write)", g, lambda: cy("INSTALL json"))
            attempt("cypher LOAD EXTENSION algo (native code from ~/.lbdb)", g, lambda: cy("LOAD EXTENSION algo"))
        if external:
            attempt("cypher LOAD FROM https url", g,
                    lambda: cy("LOAD FROM 'https://example.com/x.csv' RETURN count(*)"))

    # ---------------------------------------------------------------- reads: fake homes (sentinels)
    if a.outer_home:
        for rel in CREDENTIAL_FILES + [NON_CREDENTIAL_FILE]:
            p = str(Path(a.outer_home) / rel)
            attempt(f"outer ~/{rel} python read", "read-outer", lambda p=p: read_sentinel(p))
            if conn is not None:
                attempt(f"outer ~/{rel} cypher LOAD FROM", "read-outer",
                        lambda p=p: cy(f"LOAD FROM '{p}' (file_format='csv', header=true) RETURN *"))
        first = str(Path(a.outer_home) / CREDENTIAL_FILES[0])
        attempt("hardlink outer secret into logs dir, read it", "read-outer",
                lambda: hardlink_then_read(first, logs / f"probe-hl-{tag}"), side_effect=str(logs / f"probe-hl-{tag}"))
        attempt("symlink in logs dir -> outer secret, read it", "read-outer",
                lambda: symlink_then(logs / f"probe-rl-{tag}", first, "r"))
    if a.fixture:
        inner = Path(a.fixture) / "home"
        for rel in CREDENTIAL_FILES:
            p = str(inner / rel)
            attempt(f"inner ~/{rel} python read", "read-inner", lambda p=p: read_sentinel(p))
            if conn is not None:
                attempt(f"inner ~/{rel} cypher LOAD FROM", "read-inner",
                        lambda p=p: cy(f"LOAD FROM '{p}' (file_format='csv', header=true) RETURN *"))
        attempt("inner ~/.ssh listdir", "read-inner", lambda: list_count(str(inner / ".ssh")))
        attempt(f"inner ~/{NON_CREDENTIAL_FILE} python read (NOT on the deny list)", "read-inner-control",
                lambda: read_sentinel(str(inner / NON_CREDENTIAL_FILE)))
        for rel in ENV_FILES:
            p = str(Path(a.fixture) / rel)
            attempt(f"fixture {rel} python read (inside a readable tree)", "read-env", lambda p=p: read_sentinel(p))
        if conn is not None:
            env_file = str(Path(a.fixture) / ".env")
            attempt("fixture .env cypher LOAD FROM", "read-env",
                    lambda: cy(f"LOAD FROM '{env_file}' (file_format='csv', header=true) RETURN *"))
    attempt("repo .env open (may not exist)", "read-repo", lambda: open_only(str(repo / ".env")))
    attempt("repo root listdir", "read-repo", lambda: list_count(str(repo)))
    attempt("repo .git open", "read-repo", lambda: open_only(str(repo / ".git")))

    # ---------------------------------------------------------------- reads: the real machine (open only)
    if a.real_home and not a.control:
        for rel in REAL_HOME_PATHS:
            p = os.path.join(a.real_home, rel)
            how = (lambda p=p: list_count(p)) if os.path.isdir(p) else (lambda p=p: open_only(p))
            attempt(f"real ~/{rel}", "read-real", how)
        attempt("real ~ listdir", "read-real", lambda: list_count(a.real_home))
    if not a.control:
        for p in SYSTEM_PATHS:
            attempt(p, "read-system", (lambda p=p: list_count(p)) if os.path.isdir(p) else (lambda p=p: open_only(p)))
        if conn is not None:
            attempt("/etc/hosts cypher LOAD FROM", "read-system",
                    lambda: cy("LOAD FROM '/etc/hosts' (file_format='csv', header=false) RETURN count(*)"))

    # ---------------------------------------------------------------- network
    g = "net"
    if external:
        attempt("dns getaddrinfo example.com", g, lambda: socket.getaddrinfo("example.com", 443)[0][4][0])
        attempt("tcp 1.1.1.1:53", g, lambda: tcp("1.1.1.1", 53))
        attempt("tcp6 [2606:4700:4700::1111]:53", g, lambda: tcp("2606:4700:4700::1111", 53))
        attempt("udp sendto 1.1.1.1:9", g, udp)
        attempt("urllib https://example.com", g, urllib_get)
    attempt("tcp loopback 127.0.0.1:11434 (ollama)", g, lambda: tcp("127.0.0.1", 11434))
    attempt("bind+listen 127.0.0.1:0", g, listen)
    attempt("unix connect /var/run/mDNSResponder", g, lambda: unix_connect("/var/run/mDNSResponder"))

    # ---------------------------------------------------------------- process / IPC
    g = "proc"
    attempt("subprocess /bin/ls", g, lambda: run_cmd(["/bin/ls", "/"]))
    curl_url = "https://example.com" if external else "http://127.0.0.1:9/"   # loopback: refused, stays local
    attempt("subprocess /usr/bin/curl", g, lambda: run_cmd(["/usr/bin/curl", "-sS", "-m", "3", "-o", "/dev/null",
                                                           curl_url]))
    attempt("subprocess sys.executable", g, lambda: run_cmd([sys.executable, "-c", "pass"]))
    attempt("os.fork()", g, fork)
    if not a.control:
        attempt("os.kill(parent, 0)", g, lambda: os.kill(os.getppid(), 0))
        for svc in MACH_SERVICES:
            attempt(f"mach-lookup {svc}", "mach", lambda svc=svc: mach_lookup(svc))
        attempt("posix shm_open", "ipc", shm)
        attempt("multiprocessing.Semaphore (sem_open)", "ipc", sem)
    attempt("tempfile.mkstemp()", "info", mkstemp)

    json.dump({"pid": os.getpid(), "sandboxed_env": os.environ.get("GRAPH_SANDBOXED", ""), "control": a.control,
               "elapsed_ms": round(1000 * (time.perf_counter() - t0), 1), "results": RESULTS}, sys.stdout, indent=1)
    sys.stdout.write("\n")
    sys.stdout.flush()
    if a.exec_test and not a.control:
        # No fork is available, so a successful exec REPLACES this process (and /bin/echo prints).
        try:
            os.execv("/bin/echo", ["/bin/echo", "EXEC_PROBE: allowed (/bin/echo ran)"])
        except OSError as e:
            print(f"EXEC_PROBE: denied ({type(e).__name__}: {e})")
    return 0


# =================================================================================================== CHECK
REPO = Path(__file__).resolve().parents[1]
LAUNCH = REPO / "scripts" / "graph_mcp.sh"
PROFILE = REPO / "config" / "graph" / "sandbox.sb"
FAILS: list[str] = []
REPORT: dict = {"checks": []}


def check(ok: bool, name: str, detail: str = "") -> bool:
    print(f"  {'PASS' if ok else 'FAIL'}  {name}{('  -- ' + detail) if detail else ''}")
    REPORT["checks"].append({"name": name, "ok": bool(ok), "detail": detail})
    if not ok:
        FAILS.append(name)
    return ok


def make_home(root: Path) -> None:
    for rel in CREDENTIAL_FILES + [NON_CREDENTIAL_FILE]:
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(f"k,v\nsecret,{SENTINEL}\n", encoding="utf-8")


def launcher_env(graph_root: Path, extra: dict[str, str] | None = None) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if k in ("PATH", "HOME", "USER", "LOGNAME", "LANG", "TMPDIR", "TZ")}
    env["GRAPH_ROOT"] = str(graph_root)
    env["GRAPH_PY"] = os.environ.get("GRAPH_PY", sys.executable)
    env.update(extra or {})
    return env


def run_probe(graph_root: Path, build: str | None, probe_args: list[str],
              env_extra: dict[str, str] | None = None) -> tuple[dict | None, str, subprocess.CompletedProcess]:
    cmd = [str(LAUNCH), "--probe"] + (["--build", build] if build else []) + ["--"] + probe_args
    r = subprocess.run(cmd, env=launcher_env(graph_root, env_extra), capture_output=True, text=True, check=False,
                       timeout=240, stdin=subprocess.DEVNULL)
    body, _, tail = r.stdout.partition("\nEXEC_PROBE")
    try:
        doc = json.loads(body)
    except json.JSONDecodeError:
        doc = None
    return doc, ("EXEC_PROBE" + tail.strip()) if tail else "", r


def by_group(doc: dict) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for r in doc["results"]:
        out.setdefault(r["group"], []).append(r)
    return out


def expect_group(groups: dict[str, list[dict]], group: str, allowed_outcomes: set[str], label: str,
                 verbose: bool, no_side_effects: bool = False, no_leak: bool = True) -> None:
    rows = groups.get(group, [])
    bad = [r for r in rows if r["outcome"] not in allowed_outcomes
           or (no_side_effects and r.get("side_effect_present"))
           or (no_leak and ("LEAK" in r["detail"] or SENTINEL in r["detail"]))]
    counts: dict[str, int] = {}
    for r in rows:
        counts[r["outcome"]] = counts.get(r["outcome"], 0) + 1
    check(bool(rows) and not bad, f"{label}: {len(rows)} probes {counts}",
          "; ".join(f"{r['probe']} -> {r['outcome']} {r['detail'][:80]}" for r in bad[:4]))
    if verbose:
        for r in rows:
            print(f"        {r['outcome']:8s} {r['probe'][:70]:70s} {r['detail'][:70]}")


async def mcp_session(graph_root: Path, toolset: str, mode: str, build: str | None, unchecked: bool,
                      env_extra: dict[str, str] | None = None) -> dict:
    from mcp import Client, StdioServerParameters

    args = ["--toolset", toolset] + (["--build", build] if build else []) + (["--allow-unchecked"] if unchecked else [])
    params = StdioServerParameters(command=str(LAUNCH), args=args, env=launcher_env(graph_root, env_extra))
    t0 = time.perf_counter()
    out: dict = {"toolset": toolset, "mode": mode}
    async with Client(params, mode=mode, read_timeout_seconds=60) as c:
        out["protocol"] = c.protocol_version
        tools = (await c.list_tools()).tools
        out["list_ms"] = round(1000 * (time.perf_counter() - t0), 1)
        out["tools"] = [t.name for t in tools]
        out["hints_ok"] = all(bool(t.annotations and t.annotations.read_only_hint is True
                                   and t.annotations.destructive_hint is False
                                   and t.annotations.open_world_hint is False) for t in tools)
        out["calls"] = []
        for name, a in MCP_CALLS.get(toolset, []):
            r = await c.call_tool(name, a)
            sandboxed = (r.structured_content or {}).get("provenance", {}).get("sandboxed")
            out["calls"].append({"tool": name, "is_error": bool(r.is_error), "sandboxed": sandboxed,
                                 "chars": len(r.content[0].text) if r.content else 0})
    out["total_ms"] = round(1000 * (time.perf_counter() - t0), 1)
    return out


def _fd_path(fd: int) -> str:
    """Real path behind an inherited fd when it is a regular file, else ''."""
    import fcntl
    import stat

    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            return ""
        return fcntl.fcntl(fd, fcntl.F_GETPATH, b"\0" * 1024).split(b"\0", 1)[0].decode()
    except OSError:
        return ""


def reported_denials(start: dt.datetime) -> list[str]:
    """Kernel sandbox denials reported for python processes since `start` (needs an admin user)."""
    out = subprocess.run(["/usr/bin/log", "show", "--start", start.strftime("%Y-%m-%d %H:%M:%S"), "--style", "compact",
                          "--predicate", 'sender == "Sandbox"'], capture_output=True, text=True, check=False,
                         timeout=120).stdout
    pat = re.compile(r"Sandbox: (?:Python|python[0-9.]*)\(\d+\) deny\(\d+\) (.*)$")
    return [m.group(1) for line in out.splitlines() if (m := pat.search(line))]


CYPHER_REFUSE = ["LOAD FROM '/etc/hosts' RETURN *", "MATCH (r:Renewal) RETURN r.churned LIMIT 1",
                 "CALL timeout=0", "MATCH (a)-[*]->(b) RETURN count(*)", "INSTALL json",
                 "MATCH (n) RETURN n LIMIT 1; MATCH (m) DELETE m"]


async def cypher_session(graph_root: Path, mode: str, build: str) -> dict:
    """An MCP client against scripts/graph_mcp.sh --enable-cypher: one tool, it answers, the guard refuses."""
    from mcp import Client, StdioServerParameters

    params = StdioServerParameters(command=str(LAUNCH), args=["--enable-cypher", "--build", build],
                                   env=launcher_env(graph_root))
    out: dict = {"mode": mode}
    async with Client(params, mode=mode, read_timeout_seconds=60) as c:
        out["protocol"] = c.protocol_version
        tools = (await c.list_tools()).tools
        out["tools"] = [t.name for t in tools]
        out["hints_ok"] = all(bool(t.annotations and t.annotations.read_only_hint is True
                                   and t.annotations.destructive_hint is False) for t in tools)
        r = await c.call_tool("graph_cypher", {"query": "MATCH (r:Renewal) RETURN count(r) AS n"})
        body = r.structured_content or {}
        out["answer"] = {"is_error": bool(r.is_error), "sandboxed": body.get("provenance", {}).get("sandboxed"),
                         "n": (body.get("data", {}).get("rows") or [{}])[0].get("n")}
        out["refused"] = []
        for q in CYPHER_REFUSE:
            r = await c.call_tool("graph_cypher", {"query": q})
            out["refused"].append(bool(r.is_error) and "refused" in (r.content[0].text if r.content else ""))
    return out


def check_cypher(a: argparse.Namespace, graph_root: Path, build_dir: Path, outer: Path, fixture: Path) -> None:
    """--cypher: the guarded raw-Cypher server (PLAN Later A) in its evidence-only sandbox."""
    build = str(build_dir)
    print("[C1] cypher probe: evidence-only sandbox, the Cypher guard BYPASSED (engine opened directly)")
    if not check((build_dir / "evidence.lbdb").is_file() and (build_dir / "evidence.json").is_file(),
                 "the build has an evidence graph (scripts/build_evidence_graph.py)"):
        return
    cmd = [str(LAUNCH), "--probe", "--enable-cypher", "--build", build, "--", "--outer-home", str(outer)]
    r = subprocess.run(cmd, env=launcher_env(graph_root), capture_output=True, text=True, check=False, timeout=240,
                       stdin=subprocess.DEVNULL)
    try:
        doc = json.loads(r.stdout)
    except json.JSONDecodeError:
        doc = None
    REPORT["cypher_probe"] = doc
    if not check(doc is not None and r.returncode == 0, "cypher probe ran under the evidence-only sandbox",
                 f"rc={r.returncode} stderr={r.stderr.strip()[:300]}"):
        return
    check(doc["sandboxed_env"] == "1" and r.stderr.strip() == "", "GRAPH_SANDBOXED=1, no stderr noise",
          r.stderr.strip()[:200])
    g = by_group(doc)
    expect_group(g, "works", {"allowed"}, "graph_cypher answers and the evidence graph opens (guard on, then off)",
                 a.verbose)
    expect_group(g, "labels", {"denied"}, "with the guard bypassed, every read of a label-bearing file is DENIED by "
                                          "the OS (LOAD FROM the Renewal / BILLED Parquet, ATTACH graph.lbdb, Python "
                                          "reads of Parquet, graph.lbdb, manifest.json)", a.verbose)
    expect_group(g, "files", {"denied", "absent"}, "other files: /etc/hosts, the build dir and graph root listings, "
                                                   "the audit key, the repo, fake ~/.ssh ~/.aws ... denied", a.verbose)
    expect_group(g, "write", {"denied"}, "COPY TO /tmp, EXPORT DATABASE, COPY / Python writes into the build denied, "
                                         "no side effects", a.verbose, no_side_effects=True)
    expect_group(g, "engine", {"denied", "error", "absent"}, "CREATE, extension install / load and an https LOAD FROM "
                                                             "do not succeed", a.verbose)
    expect_group(g, "net", {"denied"}, "TCP, DNS and loopback denied", a.verbose)
    expect_group(g, "proc", {"denied"}, "subprocess and fork denied", a.verbose)
    for row in g.get("logs-info", []):
        print(f"  info  {row['probe']}: {row['outcome']} (the logs dir is write-only: never read back, never "
              f"executable; documented residual)")

    print("[C2] MCP client -> scripts/graph_mcp.sh --enable-cypher -> sandbox-exec -> cypher server")
    if a.log_check:
        time.sleep(3.0)
    log_start = dt.datetime.now()
    for mode in ("legacy", "2026-07-28"):
        try:
            s = asyncio.run(cypher_session(graph_root, mode, build))
            REPORT.setdefault("cypher_mcp", []).append(s)
            check(s["tools"] == ["graph_cypher"] and s["hints_ok"] and not s["answer"]["is_error"] and
                  s["answer"]["sandboxed"] is True and all(s["refused"]),
                  f"cypher/{mode}: exactly graph_cypher, it answers (n={s['answer']['n']}, sandboxed=true), "
                  f"{sum(s['refused'])}/{len(CYPHER_REFUSE)} deny-list statements refused",
                  f"protocol {s['protocol']}")
        except Exception as e:  # noqa: BLE001 - any failure is a check failure
            check(False, f"cypher/{mode}: MCP session", f"{type(e).__name__}: {str(e)[:200]}")
    if a.log_check:
        time.sleep(2.0)
        den = reported_denials(log_start)
        own = {_fd_path(fd) for fd in (0, 1, 2)} - {""}
        den = [d for d in den if not any(d == f"file-read-metadata {p}" for p in own)]
        # the server's own start check (cypher_guard.sandbox_status) tries these on purpose: each must be denied
        expected = {f"file-read-data {p}" for p in ("/private/etc/hosts", build_dir / "graph.lbdb",
                                                    build_dir / "manifest.json",
                                                    build_dir / "parquet" / "nodes_Renewal.parquet")}
        mine = [d for d in den if d in expected]
        den = [d for d in den if d not in expected]
        print(f"  info  {len(mine)} denials are the server's own start check (it opens /private/etc/hosts and the "
              f"build's label-bearing files and requires 'operation not permitted')")
        check(not den, f"unified log: {len(den)} other sandbox denials during the cypher MCP sessions",
              "; ".join(sorted(set(den))[:8]))

    print("[C3] graph_cypher is absent by default and refused outside the sandbox")
    r = subprocess.run([str(LAUNCH), "--toolset", "all", "--print-tools"], env=launcher_env(graph_root),
                       capture_output=True, text=True, check=False, timeout=60, stdin=subprocess.DEVNULL)
    names = [t["name"] for t in json.loads(r.stdout)] if r.returncode == 0 else []
    check(r.returncode == 0 and "graph_cypher" not in names, f"the default servers list {len(names)} tools, no "
                                                              f"graph_cypher")
    mcp_json = json.loads((REPO / ".mcp.json").read_text(encoding="utf-8"))
    check(not any("--enable-cypher" in s.get("args", []) for s in mcp_json.get("mcpServers", {}).values()),
          ".mcp.json starts no cypher server")
    for env_extra, label in (({"GRAPH_SANDBOX": "0"}, "GRAPH_SANDBOX=0"), ({}, "--toolset graph")):
        args = ["--enable-cypher", "--build", build] + (["--toolset", "graph"] if not env_extra else [])
        r = subprocess.run([str(LAUNCH), *args], env=launcher_env(graph_root, env_extra), capture_output=True,
                           text=True, check=False, timeout=60, stdin=subprocess.DEVNULL)
        check(r.returncode == 64 and r.stdout == "", f"--enable-cypher with {label}: refused (exit 64)",
              r.stderr.strip()[:120])

    if a.control:
        print("[C4] control: the same bypass UNSANDBOXED (safe subset) -- the label reads must SUCCEED")
        probe = fixture / f"{PROBE_MODULE}.py"
        r = subprocess.run([sys.executable, str(probe), "probe", "--cypher", "--control", "--build", build, "--logs",
                            str(graph_root / "logs"), "--repo", str(REPO)], capture_output=True, text=True,
                           check=False, timeout=240, stdin=subprocess.DEVNULL, cwd=str(REPO / "src"))
        try:
            doc_c = json.loads(r.stdout)
        except json.JSONDecodeError:
            doc_c = None
        REPORT["cypher_control"] = doc_c
        if check(doc_c is not None, "control probe ran", f"rc={r.returncode} {r.stderr.strip()[:200]}"):
            gc_ = by_group(doc_c)
            labels = gc_.get("labels", [])
            check(labels and all(x["outcome"] == "allowed" for x in labels),
                  f"unsandboxed: {len(labels)}/{len(labels)} label-bearing reads succeed (LOAD FROM Parquet, ATTACH "
                  f"graph.lbdb): the OS layer is what stops them")
            w = gc_.get("write", [])
            check(w and all(x["outcome"] == "allowed" for x in w), "unsandboxed: COPY TO /tmp succeeds (removed)")


def contract_ok(build: Path) -> bool:
    try:
        return json.loads((build / "contract.json").read_text(encoding="utf-8")).get("status") == "pass"
    except (OSError, ValueError):
        return False


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv[:1] == ["probe"]:
        return probe_main(argv[1:])
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--build", default=None, help="build dir (default: $GRAPH_ROOT/current)")
    ap.add_argument("--graph-root", default=None, help="default: $GRAPH_ROOT or <repo>/data/graph")
    ap.add_argument("--control", action="store_true", help="also run the unsandboxed comparison (safe subset)")
    ap.add_argument("--log-check", action="store_true", help="also require zero reported denials in the unified log")
    ap.add_argument("--control-network", action="store_true",
                    help="let the unsandboxed --control probe reach the internet (DNS, TCP 1.1.1.1, HTTPS "
                         "example.com); off by default: the control's network probes stay on this machine")
    ap.add_argument("--allow-unchecked", action="store_true", help="the build may lack a graph contract pass")
    ap.add_argument("--toolsets", default="graph,metrics,lineage,cohorts")
    ap.add_argument("--cypher", action="store_true",
                    help="also check the guarded raw-Cypher server (needs the build's evidence graph): the guard "
                         "bypassed inside its evidence-only sandbox, MCP sessions, absent by default")
    ap.add_argument("--cypher-only", action="store_true", help="only the --cypher checks (and the static ones)")
    ap.add_argument("--json", default=None, help="write the full report (all probe rows) to this file")
    ap.add_argument("-v", "--verbose", action="store_true")
    a = ap.parse_args(argv)
    a.cypher = a.cypher or a.cypher_only

    if sys.platform != "darwin":
        print("graph_sandbox_check: SKIP (macOS only; Linux runs without an OS sandbox)")
        return 0

    graph_root = Path(a.graph_root or os.environ.get("GRAPH_ROOT") or REPO / "data" / "graph").resolve()
    build_dir = Path(a.build).resolve() if a.build else (graph_root / "current").resolve()
    build = str(build_dir)
    unchecked = a.allow_unchecked or not contract_ok(build_dir)
    logs = graph_root / "logs"
    fixture = graph_root / FIXTURE_DIR
    outer = Path(tempfile.mkdtemp(prefix="graph-sandbox-check-")).resolve()
    real_home = str(Path.home().resolve())
    print(f"macOS {platform.mac_ver()[0]} ({platform.machine()}); python {platform.python_version()}; "
          f"GRAPH_ROOT={graph_root}; build={build_dir.name}")
    try:
        # ------------------------------------------------------------------ fixtures
        shutil.rmtree(fixture, ignore_errors=True)
        (fixture / "home").mkdir(parents=True)
        make_home(fixture / "home")
        make_home(outer)
        (fixture / "canary.txt").write_text("canary\n", encoding="utf-8")
        for rel in ENV_FILES:
            (fixture / rel).parent.mkdir(parents=True, exist_ok=True)
            (fixture / rel).write_text(f"k,v\nAPI_KEY,{SENTINEL}\n", encoding="utf-8")
        shutil.copyfile(Path(__file__).resolve(), fixture / f"{PROBE_MODULE}.py")

        print("[1] static")
        check(os.access("/usr/bin/sandbox-exec", os.X_OK), "/usr/bin/sandbox-exec is present and executable")
        check(PROFILE.is_file(), f"profile present: {PROFILE.relative_to(REPO)}")
        ltxt = LAUNCH.read_text(encoding="utf-8")
        check('SANDBOX_EXEC="/usr/bin/sandbox-exec"' in ltxt and "env -i" in ltxt,
              "launcher pins /usr/bin/sandbox-exec (no PATH lookup) and scrubs the environment")
        ptxt = PROFILE.read_text(encoding="utf-8")
        check("(deny default)" in ptxt and "(allow default)" not in ptxt and not re.search(r"\(allow network", ptxt)
              and not re.search(r"\(allow process-fork", ptxt) and not re.search(r"\(allow mach-lookup", ptxt),
              "profile is deny-default with no network / fork / mach-lookup allowance")

        if a.cypher_only:
            check_cypher(a, graph_root, build_dir, outer, fixture)
        else:
            print("[2] probe A: sandboxed, real $HOME (the tools answer while the attacks fail)")
            doc, exec_line, r = run_probe(graph_root, build, ["--fixture", str(fixture), "--outer-home", str(outer),
                                                              "--real-home", real_home, "--exec-test"])
            REPORT["probe_a"] = doc
            if not check(doc is not None and r.returncode == 0, "probe ran under the sandbox",
                         f"rc={r.returncode} stderr={r.stderr.strip()[:300]}"):
                return 1
            check(doc["sandboxed_env"] == "1", "launcher reported GRAPH_SANDBOXED=1")
            check(r.stderr.strip() == "", "no stderr noise from the sandboxed process", r.stderr.strip()[:200])
            g = by_group(doc)
            expect_group(g, "works", {"allowed"}, "runtime basics and the lakehouse tools still work", a.verbose)
            tools_row = next((x for x in g.get("works", []) if x["probe"] == "lakehouse_graph tools answer"), {})
            print(f"  info  {tools_row.get('detail', '')}")
            expect_group(g, "write", {"denied"}, "writes outside the logs dir denied (Python, COPY TO /tmp, EXPORT "
                                                 "DATABASE), no side effects", a.verbose, no_side_effects=True)
            expect_group(g, "logs-allowed", {"allowed"}, "regular files can be created in the logs dir", a.verbose)
            expect_group(g, "logs", {"denied"}, "logs dir is write-only + files-only (no read-back/dlopen/mkdir/unlink/"
                                                "chmod/symlink/socket)", a.verbose, no_side_effects=True)
            expect_group(g, "engine", {"denied", "error", "absent"},
                         "engine statements (CREATE / extension install and load / https) do not succeed", a.verbose)
            expect_group(g, "read-outer", {"denied"}, "fake credentials outside the readable trees denied (Python + "
                                                      "LOAD FROM ~/.ssh ..., hard/symlink tricks)", a.verbose,
                         no_side_effects=True)
            expect_group(g, "read-env", {"denied"}, ".env, .env.local, .envrc, .env-prod, .env/ denied inside a "
                                                    "readable tree", a.verbose)
            expect_group(g, "read-repo", {"denied", "absent"}, "repo root / .git / .env not readable", a.verbose)
            expect_group(g, "read-real", {"denied", "absent"}, "real ~/.ssh ~/.aws ~/.config ... denied (open-only, "
                                                               "nothing read)", a.verbose)
            expect_group(g, "read-system", {"denied", "absent"}, "/etc/hosts, /Users, /Volumes, /private/tmp, "
                                                                 "/System/Volumes ... denied", a.verbose)
            expect_group(g, "net", {"denied"}, "outbound TCP (1.1.1.1, IPv6, loopback), UDP, DNS, bind, AF_UNIX, HTTPS "
                                               "denied", a.verbose)
            expect_group(g, "proc", {"denied"}, "subprocess / fork / signalling other pids denied", a.verbose)
            expect_group(g, "mach", {"denied"}, "mach bootstrap look-ups denied (dnssd, securityd, pasteboard, ...)",
                         a.verbose)
            expect_group(g, "ipc", {"denied"}, "POSIX shm / named semaphores denied", a.verbose)
            check(exec_line.startswith("EXEC_PROBE: denied"), "os.execv(/bin/echo) denied", exec_line[:120])
            for row in g.get("info", []):
                print(f"  info  {row['probe']}: {row['outcome']} {row['detail'][:90]}")

            print("[3] probe B: sandboxed, HOME = fake home inside the readable graph root")
            inner = str(fixture / "home")
            doc_b, _, r = run_probe(graph_root, build, ["--fixture", str(fixture)], {"HOME": inner})
            REPORT["probe_b"] = doc_b
            if check(doc_b is not None and r.returncode == 0, "probe ran",
                     f"rc={r.returncode} {r.stderr.strip()[:200]}"):
                gb = by_group(doc_b)
                expect_group(gb, "read-inner", {"denied"}, "explicit $HOME deny rules hold inside a readable tree",
                             a.verbose)
                ctl = gb.get("read-inner-control", [{}])[0]
                print(f"  info  non-credential file in the readable tree: {ctl.get('outcome')} -- expected: only the "
                      f"listed credential paths are re-denied")

            print("[4] probe M: sandboxed, pandas-only (no ladybug)")
            doc_m, _, r = run_probe(graph_root, build, ["--skip-ladybug", "--outer-home", str(outer), "--fixture",
                                                        str(fixture)])
            REPORT["probe_m"] = doc_m
            if check(doc_m is not None and r.returncode == 0, "probe ran",
                     f"rc={r.returncode} {r.stderr.strip()[:200]}"):
                gm = by_group(doc_m)
                expect_group(gm, "works", {"allowed"}, "pandas / pyarrow / numpy / networkx and the metric tools work",
                             a.verbose)
                expect_group(gm, "write", {"denied"}, "writes denied", a.verbose, no_side_effects=True)
                expect_group(gm, "read-outer", {"denied"}, "fake credentials denied", a.verbose, no_side_effects=True)
                expect_group(gm, "net", {"denied"}, "network denied", a.verbose)

            print("[5] MCP client -> scripts/graph_mcp.sh -> sandbox-exec -> server (legacy and 2026-07-28)")
            if a.log_check:
                time.sleep(3.0)   # `log show --start` has 1 s granularity: keep the probe denials out of the window
            log_start = dt.datetime.now()
            REPORT["mcp"] = []
            toolsets = [t for t in a.toolsets.split(",") if t]
            for optional, needs in (("lineage", "lineage.lbdb"), ("cohorts", "cohorts.parquet")):
                if optional in toolsets and not (build_dir / needs).is_file():   # its server would answer unavailable
                    toolsets.remove(optional)
                    print(f"  info  no {needs} in this build: the {optional} toolset is skipped (its calls would "
                          f"answer unavailable)")
            for toolset in toolsets:
                for mode in ("legacy", "2026-07-28"):
                    try:
                        s = asyncio.run(mcp_session(graph_root, toolset, mode, build, unchecked))
                        REPORT["mcp"].append(s)
                        ok = (len(s["tools"]) == EXPECTED_TOOLS.get(toolset) and s["calls"] and s["hints_ok"]
                              and not any(c["is_error"] or c["sandboxed"] is not True for c in s["calls"]))
                        check(bool(ok), f"{toolset}/{mode}: {len(s['tools'])} tools listed, {len(s['calls'])} called "
                                        f"(provenance sandboxed=true), readOnlyHint on all",
                              f"protocol {s['protocol']}, list {s['list_ms']} ms, total {s['total_ms']} ms")
                    except Exception as e:  # noqa: BLE001 - the probe records every outcome
                        check(False, f"{toolset}/{mode}: MCP session", f"{type(e).__name__}: {str(e)[:200]}")
            if a.log_check:
                time.sleep(2.0)
                den = reported_denials(log_start)
                own = {_fd_path(fd) for fd in (0, 1, 2)} - {""}
                den = [d for d in den if not any(d == f"file-read-metadata {p}" for p in own)]
                check(not den, f"unified log: {len(den)} reported sandbox denials during the normal MCP sessions",
                      "; ".join(sorted(set(den))[:5]))

            print("[6] fail closed")
            t0 = time.perf_counter()
            r = subprocess.run([str(LAUNCH), "--toolset", "graph", "--build", build],
                               env=launcher_env(graph_root, {"GRAPH_SANDBOX_SIMULATE_MISSING": "1"}),
                               capture_output=True, text=True, stdin=subprocess.DEVNULL, check=False, timeout=30)
            check(r.returncode == 78 and r.stdout == "" and "fail closed" in r.stderr,
                  "sandbox-exec missing (simulated): exit 78, nothing on stdout, message on stderr",
                  f"rc={r.returncode} in {1000 * (time.perf_counter() - t0):.0f} ms; stderr={r.stderr.strip()[:110]}")
            saved, devnull = os.dup(2), os.open(os.devnull, os.O_WRONLY)   # the launcher's message is expected: hide it
            t0 = time.perf_counter()
            err: BaseException | None = None
            try:
                os.dup2(devnull, 2)
                asyncio.run(asyncio.wait_for(mcp_session(graph_root, "graph", "legacy", build, unchecked,
                                                         {"GRAPH_SANDBOX_SIMULATE_MISSING": "1"}), 20))
            except BaseException as e:  # noqa: BLE001 - any failure is the expected result
                err = e
            finally:
                os.dup2(saved, 2)
                os.close(saved)
                os.close(devnull)
            check(err is not None and not isinstance(err, (KeyboardInterrupt, asyncio.TimeoutError)),
                  "an MCP client against a fail-closed launcher errors out instead of hanging",
                  f"{type(err).__name__} after {1000 * (time.perf_counter() - t0):.0f} ms")
            r = subprocess.run([str(LAUNCH), "--probe", "--build", build, "--", "--control", "--skip-ladybug"],
                               env=launcher_env(graph_root, {"GRAPH_SANDBOX": "0"}), capture_output=True, text=True,
                               check=False, timeout=120, stdin=subprocess.DEVNULL)
            check(r.returncode == 0 and "sandbox DISABLED" in r.stderr and '"sandboxed_env": "0"' in r.stdout,
                  "GRAPH_SANDBOX=0 is the only opt-out and prints the banner on stderr")

            if a.control:
                print("[7] control: the same probe UNSANDBOXED (safe subset) -- these must SUCCEED")
                (fixture / "canary.txt").write_text("canary\n", encoding="utf-8")
                control_args = ["--control", "--fixture", str(fixture), "--outer-home", str(outer)]
                if a.control_network:
                    control_args.append("--external-network")
                doc_c, _, r = run_probe(graph_root, build, control_args, {"GRAPH_SANDBOX": "0"})
                REPORT["probe_control"] = doc_c
                if check(doc_c is not None, "control probe ran", f"rc={r.returncode}"):
                    gc_ = by_group(doc_c)
                    w = [x for x in gc_.get("write", []) if x["probe"] in (
                        "python write /tmp", "cypher COPY TO /tmp", "cypher EXPORT DATABASE /tmp",
                        "unlink canary (read-only tree)")]
                    check(w and all(x["outcome"] == "allowed" for x in w),
                          f"unsandboxed: {len(w)} write probes succeed (COPY TO /tmp, EXPORT DATABASE, unlink)")
                    o = gc_.get("read-outer", [])
                    leaks = sum("LEAK" in x["detail"] or SENTINEL in x["detail"] for x in o)
                    check(leaks >= len(CREDENTIAL_FILES), f"unsandboxed: {leaks}/{len(o)} fake-credential reads "
                                                          f"(Python and LOAD FROM) return the sentinel")
                    p = [x for x in gc_.get("proc", []) if x["probe"] in ("subprocess /bin/ls", "os.fork()")]
                    check(p and all(x["outcome"] == "allowed" for x in p), "unsandboxed: subprocess and fork succeed")
                    n = {x["probe"]: x["outcome"] for x in gc_.get("net", [])}
                    check(n.get("bind+listen 127.0.0.1:0") == "allowed", "unsandboxed: a socket can listen (the "
                                                                         "sandboxed denial is real)")
                    external = [k for k in n if "example.com" in k or "1.1.1.1" in k or "2606:" in k]
                    check(bool(external) == a.control_network,
                          "unsandboxed control reaches for the internet only with --control-network",
                          f"{len(external)} external probes")
                    print(f"  info  unsandboxed network ({'internet opt-in' if a.control_network else 'local only'}): "
                          f"{n}")
            if a.cypher:
                check_cypher(a, graph_root, build_dir, outer, fixture)
    finally:
        shutil.rmtree(fixture, ignore_errors=True)
        shutil.rmtree(outer, ignore_errors=True)
        if logs.is_dir():
            for f in logs.glob("probe-*"):
                if f.is_dir() and not f.is_symlink():
                    shutil.rmtree(f, ignore_errors=True)
                else:
                    f.unlink(missing_ok=True)
        for stray in Path("/private/tmp").glob("sbprobe-*"):
            if stray.is_dir() and not stray.is_symlink():
                shutil.rmtree(stray, ignore_errors=True)
            else:
                stray.unlink(missing_ok=True)

    if a.json:
        Path(a.json).write_text(json.dumps(REPORT, indent=1), encoding="utf-8")
    print(f"\ngraph_sandbox_check: {'OK' if not FAILS else 'FAILED'} "
          f"({len(REPORT['checks']) - len(FAILS)}/{len(REPORT['checks'])} checks passed)")
    for f in FAILS:
        print(f"  failed: {f}")
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(main())
