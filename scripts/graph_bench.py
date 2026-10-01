#!/usr/bin/env python3
"""Build and serve benchmark of one graph profile: build time, max RSS, load time, file sizes, server start time,
per-tool warm p50 / p95 in process and over MCP stdio (scripts/graph_mcp.sh), and the servers' RSS.

  python scripts/graph_bench.py [--graph-root DIR] [--profile s42] [--calls 20] [--json F] [--md F]
                                [--no-build] [--scratch DIR] [--toolsets graph,metrics,lineage,cohorts]
  make graph-bench

Build. Unless --no-build, the profile is rebuilt from its own bronze in a SCRATCH graph root (--scratch, default a
temporary directory that is removed afterwards; the served build and $GRAPH_ROOT are never touched):
scripts/build_graph_local.py build, timed by wall clock, with the builder's own figures (seconds, max RSS via
ru_maxrss, Ladybug load seconds, loader max RSS, graph.lbdb bytes) read from the scratch build's manifest. With
--no-build those figures come from the served build's manifest (recorded when it was built) and say so.

Serve. The latest build of the profile (needs a passing graph contract, or --allow-unchecked): every toolset server
is started through scripts/graph_mcp.sh (sandboxed on macOS) and timed from spawn to its tool list; each tool is
called 3 times to warm up and --calls times measured (the same calls as scripts/check_graph_tools.py --bench, so the
numbers are comparable); the server's RSS is read with ps after the warm calls (pid from the launcher's pidfile).
The in-process figures call the same tool functions directly (lakehouse_graph.tools.call).

JSON (--json): the shape of docs/graph/results/tools-bench-<profile>.json, which scripts/graph_charts.py reads
({"bench": {"in_process": {tool: {p50, p95}}, "stdio": {...}, "rss_kb": {toolset: KiB}}, "bench_calls",
"build_id", "commit", "platform", "profile", "generated_by"}), plus "server_start_s", "build" (wall_s, builder_s,
builder_max_rss_mib, load_s, loader_max_rss_mib, graph_lbdb_bytes, source) and "files" (bytes per artefact).
Latencies are from one machine under whatever else it runs: indicative, not a benchmark suite.
Exit code: 0, 1 when a server or the build failed, 3 when the graph root holds no build of the profile (nothing
measured; the same code scripts/graph_charts.py uses for a missing build).
"""
from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lakehouse_graph import manifest as mf  # noqa: E402 (after the sys.path line above)
from lakehouse_graph import tools  # noqa: E402
from lakehouse_graph.context import ToolContext  # noqa: E402

LAUNCH = ROOT / "scripts" / "graph_mcp.sh"
EXIT_NO_BUILD = 3      # like scripts/graph_charts.py: "the build is missing", not a failed measurement


def bench_calls() -> list[tuple[str, dict]]:
    """The tool calls scripts/check_graph_tools.py --bench measures (one source of truth)."""
    path = ROOT / "scripts" / "check_graph_tools.py"
    spec_ = importlib.util.spec_from_file_location("check_graph_tools_for_bench", path)
    mod = importlib.util.module_from_spec(spec_)
    spec_.loader.exec_module(mod)
    return list(mod.BENCH_CALLS)


def pct(xs: list[float], q: float) -> float:
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(round(q * (len(xs) - 1))))], 2)


def latest(graph_root: Path, profile: str) -> Path | None:
    link = graph_root / profile / "latest"
    if (link / "manifest.json").is_file():
        return link.resolve()
    pdir = graph_root / profile
    builds = sorted((pdir / "builds").glob("*/manifest.json")) if pdir.is_dir() else []
    return builds[-1].parent.resolve() if builds else None


def dir_bytes(p: Path) -> int:
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file()) if p.is_dir() else (p.stat().st_size
                                                                                          if p.is_file() else 0)


def build_figures(man: dict) -> dict:
    b, lb = man.get("builder") or {}, man.get("ladybug") or {}
    return {"builder_s": b.get("seconds"), "builder_max_rss_mib": b.get("max_rss_mib"), "load_s": lb.get("load_s"),
            "loader_max_rss_mib": round(lb["max_rss_bytes"] / 2 ** 20, 1) if lb.get("max_rss_bytes") else None,
            "graph_lbdb_bytes": lb.get("db_bytes"), "nodes": (man.get("counts") or {}).get("total_nodes"),
            "edges": (man.get("counts") or {}).get("total_edges")}


def fresh_build(graph_root: Path, profile: str, scratch: Path) -> dict:
    """Rebuild the profile from its bronze in a scratch graph root; never touches graph_root's builds."""
    env = dict(os.environ, PYTHONPATH=str(ROOT / "src"), GRAPH_ROOT=str(scratch))
    if profile == "default":
        env.setdefault("CHURN_SAMPLE_DIR", str(ROOT / "data" / "sample" / "churn"))
        env.setdefault("CHURN_EXPORT_DIR", str(ROOT / "data" / "export"))
    else:
        for part in ("sample", "export"):
            src = graph_root / profile / part
            if src.is_dir():
                shutil.copytree(src, scratch / profile / part, dirs_exist_ok=True)
        meta = graph_root / profile / "sample_meta.json"
        if meta.is_file():
            shutil.copy2(meta, scratch / profile / meta.name)
    cmd = [sys.executable, str(ROOT / "scripts" / "build_graph_local.py"), "build", "--profile", profile,
           "--graph-root", str(scratch)]
    t0 = time.perf_counter()
    p = subprocess.run(cmd, cwd=ROOT, env=env, capture_output=True, text=True, check=False)
    wall = round(time.perf_counter() - t0, 2)
    if p.returncode:
        return {"ok": False, "wall_s": wall, "error": (p.stdout + p.stderr)[-800:]}
    b = latest(scratch, profile)
    man = json.loads((b / "manifest.json").read_text(encoding="utf-8"))
    return {"ok": True, "source": "fresh build in a scratch graph root", "wall_s": wall, **build_figures(man),
            "parquet_bytes": dir_bytes(b / "parquet")}


def in_process(ctx: ToolContext, calls: list[tuple[str, dict]], toolsets: list[str], n: int) -> dict:
    out = {}
    for name, args in calls:
        if tools.SPECS[name].toolset not in toolsets:
            continue
        for _ in range(3):
            tools.call(ctx, name, args)
        ts = []
        for _ in range(n):
            t0 = time.perf_counter()
            tools.call(ctx, name, args)
            ts.append((time.perf_counter() - t0) * 1000)
        out[name] = {"p50": pct(ts, 0.5), "p95": pct(ts, 0.95)}
    return out


async def stdio(graph_root: Path, build: Path, toolset: str, calls: list[tuple[str, dict]], n: int, gpy: str,
                unchecked: bool) -> dict:
    from mcp import Client, StdioServerParameters

    env = {"GRAPH_PY": gpy, "GRAPH_ROOT": str(graph_root), "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
           "HOME": os.environ.get("HOME", "/")}
    args = ["--toolset", toolset, "--build", str(build)] + (["--allow-unchecked"] if unchecked else [])
    started = time.time() - 1
    t0 = time.perf_counter()
    lat: dict = {}
    async with Client(StdioServerParameters(command=str(LAUNCH), args=args, env=env), mode="legacy",
                      read_timeout_seconds=90) as c:
        await c.list_tools()
        start_s = round(time.perf_counter() - t0, 2)
        pids = [p for p in (build / ".pids").glob("*.pid") if p.stat().st_mtime >= started]
        pid = int(max(pids, key=lambda p: p.stat().st_mtime).stem) if pids else None
        for name, a in calls:
            if tools.SPECS[name].toolset != toolset:
                continue
            for _ in range(3):
                await c.call_tool(name, a)
            ts = []
            for _ in range(n):
                t1 = time.perf_counter()
                await c.call_tool(name, a)
                ts.append((time.perf_counter() - t1) * 1000)
            lat[name] = {"p50": pct(ts, 0.5), "p95": pct(ts, 0.95)}
        rss = None
        if pid:
            p = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True, check=False)
            rss = int(p.stdout.strip()) if p.stdout.strip().isdigit() else None
    return {"latency": lat, "rss_kb": rss, "start_s": start_s}


def markdown(rep: dict) -> str:
    b = rep["build"]
    lines = ["# Build and serve benchmark", "",
             f"Profile {rep['profile']}, build `{rep['build_id']}`, {rep['platform']}, commit {rep['commit']}; "
             f"{rep['bench_calls']} warm calls per tool. Generated by `scripts/graph_bench.py`.", "",
             "| build | value |", "|---|---:|",
             f"| source | {b.get('source')} |", f"| wall clock (s) | {b.get('wall_s')} |",
             f"| builder (s) | {b.get('builder_s')} |", f"| builder max RSS (MiB) | {b.get('builder_max_rss_mib')} |",
             f"| Ladybug load (s) | {b.get('load_s')} |", f"| loader max RSS (MiB) | {b.get('loader_max_rss_mib')} |",
             f"| graph.lbdb (MB) | {round((b.get('graph_lbdb_bytes') or 0) / 1e6, 1)} |",
             f"| nodes / edges | {b.get('nodes')} / {b.get('edges')} |", "",
             "| file | bytes |", "|---|---:|"]
    lines += [f"| {k} | {v:,} |" for k, v in rep["files"].items()]
    lines += ["", "| toolset | server start (s) | RSS (MiB) |", "|---|---:|---:|"]
    for ts, s in rep["server_start_s"].items():
        rss = rep["bench"]["rss_kb"].get(ts)
        lines.append(f"| {ts} | {s} | {round(rss / 1024, 1) if rss else '-'} |")
    lines += ["", "| tool | in process p50 / p95 (ms) | stdio p50 / p95 (ms) |", "|---|---:|---:|"]
    for name, v in rep["bench"]["stdio"].items():
        ip = rep["bench"]["in_process"].get(name, {})
        lines.append(f"| {name} | {ip.get('p50')} / {ip.get('p95')} | {v['p50']} / {v['p95']} |")
    lines += ["", "One machine under whatever else it runs: indicative, not a benchmark suite.", ""]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Build and serve benchmark of one graph profile.")
    ap.add_argument("--graph-root", default=None)
    ap.add_argument("--profile", default=None, help="default: s42 if built, else default, else tiny")
    ap.add_argument("--calls", type=int, default=20)
    ap.add_argument("--toolsets", default=",".join(tools.TOOLSETS))
    ap.add_argument("--no-build", action="store_true", help="read the build figures from the served build's manifest")
    ap.add_argument("--scratch", default=None, help="scratch graph root for the fresh build (kept when given)")
    ap.add_argument("--allow-unchecked", action="store_true")
    ap.add_argument("--json", default=None)
    ap.add_argument("--md", default=None)
    a = ap.parse_args(argv)
    graph_root = Path(a.graph_root or os.environ.get("GRAPH_ROOT") or ROOT / "data" / "graph").resolve()
    profiles = [a.profile] if a.profile else ["s42", "default", "tiny"]
    profile = next((p for p in profiles if latest(graph_root, p)), None)
    if profile is None:
        print(f"graph_bench: SKIP: no build for {', '.join(profiles)} under {graph_root} (make graph-local); "
              f"nothing measured", file=sys.stderr)
        return EXIT_NO_BUILD
    build = latest(graph_root, profile)
    man = json.loads((build / "manifest.json").read_text(encoding="utf-8"))
    rc = 0
    if a.no_build:
        figures = {"ok": True, "source": "manifest of the served build (measured when it was built)", "wall_s": None,
                   **build_figures(man), "parquet_bytes": dir_bytes(build / "parquet")}
    else:
        tmp = None
        scratch = Path(a.scratch).resolve() if a.scratch else Path(tmp := tempfile.mkdtemp(prefix="graph-bench-"))
        try:
            figures = fresh_build(graph_root, profile, scratch)
        finally:
            if tmp:
                shutil.rmtree(tmp, ignore_errors=True)
        rc |= not figures["ok"]
    toolsets = [t for t in a.toolsets.split(",") if t]
    calls = bench_calls()
    ctx = ToolContext(build, graph_root=graph_root, allow_unchecked=a.allow_unchecked)
    try:
        ip = in_process(ctx, calls, toolsets, a.calls)
    finally:
        ctx.close()
    bench = {"in_process": ip, "stdio": {}, "rss_kb": {}}
    starts = {}
    for ts in toolsets:
        try:
            r = asyncio.run(stdio(graph_root, build, ts, calls, a.calls, sys.executable, a.allow_unchecked))
        except Exception as exc:  # noqa: BLE001 - a server that fails is reported, the others still run
            print(f"graph_bench: {ts} server failed: {type(exc).__name__}: {exc}", file=sys.stderr)
            rc = 1
            continue
        bench["stdio"].update(r["latency"])
        bench["rss_kb"][ts] = r["rss_kb"]
        starts[ts] = r["start_s"]
    rep = {"generated_by": "scripts/graph_bench.py", "profile": profile, "build_id": man.get("business_build_id"),
           "commit": man.get("commit"), "platform": mf.platform_tag(), "bench_calls": a.calls, "bench": bench,
           "server_start_s": starts, "build": figures,
           "files": {"graph.lbdb": dir_bytes(build / "graph.lbdb"), "parquet/": dir_bytes(build / "parquet"),
                     "lineage.lbdb": dir_bytes(build / "lineage.lbdb"), "lineage/": dir_bytes(build / "lineage"),
                     "cohorts.parquet": dir_bytes(build / "cohorts.parquet"),
                     "similar_to_scaler.parquet": dir_bytes(build / "similar_to_scaler.parquet")}}
    if a.json:
        Path(a.json).write_text(json.dumps(rep, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    md = markdown(rep)
    if a.md:
        Path(a.md).write_text(md, encoding="utf-8")
    print(md)
    print(f"graph_bench: {'OK' if not rc else 'FAILED'} (profile {profile}, build {rep['build_id']})")
    return rc


if __name__ == "__main__":
    sys.exit(main())
