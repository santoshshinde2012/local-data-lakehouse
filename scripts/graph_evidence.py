#!/usr/bin/env python3
"""Run the graph checks and record what they printed in docs/graph/results/.

It runs the repo's existing checks against the builds already in a GRAPH_ROOT (it never builds; build
first with `make graph-sample` / `make graph-local` / scripts/build_lineage_local.py /
scripts/build_graph_cohorts.py), writes
one markdown file per check with a provenance header and the redacted output, writes
docs/graph/results/index.md (pass / fail / not run per check, with links), then calls
scripts/graph_charts.py so docs/graph/img/ and the generated regions of docs/graph/*.md come from the
same builds and results.

  graph-contract-<profile>.md  scripts/check_graph_contract.py --strict, per profile (tiny, s42, default)
  graph-contract-iceberg.md    the same on an Iceberg-sourced build, when the GRAPH_ROOT holds one
  lineage-contract.md          scripts/check_lineage_contract.py --strict (default build, else s42)
  repo-contracts.md            scripts/check_repo_contracts.py
  graph-tools-<profile>.md     scripts/check_graph_tools.py (tiny; s42 with --bench N): goldens, schema, leak
                               sweep over every renewal, hygiene, audit, small cells, lint, junk args, MCP smoke
  tools-bench-s42.json         the bench section of the s42 tools check (p50 / p95 per tool, server RSS)
  sandbox-check.md             scripts/graph_sandbox_check.py --control --log-check (macOS only)
  graph-parity-<profile>.md    scripts/check_graph_parity.py parity --strict (needs .venv-graph-spark + JDK 17 or 21)
  cohorts.md                   scripts/build_graph_cohorts.py list (leiden, louvain) + the hero's cohort
  bench.md / eval.md / leakage.md   when scripts/graph_bench.py, an eval report or scripts/graph_leakage_demo.py
                               exist; otherwise the file says "not available yet"
  docker-e2e.md                timings and summary lines of a recorded Docker run (--docker-timings, --docker-log)
  tool-catalogue.md            the tool registry (lakehouse_graph.tools.TOOLSETS): names, arguments, descriptions
  index.md                     the summary

Every file starts with: status and exit code, the command line, commit + dirty flag, the date (UTC),
the host platform, Python and package versions, the duration. Output is redacted before it is written:
the repo root becomes repo-relative, the GRAPH_ROOT becomes $GRAPH_ROOT, the home directory ~, temp
directories <tmp>, and anything that looks like a credential (password=, token=, a URI's user:password@,
an AWS key id) <redacted>. Nothing here writes outside --out, --img-dir and the generated regions of
--docs-dir, and nothing touches data/sample or data/export (the checks guard that themselves).

Exit code: 0 when every check that ran passed (pieces that are absent are "not run", not failures;
so is a chart build that does not exist: the index then says "charts not regenerated"), 1 when one failed.
`make graph-evidence` runs it with the defaults (GRAPH_ROOT from the Makefile).

Usage (Python 3.12 venv; `make graph-venv`):
  python scripts/graph_evidence.py [--graph-root DIR] [--profiles tiny,s42,default] [--bench 20]
        [--parity-profiles tiny,s42] [--docker-timings FILE --docker-log FILE ...] [--eval-json F]
        [--only repo-contracts,graph-contract,...] [--no-charts] [--skip-sweep]
"""
from __future__ import annotations

import argparse
import datetime as dt
import importlib.metadata
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lakehouse_graph import charts, spec  # noqa: E402 (after the sys.path line above)
from lakehouse_graph import manifest as mf  # noqa: E402

PACKAGES = ("ladybug", "pandas", "numpy", "pyarrow", "networkx", "mcp", "pydantic", "sqlglot")
CHECKS = ("graph-contract", "iceberg-contract", "lineage-contract", "repo-contracts", "graph-tools", "sandbox-check",
          "graph-parity", "cohorts", "bench", "eval", "leakage", "docker-e2e", "tool-catalogue")
MAX_LINES = 400
NOT_AVAILABLE = "not available"
NOT_RUN = "not run"
EXIT_NO_BUILD = 3   # scripts/graph_bench.py, graph_leakage_demo.py, graph_charts.py: "no build", not a failed measurement
SUMMARY_RE = re.compile(r"^(Graph contract|Lineage contract|Repo contracts|check_graph_tools|graph_sandbox_check|"
                        r"Graph parity|Graph cohorts|Spark SQL twin|Parity|graph_bench|SKIP|.*\b(OK|FAILED|FAIL)\b)")


# --------------------------------------------------------------------------- redaction
class Redactor:
    """Paths to repo-relative / $GRAPH_ROOT / ~ / <tmp>; credentials to <redacted>."""

    CREDENTIALS = (
        (re.compile(r"(?i)\b(password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key(?:_id)?|"
                    r"secret[_-]?access[_-]?key|session[_-]?token)(\s*[=:]\s*)(\S+)"), r"\1\2<redacted>"),
        (re.compile(r"(?i)([a-z][a-z0-9+.-]*://)[^/\s:@]+:[^/\s@]+@"), r"\1<redacted>@"),
        (re.compile(r"\bAKIA[0-9A-Z]{16}\b"), "<redacted>"),
        (re.compile(r"(?i)\b(sk-ant-[a-z0-9_-]{8,}|ghp_[A-Za-z0-9]{20,}|gho_[A-Za-z0-9]{20,})"), "<redacted>"),
        (re.compile(r"\bminioadmin\b"), "<redacted>"),
    )
    TMP = re.compile(r"(/private)?/(var/folders|tmp)/[^\s'\"),:]*")

    def __init__(self, repo: Path, graph_root: Path | None, home: Path | None = None, extra: dict | None = None):
        self.pairs: list[tuple[str, str]] = []
        if graph_root is not None:
            for p in {str(graph_root), str(graph_root.resolve())}:
                self.pairs.append((p, "$GRAPH_ROOT"))
                for r in {str(repo), str(repo.resolve())}:
                    if p.startswith(r + os.sep):
                        self.pairs.append((p[len(r) + 1:], "$GRAPH_ROOT"))
        for k, v in (extra or {}).items():
            self.pairs.append((str(k), v))
        for r in {str(repo), str(repo.resolve())}:
            self.pairs.append((r + os.sep, ""))
            self.pairs.append((r, "."))
        h = str(home or Path.home())
        self.pairs.append((h, "~"))
        self.pairs.sort(key=lambda kv: -len(kv[0]))

    def __call__(self, text: str) -> str:
        for a, b in self.pairs:
            if a:
                text = text.replace(a, b)
        text = re.sub(r"/Users/[^/\s]+", "~", text)
        text = re.sub(r"/home/[^/\s]+", "~", text)
        text = self.TMP.sub("<tmp>", text)
        text = re.sub(r"\.graph-work/\S*", "<scratch>", text)
        for pat, rep in self.CREDENTIALS:
            text = pat.sub(rep, text)
        return text


# --------------------------------------------------------------------------- environment
def environment(spark_py: Path | None) -> dict:
    git = mf.git_state(ROOT)
    versions = {}
    for n in PACKAGES:
        try:
            versions[n] = importlib.metadata.version(n)
        except importlib.metadata.PackageNotFoundError:
            versions[n] = "not installed"
    spark = {}
    if spark_py and spark_py.is_file():
        code = ("import importlib.metadata as m\nfor n in ('pyspark','pyiceberg'):\n try: print(n, m.version(n))\n"
                " except Exception: print(n, 'not-installed')")
        p = subprocess.run([str(spark_py), "-c", code], capture_output=True, text=True, timeout=60, check=False)
        spark = dict(line.split(" ", 1) for line in p.stdout.splitlines() if " " in line)
    return {"commit": git.get("commit") or "unknown", "dirty": git.get("dirty"),
            "date": dt.datetime.now(dt.UTC).date().isoformat(), "platform": platform.platform(terse=True),
            "platform_tag": mf.platform_tag(), "python": platform.python_version(), "versions": versions,
            "spark_venv": spark}


def env_lines(env: dict) -> list[tuple[str, str]]:
    dirty = {True: "yes", False: "no"}.get(env["dirty"], "unknown")
    pk = " · ".join(f"{k} {v}" for k, v in env["versions"].items())
    rows = [("Commit", f"`{env['commit']}` (working tree dirty: {dirty})"), ("Date", f"{env['date']} (UTC)"),
            ("Host", f"{env['platform']} ({env['platform_tag']})"), ("Python", f"{env['python']} · {pk}")]
    if env.get("spark_venv"):
        rows.append(("Spark venv", " · ".join(f"{k} {v}" for k, v in sorted(env["spark_venv"].items()))))
    return rows


# --------------------------------------------------------------------------- running and recording
@dataclass
class Result:
    slug: str            # file stem under results/
    title: str
    check: str           # one of CHECKS
    profile: str = ""
    status: str = NOT_RUN  # pass | FAIL | not run | not available
    rc: int | None = None
    command: str = ""
    seconds: float | None = None
    output: str = ""
    summary: str = ""
    note: str = ""
    extra_md: str = ""
    files: list[str] = field(default_factory=list)
    keep: bool = False   # an earlier file is kept as it is (a recorded run nobody re-ran)


def _trim(text: str) -> str:
    lines = text.rstrip("\n").splitlines()
    if len(lines) <= MAX_LINES:
        return "\n".join(lines)
    head, tail = lines[: MAX_LINES // 2], lines[-MAX_LINES // 2:]
    return "\n".join([*head, f"... {len(lines) - MAX_LINES} lines omitted (run the command to see them) ...", *tail])


def _summary(text: str) -> str:
    """The last OK / FAILED line of a check's output; a line ending in ':' takes the next one with it."""
    lines = [ln.strip() for ln in text.splitlines()]
    idx = [i for i, ln in enumerate(lines) if SUMMARY_RE.match(ln)]
    i = idx[-1] if idx else max(len(lines) - 1, 0)
    s = lines[i] if lines else ""
    if s.endswith(":") and i + 1 < len(lines) and lines[i + 1]:
        s = f"{s} {lines[i + 1].lstrip('- ')}"
    return s if len(s) <= 300 else s[:297] + "..."


def run(res: Result, argv: list[str], red: Redactor, timeout: int, env: dict | None = None) -> Result:
    shown = [("python" if a == sys.executable else a) for a in argv]
    res.command = red(" ".join(_quote(a) for a in shown))
    t0 = time.monotonic()
    try:
        p = subprocess.run(argv, cwd=ROOT, capture_output=True, text=True, timeout=timeout, check=False,
                           env={**os.environ, "PYTHONPATH": str(ROOT / "src"), **(env or {})})
        res.rc = p.returncode
        out = p.stdout + (("\n" + p.stderr) if p.stderr.strip() else "")
    except subprocess.TimeoutExpired as exc:
        res.rc = 124
        out = (exc.stdout or "") if isinstance(exc.stdout, str) else ""
        out += f"\nTIMEOUT after {timeout} s"
    res.seconds = round(time.monotonic() - t0, 1)
    res.output = _trim(red(out))
    res.status = "pass" if res.rc == 0 else "FAIL"
    res.summary = _summary(res.output)
    return res


def _quote(a: str) -> str:
    return a if re.fullmatch(r"[\w@%+=:,./$<>~-]+", a) else "'" + a.replace("'", "'\\''") + "'"


def render(res: Result, env: dict) -> str:
    status = {"pass": "**pass**", "FAIL": "**FAIL**", "partial": "**partial**"}.get(res.status, res.status)
    rows = [("Status", f"{status}" + (f" (exit {res.rc})" if res.rc is not None else ""))]
    if res.profile:
        rows.append(("Profile", res.profile))
    if res.command:
        rows.append(("Command", f"`{res.command}`"))
    rows += env_lines(env)
    if res.seconds is not None:
        rows.append(("Duration", f"{res.seconds} s"))
    rows.append(("Summary", res.summary.replace("|", "/").replace("\n", " ") or "-"))
    out = [f"# {res.title}", "", f"<!-- generated by scripts/graph_evidence.py; do not edit; check={res.check} -->",
           "", "| | |", "|---|---|", *[f"| {k} | {v} |" for k, v in rows], ""]
    if res.note:
        out += [res.note, ""]
    if res.extra_md:
        out += [res.extra_md.rstrip(), ""]
    if res.output:
        out += ["## Output", "", "```text", res.output.replace("```", "'''"), "```", ""]
    out += ["[Back to the results index](index.md)", ""]
    return "\n".join(out)


def write(res: Result, out_dir: Path, env: dict) -> None:
    (out_dir / f"{res.slug}.md").write_text(render(res, env), encoding="utf-8", newline="\n")
    res.files.insert(0, f"{res.slug}.md")


def latest_build(graph_root: Path, profile: str) -> Path | None:
    link = graph_root / profile / "latest"
    if not link.exists():
        return None
    b = link.resolve()
    return b if (b / "manifest.json").is_file() else None


def iceberg_build(graph_root: Path) -> Path | None:
    found = []
    for m in sorted(graph_root.glob("*/builds/*/manifest.json")):
        try:
            if json.loads(m.read_text(encoding="utf-8")).get("iceberg"):
                found.append((m.stat().st_mtime, m.parent))
        except (OSError, ValueError):
            continue
    return max(found)[1] if found else None


# --------------------------------------------------------------------------- tool catalogue
def tool_catalogue() -> str:
    from lakehouse_graph import tools

    def arg(name: str, p: dict, required: set) -> str:
        if "enum" in p:
            t = "|".join(map(str, p["enum"]))
        elif p.get("type") == "array":
            items = p.get("items", {})
            t = f"list[{'|'.join(map(str, items['enum'])) if 'enum' in items else items.get('type', 'any')}]"
            if "maxItems" in p:
                t += f" (max {p['maxItems']})"
        elif "anyOf" in p:
            t = " or ".join(x.get("type", "?") if "enum" not in x else "|".join(map(str, x["enum"]))
                            for x in p["anyOf"])
        else:
            t = p.get("type", "any")
            lo, hi = p.get("minimum"), p.get("maximum")
            if lo is not None or hi is not None:
                t += f" {lo if lo is not None else ''}..{hi if hi is not None else ''}"
            if "maxLength" in p:
                t += f" (max {p['maxLength']} chars)"
            if "pattern" in p:
                t += " (id pattern)"
        d = "" if name in required else f" = {json.dumps(p.get('default'))}"
        return f"`{name}`: {t}{d}"

    rows = ["| Toolset | Tool | Arguments | What it does |", "|---|---|---|---|"]
    for ts, specs in tools.TOOLSETS.items():
        for s in specs:
            sch = s.args.model_json_schema()
            props = sch.get("properties", {})
            req = set(sch.get("required", []))
            args = "<br>".join(arg(n, p, req) for n, p in props.items()) or "(none)"
            rows.append(f"| {ts} | `{s.name}` | {args} | {s.description.replace('|', '/')} |")
    n = sum(len(v) for v in tools.TOOLSETS.values())
    return (f"{n} tools in {len(tools.TOOLSETS)} toolsets, read from `lakehouse_graph.tools.TOOLSETS` (the registry "
            "the MCP server publishes). Every tool carries readOnlyHint=true, destructiveHint=false, "
            "idempotentHint=true, openWorldHint=false (hints only, not a safety layer).\n\n" + "\n".join(rows))


# --------------------------------------------------------------------------- docker run record
NEGATIVE_STEP = re.compile(r"negative|must_fail", re.I)   # a step that passes by failing
TASK_STATE = re.compile(r"^\S+\s+\S+\s+(\w+)\s+(success|failed|upstream_failed|skipped|up_for_retry|running|queued)"
                        r"(\s|$)")


def docker_record(timings: Path, logs: list[Path], red: Redactor) -> tuple[str, str, str]:
    """(status, summary, markdown) from a recorded Docker run: step timings + the OK / FAIL lines of its logs.

    A step whose name says it is a negative test (negative, must_fail) is expected to exit non-zero; the table
    says so, and its log's summary lines are copied too. Airflow task-state lines (``airflow tasks
    states-for-dag-run -o plain``) are copied and counted."""
    data = json.loads(timings.read_text(encoding="utf-8"))
    rows = ["| Step | Exit | Seconds | Outcome |", "|---|---:|---:|---|"]
    notes = []
    failed = []
    logs = list(logs)
    for k, v in data.items():
        if isinstance(v, dict) and "rc" in v:
            negative = bool(NEGATIVE_STEP.search(k))
            if v["rc"] in (0, None):
                outcome = "**unexpected: a negative test passed**" if negative else "ok"
                if negative:
                    failed.append(k)
            elif negative:
                outcome = "expected failure (negative test: it must be refused)"
            else:
                outcome = "**failed** (see the summary lines below)"
                failed.append(k)
            rows.append(f"| {k} | {v['rc']} | {v.get('s', '')} | {outcome} |")
            if v.get("log") and (v["rc"] not in (0, None) or negative):
                log = timings.parent / Path(v["log"]).name
                if log.is_file() and log not in logs:
                    logs.append(log)
        elif isinstance(v, int | float):
            notes.append(f"{k}: {v}")
    lines, states = [], []
    keep = re.compile(r"^(==> Graph E2E|Graph build (OK|FAILED)|Graph contract (OK|FAILED)|"
                      r"Lineage (build|contract) (OK|FAILED)|Graph cohorts OK|Graph promote OK|Graph E2E FAILED|"
                      r"\s*FAIL\s|.*already published|.*state=|dag_id\s+)", re.I)
    for log in sorted(set(logs), key=lambda p: p.name):
        if not log.is_file():
            continue
        sel = []
        for ln in log.read_text(encoding="utf-8", errors="replace").splitlines():
            s = ln.strip()
            if re.search(r"(?i)password|secret|token", ln):
                continue
            if m := TASK_STATE.match(s):
                states.append((m.group(1), m.group(2)))
                sel.append(red(ln.rstrip())[:300])
            elif keep.match(s):
                sel.append(red(ln.rstrip())[:300])
        lines += [f"[{log.stem}]", *sel]
    md = (f"Recorded run: `{timings.name}` (only step timings and summary lines are copied; full logs stay with the "
          "operator).\n\n" + "\n".join(rows))
    if notes:
        md += "\n\nHost notes: " + "; ".join(notes)
    if states:
        ok = sum(1 for _, st in states if st == "success")
        md += (f"\n\nAirflow task states: {ok} of {len(states)} tasks `success` ("
               + ", ".join(f"{t} {st}" for t, st in states) + ").")
    if lines:
        md += "\n\n## Summary lines from the logs\n\n```text\n" + "\n".join(lines) + "\n```"
    # a record of an earlier run, not a check run now: a failed step makes it "partial", never a current FAIL
    status = "partial" if failed else "pass"
    summary = (f"recorded run; step(s) failed: {', '.join(failed)}" if failed
               else "recorded run; every step exited as expected")
    return status, summary, md


# --------------------------------------------------------------------------- main
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--graph-root", default=None, help="default: $GRAPH_ROOT or <repo>/data/graph")
    ap.add_argument("--profiles", default="tiny,s42,default")
    ap.add_argument("--chart-profile", default="s42", help="the build the charts are drawn from (default s42)")
    ap.add_argument("--out", default=str(ROOT / "docs/graph/results"))
    ap.add_argument("--img-dir", default=str(ROOT / "docs/graph/img"))
    ap.add_argument("--docs-dir", default=str(ROOT / "docs/graph"))
    ap.add_argument("--bench", type=int, default=20, help="warm calls per tool for the s42 bench (0: no bench)")
    ap.add_argument("--skip-sweep", action="store_true", help="skip the every-renewal leak sweep of the tools check")
    ap.add_argument("--parity-profiles", default="tiny,s42")
    ap.add_argument("--spark-py", default=str(ROOT / ".venv-graph-spark/bin/python"))
    ap.add_argument("--docker-timings", default=None, help="timings JSON of a recorded Docker run")
    ap.add_argument("--docker-log", action="append", default=[], help="a log of that run (summary lines only)")
    ap.add_argument("--eval-json", default=None, help="an eval report (pass^3 by arm and shape)")
    ap.add_argument("--only", default=None, help=f"comma list of checks to run: {', '.join(CHECKS)}")
    ap.add_argument("--no-charts", action="store_true", help="do not call scripts/graph_charts.py")
    ap.add_argument("--timeout", type=int, default=1800, help="seconds per check")
    a = ap.parse_args(argv)

    graph_root = Path(a.graph_root or os.environ.get("GRAPH_ROOT") or spec.graph_root()).absolute()
    out_dir = Path(a.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    only = set(a.only.split(",")) if a.only else set(CHECKS)
    unknown = only - set(CHECKS)
    if unknown:
        print(f"graph_evidence: unknown check(s) {', '.join(sorted(unknown))}; choose from {', '.join(CHECKS)}",
              file=sys.stderr)
        return 2
    spark_py = Path(a.spark_py)
    env = environment(spark_py)
    red = Redactor(ROOT, graph_root)
    py = sys.executable
    gr = str(graph_root)
    profiles = [p for p in a.profiles.split(",") if p]
    results: list[Result] = []
    scratch = Path(tempfile.mkdtemp(prefix="graph-evidence-"))

    def absent(res: Result, why: str, status: str = NOT_RUN) -> Result:
        res.status, res.summary, res.note = status, why, why
        return res

    def no_build_is_not_run(res: Result) -> Result:
        """A measurement script that found no build (exit EXIT_NO_BUILD and a SKIP line) did not run: 'not run',
        never FAIL. A real failure (any other non-zero exit) stays FAIL."""
        if res.rc == EXIT_NO_BUILD and "SKIP" in res.output:
            line = next((ln.strip() for ln in res.output.splitlines() if "SKIP" in ln), "SKIP: no build")
            res.status, res.summary, res.note = NOT_RUN, line[:300], line
        return res

    try:
        if "graph-contract" in only:
            for p in profiles:
                res = Result(f"graph-contract-{p}", f"Graph contract ({p})", "graph-contract", p)
                if latest_build(graph_root, p) is None:
                    absent(res, f"No build for profile {p} in this GRAPH_ROOT: run make graph-local PROFILE={p}.")
                else:
                    run(res, [py, "scripts/check_graph_contract.py", "--profile", p, "--graph-root", gr, "--strict"],
                        red, a.timeout)
                results.append(res)
        if "iceberg-contract" in only:
            res = Result("graph-contract-iceberg", "Graph contract (Iceberg-sourced build)", "iceberg-contract")
            ib = iceberg_build(graph_root)
            if ib is None:
                absent(res, "No Iceberg-sourced build in this GRAPH_ROOT. The Docker path builds one "
                            "(pipelines/run_graph_e2e.sh); its recorded result is in docker-e2e.md.", NOT_AVAILABLE)
            elif not spark_py.is_file():
                absent(res, "An Iceberg-sourced build exists, but re-reading its pins needs .venv-graph-spark "
                            "(PyIceberg).", NOT_AVAILABLE)
            else:
                run(res, [str(spark_py), "scripts/check_graph_contract.py", "--build", str(ib), "--graph-root", gr,
                          "--strict"], red, a.timeout)
            results.append(res)
        if "lineage-contract" in only:
            res = Result("lineage-contract", "Lineage contract (core profile)", "lineage-contract")
            target = next((p for p in ("default", "s42", "tiny") if (b := latest_build(graph_root, p)) is not None
                           and (b / "lineage.lbdb").exists()), None)
            if target is None:
                absent(res, "No build with a lineage graph: run make lineage-local (PROFILE=default).")
            else:
                res.profile = target
                run(res, [py, "scripts/check_lineage_contract.py", "--graph-profile", target, "--graph-root", gr,
                          "--strict"], red, a.timeout)
            results.append(res)
        if "repo-contracts" in only:
            results.append(run(Result("repo-contracts", "Repo contracts (Tier-0 mini)", "repo-contracts"),
                               [py, "scripts/check_repo_contracts.py"], red, a.timeout))
        bench_json = None
        if "graph-tools" in only:
            for p in [x for x in profiles if x in ("tiny", "s42")]:
                res = Result(f"graph-tools-{p}", f"Agent tools check ({p})", "graph-tools", p)
                if latest_build(graph_root, p) is None:
                    absent(res, f"No build for profile {p}: run make graph-local PROFILE={p}.")
                elif not (ROOT / "scripts/check_graph_tools.py").is_file():
                    absent(res, "scripts/check_graph_tools.py is not in this checkout.", NOT_AVAILABLE)
                else:
                    jf = scratch / f"tools-{p}.json"
                    argv_ = [py, "scripts/check_graph_tools.py", "--profile", p, "--graph-root", gr, "--json", str(jf)]
                    if p == "s42" and a.bench:
                        argv_ += ["--bench", str(a.bench)]
                    if a.skip_sweep:
                        argv_.append("--skip-sweep")
                    run(res, argv_, red, a.timeout)
                    if jf.is_file():
                        rep = json.loads(jf.read_text(encoding="utf-8"))
                        if rep.get("bench"):
                            bench_json = out_dir / f"tools-bench-{p}.json"
                            bman = mf.read_manifest(latest_build(graph_root, p))
                            doc = {"generated_by": "scripts/graph_evidence.py", "profile": p, "bench_calls": a.bench,
                                   "build_id": bman.get("business_build_id"), "commit": env["commit"],
                                   "platform": env["platform_tag"], "bench": rep["bench"]}
                            bench_json.write_text(red(json.dumps(doc, indent=1, sort_keys=True)) + "\n",
                                                  encoding="utf-8")
                            res.files.append(bench_json.name)
                results.append(res)
        if "sandbox-check" in only:
            res = Result("sandbox-check", "macOS sandbox check", "sandbox-check")
            b = latest_build(graph_root, a.chart_profile) or latest_build(graph_root, "default")
            if sys.platform != "darwin":
                absent(res, "Not macOS: Linux runs the MCP servers without an OS sandbox (one stderr banner).",
                       NOT_AVAILABLE)
            elif b is None:
                absent(res, "No build to serve: run make graph-local first.")
            else:
                run(res, [py, "scripts/graph_sandbox_check.py", "--build", str(b), "--graph-root", gr, "--control",
                          "--log-check"], red, a.timeout)
            results.append(res)
        if "graph-parity" in only:
            for p in [x for x in a.parity_profiles.split(",") if x]:
                res = Result(f"graph-parity-{p}", f"Spark SQL twin parity ({p})", "graph-parity", p)
                sample = spec.sample_dir(p, graph_root)
                if not spark_py.is_file():
                    absent(res, "Needs .venv-graph-spark (pyspark 4.1.3) and a JDK 17 or 21; see docs/graph/operations.md.",
                           NOT_AVAILABLE)
                elif any(not (sample / f).is_file() for f in spec.BRONZE_FILES):
                    absent(res, f"No bronze sample for profile {p} in this GRAPH_ROOT: run make graph-sample "
                                f"PROFILE={p}." if p not in ("tiny", "default") else
                                f"The bronze CSVs of profile {p} are missing ({red(str(sample))}).")
                else:
                    argv_ = [str(spark_py), "scripts/check_graph_parity.py", "parity", "--profile", p, "--strict"]
                    if p != "tiny":
                        argv_ += ["--graph-root", gr]
                    run(res, argv_, red, a.timeout)
                results.append(res)
        if "cohorts" in only:
            res = Result("cohorts", f"Feature cohorts ({a.chart_profile})", "cohorts", a.chart_profile)
            b = latest_build(graph_root, a.chart_profile)
            if b is None or not (b / "cohorts.parquet").is_file():
                absent(res, f"No cohorts.parquet for profile {a.chart_profile}: run make graph-cohorts "
                            f"PROFILE={a.chart_profile}.")
            else:
                parts, rcs, secs = [], [], 0.0
                for extra in (["list", "--algorithm", "leiden"], ["list", "--algorithm", "louvain"],
                              ["summary", "--renewal", "sub_maya:2026-10-07"]):
                    r = run(Result("x", "x", "cohorts"), [py, "scripts/build_graph_cohorts.py", *extra, "--profile",
                                                          a.chart_profile, "--graph-root", gr], red, a.timeout)
                    parts.append(f"$ {r.command}\n{r.output}")
                    rcs.append(r.rc or 0)
                    secs += r.seconds or 0
                res.command = "scripts/build_graph_cohorts.py list (leiden, louvain) + summary --renewal sub_maya"
                res.rc, res.seconds = max(rcs), round(secs, 1)
                res.status = "pass" if res.rc == 0 else "FAIL"
                res.output = "\n\n".join(parts)
                done = [ln.strip().rstrip(":") for part in parts for ln in part.splitlines()
                        if re.match(r"^(leiden|louvain)-\d+|^\{", ln.strip()) is None and "cohorts" in ln
                        and ("modularity" in ln or "communities" in ln)]
                res.summary = ("outside the graph contract; " + "; ".join(done[:2] or [_summary(parts[0])]))[:300]
            results.append(res)
        if "bench" in only:
            res = Result("bench", "Build and serve benchmark", "bench")
            if (ROOT / "scripts/graph_bench.py").is_file():
                no_build_is_not_run(run(res, [py, "scripts/graph_bench.py", "--graph-root", gr], red, a.timeout))
            else:
                absent(res, "scripts/graph_bench.py (PHASE 3a) is not in this checkout yet. Measured figures "
                            "today: the builder and loader RSS in each graph contract (Resources section) and the "
                            "per-tool warm p50 / p95 in graph-tools-s42.md.", NOT_AVAILABLE)
            results.append(res)
        if "eval" in only:
            res = Result("eval", "Agent eval report", "eval")
            if a.eval_json and Path(a.eval_json).is_file():
                rep = json.loads(Path(a.eval_json).read_text(encoding="utf-8"))
                res.status, res.rc = "pass", 0
                res.extra_md = "```json\n" + red(json.dumps(rep, indent=1, sort_keys=True))[:20000] + "\n```"
                res.summary = f"eval report {Path(a.eval_json).name} recorded"
            else:
                absent(res, "No eval report given (--eval-json): run scripts/graph_eval.py on evals/graph_cases.yaml "
                            "and pass its report. LLM results gate article claims, never merges.",
                       NOT_AVAILABLE)
            results.append(res)
        leak_json = None
        if "leakage" in only:
            res = Result("leakage", "Leakage demo (AUCs)", "leakage")
            if (ROOT / "scripts/graph_leakage_demo.py").is_file():
                leak_json = scratch / "leakage.json"
                no_build_is_not_run(run(res, [py, "scripts/graph_leakage_demo.py", "--graph-root", gr, "--json",
                                              str(leak_json)], red, a.timeout))
            else:
                absent(res, "scripts/graph_leakage_demo.py (PHASE 3a) is not in this checkout yet; the planning "
                            "prototype's figures are quoted in docs/graph/evaluation.md and marked as such.",
                       NOT_AVAILABLE)
            results.append(res)
        if "docker-e2e" in only:
            res = Result("docker-e2e", "Docker end-to-end run (recorded)", "docker-e2e")
            if a.docker_timings and Path(a.docker_timings).is_file():
                res.status, res.summary, res.extra_md = docker_record(Path(a.docker_timings),
                                                                      [Path(x) for x in a.docker_log], red)
                res.note = ("This file records a run made by an operator (Docker is not started by "
                            "graph_evidence.py). Reproduce: make up && make wait && make churn-e2e, then the overlay "
                            "and pipelines/run_graph_e2e.sh (docs/graph/lakehouse-twin.md).")
            else:
                prev = out_dir / "docker-e2e.md"
                if prev.is_file():   # keep the last recorded run rather than erase it
                    text = prev.read_text(encoding="utf-8")
                    m = re.search(r"\| Status \| (\*\*)?(\w+)", text)
                    res.status = m.group(2) if m and m.group(2) in ("pass", "partial") else NOT_AVAILABLE
                    res.summary = "kept from an earlier recorded run (no --docker-timings given)"
                    res.files.append(prev.name)
                    res.keep = True
                else:
                    absent(res, "No recorded Docker run given (--docker-timings).", NOT_AVAILABLE)
            results.append(res)
        if "tool-catalogue" in only:
            res = Result("tool-catalogue", "Tool catalogue", "tool-catalogue")
            try:
                res.extra_md = tool_catalogue()
                res.status, res.rc, res.summary = "pass", 0, res.extra_md.split(",")[0]
            except Exception as exc:  # noqa: BLE001 - the registry may be mid-edit; record why
                absent(res, f"Could not import the tool registry: {type(exc).__name__}: {exc}", "FAIL")
            results.append(res)

        for res in results:
            if not res.keep:
                write(res, out_dir, env)

        charts_rc, charts_note = None, "not regenerated (--no-charts)"
        if a.no_charts:
            pass
        elif latest_build(graph_root, a.chart_profile) is None:
            charts_note = (f"not regenerated: no {a.chart_profile} build in this GRAPH_ROOT (run make graph-local "
                           f"PROFILE={a.chart_profile}); the committed charts are unchanged")
        else:
            cargs = ["--profile", a.chart_profile, "--graph-root", gr, "--img-dir", a.img_dir,
                     "--mermaid-dir", str(out_dir / "mermaid"), "--docs-dir", a.docs_dir]
            prof_bench = out_dir / f"tools-bench-{a.chart_profile}.json"   # this run's bench, or an earlier one
            cargs += ["--tools-json", str(prof_bench) if prof_bench.is_file() else ""]
            if a.eval_json:
                cargs += ["--eval-json", a.eval_json]
            if leak_json is not None and leak_json.is_file():
                cargs += ["--leakage-json", str(leak_json)]
            p = subprocess.run([py, "scripts/graph_charts.py", *cargs], cwd=ROOT, check=False, capture_output=True,
                               text=True, timeout=a.timeout)
            print(red(p.stdout + p.stderr).rstrip())
            charts_rc = p.returncode
            charts_note = {0: "regenerated", 3: "not regenerated: the chart build is missing"}.get(
                charts_rc, f"FAILED (scripts/graph_charts.py exit {charts_rc})")
            if charts_rc == 3:
                charts_rc = None

        on_disk = collect_results(out_dir)
        index = render_index(on_disk, env, charts_note)
        (out_dir / "index.md").write_text(index, encoding="utf-8", newline="\n")
        if Path(a.docs_dir).is_dir():
            table = index_table(on_disk, link_prefix="results/")
            for page in sorted(Path(a.docs_dir).glob("*.md")):
                old = page.read_text(encoding="utf-8")
                new, _ = charts.fill_regions(old, {"results:checks": table})
                if new != old:
                    page.write_text(new, encoding="utf-8", newline="\n")
    finally:
        shutil.rmtree(scratch, ignore_errors=True)

    failed = [r for r in results if r.status == "FAIL"]
    for r in results:
        print(f"  {r.status:<13} {r.title}: {r.summary[:140]}")
    print(f"graph_evidence {'FAILED' if failed else 'OK'}: {len(results)} checks recorded in "
          f"{red(str(out_dir))} ({sum(r.status == 'pass' for r in results)} pass, {len(failed)} fail, "
          f"{sum(r.status in (NOT_RUN, NOT_AVAILABLE) for r in results)} not run / not available)"
          + f"; charts {charts_note}")
    return 1 if failed or charts_rc not in (None, 0) else 0


PROFILE_ORDER = {"tiny": 0, "s42": 1, "default": 2}
_ROW = re.compile(r"^\| (Status|Profile|Summary) \| (.*) \|$")


def collect_results(out_dir: Path) -> list[Result]:
    """Every result file in ``out_dir`` (this run's and earlier ones'), in a fixed order."""
    found = []
    for f in sorted(out_dir.glob("*.md")):
        text = f.read_text(encoding="utf-8")
        m = re.search(r"check=([\w-]+) -->", text)
        if not m or m.group(1) not in CHECKS:
            continue
        title = text.splitlines()[0].lstrip("# ").strip()
        fields = {k: v for line in text.splitlines() if (mm := _ROW.match(line)) for k, v in [mm.groups()]}
        status = re.sub(r"[*]|\s*\(exit \d+\)", "", fields.get("Status", NOT_RUN)).strip()
        r = Result(f.stem, title, m.group(1), fields.get("Profile", ""), status, summary=fields.get("Summary", ""))
        r.files = [f.name] + ([f"tools-bench-{r.profile}.json"] if (out_dir / f"tools-bench-{r.profile}.json").is_file()
                              and r.check == "graph-tools" else [])
        found.append(r)
    found.sort(key=lambda r: (CHECKS.index(r.check), PROFILE_ORDER.get(r.profile, 9), r.slug))
    return found


def index_table(results: list[Result], link_prefix: str = "") -> str:
    rows = ["| Check | Profile | Status | Result | Summary |", "|---|---|---|---|---|"]
    for r in results:
        status = {"pass": "pass", "FAIL": "**FAIL**"}.get(r.status, r.status)
        files = ", ".join(f"[{f}]({link_prefix}{f})" for f in r.files) or "-"
        summary = r.summary.replace("|", "/").replace("\n", " ")
        rows.append(f"| {r.title} | {r.profile or '-'} | {status} | {files} | {summary} |")
    return "\n".join(rows)


def render_index(results: list[Result], env: dict, charts_note: str) -> str:
    n_fail = sum(r.status == "FAIL" for r in results)
    out = ["# Graph evidence: results index", "",
           "<!-- generated by scripts/graph_evidence.py; do not edit -->", "",
           "Every file here was written by `scripts/graph_evidence.py` from the checks it ran. Paths are "
           "redacted (repo-relative, `$GRAPH_ROOT`, `~`) and credentials never appear. Charts in "
           "[../img/](../img/) are regenerated from the same builds by `scripts/graph_charts.py`.", "",
           "| | |", "|---|---|", *[f"| {k} | {v} |" for k, v in env_lines(env)],
           f"| Checks | {sum(r.status == 'pass' for r in results)} pass, {n_fail} fail, "
           f"{sum(r.status in (NOT_RUN, NOT_AVAILABLE) for r in results)} not run or not available |",
           f"| Charts | {charts_note} |",
           "", index_table(results), "",
           "Also here: [palette-validation.md](palette-validation.md) (the chart palette, validated light and dark) "
           "and [mermaid/](mermaid/) (the generated diagrams).", "",
           "Regenerate: `.venv-graph/bin/python scripts/graph_evidence.py --graph-root <dir>` (there is no "
           "`make graph-evidence` target yet), after `make graph-sample PROFILE=s42`, `make graph-sample "
           "PROFILE=tiny`, `make graph-local` for tiny, s42 and default, `scripts/build_lineage_local.py` and "
           "`scripts/build_graph_cohorts.py` ([operations.md](../operations.md)).", ""]
    return "\n".join(out)


if __name__ == "__main__":
    raise SystemExit(main())
