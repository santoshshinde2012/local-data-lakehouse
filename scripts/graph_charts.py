#!/usr/bin/env python3
"""Charts and Mermaid diagrams for docs/graph, generated from a graph build and results JSON (no hand-typed numbers).

Writes, for every chart, a light and a dark SVG (lakehouse_graph.charts; same input -> same bytes):

  docs/graph/img/<chart>-light.svg, <chart>-dark.svg
      graph-composition, leak-surface, naive-vs-pit, santosh-timeline, santosh-neighbours, inc-002-exposure,
      lapse-first-after-cut, cohort-lapse-rates (needs cohorts.parquet), tool-latency (needs --tools-json),
      eval-pass3 (needs --eval-json), leakage-aucs (needs --leakage-json)
  docs/graph/results/mermaid/{schema,er,lineage-<column-in-kebab-case>}.mmd

and, with --docs-dir, rewrites the generated regions of the docs pages, i.e. the text between
``<!-- graph-evidence:begin NAME -->`` and ``<!-- graph-evidence:end NAME -->``: ``figure:<chart>`` gets the
<picture> (light + dark), its alt text and the same numbers as a markdown table; ``mermaid:<name>`` gets the
diagram. A region whose input is missing gets a "Pending final run" note instead. Nothing outside the
markers is touched.

Inputs:
  --build DIR | --profile P   one graph build (Parquet + manifest; the s42 profile by default). Read only.
  --tools-json FILE           scripts/check_graph_tools.py --bench N --json FILE (its "bench" section); default:
                              docs/graph/results/tools-bench-<build profile>.json when it exists ('' = none)
  --headline-profiles P,Q     the builds of the README headline table (default s42,tiny; with --build: that build)
  --eval-json FILE            an eval report with {"model", "pass3": {arm: {shape: [passed, cases]}}}
  --leakage-json FILE         a leakage demo report with {"variants": [{"label", "single_feature", "lr"}]}

Usage (Python 3.12 venv; `make graph-venv`):
  python scripts/graph_charts.py --profile s42 [--graph-root DIR] [--tools-json F] [--docs-dir docs/graph]
  python scripts/graph_charts.py --profile s42 --check      # regenerate in memory, exit 1 if a file differs
Normally run by scripts/graph_evidence.py, which produces the JSON inputs first.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lakehouse_graph import charts, spec  # noqa: E402 (after the sys.path line above)

OPTIONAL = {
    "tool-latency": ("Tool latency (warm p50 / p95 per tool) comes from the bench section of the tools check.",
                     "Run `scripts/graph_evidence.py` (it runs scripts/check_graph_tools.py --bench)."),
    "eval-pass3": ("The agent eval (pass^3 by arm and question shape) has not run yet: the eval harness "
                   "(scripts/graph_eval.py, PHASE 3a) is not in this checkout.",
                   "When it exists, `scripts/graph_evidence.py` records it and fills this chart."),
    "leakage-aucs": ("No leakage demo record yet: run scripts/graph_leakage_demo.py on an s42 build "
                     "(scripts/graph_evidence.py records it).",
                     "The planning prototype's figures are quoted in the text above, marked as such."),
    "cohort-lapse-rates": ("This build has no cohorts.parquet.",
                           "Run `make graph-cohorts PROFILE=s42`."),
}


NO_BUILD = 3   # exit code: the chart build is absent (scripts/graph_evidence.py records "charts not regenerated")


class NoBuild(Exception):
    pass


def _show(p: Path) -> str:
    """A path for messages: repo-relative inside the repo, else its last two parts (never a home path)."""
    p = Path(p)
    if p.is_relative_to(ROOT):
        return str(p.relative_to(ROOT))
    return str(Path(*p.parts[-2:])) if len(p.parts) >= 2 else p.name


def _host(tag: str | None) -> str:
    return {"macosx_arm64": "macOS arm64", "linux_x86_64": "Linux x86_64", "linux_aarch64": "Linux arm64"}.get(
        tag or "", tag or "host not recorded")


def headline(a, build: Path, man: dict, lat: dict | None) -> str:
    """The README headline table: one column per profile build (manifest.json + contract.json)."""
    cols = []
    if a.build:
        pairs = [(man.get("profile"), build)]
    else:
        pairs = [(p, spec.latest_link(p, a.graph_root)) for p in a.headline_profiles.split(",") if p]
    for p, link in pairs:
        b = Path(link).resolve()
        if not (b / "manifest.json").is_file():
            continue
        m = man if b == build else json.loads((b / "manifest.json").read_text(encoding="utf-8"))
        c = json.loads((b / "contract.json").read_text(encoding="utf-8")) if (b / "contract.json").is_file() else None
        cols.append({"profile": p, "manifest": m, "contract": c})
    if not cols:
        return charts.pending("No build of the headline profiles in this GRAPH_ROOT.",
                              "Run `make graph-local PROFILE=s42` and `make graph-local PROFILE=tiny`.")
    return charts.headline_table(cols, lat, bench_profile=(lat or {}).get("profile") or man.get("profile"))


def _bench_calls(report: dict) -> int | None:
    if isinstance(report.get("bench_calls"), int):
        return report["bench_calls"]
    for key in report.get("sections", {}):
        m = re.search(r"bench \(warm, (\d+) calls per tool\)", key)
        if m:
            return int(m.group(1))
    return None


def _load_json(path: str | None) -> dict | None:
    if not path:
        return None
    p = Path(path)
    if not p.is_file():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def collect(a) -> tuple[list[charts.Figure], dict[str, str], dict[str, str], dict]:
    """(figures, mermaid texts, region texts, manifest) for the inputs in ``a``."""
    build = Path(a.build).resolve() if a.build else (spec.latest_link(a.profile, a.graph_root)).resolve()
    if not (build / "manifest.json").is_file():
        raise NoBuild(f"no graph build at {_show(build)}: run make graph-local PROFILE={a.profile} first")
    t, man = charts.load_build(build)
    figs = [charts.composition_figure(charts.composition_data(t, man)),
            charts.leak_surface_figure(charts.leak_surface_data(t, man)),
            charts.naive_pit_figure(charts.naive_pit_data(t, man)),
            charts.timeline_figure(charts.timeline_data(t, man, a.renewal)),
            charts.neighbours_figure(charts.neighbours_data(t, man, a.renewal)),
            charts.exposure_figure(charts.exposure_data(t, man, a.incident)),
            charts.lapse_rate_figure(charts.lapse_rate_data(t, man))]
    if (build / "cohorts.parquet").is_file():
        figs.append(charts.cohorts_figure(charts.cohorts_data(build, man)))
    tools_json = a.tools_json
    if tools_json is None:   # the bench recorded next to the docs for this build's profile, when there is one
        auto = ROOT / "docs/graph/results" / f"tools-bench-{man.get('profile')}.json"
        tools_json = str(auto) if auto.is_file() else None
    tools = _load_json(tools_json or None)
    lat = None
    if tools:
        bid = tools.get("build_id") or man.get("business_build_id")
        note = (f"Source: scripts/check_graph_tools.py --bench on graph build {bid} (profile "
                f"{tools.get('profile') or man.get('profile')}); {_host(tools.get('platform'))}, warm calls, "
                "sandboxed stdio servers. Regenerate with scripts/graph_evidence.py.")
        lat = charts.latency_data({**tools, "bench_calls": _bench_calls(tools)}, note)
        if lat:
            lat["profile"] = tools.get("profile") or man.get("profile")
            figs.append(charts.latency_figure(lat))
    ev = _load_json(a.eval_json)
    if ev and (d := charts.eval_data(ev, f"Source: {Path(a.eval_json).name} (eval report).")):
        figs.append(charts.eval_figure(d))
    lk = _load_json(a.leakage_json)
    if lk and (d := charts.leakage_data(lk, f"Source: {Path(a.leakage_json).name} (leakage demo report).")):
        figs.append(charts.leakage_figure(d))

    mermaid = {"schema": charts.mermaid_schema(man["counts"]), "er": charts.mermaid_er()}
    lin_note = ""
    if (build / "lineage" / "manifest.json").is_file():
        trace = charts.lineage_trace_data(build, a.lineage_target, "upstream")
        col = a.lineage_target.rsplit(".", 1)[-1].replace("_", "-")  # file names are kebab-case
        mermaid[f"lineage-{col}"] = charts.mermaid_lineage(trace)
        lman = json.loads((build / "lineage" / "manifest.json").read_text(encoding="utf-8"))
        lin_note = (f"Generated from the lineage Parquet of build {man.get('business_build_id')} (lineage build "
                    f"{lman.get('lineage_build_id')}, {lman.get('profile', 'core')} profile) by the pure-Python "
                    f"oracle: {trace['summary']['edges']} edges, {trace['summary']['columns']} columns.")
    regions: dict[str, str] = {}
    for f in figs:
        regions[f"figure:{f.name}"] = f.markdown("img/")
    for name, (what, how) in OPTIONAL.items():
        regions.setdefault(f"figure:{name}", charts.pending(what, how))
    counts = man["counts"]
    loops = [rel for rel, e in spec.EDGE_SCHEMA.items() if e.src == e.dst]
    regions["mermaid:schema"] = (charts.fence("mermaid", mermaid["schema"]) + "\n\n" +
                                 f"<sub>Counts from graph build {man.get('business_build_id')} (profile "
                                 f"{man.get('profile')}, seed {man.get('seed')}, N_USERS {man.get('n_users')}): "
                                 f"{charts.fmt_int(counts['total_nodes'])} nodes, "
                                 f"{charts.fmt_int(counts['total_edges'])} edges."
                                 + (f" The dotted hexagon is {', '.join(loops)}, an edge type from one node to "
                                    "another node of the same label (never to itself)." if loops else "")
                                 + " Generated by scripts/graph_charts.py.</sub>")
    regions["summary:headline"] = headline(a, build, man, lat)
    regions["mermaid:er"] = (charts.fence("mermaid", mermaid["er"]) + "\n\n<sub>Generated from "
                             "src/lakehouse_graph/spec.py (NODE_SCHEMA, EDGE_SCHEMA) by scripts/graph_charts.py. "
                             "Edge properties are listed in the table below.</sub>")
    for key in [k for k in mermaid if k.startswith("lineage-")]:
        regions[f"mermaid:{key}"] = charts.fence("mermaid", mermaid[key]) + f"\n\n<sub>{lin_note}</sub>"
    if not any(k.startswith("lineage-") for k in mermaid):
        col = a.lineage_target.rsplit(".", 1)[-1].replace("_", "-")
        regions[f"mermaid:lineage-{col}"] = charts.pending("This build has no lineage graph.",
                                                           "Run `make lineage-local PROFILE=s42`.")
    return figs, mermaid, regions, man


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    who = ap.add_mutually_exclusive_group()
    who.add_argument("--build", default=None, help="a graph build directory")
    who.add_argument("--profile", default="s42", help="the latest build of this profile (default s42)")
    ap.add_argument("--graph-root", default=None, help="default: $GRAPH_ROOT or <repo>/data/graph")
    ap.add_argument("--renewal", default=None, help="hero renewal for the timeline / neighbours (default: sub_santosh's)")
    ap.add_argument("--incident", default="inc-002")
    ap.add_argument("--lineage-target", default="gold.churn_renewal_features.limit_hits_14d")
    ap.add_argument("--tools-json", default=None,
                    help="bench JSON (default: docs/graph/results/tools-bench-<profile>.json if present; '' = none)")
    ap.add_argument("--headline-profiles", default="s42,tiny", help="builds of the README headline table")
    ap.add_argument("--eval-json", default=None)
    ap.add_argument("--leakage-json", default=None)
    ap.add_argument("--img-dir", default=str(ROOT / "docs/graph/img"))
    ap.add_argument("--mermaid-dir", default=str(ROOT / "docs/graph/results/mermaid"))
    ap.add_argument("--docs-dir", default=None, help="fill the generated regions of the *.md pages in this directory")
    ap.add_argument("--check", action="store_true", help="write nothing; exit 1 if any output differs from disk")
    a = ap.parse_args(argv)
    try:
        figs, mermaid, regions, man = collect(a)
    except NoBuild as exc:
        print(f"graph_charts SKIPPED: {exc}", file=sys.stderr)
        return NO_BUILD
    except (ValueError, KeyError, FileNotFoundError) as exc:
        print(f"graph_charts FAILED: {exc}", file=sys.stderr)
        return 1
    outputs: dict[Path, str] = {}
    img = Path(a.img_dir)
    for f in figs:
        for theme in charts.THEMES:
            outputs[img / f"{f.name}-{theme.name}.svg"] = f.svg(theme)
    for name, text in mermaid.items():
        outputs[Path(a.mermaid_dir) / f"{name}.mmd"] = text
    if a.docs_dir:
        for page in sorted(Path(a.docs_dir).glob("*.md")):
            old = page.read_text(encoding="utf-8")
            new, _ = charts.fill_regions(old, regions)
            if new != old or a.check:
                outputs[page] = new
    differ = []
    for path, text in outputs.items():
        current = path.read_text(encoding="utf-8") if path.is_file() else None
        if current == text:
            continue
        differ.append(path)
        if not a.check:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8", newline="\n")
    for f in figs:
        print(f"  {f.name}: light {_sha(f.svg(charts.LIGHT))} dark {_sha(f.svg(charts.DARK))}")
    shown = [str(p.relative_to(ROOT)) if p.is_relative_to(ROOT) else p.name for p in differ]
    if a.check:
        print(f"graph_charts {'OK: up to date' if not differ else 'STALE: ' + ', '.join(shown)}")
        return 1 if differ else 0
    print(f"graph_charts OK: {len(figs)} charts x 2 themes, {len(mermaid)} Mermaid diagrams from build "
          f"{man.get('business_build_id')} (profile {man.get('profile')}); {len(differ)} file(s) changed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
