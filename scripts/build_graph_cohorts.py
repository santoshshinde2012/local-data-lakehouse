#!/usr/bin/env python3
"""Feature cohorts of a graph build: NetworkX Louvain + Leiden over SIMILAR_TO (outside the contract).

Reads a business build's Parquet (nodes_Renewal, edges_SIMILAR_TO; nothing else, no Ladybug,
no network) and writes, inside that build directory:

  cohorts.parquet               one row per renewal: its Leiden and Louvain cohort ids (largest
                                cohort = <algorithm>-01), is_reference, how it was assigned
  manifest.json["cohorts"]      library + version, seed, resolution, weight rule, per algorithm
                                the number of cohorts, modularity, sizes (small cells and their
                                complements null), plan purity, sha256

Spec cohorts/renewal-v1 (src/lakehouse_graph/cohorts.py): the undirected union of SIMILAR_TO
among reference renewals (route = model), weight 1 / (1 + dist), seed 42, resolution 1.0;
non-reference renewals take the cohort of their rank-1 neighbour. The same build and seed give
byte-identical Parquet. Cohorts are labels, not structure: they rediscover feature segments,
and the graph contract never reads them. A byte-identical rebuild of the business graph keeps
them while their record still matches (registered in the build.CARRY_OVER registry:
register_carry_over("cohorts", ...)). ``list`` and ``summary`` suppress counts under 5 and any
count that would give one back by subtraction (cohorts.withheld_cohorts).

Usage (Python 3.12 venv from requirements-graph.txt; `make graph-venv`):
  python scripts/build_graph_cohorts.py [build] [--profile default] [--build <dir>] [--seed 42]
  python scripts/build_graph_cohorts.py list [--algorithm leiden] [--profile P | --build <dir>] [--json]
  python scripts/build_graph_cohorts.py summary (--cohort leiden-01 | --renewal sub_maya:2026-10-07)
                                        [--algorithm leiden] [--profile P | --build <dir>] [--json]

``--build`` defaults to the latest build of ``--profile`` under $GRAPH_ROOT (default data/graph).
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lakehouse_graph import cohorts, spec  # noqa: E402 (after the sys.path line above)

COMMANDS = ("build", "list", "summary")


def resolve_build_dir(a) -> Path:
    if a.build:
        return Path(a.build).absolute()
    link = spec.latest_link(a.profile, a.graph_root)
    if not link.exists():
        raise cohorts.CohortsUnavailable(
            f"no graph build for profile {a.profile} under {spec.graph_root(a.graph_root)}: run make graph-build "
            f"PROFILE={a.profile} first, or pass --build <dir>")
    return link.resolve()


def cmd_build(a) -> int:
    try:
        bdir = resolve_build_dir(a)
        path, rec = cohorts.build_cohorts(bdir, seed=a.seed, resolution=a.resolution, lock_timeout=a.lock_timeout)
    except (cohorts.CohortsUnavailable, TimeoutError, ValueError, OSError) as e:
        print(f"Graph cohorts FAILED: {e}", file=sys.stderr)
        return 1
    algos = "; ".join(f"{name} {s['communities']} cohorts (modularity {s['modularity']:.4f}, plan purity "
                      f"{s['plan_purity']:.2f})" for name, s in rec["algorithms"].items())
    assigned = rec["assigned"].get("nearest_reference", 0)
    print(f"Graph cohorts OK ({rec['spec']}, {rec['library']} {rec['library_version']}, seed {rec['seed']}, "
          f"weight {rec['weight']}): {algos}; {rec['graph']['nodes']:,} reference renewals, {assigned:,} assigned "
          f"by nearest reference neighbour; outside the contract -> {path}")
    return 0


def _show(data: dict, caveats: list[str], as_json: bool) -> None:
    if as_json:
        print(json.dumps({"data": data, "caveats": caveats}, indent=2, sort_keys=True))
        return
    print(json.dumps(data, indent=2, sort_keys=True))
    for c in caveats:
        print(f"caveat: {c}")


def cmd_list(a) -> int:
    try:
        data, caveats = cohorts.cohort_list(resolve_build_dir(a), a.algorithm)
    except (cohorts.CohortsUnavailable, ValueError) as e:
        print(f"graph cohorts list: {e}", file=sys.stderr)
        return 1
    if a.json:
        _show(data, caveats, True)
        return 0
    print(f"{data['communities']} {data['algorithm']} cohorts (modularity {data['modularity']}, seed {data['seed']}, "
          f"{data['library']}):")
    for r in data["cohorts"]:
        rate = "suppressed (small cell)" if r["suppressed"] else \
            f"{r['voluntary_lapses']}/{r['model_renewals']} = {r['rate']:.1%} [{r['wilson_95'][0]:.3f}, " \
            f"{r['wilson_95'][1]:.3f}]"
        print(f"  {r['cohort_id']:12s} {rate:40s} {r['name']}")
    for c in caveats:
        print(f"caveat: {c}")
    return 0


def cmd_summary(a) -> int:
    try:
        data, caveats = cohorts.cohort_summary(resolve_build_dir(a), cohort_id=a.cohort, renewal_id=a.renewal,
                                               algorithm=a.algorithm)
    except (cohorts.CohortsUnavailable, ValueError) as e:
        print(f"graph cohorts summary: {e}", file=sys.stderr)
        return 1
    _show(data, caveats, a.json)
    return 0


def parse(argv: list[str]) -> argparse.Namespace:
    if not argv or argv[0] not in (*COMMANDS, "-h", "--help"):
        argv = ["build", *argv]
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p):
        p.add_argument("--graph-root", default=None, help="default: $GRAPH_ROOT or <repo>/data/graph")
        p.add_argument("--profile", default="default",
                       help="the build is <profile>/latest (default | tiny | s<digits>)")
        p.add_argument("--build", default=None, help="a build directory (overrides --profile)")

    b = sub.add_parser("build", help="detect the cohorts of a build (default command)")
    common(b)
    b.add_argument("--seed", type=int, default=cohorts.SEED)
    b.add_argument("--resolution", type=float, default=cohorts.RESOLUTION)
    b.add_argument("--lock-timeout", type=float, default=600.0, help="seconds to wait for $GRAPH_ROOT/.lock")
    b.set_defaults(fn=cmd_build)

    li = sub.add_parser("list", help="every cohort of one algorithm, largest first")
    common(li)
    li.add_argument("--algorithm", default=cohorts.DEFAULT_ALGORITHM, choices=cohorts.ALGORITHMS)
    li.add_argument("--json", action="store_true")
    li.set_defaults(fn=cmd_list)

    s = sub.add_parser("summary", help="one cohort by id, or the cohort of a renewal")
    common(s)
    who = s.add_mutually_exclusive_group(required=True)
    who.add_argument("--cohort", default=None, help="e.g. leiden-01")
    who.add_argument("--renewal", default=None, help="e.g. sub_maya:2026-10-07")
    s.add_argument("--algorithm", default=None, choices=cohorts.ALGORITHMS, help="default: leiden (or the cohort's)")
    s.add_argument("--json", action="store_true")
    s.set_defaults(fn=cmd_summary)
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    a = parse(list(sys.argv[1:] if argv is None else argv))
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
