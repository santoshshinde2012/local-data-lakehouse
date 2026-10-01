#!/usr/bin/env python3
"""Build the renewal graph from the pandas gold twin (no Docker, no LLM, ~10 s at seed 42).

bronze CSVs (per build profile, read only) -> scripts/build_churn_gold_local.silver()/gold()
(imported, unchanged) -> deterministic Parquet node/edge tables + similar_to_scaler.parquet
+ manifest.json + graph.lbdb (LadybugDB) under

  $GRAPH_ROOT/<profile>/builds/<business_build_id>/     (GRAPH_ROOT default: data/graph)

Profiles. A profile's seed is derived from its name, so these four kinds are the only valid
names (nothing here ever writes data/sample/churn or data/export):
  default    bronze = CHURN_SAMPLE_DIR or data/sample/churn, exports = CHURN_EXPORT_DIR or
             data/export (both read only); the only profile that can be promoted
  s<digits>  bronze + exports under $GRAPH_ROOT/s<seed>/ (make graph-sample PROFILE=s42 ...);
             the digits are the generator seed
  tiny       bronze = data/sample/churn/fixtures/tiny (committed, read only: seed 42, N_USERS
             120), exports under $GRAPH_ROOT/tiny/
  inject     a scratch copy of the tiny bronze under $GRAPH_ROOT/inject/sample with ONE poisoned
             user_name (an instruction aimed at an agent; spec.INJECT_*), exports under
             $GRAPH_ROOT/inject/export. For the eval injection case and output-hygiene tests.

Usage (Python 3.12 venv from requirements-graph.txt; `make graph-venv`):
  python scripts/build_graph_local.py [build] [--profile default] [--verify-seed] [--rebuild]
      (an unchanged build is kept; its manifest pins for exports / guarded files / seed are
       refreshed. --rebuild swaps a new build in atomically; when the Parquet is byte-identical
       it keeps what build.CARRY_OVER registers: contract.json, lineage/ + lineage.lbdb while
       fresh, cohorts.parquet, with their manifest keys. --profile inject prepares its bronze +
       exports first.)
  python scripts/build_graph_local.py build --source iceberg [--iceberg-tag graph_<id>] [--catalog-uri URI]
      (the Spark twin gold.graph_* + its inputs read by tag + snapshot id with PyIceberg, checked and
       rebuilt with the same builder; provenance in manifest.json; lakehouse_graph/iceberg_source.py)
  python scripts/build_graph_local.py promote [--build <id>]      # default profile, needs a strict contract pass
      (no --build: the newest fresh passing build; --build <id>: that build, even if stale = roll back)
  python scripts/build_graph_local.py gc [--profile P] [--keep 3]
  python scripts/build_graph_local.py sample --profile s42 [--seed 42] [--n-users 8000] [--check]
      (`make graph-sample`: the user's generator + gold script into the profile tree; tiny writes
       EXPORTS ONLY; inject copies + poisons the tiny bronze; default is refused)
  python scripts/build_graph_local.py golden [--write] [--only tiny]
      (`make graph-golden [CONFIRM=1]`: fresh tiny + s42 builds in a scratch GRAPH_ROOT, diff
       against src/lakehouse_graph/goldens/*.json; writes only with --write)

Then check it: python scripts/check_graph_contract.py --profile <profile> --strict
(an Iceberg-sourced build is checked source-aware: its pins are re-read and rebuilt, its gold drift
from the pandas twin is reported as info and the goldens derived; --catalog-uri / --warehouse, or
PYICEBERG_CATALOG__LAKEHOUSE__*, or the SQLite catalog its manifest records)
"""
from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lakehouse_graph import build, oracle, spec, store  # noqa: E402 (after the sys.path line above)
from lakehouse_graph import manifest as mf  # noqa: E402

COMMANDS = ("build", "promote", "gc", "sample", "golden")
# The Iceberg client stack --source iceberg imports lazily (requirements-graph-spark*.txt, not the core lock).
ICEBERG_CLIENT_MODULES = ("pyiceberg", "sqlalchemy", "psycopg2", "pg8000")


def cmd_build_iceberg(a) -> int:
    """--source iceberg: the lakehouse twin pinned by tag + snapshot (lakehouse_graph.iceberg_source)."""
    from lakehouse_graph import iceberg_source  # lazy: needs pyiceberg (requirements-graph-spark*.txt)

    try:
        bdir, man = iceberg_source.build_from_iceberg(
            a.profile, a.graph_root, tag=a.iceberg_tag, catalog_uri=a.catalog_uri, warehouse=a.warehouse,
            sample_dir=a.sample_dir, export_dir=a.export_dir, rebuild=a.rebuild, verify_seed=a.verify_seed,
            lock_timeout=a.lock_timeout)
    except (iceberg_source.ProvenanceUnavailable, build.GraphBuildError, TimeoutError, ValueError) as e:
        print(f"Graph build FAILED: {e}", file=sys.stderr)
        return 1
    except ImportError as e:
        missing = (e.name or "").split(".")[0]
        if missing not in ICEBERG_CLIENT_MODULES:
            raise   # not the optional Iceberg client: a real bug, keep the traceback
        print(f"Graph build FAILED: --source iceberg needs {missing}, which this Python ({sys.executable}) does not "
              f"have: run it with .venv-graph-spark/bin/python (requirements-graph-spark.txt) or inside the ldl-graph "
              f"container (docker-compose.graph.yml)", file=sys.stderr)
        return 1
    print(f"Graph build OK ({man['spec']['graph']}, {man['spec']['similar_to']}) from Iceberg: "
          f"{man['counts']['total_nodes']:,} nodes / {man['counts']['total_edges']:,} edges; "
          f"{iceberg_source.provenance_line(man)} -> {bdir}")
    return 0


def cmd_build(a) -> int:
    if a.source == "iceberg":
        return cmd_build_iceberg(a)
    try:
        if a.profile == "inject" and not a.sample_dir:
            build.prepare_inject_profile(a.graph_root, lock_timeout=a.lock_timeout)
        bdir, man = build.build_profile(a.profile, graph_root=a.graph_root, sample_dir=a.sample_dir,
                                        export_dir=a.export_dir, rebuild=a.rebuild, verify_seed=a.verify_seed,
                                        lock_timeout=a.lock_timeout)
    except (build.GraphBuildError, TimeoutError, ValueError) as e:
        print(f"Graph build FAILED: {e}", file=sys.stderr)
        return 1
    b = man["builder"]
    if b.get("rss_over_soft_limit"):
        print(f"NOTE (soft): builder max RSS {b['max_rss_mib']} MiB is above {b['rss_soft_limit_mib']} MiB",
              file=sys.stderr)
    print(f"Graph build OK ({man['spec']['graph']}, {man['spec']['similar_to']}): "
          f"{man['counts']['total_nodes']:,} nodes / {man['counts']['total_edges']:,} edges, "
          f"seed {man['seed']} N_USERS {man['n_users']} ({man['seed_n_status']}), "
          f"commit {man['commit']}{'+dirty' if man['dirty'] else ''} -> {bdir}")
    return 0


def cmd_promote(a) -> int:
    try:
        target = store.promote(a.profile, a.build, root=a.graph_root, lock_timeout=a.lock_timeout, log=print)
    except (RuntimeError, TimeoutError, ValueError) as e:
        print(f"Graph promote FAILED: {e}", file=sys.stderr)
        return 1
    link = spec.current_link(a.graph_root)
    stale = "" if mf.is_fresh(mf.read_manifest(target)) else \
        "; NOTE: this build is stale (bronze or build code changed since it was built)"
    print(f"Graph promote OK: {link} -> {target} (under the build lock; temp symlink + os.replace){stale}")
    return 0


def cmd_gc(a) -> int:
    try:
        report = store.gc(a.profile, a.keep, root=a.graph_root, lock_timeout=a.lock_timeout, log=print)
    except (TimeoutError, ValueError) as e:
        print(f"Graph gc FAILED: {e}", file=sys.stderr)
        return 1
    if not report:
        print(f"Graph gc: nothing under {spec.graph_root(a.graph_root)}")
    for prof, r in report.items():
        held = "; ".join(f"{h['build']} ({h['reason']})" for h in r["held"]) or "none"
        removed = f" ({', '.join(r['removed'])})" if r["removed"] else ""
        restored = f", restored after an interrupted replace: {', '.join(r['restored'])}" if r["restored"] else ""
        print(f"Graph gc {prof}: kept {len(r['kept'])} (newest {a.keep}), removed {len(r['removed'])}{removed}, "
              f"held {held}, stale temp dirs removed {len(r['stale_tmp_removed'])}{restored}")
    return 0


def _optional_int(raw: str | None) -> int | None:
    """'' / None (make passes an unset SEED or N_USERS as an empty string) -> None."""
    return None if raw is None or not str(raw).strip() else int(raw)


def cmd_sample(a) -> int:
    """`make graph-sample`: exit 2 for a request that cannot be honoured, 1 for a failed generation."""
    try:
        spec.check_profile(a.profile)      # first: a name no seed can be derived from is the real problem,
        try:                               # not whatever make derived from it for SEED
            seed, n_users = _optional_int(a.seed), _optional_int(a.n_users)
        except ValueError:
            raise ValueError(f"SEED and N_USERS must be integers (got SEED={a.seed!r}, N_USERS={a.n_users!r}); "
                             f"pass SEED=<n> N_USERS=<n>") from None
        seed, n_users = build.sample_params(a.profile, seed, n_users)
    except ValueError as e:
        print(f"graph-sample: {e}", file=sys.stderr)
        return 2
    if a.check:
        print(f"graph-sample: PROFILE={a.profile} is valid (seed {seed}, N_USERS {n_users}); nothing written (--check)")
        return 0
    try:
        info = build.prepare_sample(a.profile, seed, n_users, a.graph_root, lock_timeout=a.lock_timeout)
    except (build.GraphBuildError, TimeoutError, ValueError) as e:
        print(f"graph-sample FAILED: {e}", file=sys.stderr)
        return 1
    what = {"tiny": "exports only; bronze = the committed fixture",
            "inject": f"tiny bronze copy with 1 poisoned user_name ({info.get('subscription_id')})"
            }.get(a.profile, f"{len(spec.BRONZE_FILES)} bronze CSVs generated")
    print(f"graph-sample OK: profile {a.profile} (seed {seed}, N_USERS {n_users}; {what}; bronze "
          f"{info['sample_dir']}, {len(spec.EXPORT_FILES)} exports in {info['export_dir']}); data/sample/churn and "
          f"data/export untouched")
    return 0


def cmd_golden(a) -> int:
    """`make graph-golden`: regenerate the committed goldens from fresh builds; write only with --write."""
    names = a.only or list(oracle.GOLDEN_PROFILES)
    unknown = [n for n in names if n not in oracle.GOLDEN_PROFILES]
    if unknown:
        print(f"graph-golden: no such golden {unknown}; goldens: {', '.join(oracle.GOLDEN_PROFILES)}", file=sys.stderr)
        return 2
    scratch = Path(a.scratch).absolute() if a.scratch else Path(tempfile.mkdtemp(prefix="graph-golden-"))
    print(f"graph-golden: fresh {' + '.join(names)} build(s) in the scratch GRAPH_ROOT {scratch} "
          f"({'WRITE mode (CONFIRM=1)' if a.write else 'diff only; CONFIRM=1 writes'})")
    status = {}
    try:
        for name in names:
            profile, seed, n_users = oracle.GOLDEN_PROFILES[name]
            build.prepare_sample(profile, seed, n_users, scratch, lock_timeout=a.lock_timeout)
            bdir, _ = build.build_profile(profile, graph_root=scratch, lock_timeout=a.lock_timeout)
            status[name] = oracle.refresh_golden(name, bdir, write=a.write,
                                                 golden_dir=Path(a.golden_dir) if a.golden_dir else None)
    except (build.GraphBuildError, TimeoutError, ValueError) as e:
        print(f"graph-golden FAILED: {e}", file=sys.stderr)
        return 1
    finally:
        if not a.scratch:
            shutil.rmtree(scratch, ignore_errors=True)
    summary = ", ".join(f"{n}.json {s}" for n, s in status.items())
    if "differs" in status.values():
        print(f"graph-golden: {summary}. Nothing was written: rerun with CONFIRM=1 (make graph-golden CONFIRM=1) "
              f"to overwrite the goldens that differ", file=sys.stderr)
        return 1
    print(f"graph-golden OK: {summary}")
    return 0


def parse(argv: list[str]) -> argparse.Namespace:
    if not argv or argv[0] not in COMMANDS + ("-h", "--help"):
        argv = ["build", *argv]
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p, lock=True):
        p.add_argument("--graph-root", default=None, help="default: $GRAPH_ROOT or <repo>/data/graph")
        if lock:
            p.add_argument("--lock-timeout", type=float, default=600.0,
                           help="seconds to wait for $GRAPH_ROOT/.lock (build, promote and gc share it)")

    b = sub.add_parser("build", help="build one profile (default command)")
    common(b)
    b.add_argument("--profile", default="default", help="default | tiny | inject | s<digits>")
    b.add_argument("--sample-dir", default=None, help="override the profile's bronze directory")
    b.add_argument("--export-dir", default=None, help="override the profile's export directory")
    b.add_argument("--rebuild", action="store_true",
                   help="replace an existing build with the same id (atomic swap; contract.json is kept when the "
                        "Parquet is byte-identical)")
    b.add_argument("--verify-seed", action="store_true",
                   help="regenerate bronze in scratch and mark seed/N_USERS verified if sha256 match")
    b.add_argument("--source", choices=("csv", "iceberg"), default="csv",
                   help="csv (default): the profile's bronze CSVs; iceberg: the lakehouse twin published by "
                        "src/jobs/graph/01_publish_gold_graph.py, read pinned by tag + snapshot id (PyIceberg)")
    b.add_argument("--iceberg-tag", default=None,
                   help="--source iceberg: the publish to read (graph_<12 hex>; default: the newest in "
                        "lakehouse.gold.graph_build_manifest)")
    b.add_argument("--catalog-uri", default=None,
                   help="--source iceberg: SQLAlchemy URI of the Iceberg JDBC catalog (default: "
                        "PYICEBERG_CATALOG__LAKEHOUSE__URI; sqlite:////abs/catalog.db opens read-only)")
    b.add_argument("--warehouse", default=None,
                   help="--source iceberg: warehouse URI (default: PYICEBERG_CATALOG__LAKEHOUSE__WAREHOUSE)")
    b.set_defaults(fn=cmd_build)

    p = sub.add_parser("promote", help="point $GRAPH_ROOT/current at the newest fresh default build with a "
                                       "strict contract pass")
    common(p)
    p.add_argument("--profile", default="default", help="only the default profile can be promoted")
    p.add_argument("--build", default=None,
                   help="a specific business_build_id or build directory (needs its strict pass; may be stale: "
                        "this is how to roll back)")
    p.set_defaults(fn=cmd_promote)

    g = sub.add_parser("gc", help="keep the newest builds per profile; never remove a held build")
    common(g)
    g.add_argument("--profile", default=None, help="default: every profile under the graph root")
    g.add_argument("--keep", type=int, default=3)
    g.set_defaults(fn=cmd_gc)

    s = sub.add_parser("sample", help="make graph-sample: fill a profile's bronze + exports (tiny: exports only; "
                                      "inject: tiny copy + one poisoned user_name; default: refused)")
    common(s)
    s.add_argument("--profile", required=True, help="s<digits> | tiny | inject")
    s.add_argument("--seed", default=None, help="must equal the digits of s<digits>; tiny / inject are fixed at 42")
    s.add_argument("--n-users", default=None, help="s<digits> only (default 8000); tiny / inject are fixed at 120")
    s.add_argument("--check", action="store_true", help="validate the request and write nothing")
    s.set_defaults(fn=cmd_sample)

    o = sub.add_parser("golden", help="make graph-golden: diff the committed goldens against fresh tiny + s42 "
                                      "builds; --write (CONFIRM=1) overwrites the ones that differ")
    o.add_argument("--write", action="store_true", help="overwrite goldens that differ (never done silently)")
    o.add_argument("--only", action="append", default=None, help="one golden (tiny or s42); repeatable")
    o.add_argument("--scratch", default=None, help="scratch GRAPH_ROOT to build in and keep (default: a temp dir, "
                                                   "removed afterwards)")
    o.add_argument("--golden-dir", default=None, help="default: src/lakehouse_graph/goldens")
    o.add_argument("--lock-timeout", type=float, default=600.0)
    o.set_defaults(fn=cmd_golden)
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    a = parse(list(sys.argv[1:] if argv is None else argv))
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
