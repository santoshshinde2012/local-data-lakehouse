#!/usr/bin/env python3
"""Build the Tier-0 lineage / metadata graph from the repo's own code (offline, a few seconds).

sql/churn/gold_renewal_features.sql (sqlglot qualify + scope walk), sql/retail/*.sql, the
Spark jobs and scripts (Python ast), the DAGs, the Makefile, the shell pipelines, the CI
workflow and the README numbers -> deterministic Parquet node / edge tables + lineage.lbdb
(LadybugDB) inside a graph build directory:

  <build_dir>/lineage/{nodes_<Label>,edges_<TYPE>}.parquet, lineage/manifest.json
  <build_dir>/lineage.lbdb

``<build_dir>`` is the business graph build the lineage describes: by default the latest
build of ``--graph-profile`` under $GRAPH_ROOT (default data/graph), or any directory given
with ``--build``. The lineage has its own identity, ``lineage_build_id`` (sha256 over every
file the extractor read + the lineage code + versions); it never changes business_build_id.

Profiles:
  core   code-derived only, no network, no data files: what CI checks (default)
  full   core + FileSnapshot nodes for the bronze CSVs / exports that exist + the
         retention-radar interface when RADAR_DIR (or --radar-dir) points at a local
         checkout. The network is used only with --radar-public (gh api, public main).
         Without RADAR_DIR (or without export files) the build still succeeds and prints
         "WARN (environment)": that describes this machine, not the repo, and the
         contract reports it without failing, even with --strict.

Overlays (either profile; off by default, so CI's core build stays offline and code-only):
  --iceberg      Tier 1: Snapshot / Ref nodes and HAS_SNAPSHOT / POINTS_TO / SUPERSEDES /
                 PRODUCED_BY_RUN edges from the Iceberg catalog's metadata (``.snapshots`` and
                 ``.refs`` only, never ``.history``), read through lakehouse_graph.iceberg_source
                 with every safety rule (catalog `lakehouse`, init_catalog_tables=false, no
                 schema_version, local S3 endpoint + region, SQLite read-only; the catalog's own
                 schema is compared before and after: no ALTER). When the build directory holds an
                 Iceberg-sourced business build, its Run CONSUMED_SNAPSHOT every snapshot it pinned.
                 Catalog: --catalog-uri / --warehouse, else PYICEBERG_CATALOG__LAKEHOUSE__*, else
                 the SQLite catalog that business build records. Needs PyIceberg
                 (.venv-graph-spark/bin/python, or the ldl-graph container).
  --openlineage [FILE]
                 Tier 2: Run nodes, RAN_AS (appName -> job file) and PARENT edges from an
                 OpenLineage JSONL file (default $GRAPH_ROOT/lineage/openlineage.jsonl, what
                 OPENLINEAGE=1 ./pipelines/run_graph_e2e.sh writes); action-level runs are
                 collapsed into their application run through the ParentRunFacet.
Both re-key lineage_build_id (the facts and the file are hashed into it); the semantic golden
answers do not move.

Nothing under data/sample or data/export is ever written, and nothing is ever written to the
Iceberg catalog. A name the extractor cannot resolve is listed (and fails
scripts/check_lineage_contract.py); an unreadable pipeline fails the build.

Usage (Python 3.12 venv from requirements-graph.txt; `make graph-venv`):
  python scripts/build_lineage_local.py [--profile core] [--graph-profile default]
  python scripts/build_lineage_local.py --profile full --radar-dir ../retention-radar
  python scripts/build_lineage_local.py --build /tmp/lineage-scratch        # no business build needed
  .venv-graph-spark/bin/python scripts/build_lineage_local.py --iceberg \\
      --catalog-uri sqlite:////abs/lakehouse/catalog.db --warehouse file:///abs/lakehouse/warehouse \\
      [--openlineage data/graph/lineage/openlineage.jsonl]

Then check it: python scripts/check_lineage_contract.py [--graph-profile default] --strict
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lakehouse_graph import manifest as mf  # noqa: E402 (after the sys.path line above)
from lakehouse_graph import spec as gspec  # noqa: E402
from lakehouse_graph.lineage import build as lbuild  # noqa: E402
from lakehouse_graph.lineage import spec  # noqa: E402


def resolve_build_dir(a) -> Path:
    if a.build:
        return Path(a.build).absolute()
    link = gspec.latest_link(a.graph_profile, a.graph_root)
    if not link.exists():
        raise lbuild.LineageBuildError(
            f"no graph build for profile {a.graph_profile} under {gspec.graph_root(a.graph_root)}: run "
            f"make graph-build PROFILE={a.graph_profile} first, or pass --build <dir> to write the lineage "
            f"tables into a directory of your choice")
    return link.resolve()


def data_dirs(a, bdir: Path) -> tuple[Path | None, Path | None]:
    """(sample_dir, export_dir) the full profile snapshots: flags > the business manifest > the profile's."""
    sample, export = a.sample_dir, a.export_dir
    try:
        man = mf.read_manifest(bdir)
    except (OSError, ValueError):
        man = None
    if man:
        sample = sample or mf.resolve_path(man["inputs"]["sample_dir"])
        export = export or mf.resolve_path(man["exports"]["dir"])
    elif not a.build:
        sample = sample or gspec.sample_dir(a.graph_profile, a.graph_root)
        export = export or gspec.export_dir(a.graph_profile, a.graph_root)
    return (Path(sample) if sample else None), (Path(export) if export else None)


# The Iceberg client --iceberg imports lazily (requirements-graph-spark*.txt, not the core lock).
ICEBERG_CLIENT_MODULES = ("pyiceberg", "sqlalchemy", "psycopg2", "pg8000")


def business_manifest(bdir: Path) -> dict | None:
    try:
        return mf.read_manifest(bdir)
    except (OSError, ValueError):
        return None


def overlays(a, bdir: Path) -> list:
    """The Tier-1 (--iceberg) and Tier-2 (--openlineage) overlays, in that order (the OpenLineage runs
    then meet the snapshots their Spark application committed)."""
    out = []
    if a.iceberg:
        from lakehouse_graph.lineage import iceberg_facts  # lazy: PyIceberg

        business = business_manifest(bdir)
        uri = a.catalog_uri
        recorded = ((business or {}).get("iceberg") or {}).get("catalog_uri") or ""
        if uri is None and not os.environ.get("PYICEBERG_CATALOG__LAKEHOUSE__URI") and recorded.startswith("sqlite:"):
            uri = recorded   # a local SQLite lakehouse carries no credentials: the build's own record is enough
        warehouse = a.warehouse or (None if a.catalog_uri or os.environ.get("PYICEBERG_CATALOG__LAKEHOUSE__WAREHOUSE")
                                    else ((business or {}).get("iceberg") or {}).get("warehouse") or None)
        facts = iceberg_facts.load_facts(uri, warehouse)
        out.append(iceberg_facts.overlay(facts, business))
    if a.openlineage is not None:
        from lakehouse_graph.lineage import openlineage

        path = Path(a.openlineage) if a.openlineage else gspec.graph_root(a.graph_root) / spec.OPENLINEAGE_FILE
        if not path.is_file():
            raise lbuild.LineageBuildError(f"--openlineage: {path} does not exist (OPENLINEAGE=1 "
                                           f"./pipelines/run_graph_e2e.sh writes {spec.OPENLINEAGE_FILE} under "
                                           f"$GRAPH_ROOT; or pass the file)")
        out.append(openlineage.overlay(path))
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profile", default="core", choices=spec.PROFILES, help="lineage profile (default: core)")
    ap.add_argument("--graph-profile", default="default", help="business build profile whose latest build "
                                                               "receives the lineage tables (default: default)")
    ap.add_argument("--build", default=None, help="write into this build directory instead")
    ap.add_argument("--graph-root", default=None, help="default: $GRAPH_ROOT or <repo>/data/graph")
    ap.add_argument("--sample-dir", default=None, help="full: bronze CSV directory to snapshot")
    ap.add_argument("--export-dir", default=None, help="full: export directory to snapshot")
    ap.add_argument("--radar-dir", default=None, help="full: local retention-radar checkout (default: $RADAR_DIR)")
    ap.add_argument("--radar-public", action="store_true",
                    help="full: also read the public retention-radar main branch with `gh api` (network)")
    ap.add_argument("--rebuild", action="store_true", help="replace an existing lineage build with the same id")
    ap.add_argument("--lock-timeout", type=float, default=600.0)
    ap.add_argument("--iceberg", action="store_true",
                    help="Tier 1: add Snapshot / Ref facts from the Iceberg catalog (read-only; needs PyIceberg)")
    ap.add_argument("--catalog-uri", default=None,
                    help="--iceberg: catalog URI, the Lakekeeper REST endpoint (http...) or a local SQLite harness (default: "
                         "PYICEBERG_CATALOG__LAKEHOUSE__URI, else the SQLite catalog the business build records; "
                         "sqlite:////abs/catalog.db opens read-only)")
    ap.add_argument("--warehouse", default=None,
                    help="--iceberg: warehouse URI (default: PYICEBERG_CATALOG__LAKEHOUSE__WAREHOUSE)")
    ap.add_argument("--openlineage", nargs="?", const="", default=None, metavar="FILE",
                    help=f"Tier 2: add Run facts from an OpenLineage JSONL file (default "
                         f"$GRAPH_ROOT/{spec.OPENLINEAGE_FILE})")
    a = ap.parse_args(argv)

    radar_dir = a.radar_dir or os.environ.get("RADAR_DIR") or None
    if a.profile == "core" and (a.radar_dir or a.radar_public or a.sample_dir or a.export_dir):
        print("NOTE: --radar-dir / --radar-public / --sample-dir / --export-dir are ignored by the core profile "
              "(code-derived only); use --profile full", file=sys.stderr)
    if (a.catalog_uri or a.warehouse) and not a.iceberg:
        print("NOTE: --catalog-uri / --warehouse are read only with --iceberg", file=sys.stderr)
    try:
        bdir = resolve_build_dir(a)
        sample, export = data_dirs(a, bdir) if a.profile == "full" else (None, None)
        layers = overlays(a, bdir)
        _dir, man = lbuild.build_lineage(
            bdir, a.profile, sample_dir=sample, export_dir=export, radar_dir=radar_dir if a.profile == "full" else None,
            radar_public=a.radar_public and a.profile == "full", rebuild=a.rebuild, lock_timeout=a.lock_timeout,
            overlays=layers)
    except (lbuild.LineageBuildError, spec.LineageExtractError, TimeoutError, ValueError) as e:
        print(f"Lineage build FAILED: {e}", file=sys.stderr)
        return 1
    except ImportError as e:
        missing = (e.name or "").split(".")[0]
        if missing not in ICEBERG_CLIENT_MODULES:
            raise   # not the optional Iceberg client: a real bug, keep the traceback
        print(f"Lineage build FAILED: --iceberg needs {missing}, which this Python ({sys.executable}) does not have: "
              f"run it with .venv-graph-spark/bin/python (requirements-graph-spark.txt) or inside the ldl-graph "
              f"container (docker-compose.graph.yml)", file=sys.stderr)
        return 1
    except RuntimeError as e:   # iceberg_source.ProvenanceUnavailable (catalog, safety rule, schema changed)
        if type(e).__name__ != "ProvenanceUnavailable":
            raise
        print(f"Lineage build FAILED: --iceberg: {e}", file=sys.stderr)
        return 1
    for w in man["warnings"]:
        print(f"WARN: {w}", file=sys.stderr)
    for w in man.get("environment_warnings", []):   # about this machine, not the repo: the contract never gates on it
        print(f"WARN (environment): {w}", file=sys.stderr)
    for u in man["unresolved"]:
        print(f"UNRESOLVED: {u['where']}: {u['what']}", file=sys.stderr)
    if man["unresolved"]:
        print(f"NOTE: {len(man['unresolved'])} unresolved name(s): scripts/check_lineage_contract.py fails until "
              f"they resolve", file=sys.stderr)
    for name, info in sorted((man.get("overlays") or {}).items()):
        if name == "iceberg":
            from lakehouse_graph.lineage import iceberg_facts

            print(f"    Tier 1: {iceberg_facts.summary_line(info)}")
        elif name == "openlineage":
            from lakehouse_graph.lineage import openlineage

            print(f"    Tier 2: {openlineage.summary_line(info)}")
    c = man["counts"]
    print(f"Lineage build OK ({man['spec']}, profile {man['profile']}): {c['total_nodes']:,} nodes / "
          f"{c['total_edges']:,} edges ({c['nodes']['DataColumn']} columns, {c['edges']['DERIVED_FROM']} "
          f"DERIVED_FROM, {c['nodes']['GraphElement']} graph elements), lineage_build_id {man['lineage_build_id']}, "
          f"{len(man['inputs_sha256'])} files hashed, commit {man['commit']}{'+dirty' if man['dirty'] else ''} "
          f"-> {bdir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
