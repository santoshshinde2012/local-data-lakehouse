#!/usr/bin/env python3
"""Write a standalone Cytoscape.js HTML view of a renewal's point-in-time evidence (or of a lineage trace).

  --renewal <id>   the renewal's evidence timeline (x = event date, a dashed line at as_of): what the
                   evidence tool serves (nothing after as_of except the flagged FIRST_RENEWAL_AFTER
                   declared exception, never outcome evidence), its 10 SIMILAR_TO neighbours with
                   outcomes under the neighbour tool's visibility rule, and the shared hubs
  --lineage <ref>  a lineage_trace of a ColumnRef as a layered DAG (needs lineage.lbdb in the build:
                   python scripts/build_lineage_local.py --build <dir>)

The page is one self-contained file: Cytoscape.js 3.34.3 (MIT, vendored in
src/lakehouse_graph/vendor/, sha256-checked) is inlined, so it opens offline and makes no network
request; ``--cdn`` swaps in the pinned jsDelivr URL with SRI instead (small file, needs the network).
The same build and arguments give byte-identical HTML. Light / dark follows the OS (``--theme auto``)
or is forced.

Usage (Python 3.12 venv from requirements-graph.txt; `make graph-venv`):
  python scripts/graph_viz.py --renewal sub_maya:2026-10-07 [--build <dir> | --profile P] [--out <file>]
  python scripts/graph_viz.py --lineage gold.churn_renewal_features.limit_hits_14d [--direction upstream]
  options: --theme auto|light|dark  --cdn  --graph-root <dir>

``--build`` defaults to $GRAPH_ROOT/current (the promoted default build), or <profile>/latest with
``--profile``. ``--out`` defaults to $GRAPH_ROOT/viz/<business_build_id>/<renewal with ':' -> '_'>.html
(lineage: lineage_<ref>_<direction>.html). Prints the path, the byte size and the sha256; exits 1 on an
unknown or malformed id.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lakehouse_graph import manifest as mf  # noqa: E402 (after the sys.path line above)
from lakehouse_graph import spec, viz  # noqa: E402


def resolve_build_dir(a) -> Path:
    if a.build:
        return Path(a.build).absolute()
    link = spec.latest_link(a.profile, a.graph_root) if a.profile else spec.current_link(a.graph_root)
    if not link.exists():
        hint = f"make graph-build PROFILE={a.profile}" if a.profile else "make graph-local and make graph-promote"
        raise FileNotFoundError(f"no graph build at {link}: run {hint}, or pass --build <dir>")
    return link.resolve()


def lineage_hint(a, error: Exception, bdir: Path | None) -> str:
    """For a build without lineage.lbdb: the exact command that builds it into this build."""
    if a.lineage is None or bdir is None:
        return ""
    from lakehouse_graph.lineage import tools as ltools  # lazily, as viz does: only the lineage view needs it

    if not isinstance(error, ltools.LineageUnavailable):
        return ""
    return f"; to build it into this build: python scripts/build_lineage_local.py --build {bdir}"


def default_out(a, bdir: Path) -> Path:
    try:
        bid = mf.read_manifest(bdir).get("business_build_id") or bdir.name
    except (OSError, ValueError):
        bid = bdir.name
    name = viz.output_name(a.renewal) if a.renewal else \
        f"lineage_{a.lineage.replace('.', '_')}_{a.direction}.html"
    return spec.graph_root(a.graph_root) / "viz" / bid / name


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    what = ap.add_mutually_exclusive_group(required=True)
    what.add_argument("--renewal", default=None, help="a renewal id, e.g. sub_maya:2026-10-07")
    what.add_argument("--lineage", default=None, help="a ColumnRef, e.g. gold.churn_renewal_features.limit_hits_14d")
    ap.add_argument("--direction", default="upstream", choices=("upstream", "downstream"), help="lineage only")
    ap.add_argument("--max-depth", type=int, default=6, help="lineage only (1-6)")
    ap.add_argument("--build", default=None, help="a build directory (default: $GRAPH_ROOT/current)")
    ap.add_argument("--profile", default=None, help="use <profile>/latest instead of current")
    ap.add_argument("--graph-root", default=None, help="default: $GRAPH_ROOT or <repo>/data/graph")
    ap.add_argument("--out", default=None, help="output file (default under $GRAPH_ROOT/viz/<build id>/)")
    ap.add_argument("--theme", default="auto", choices=viz.THEMES)
    ap.add_argument("--cdn", action="store_true", help="load the pinned library from jsDelivr with SRI (needs the "
                                                       "network) instead of inlining the vendored copy")
    a = ap.parse_args(sys.argv[1:] if argv is None else argv)
    js_mode = "cdn" if a.cdn else "inline"
    bdir: Path | None = None
    try:
        if a.renewal is not None:
            viz.check_renewal_id(a.renewal)
        if a.profile:
            spec.check_profile(a.profile)
        bdir = resolve_build_dir(a)
        with viz.VizContext(bdir) as ctx:
            if a.renewal is not None:
                page, _height = viz.evidence_view(ctx, a.renewal, theme=a.theme, js_mode=js_mode)
            else:
                page, _height = viz.lineage_view(ctx, a.lineage, direction=a.direction, max_depth=a.max_depth,
                                                 theme=a.theme, js_mode=js_mode)
    except (ValueError, FileNotFoundError, RuntimeError) as e:
        print(f"graph-viz FAILED: {e}{lineage_hint(a, e, bdir)}", file=sys.stderr)
        return 1
    out = Path(a.out).absolute() if a.out else default_out(a, bdir)
    out.parent.mkdir(parents=True, exist_ok=True)
    data = page.encode("utf-8")
    tmp = out.with_name(f".{out.name}.tmp")
    tmp.write_bytes(data)
    tmp.replace(out)
    info = {"path": str(out), "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest(), "js_mode": js_mode,
            "theme": a.theme, "build": bdir.name}
    print(f"graph-viz OK: {out} ({len(data):,} bytes, sha256 {info['sha256']}; {js_mode} Cytoscape.js "
          f"{viz.CYTOSCAPE_VERSION}, theme {a.theme})")
    print(json.dumps(info, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
