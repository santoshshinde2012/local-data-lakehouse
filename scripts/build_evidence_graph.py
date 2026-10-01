#!/usr/bin/env python3
"""build_evidence_graph.py -- the physically pruned, label-free evidence graph for guarded raw Cypher (PLAN Later A).

    $(GRAPH_PY) scripts/build_evidence_graph.py --profile tiny|s42|<profile> [--graph-root <dir>] [--json <file>]
    $(GRAPH_PY) scripts/build_evidence_graph.py --build <build_dir> [--json <file>]

Writes <build>/evidence/ (Parquet), <build>/evidence.lbdb and <build>/evidence.json (and manifest.json["evidence"])
from the build's own Parquet (lakehouse_graph.pruned): every Subscription->event edge after its renewal's as_of
dropped, BILLED outcome evidence dropped, the declared-exception FIRST_RENEWAL_AFTER edges kept and flagged, every
label property (churned, outcome, route, is_reference, outcome_observed_on) and identity property (user_name,
city) dropped, SIMILAR_TO dropped (its candidate set reveals the route). Then it proves, on the written Parquet
(pandas) and on the loaded database (Cypher), that 0 Subscription->event edges after as_of remain, exactly the
build's declared-exception edges are kept and flagged, BILLED holds no outcome evidence, no table or property names
a label, and the 6 graph-verified features have point-in-time parity 0 WITHOUT any as_of filter.

Follows the repo's check_* convention: prints a summary, exit 0 when every invariant holds, 1 otherwise (nothing is
replaced then). An unchanged evidence graph is kept. A byte-identical rebuild of the business build carries the
evidence files over (build.CARRY_OVER, registered by lakehouse_graph.pruned) while their record still matches.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from lakehouse_graph import pruned, spec  # noqa: E402 (after the sys.path line above)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    where = ap.add_mutually_exclusive_group(required=True)
    where.add_argument("--profile", help="a build profile (its <profile>/latest build)")
    where.add_argument("--build", help="a build directory")
    ap.add_argument("--graph-root", default=None, help="GRAPH_ROOT (default $GRAPH_ROOT or <repo>/data/graph)")
    ap.add_argument("--json", default=None, help="also write the full record here")
    a = ap.parse_args(argv)
    if a.build:
        bdir = Path(a.build)
    else:
        try:
            bdir = spec.latest_link(spec.check_profile(a.profile), spec.graph_root(a.graph_root))
        except ValueError as e:
            print(f"build_evidence_graph: {e}", file=sys.stderr)
            return 2
    if not (bdir / "manifest.json").is_file():
        print(f"build_evidence_graph: no graph build at {bdir} (make graph-build first)", file=sys.stderr)
        return 1
    print(f"==> evidence graph ({pruned.EVIDENCE_SPEC_VERSION}) for {bdir.resolve()}")
    try:
        rec = pruned.build_evidence(bdir.resolve())
    except pruned.EvidenceError as e:
        print(f"Evidence graph FAILED: {e}")
        return 1
    c, full, comp = rec["counts"], rec["full_graph"], rec["with_similar_to_for_comparison"]
    removed = rec["removed"]
    cy, pd_ = rec["checks"]["cypher"], rec["checks"]["pandas"]
    leak = rec["similar_to_dropped"]
    print(f"  nodes {c['total_nodes']:,} / edges {c['total_edges']:,} (full graph {full['total_nodes']:,} / "
          f"{full['total_edges']:,}; with SIMILAR_TO kept it would be {comp['total_nodes']:,} / "
          f"{comp['total_edges']:,})")
    print(f"  removed edges {json.dumps(removed['removed_edges'])}; removed nodes "
          f"{json.dumps(removed['removed_nodes'])}")
    print(f"  FIRST_RENEWAL_AFTER: {removed['first_renewal_after_edges']:,} edges kept, "
          f"{cy['first_renewal_after_flagged']} after as_of flagged declared_exception (known_by_as_of=false)")
    post = sum(pd_["post_as_of_subscription_event_edges"].values())
    post_cy = sum(cy["post_as_of_subscription_event_edges"].values())
    print(f"  Subscription->event edges after as_of: pandas {post}, Cypher {post_cy}; BILLED event types "
          f"{json.dumps(cy['billed_event_types'])}")
    for side, par in (("pandas", pd_["pit_parity"]), ("Cypher", cy["pit_parity"])):
        print(f"  PIT parity ({side}) mismatches, 6 features: naive (no as_of filter) "
              f"{sum(par['naive'].values())}, windowed {sum(par['pit_window'].values())}")
    print(f"  label-free: no table or property names a label ({len(cy['tables'])} tables checked); dropped "
          f"{json.dumps(rec['dropped_properties'])}")
    print(f"  SIMILAR_TO dropped: destinations are routes {leak['destinations_route']} only; a renewal with no "
          f"incoming edge lapsed {leak['lapse_share_without_incoming_edge']} vs {leak['lapse_share_all']} overall")
    print(f"  {pruned.EVIDENCE_DB} {rec['db']['bytes'] / 1e6:.1f} MB, sha256 {rec['db']['sha256'][:12]}; record "
          f"{bdir / pruned.EVIDENCE_META}")
    if a.json:
        Path(a.json).write_text(json.dumps(rec, indent=1, sort_keys=True, default=str) + "\n", encoding="utf-8")
    print(f"Evidence graph OK ({pruned.describe(rec)})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
