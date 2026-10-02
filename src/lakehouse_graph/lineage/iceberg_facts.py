"""Tier-1 lineage facts from the Iceberg catalog: snapshots, refs, the Spark runs that committed them
and the graph build that read them (overlay ``--iceberg`` of scripts/build_lineage_local.py).

  load_facts(uri, warehouse, props)   open catalog ``lakehouse`` through lakehouse_graph.iceberg_source
                                      (every safety rule there: init_catalog_tables=false in code, no
                                      schema_version, a local s3 endpoint + region, SQLite opened
                                      read-only, both catalog tables probed), read the facts, and prove
                                      that nothing was ALTERed: the catalog database's own schema is
                                      read before and after (a difference is ProvenanceUnavailable)
  read_facts(catalog) -> facts        metadata only, per table: ``table.snapshots()`` and
                                      ``table.refs()``. Never the snapshot log (``history``), which
                                      expiry trims and ``createOrReplace`` rewrites; never a data scan
  overlay(facts, business) -> f(g)    the nodes and edges below, merged into a lineage build

Nodes and edges (spec metadata-graph/0.1, Tier 1):
  Snapshot  snap:<table>@<snapshot_id>   sequence number, committed_at (UTC), operation, records,
                                         spark.app.id and the whole summary (JSON), is_current
  Ref       ref:<table>@<name>           tag or branch, retention settings
  Run       run:spark:<spark.app.id>     the Spark application that committed a snapshot
            run:graph-build:<id>         the business graph build that read snapshots (manifest["iceberg"])
  HAS_SNAPSHOT       Dataset -> Snapshot
  POINTS_TO          Ref -> Snapshot
  SUPERSEDES         Snapshot -> the previous snapshot of its table BY SEQUENCE NUMBER: createOrReplace
                     commits a snapshot with no parent, so parent ids cannot chain a replaced table
                     (parent_matches says whether the parent id agrees; sequence_gap counts the numbers
                     expiry removed). A format-v1 table has no sequence numbers: ordered by commit time.
  PRODUCED_BY_RUN    Snapshot -> Run, by the summary's spark.app.id
  CONSUMED_SNAPSHOT  Run (graph build) -> every Snapshot its manifest pins (role input / output, tag, rows)
  RAN_AS             Run (graph build) -> scripts/build_graph_local.py; the publish run recorded in
                     manifest["iceberg"]["spark_app_id"] -> src/jobs/graph/01_publish_gold_graph.py

Nothing is dropped: a table the catalog holds that no code of the repo declares becomes a Dataset
(declared_by "iceberg catalog"), a pinned snapshot the catalog no longer has becomes a Snapshot with
in_catalog false, a ref to a snapshot the table does not list is named; each with an environment
warning (the lakehouse's state, not the repo's). The facts enter lineage_build_id as
``iceberg:<catalog>`` (sha256 of the facts) and ``iceberg-build:<business_build_id>`` (sha256 of the
pins), so the id moves when a snapshot is committed, a tag moves or a build reads other snapshots.

pyiceberg is imported lazily (requirements-graph-spark*.txt or the ldl-graph container); ``overlay``
needs only the facts document, so it runs (and is tested) in the core venv too.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone

from .. import manifest as mf
from . import extract as ex
from . import spec
from .graph import LineageGraph

CATALOG = "lakehouse"   # = lakehouse_graph.iceberg_source.CATALOG (imported lazily there: pyiceberg)
GRAPH_BUILD_JOB = spec.GRAPH_BUILD_SCRIPT
PUBLISH_JOB = spec.GRAPH_SPARK_JOB


# --------------------------------------------------------------------------- reading (pyiceberg)
def _operation(snap) -> str | None:
    op = getattr(snap.summary, "operation", None) if snap.summary is not None else None
    return None if op is None else str(getattr(op, "value", op)).rsplit(".", 1)[-1].lower()


def _summary(snap) -> dict[str, str]:
    s = snap.summary
    if s is None:
        return {}
    extra = getattr(s, "additional_properties", None)
    return {str(k): str(v) for k, v in (dict(extra) if extra is not None else {k: s[k] for k in s}).items()}


def snapshot_fact(snap) -> dict:
    return {"snapshot_id": int(snap.snapshot_id),
            "parent_id": None if snap.parent_snapshot_id is None else int(snap.parent_snapshot_id),
            "sequence_number": None if snap.sequence_number is None else int(snap.sequence_number),
            "timestamp_ms": int(snap.timestamp_ms), "operation": _operation(snap), "summary": _summary(snap)}


def ref_fact(name: str, ref) -> dict:
    kind = getattr(ref.snapshot_ref_type, "value", ref.snapshot_ref_type)
    return {"name": str(name), "kind": str(kind).rsplit(".", 1)[-1].lower(), "snapshot_id": int(ref.snapshot_id),
            "max_ref_age_ms": ref.max_ref_age_ms, "min_snapshots_to_keep": ref.min_snapshots_to_keep,
            "max_snapshot_age_ms": ref.max_snapshot_age_ms}


def _namespaces(catalog, parent: tuple = (), depth: int = 0) -> list[tuple]:
    """Every namespace of the catalog, nested ones included (the lakehouse's are flat: bronze, silver, gold)."""
    out: list[tuple] = []
    found = catalog.list_namespaces(parent) if parent else catalog.list_namespaces()
    for ns in sorted({tuple(n) for n in found}):
        if len(ns) <= len(parent) or ns[:len(parent)] != parent:
            continue
        out.append(ns)
        if depth < 4:
            out += [c for c in _namespaces(catalog, ns, depth + 1) if c not in out]
    return out


def read_facts(catalog) -> dict:
    """{"tables": {lakehouse.<ns>.<t>: {table_uuid, format_version, current_snapshot_id, snapshots, refs}}}.

    Reads table metadata only: ``load_table`` (the current metadata file), ``snapshots()`` and
    ``refs()``. No data file, no manifest list, no snapshot log."""
    tables: dict[str, dict] = {}
    for ns in _namespaces(catalog):
        for ident in sorted(tuple(i) for i in catalog.list_tables(ns)):
            table = catalog.load_table(ident)
            meta = table.metadata
            fq = ".".join((CATALOG, *ident))
            snaps = sorted((snapshot_fact(s) for s in table.snapshots()),
                           key=lambda s: (s["sequence_number"] or 0, s["timestamp_ms"], s["snapshot_id"]))
            tables[fq] = {"table_uuid": str(meta.table_uuid), "format_version": int(meta.format_version),
                          "current_snapshot_id": None if meta.current_snapshot_id is None
                          else int(meta.current_snapshot_id),
                          "snapshots": snaps,
                          "refs": [ref_fact(name, ref) for name, ref in sorted(table.refs().items())]}
    return {"catalog": CATALOG, "tables": dict(sorted(tables.items()))}


def load_facts(catalog_uri: str | None = None, warehouse: str | None = None, catalog_props: dict | None = None,
               log=print) -> dict:
    """Open the catalog read-only (lakehouse_graph.iceberg_source.open_catalog), read the facts, check
    the catalog schema is unchanged. Raises iceberg_source.ProvenanceUnavailable."""
    from .. import iceberg_source as ice  # lazy: pyiceberg

    t0 = time.perf_counter()
    catalog = ice.open_catalog(catalog_uri, warehouse, **(catalog_props or {}))
    try:
        before = ice.catalog_schema(catalog)
        facts = read_facts(catalog)
        if ice.catalog_schema(catalog) != before:
            raise ice.ProvenanceUnavailable("the Iceberg catalog's own schema changed while its metadata was read")
        props = getattr(catalog, "properties", {}) or {}
        facts.update(catalog_uri=ice._redact(str(props.get("uri", ""))), warehouse=str(props.get("warehouse", "")),
                     catalog_schema_unchanged=True, read_s=round(time.perf_counter() - t0, 2))
    finally:
        ice.close_catalog(catalog)
    n_snaps = sum(len(t["snapshots"]) for t in facts["tables"].values())
    log(f"    Iceberg catalog {CATALOG} ({facts['catalog_uri']}): {len(facts['tables'])} tables, {n_snaps} snapshots, "
        f"{sum(len(t['refs']) for t in facts['tables'].values())} refs read in {facts['read_s']} s (metadata only; "
        f"catalog schema unchanged)")
    return facts


# --------------------------------------------------------------------------- the overlay (pure)
def committed_at(timestamp_ms: int) -> str:
    """Iceberg commit time (ms since the epoch) as ISO 8601 UTC with milliseconds."""
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat(timespec="milliseconds") \
        .replace("+00:00", "Z")


def snapshot_id(table: str, sid: int | str) -> str:
    return f"snap:{table}@{sid}"


def spark_run_id(app_id: str) -> str:
    return f"run:spark:{app_id}"


def graph_build_run_id(business_build_id: str) -> str:
    return f"run:graph-build:{business_build_id}"


def _int(value) -> int | None:
    try:
        return None if value is None else int(value)
    except (TypeError, ValueError):
        return None


def _dataset(g: LineageGraph, table: str, undeclared: list[str]) -> str:
    did = f"ds:{table}"
    if not g.has(did):   # in the lakehouse, not in the code: the catalog is the only declaration
        parts = table.split(".")
        g.node("Dataset", did, name=table, ref=spec.dataset_ref(table), kind="iceberg_table",
               layer=parts[1] if len(parts) > 2 else "unknown", domain=ex.job_domain(table), format="iceberg",
               declared_by="iceberg catalog", status="in the catalog; no code of the repo declares it")
        undeclared.append(table)
    return did


def spark_run(g: LineageGraph, app_id: str) -> str:
    """The Run node of a Spark application (shared by the Iceberg and the OpenLineage overlays)."""
    return g.node("Run", spark_run_id(app_id), engine="spark", kind="spark_application", spark_app_id=app_id)


def _snapshot_node(g: LineageGraph, table: str, s: dict, *, uuid: str | None, current: int | None,
                   in_catalog: bool) -> str:
    summary = s.get("summary") or {}
    return g.node(
        "Snapshot", snapshot_id(table, s["snapshot_id"]), table_name=table, snapshot_id=str(s["snapshot_id"]),
        parent_id=None if s.get("parent_id") is None else str(s["parent_id"]),
        sequence_number=s.get("sequence_number"), timestamp_ms=s.get("timestamp_ms"),
        committed_at=committed_at(s["timestamp_ms"]) if s.get("timestamp_ms") is not None else None,
        operation=s.get("operation"), spark_app_id=summary.get("spark.app.id"),
        added_records=_int(summary.get("added-records")), total_records=_int(summary.get("total-records")),
        summary=ex.dumps(summary) if summary else None, table_uuid=uuid,
        is_current=(s["snapshot_id"] == current) if in_catalog else None, in_catalog=in_catalog)


def _chain(snaps: list[dict]) -> tuple[list[dict], str]:
    """Snapshots in commit order, and the key used: sequence numbers when the table has them (format v2+:
    distinct and > 0), else commit time (format v1 numbers every snapshot 0)."""
    seqs = [s["sequence_number"] for s in snaps]
    if all(isinstance(x, int) and x > 0 for x in seqs) and len(set(seqs)) == len(seqs):
        return sorted(snaps, key=lambda s: s["sequence_number"]), "sequence_number"
    return sorted(snaps, key=lambda s: (s["timestamp_ms"], s["snapshot_id"])), \
        "timestamp_ms (no distinct sequence numbers: format v1)"


def build_pins(business: dict) -> dict:
    """What a business build read from Iceberg (manifest["iceberg"]): the part that enters lineage_build_id."""
    ice = business.get("iceberg") or {}
    return {"business_build_id": business.get("business_build_id"), "tag": ice.get("tag"),
            "lakehouse_build_id": ice.get("lakehouse_build_id"), "spark_app_id": ice.get("spark_app_id"),
            "tables": {t: {k: p.get(k) for k in ("snapshot_id", "sequence_number", "role", "rows", "tag")}
                       for t, p in sorted((ice.get("tables") or {}).items())}}


def overlay(facts: dict, business: dict | None = None):
    """``f(graph)`` adding the Tier-1 facts (``facts`` from load_facts / read_facts) and, when
    ``business`` (the business manifest of the build the lineage lands in) was built from Iceberg, the
    graph build's CONSUMED_SNAPSHOT edges."""
    def apply(g: LineageGraph) -> None:
        catalog = facts.get("catalog", CATALOG)
        tables = facts.get("tables") or {}
        g.inputs[f"iceberg:{catalog}"] = mf.sha256_json(tables)
        undeclared: list[str] = []
        runs: set[str] = set()
        counts = {"tables": len(tables), "snapshots": 0, "tags": 0, "branches": 0, "supersedes": 0,
                  "parent_cut": 0, "produced_by_run": 0}
        for table, t in sorted(tables.items()):
            did = _dataset(g, table, undeclared)
            by_id = {s["snapshot_id"]: s for s in t["snapshots"]}
            for s in t["snapshots"]:
                sid = _snapshot_node(g, table, s, uuid=t.get("table_uuid"), current=t.get("current_snapshot_id"),
                                     in_catalog=True)
                g.edge("HAS_SNAPSHOT", did, sid)
                app = (s.get("summary") or {}).get("spark.app.id")
                if app:
                    runs.add(app)
                    g.edge("PRODUCED_BY_RUN", sid, spark_run(g, app), via="snapshot summary spark.app.id")
                    counts["produced_by_run"] += 1
            counts["snapshots"] += len(t["snapshots"])
            ordered, key = _chain(t["snapshots"])
            for older, newer in zip(ordered, ordered[1:], strict=False):
                matches = newer.get("parent_id") == older["snapshot_id"]
                gap = (newer["sequence_number"] - older["sequence_number"] - 1) if key == "sequence_number" else None
                g.edge("SUPERSEDES", snapshot_id(table, newer["snapshot_id"]), snapshot_id(table, older["snapshot_id"]),
                       ordered_by=key, parent_matches=matches, sequence_gap=gap)
                counts["supersedes"] += 1
                counts["parent_cut"] += not matches
            for r in t["refs"]:
                rid = g.node("Ref", f"ref:{table}@{r['name']}", table_name=table, name=r["name"], kind=r["kind"],
                             snapshot_id=str(r["snapshot_id"]), max_ref_age_ms=r.get("max_ref_age_ms"),
                             min_snapshots_to_keep=r.get("min_snapshots_to_keep"),
                             max_snapshot_age_ms=r.get("max_snapshot_age_ms"))
                counts["tags" if r["kind"] == "tag" else "branches"] += 1
                if r["snapshot_id"] in by_id:
                    g.edge("POINTS_TO", rid, snapshot_id(table, r["snapshot_id"]))
                else:
                    g.environment_warnings.append(
                        f"iceberg: {r['kind']} {r['name']} of {table} points at snapshot {r['snapshot_id']}, which "
                        f"the table's metadata does not list (no POINTS_TO edge)")
        consumed = _consumed(g, business, undeclared) if business is not None else None
        if undeclared:   # (after the pins: a pinned table may be one more)
            g.environment_warnings.append(
                f"iceberg: {len(undeclared)} table(s) of catalog {catalog} that no code of the repo declares (added "
                f"as Dataset nodes, declared_by 'iceberg catalog'): {', '.join(undeclared[:8])}"
                f"{' ...' if len(undeclared) > 8 else ''}")
        g.overlays["iceberg"] = {
            "catalog": catalog, "catalog_uri": facts.get("catalog_uri"), "warehouse": facts.get("warehouse"),
            "catalog_schema_unchanged": facts.get("catalog_schema_unchanged"), "read_s": facts.get("read_s"),
            **counts, "spark_runs": len(runs), "undeclared_tables": undeclared, "graph_build": consumed,
            "facts_sha256": g.inputs[f"iceberg:{catalog}"]}
    return apply


def _consumed(g: LineageGraph, business: dict, undeclared: list[str]) -> dict | None:
    """The business build's Run -> every snapshot it pinned (manifest["iceberg"]["tables"])."""
    bid = business.get("business_build_id")
    ice = business.get("iceberg")
    if not ice:
        g.environment_warnings.append(f"iceberg: business build {bid} was built from the bronze CSVs, not from "
                                      f"Iceberg: no CONSUMED_SNAPSHOT edges")
        return None
    g.inputs[f"iceberg-build:{bid}"] = mf.sha256_json(build_pins(business))
    run = g.node("Run", graph_build_run_id(bid), job=GRAPH_BUILD_JOB, kind="graph_build", engine="pyiceberg",
                 ended_at=business.get("built_at"), state="COMPLETE", source="manifest.json[\"iceberg\"]",
                 env=ex.dumps({"tag": ice.get("tag"), "lakehouse_build_id": ice.get("lakehouse_build_id"),
                               "identity": ice.get("identity")}))
    if g.has(f"job:{GRAPH_BUILD_JOB}"):
        g.edge("RAN_AS", run, f"job:{GRAPH_BUILD_JOB}", via="business manifest (build --source iceberg)")
    gone = []
    for table, p in sorted((ice.get("tables") or {}).items()):
        sid = snapshot_id(table, p["snapshot_id"])
        if not g.has(sid):   # pinned, but not (or no longer) in the catalog read: keep the fact from the pins
            did = _dataset(g, table, undeclared)
            _snapshot_node(g, table, {"snapshot_id": p["snapshot_id"], "sequence_number": p.get("sequence_number"),
                                      "timestamp_ms": p.get("timestamp_ms"), "operation": p.get("operation"),
                                      "summary": {"spark.app.id": p["spark_app_id"]} if p.get("spark_app_id")
                                      else {}},
                           uuid=p.get("table_uuid"), current=None, in_catalog=False)
            g.edge("HAS_SNAPSHOT", did, sid)
            if p.get("spark_app_id"):
                g.edge("PRODUCED_BY_RUN", sid, spark_run(g, p["spark_app_id"]), via="build pins spark_app_id")
            gone.append(f"{table}@{p['snapshot_id']}")
        g.edge("CONSUMED_SNAPSHOT", run, sid, role=p.get("role"), tag=p.get("tag"), n_rows=p.get("rows"))
    if gone:
        g.environment_warnings.append(f"iceberg: business build {bid} read {len(gone)} snapshot(s) the catalog no "
                                      f"longer lists (expired, or another catalog): {', '.join(gone[:6])}")
    publish = ice.get("spark_app_id")
    if publish:
        pid = spark_run(g, publish)
        if g.has(f"job:{PUBLISH_JOB}"):
            g.edge("RAN_AS", pid, f"job:{PUBLISH_JOB}",
                   via=f"gold.graph_build_manifest spark_app_id (publish {ice.get('tag')})")
    return {"run": run, "tag": ice.get("tag"), "lakehouse_build_id": ice.get("lakehouse_build_id"),
            "consumed": len(ice.get("tables") or {}), "not_in_catalog": gone, "publish_spark_app_id": publish}


def summary_line(info: dict) -> str:
    """One line for the CLI from the lineage manifest's overlays["iceberg"]."""
    b = info.get("graph_build") or {}
    return (f"Iceberg {info['catalog']}: {info['tables']} tables, {info['snapshots']} snapshots "
            f"({info['supersedes']} SUPERSEDES, {info['parent_cut']} with a cut parent id), {info['tags']} tags / "
            f"{info['branches']} branches, {info['spark_runs']} Spark runs"
            + (f"; graph build read {b['consumed']} snapshots at {b['tag']}" if b
               else "; no graph build read from Iceberg"))


__all__ = ["load_facts", "overlay", "read_facts", "summary_line"]


if __name__ == "__main__":   # python -m lakehouse_graph.lineage.iceberg_facts --catalog-uri ... --warehouse ...
    import argparse

    ap = argparse.ArgumentParser(description="Print the Tier-1 Iceberg facts (metadata only, read-only) as JSON.")
    ap.add_argument("--catalog-uri", default=None)
    ap.add_argument("--warehouse", default=None)
    a = ap.parse_args()
    print(json.dumps(load_facts(a.catalog_uri, a.warehouse, log=lambda *_: None), indent=1, sort_keys=True))
