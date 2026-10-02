"""The source-aware graph contract on Iceberg-sourced builds (manifest["iceberg"], PHASE 2c).

Without Spark: a fake lakehouse replaces only the catalog side of lakehouse_graph.iceberg_source
(open_catalog, catalog_schema, resolve_publish, read_inputs, read_twin) with in-memory frames of the
tiny fixture (the pandas twin's silver + gold, drifted or tampered on purpose) pinned by a tag and
snapshot ids. Everything else is the real code: build_from_iceberg() builds and records the
provenance, verify_build() re-reads the pins and rebuilds, and scripts/check_graph_contract.py
(in process) checks the build:

  * a gold drift (two accept_rate_change cells, like Spark vs pandas rounding) passes --strict:
    the drift is INFO with its exact cells, the golden values are derived (the builder on the
    bronze's events + the build's own gold gives the build byte for byte), the export mismatch is
    exactly the drift cells, determinism re-reads the pins, and promote accepts the build;
  * a build that kept the bronze identity applies the golden exactly;
  * strict still fails when an invariant breaks (a PIT feature off by one in the lakehouse, edited
    Parquet), when the lakehouse's events are not the bronze's, when the pins no longer hold or are
    inconsistent, and when no catalog can be reached (a warning).

At the bottom, one real local Iceberg build (PySpark 3.5 + JdbcCatalog(SQLite), the
scripts/check_graph_parity.py harness), skipped without pyspark / a JDK 17 or 21 / the jars.
"""
from __future__ import annotations

import contextlib
import importlib.util
import io
import os
import shutil
import sys
import uuid
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest
from conftest import REPO, run_script

from lakehouse_graph import build, oracle, spec, store
from lakehouse_graph import iceberg_source as ice
from lakehouse_graph import manifest as mf

TINY = REPO / spec.TINY_FIXTURE
TAG = "graph_0123456789ab"
LAKE_URI = "sqlite:///file:/nonexistent/lakehouse/catalog.db?mode=ro&uri=true"   # recorded; the fake never opens it
LAKE_WAREHOUSE = "file:///nonexistent/lakehouse/warehouse"
DRIFT = {"sub_maya": +1e-4, "sub_00001": -1e-4}   # accept_rate_change: Spark vs pandas rounding at a 4-decimal edge
GOLD = f"{ice.CATALOG}.{ice.GOLD_TABLE}"
QUIET = {"log": lambda *_: None}


def _contract_module():
    mspec = importlib.util.spec_from_file_location("check_graph_contract_iceberg",
                                                   REPO / "scripts/check_graph_contract.py")
    mod = importlib.util.module_from_spec(mspec)
    mspec.loader.exec_module(mod)
    return mod


C = _contract_module()
REAL = {name: getattr(ice, name) for name in ("open_catalog", "catalog_schema", "resolve_publish", "read_inputs",
                                              "read_twin")}


# --------------------------------------------------------------------------- the fake lakehouse
class _Engine:
    def dispose(self) -> None:
        pass


class _Catalog:
    properties = {"uri": LAKE_URI, "warehouse": LAKE_WAREHOUSE}
    engine = _Engine()


class FakeLake:
    """One publish of an in-memory lakehouse: silver + gold frames, every table pinned by a snapshot id
    (``epoch`` numbers the publish). ``reachable`` False: the catalog cannot be opened; ``moved``: a
    tag no longer points at the snapshot the build pinned."""

    def __init__(self, silver: dict, gold: pd.DataFrame, *, epoch: int = 1, tag: str = TAG):
        self.silver, self.gold, self.epoch, self.tag = silver, gold, epoch, tag
        self.reachable, self.moved = True, False

    def _tables(self) -> dict[str, dict]:
        tables = build.build_tables(self.silver, self.gold, self.today, self.consts)
        counts = build.graph_counts(tables)
        rows = {f"{ice.CATALOG}.{t}": len(self.silver[k]) for k, (t, _) in ice.SILVER.items()}
        rows[GOLD] = len(self.gold)
        out = {t: {"role": "input", "rows": n, "parts": None} for t, n in rows.items()}
        out[f"{ice.CATALOG}.{ice.NODES_TABLE}"] = {"role": "output", "rows": counts["total_nodes"],
                                                    "parts": counts["nodes"]}
        out[f"{ice.CATALOG}.{ice.EDGES_TABLE}"] = {"role": "output", "rows": counts["total_edges"],
                                                    "parts": counts["edges"]}
        out[f"{ice.CATALOG}.{ice.SCALER_TABLE}"] = {"role": "output", "rows": len(spec.FEATURES), "parts": None}
        for i, t in enumerate(sorted(out)):
            out[t]["snapshot_id"] = 7_000_000 + 1_000 * self.epoch + i
        return out

    @property
    def today(self):
        return self.silver["snapshots"]["snapshot_date"].max()

    @property
    def consts(self) -> dict:
        return build.gold_constants(build.load_gold_twin(TINY))

    def publish(self) -> ice.Publish:
        return ice.Publish(build_id=self.tag[len("graph_"):], tag=self.tag, tables=self._tables(),
                           info={"spec": {"graph": spec.GRAPH_SPEC_VERSION, "similar_to": spec.SIMILAR_TO_SPEC_VERSION},
                                 "scaler_kind": "sql", "code_sha256": {}, "spark_version": "3.5.3 (fake)",
                                 "spark_app_id": "local-fake", "published_at": "2026-10-01 00:00:00"},
                           manifest={"table": f"{ice.CATALOG}.{ice.MANIFEST_TABLE}", "tag": self.tag})

    def prov(self, pub: ice.Publish, table: str, role: str) -> dict:
        pin = pub.pin(table)
        return {"table": table, "table_uuid": str(uuid.uuid5(uuid.NAMESPACE_URL, table)), "tag": pub.tag,
                "snapshot_id": pin["snapshot_id"], "sequence_number": self.epoch, "timestamp_ms": 0,
                "rows": pin["rows"], "operation": "append", "spark_app_id": "local-fake", "role": role}


class Holder:
    lake: FakeLake | None = None


def install(mp, holder: Holder) -> None:
    """Point iceberg_source's catalog side at ``holder.lake`` (the rest of the module stays real)."""
    def open_catalog(uri=None, warehouse=None, **props):
        if not holder.lake.reachable:
            raise ice.ProvenanceUnavailable("cannot reach the Iceberg catalog at fake: connection refused")
        return _Catalog()

    def resolve_publish(catalog, tag=None):
        if tag is not None and tag != holder.lake.tag:
            raise ice.ProvenanceUnavailable(f"tag {tag} is not on gold.graph_build_manifest")
        return holder.lake.publish()

    def read_inputs(catalog, pub):
        lake = holder.lake
        if lake.moved:
            raise ice.ProvenanceUnavailable(f"tag {pub.tag} on gold.churn_renewal_features points at snapshot 1, but "
                                            f"the build pins {pub.pin(GOLD)['snapshot_id']}")
        silver = {k: v.copy() for k, v in lake.silver.items()}
        prov = [lake.prov(pub, f"{ice.CATALOG}.{t}", "input") for t, _ in ice.SILVER.values()]
        return silver, lake.gold.copy(), [*prov, lake.prov(pub, GOLD, "input")]

    def read_twin(catalog, pub):
        lake = holder.lake
        tables = build.build_tables(lake.silver, lake.gold, lake.today, lake.consts)
        outs = (ice.NODES_TABLE, ice.EDGES_TABLE, ice.SCALER_TABLE)
        return (ice.local_arrow(tables), tables["similar_to_scaler"][list(spec.SCALER_SCHEMA.names)],
                [lake.prov(pub, f"{ice.CATALOG}.{t}", "output") for t in outs])

    for name, fn in (("open_catalog", open_catalog), ("catalog_schema", lambda catalog: []),
                     ("resolve_publish", resolve_publish), ("read_inputs", read_inputs), ("read_twin", read_twin)):
        mp.setattr(ice, name, fn)


def check(bdir: Path, *flags: str) -> tuple[int, str, dict | None]:
    """The graph contract, in process (the fake lakehouse is visible to it): (exit code, output, record)."""
    out = io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        rc = C.main(["--build", str(bdir), *flags])
    return rc, out.getvalue(), store.read_contract(bdir)


def flagged(out: str) -> list[str]:
    """The contract's warning and failure lines ('  WARN  ...', '  FAIL  ...', and the 'WARN: ...' /
    'Graph contract FAILED' summary on stderr). Notes and info are not flags, whatever words they hold
    (the builder RSS note of an in-process build, say)."""
    return [line for line in out.splitlines()
            if line.startswith(("  WARN ", "  FAIL ", "WARN:", "Graph contract FAILED"))]


def drifted(gold: pd.DataFrame, cells: dict, column: str = "accept_rate_change") -> pd.DataFrame:
    g = gold.copy()
    for uid, delta in cells.items():
        g.loc[g["user_id"] == uid, column] += delta
    return g


def build_from(holder: Holder, lake: FakeLake, root: Path, exports: Path, profile: str = "default"):
    holder.lake = lake
    return ice.build_from_iceberg(profile, root, sample_dir=TINY, export_dir=exports, **QUIET)


# --------------------------------------------------------------------------- fixtures
@pytest.fixture(scope="module")
def twin() -> tuple[dict, pd.DataFrame]:
    """The pandas twin's silver and gold of the tiny fixture (what a faithful lakehouse would hold)."""
    silver, gold, _ = build.run_gold(build.load_gold_twin(TINY))
    return silver, gold


@pytest.fixture(scope="module")
def exports(tiny_build, graph_root) -> Path:
    return spec.export_dir("tiny", graph_root)     # the pandas exports of the tiny bronze (conftest made them)


@pytest.fixture(scope="module")
def drift_build(twin, exports, tmp_path_factory) -> dict:
    """A default-profile build from a lakehouse whose gold drifted in two cells, checked with --strict."""
    root = tmp_path_factory.mktemp("iceberg_drift")
    holder = Holder()
    with pytest.MonkeyPatch.context() as mp:
        install(mp, holder)
        bdir, man = build_from(holder, FakeLake(twin[0], drifted(twin[1], DRIFT)), root, exports)
        rc, out, rec = check(bdir, "--strict")
    return {"root": root, "bdir": bdir, "man": man, "rc": rc, "out": out, "rec": rec}


@pytest.fixture
def fake(monkeypatch) -> Holder:
    holder = Holder()
    install(monkeypatch, holder)
    return holder


def _copy(bdir: Path, root: Path) -> Path:
    dst = spec.builds_dir("default", root) / bdir.name
    shutil.copytree(bdir, dst)
    return dst


# --------------------------------------------------------------------------- gold content
def test_gold_content_is_the_gold_a_build_carries_and_a_drift_is_named_cell_by_cell(twin):
    silver, gold = twin
    today = silver["snapshots"]["snapshot_date"].max()
    consts = build.gold_constants(build.load_gold_twin(TINY))
    tables = build.build_tables(silver, gold, today, consts)
    mine, theirs = oracle.gold_content(tables["Renewal"]), oracle.twin_gold_content(gold)
    assert list(mine.columns) == list(oracle.GOLD_CONTENT) and len(mine) == 121
    d = oracle.gold_drift(mine, theirs)
    assert d["cells"] == 0 and d["sha256"][0] == d["sha256"][1] and not (d["only_build"] or d["only_twin"])
    # the frame build_tables() reads back from a build's gold content rebuilds the same Renewal table
    again = build.build_tables(silver, oracle.gold_frame(mine), today, consts)
    pd.testing.assert_frame_equal(again["Renewal"], tables["Renewal"], check_dtype=False)   # text: object vs str
    schema = spec.NODE_SCHEMA["Renewal"].schema
    assert build.to_arrow(again["Renewal"], schema).equals(build.to_arrow(tables["Renewal"], schema))
    # two float cells off by 1e-4 and a label: exactly those cells, floats bit for bit
    g2 = drifted(gold, DRIFT)
    g2.loc[g2["user_id"] == "sub_00002", "route"] = "dunning"
    d = oracle.gold_drift(oracle.twin_gold_content(g2), theirs)
    assert d["columns"] == {"accept_rate_change": 2, "route": 1} and d["cells"] == 3
    assert [(c["subscription_id"], c["column"]) for c in d["cell_list"]] == [
        ("sub_00001", "accept_rate_change"), ("sub_00002", "route"), ("sub_maya", "accept_rate_change")]
    assert d["sha256"][0] != d["sha256"][1]
    one_ulp = gold.copy()
    one_ulp["engagement_trend"] = one_ulp["engagement_trend"].astype(float)
    i = one_ulp.index[0]
    one_ulp.at[i, "engagement_trend"] = float(pd.Series([one_ulp.at[i, "engagement_trend"]]).to_numpy()[0] + 1e-15)
    assert oracle.gold_drift(oracle.twin_gold_content(one_ulp), theirs)["columns"] == {"engagement_trend": 1}
    # rows on one side only
    d = oracle.gold_drift(oracle.twin_gold_content(gold.iloc[1:]), theirs)
    assert d["only_twin"] == [gold["user_id"].iloc[0]] and d["cells"] == 0


# --------------------------------------------------------------------------- the strict pass
def test_a_drifted_iceberg_build_passes_strict_with_the_drift_as_info(drift_build):
    bdir, man, out, rec = drift_build["bdir"], drift_build["man"], drift_build["out"], drift_build["rec"]
    icb = man["iceberg"]
    assert man["source"] == "iceberg" and icb["identity"] == "iceberg inputs" and icb["tag"] == TAG
    assert icb["local_path"]["byte_identical"] is False and icb["twin"]["ok"]
    assert man["business_build_id"] != mf.build_identity(TINY)["business_build_id"]   # the CSV identity calls it stale
    assert mf.is_fresh(man) and mf.current_identity(man)["business_build_id"] == man["business_build_id"]
    assert drift_build["rc"] == 0, out[-4000:]
    assert "Graph contract OK" in out and "golden tiny (derived); source Iceberg graph_0123456789ab; gold drift 2 " \
                                          "cell(s), info; strict" in out
    for phrase in ("source: Iceberg graph_0123456789ab", "11 inputs + 3 twin tables read by tag + snapshot id",
                   "sha256(Iceberg input pins, bronze, code, spec, versions, platform)",
                   "info  gold drift vs the pandas twin on data/sample/churn/fixtures/tiny: 2 cell(s) differ "
                   "(accept_rate_change 2)",
                   "info     sub_00001 accept_rate_change: 0.7138 in this build, 0.7139 in the pandas twin",
                   "info     sub_maya accept_rate_change: 0.8335 in this build, 0.8334 in the pandas twin",
                   "derived: the builder on data/sample/churn/fixtures/tiny's events",
                   "rebuilds this build byte for byte",
                   "golden value(s) of tiny.json moved with the drift (SIMILAR_TO-derived",
                   "no golden value outside the drift's reach differs",
                   "exactly the recorded gold drift cells", "sub_maya accept_rate_change",
                   "re-read the 11 inputs at graph_0123456789ab", "and rebuilt: byte-identical Parquet",
                   "the identity recomputed from the pins",
                   "PIT parity mismatches limit_hits_14d over 121 renewals = 0",
                   "unchanged by this check"):
        assert phrase in out, phrase
    assert flagged(out) == [], flagged(out)
    assert "2 within rounding, 0 beyond" in out and "pandas twin (rounding)" in out
    assert rec["status"] == "pass" and rec["strict"] and rec["warnings"] == [] and rec["errors"] == []
    assert rec["golden"] == "tiny" and rec["golden_mode"] == "derived"
    assert rec["source"] == {"kind": "iceberg", "tag": TAG, "identity": "iceberg inputs",
                             "lakehouse_build_id": TAG[len("graph_"):]}
    drift = rec["summary"]["gold_drift"]
    assert drift["cells"] == 2 and drift["columns"] == {"accept_rate_change": 2}
    assert {(c["subscription_id"], c["build"], c["twin"], c["kind"]) for c in drift["cell_list"]} == {
        ("sub_00001", 0.7138, 0.7139, "rounding"), ("sub_maya", 0.8335, 0.8334, "rounding")}
    assert drift["beyond_rounding"] == 0 and drift["max_abs_delta"]["accept_rate_change"] == pytest.approx(1e-4)
    assert any("gold drift vs the pandas twin" in i for i in rec["info"])
    assert rec["summary"]["export"]["explained_by_gold_drift"]
    assert rec["summary"]["iceberg_verify"] == {"tag": TAG, "byte_identical": True, "files_differ": [], "fresh": True}
    assert store.is_strict_pass(rec, mf.read_manifest(bdir))


def test_promote_accepts_the_strict_passing_iceberg_build(drift_build):
    root, bdir = drift_build["root"], drift_build["bdir"]
    assert drift_build["rc"] == 0
    assert store.promote(root=root) == bdir                  # fresh by its Iceberg identity, strict pass on record
    assert spec.current_link(root).resolve() == bdir.resolve()
    p = run_script("build_graph_local.py", "promote", "--graph-root", str(root))
    assert p.returncode == 0 and "stale" not in p.stdout, p.stdout + p.stderr


def test_freshness_follows_the_iceberg_pins(drift_build):
    man = drift_build["man"]
    assert mf.iceberg_pins(man) == {t: p["snapshot_id"] for t, p in man["iceberg"]["tables"].items()
                                    if p["role"] == "input"}
    pins = {**mf.iceberg_pins(man), GOLD: 1}
    moved = {**man, "iceberg": {**man["iceberg"], "identity_payload": {**man["iceberg"]["identity_payload"],
                                                                        "iceberg_inputs": pins}}}
    assert not mf.is_fresh(moved)                           # another gold snapshot is another build
    assert mf.iceberg_pins({**man, "iceberg": {**man["iceberg"], "identity_payload": None}}) is None
    bronze = {k: v for k, v in man.items() if k not in ("iceberg", "source")}
    assert not mf.is_fresh(bronze)                          # read as a CSV build, it is stale


@pytest.mark.slow
def test_an_iceberg_build_with_the_bronze_identity_applies_the_golden_exactly(fake, twin, exports, tmp_path):
    bdir, man = build_from(fake, FakeLake(*twin), tmp_path / "g", exports)
    assert man["iceberg"]["identity"].startswith("bronze") and man["iceberg"]["local_path"]["byte_identical"]
    assert man["business_build_id"] == mf.build_identity(TINY)["business_build_id"]
    rc, out, rec = check(bdir, "--strict")
    assert rc == 0, out[-4000:]
    assert "= the pandas twin's on data/sample/churn/fixtures/tiny" in out and "info  gold drift" not in out
    assert "golden tiny.json (bronze sha256 match) applies" in out and "oracle = golden tiny.json" in out
    assert "re-read the 11 inputs" in out and "rebuild from the same inputs + code" in out      # pins AND bronze
    assert rec["golden_mode"] == "exact" and rec["summary"]["gold_drift"]["cells"] == 0
    assert "golden tiny; source Iceberg graph_0123456789ab; strict" in out


# --------------------------------------------------------------------------- strict still fails
@pytest.mark.slow
def test_a_broken_invariant_fails_strict(fake, twin, exports, drift_build, tmp_path):
    """A PIT feature off by one in the lakehouse's gold is not a drift to wave through: parity breaks
    (in pandas and in Cypher), strict fails, nothing can be promoted. Edited Parquet fails too."""
    silver, gold = twin
    sub = "sub_00002"
    bad = drifted(drifted(gold, DRIFT), {sub: 1}, "limit_hits_14d")
    root = tmp_path / "g"
    bdir, _ = build_from(fake, FakeLake(silver, bad), root, exports)
    rc, out, rec = check(bdir, "--strict")
    assert rc == 1 and rec["status"] == "fail"
    assert "FAIL  #2 PIT parity mismatches limit_hits_14d (event_date <= as_of + window) = 0" in out
    assert "FAIL  #2 PIT parity mismatches limit_hits_14d over 121 renewals = 0" in out           # Cypher half
    assert f"{sub} limit_hits_14d:" in out and "limit_hits_14d 1" in out                           # still recorded
    with pytest.raises(RuntimeError, match="no strict contract pass"):
        store.promote(root=root)

    # the build's own bytes edited (one SIMILAR_TO rank pair swapped) with its manifest kept consistent
    edited = _copy(drift_build["bdir"], tmp_path / "e")
    rel = "parquet/" + spec.EDGE_SCHEMA["SIMILAR_TO"].file
    t = pq.read_table(edited / rel).to_pandas()
    i = t.index[(t["src"] == t["src"].iloc[0]) & (t["rank"] <= 2)]
    t.loc[i, "rank"] = t.loc[i, "rank"].to_numpy()[::-1]
    build.write_parquet(t, edited / rel, spec.EDGE_SCHEMA["SIMILAR_TO"].schema)
    man = mf.read_manifest(edited)
    man["files"][rel]["sha256"] = mf.sha256_file(edited / rel)
    mf.write_manifest(edited, man)
    fake.lake = FakeLake(silver, drifted(gold, DRIFT))
    rc, out, _ = check(edited, "--strict")
    assert rc == 1 and "FAIL  #6 rank order violations of (d2_q, dst) = 0" in out
    assert f"files differ: ['{rel}']" in out                 # the pins rebuild other bytes


@pytest.mark.slow
def test_a_cent_of_pit_feature_drift_is_beyond_rounding_as_parity_says(fake, twin, exports, tmp_path):
    """The verifier's drift_probe case 1 (p1-hardening-2 round 2): overage_usd_28d one cent off the twin's
    (Spark HALF_UP vs pandas round(2) at a half cent gives exactly this) is not rounding. Invariant #2
    recomputes the feature from the edges with atol 1e-9, in pandas and in Cypher, and fails; the gold-drift
    section now agrees with it (that cell is 'beyond rounding', named in the warning) instead of calling it
    harmless next to a failing parity check. Strict and plain checks fail, nothing can be promoted."""
    silver, gold = twin
    sub = gold.loc[gold["overage_usd_28d"] > 0, "user_id"].sort_values().iloc[0]
    bad = drifted(drifted(gold, DRIFT), {sub: 0.01}, "overage_usd_28d")
    root = tmp_path / "g"
    bdir, _ = build_from(fake, FakeLake(silver, bad), root, exports)
    rc, out, rec = check(bdir, "--strict")
    assert rc == 1 and rec["status"] == "fail", out[-3000:]
    assert "FAIL  #2 PIT parity mismatches overage_usd_28d (event_date <= as_of + window) = 0" in out
    assert "FAIL  #2 PIT parity mismatches overage_usd_28d over 121 renewals = 0" in out           # Cypher half
    drift = rec["summary"]["gold_drift"]
    kinds = {(c["subscription_id"], c["column"]): c["kind"] for c in drift["cell_list"]}
    assert kinds == {(sub, "overage_usd_28d"): "beyond rounding", ("sub_00001", "accept_rate_change"): "rounding",
                     ("sub_maya", "accept_rate_change"): "rounding"}, kinds
    assert drift["beyond_rounding_columns"] == {"overage_usd_28d": 1} and drift["beyond_rounding"] == 1
    assert f"{sub} overage_usd_28d:" in out and "pandas twin (beyond rounding)" in out
    assert any(f.startswith("  WARN  gold drift beyond rounding") and "overage_usd_28d 1" in f for f in flagged(out))
    assert not [line for line in out.splitlines() if "overage_usd_28d" in line and "(rounding)" in line]
    rc, out, rec = check(bdir)                                    # not a warning to wave through: #2 is an error
    assert rc == 1 and rec["status"] == "fail"
    with pytest.raises(RuntimeError, match="no strict contract pass"):
        store.promote(root=root)


def test_rounding_is_one_unit_in_the_last_decimal_the_twin_rounds_to(twin):
    """What counts as rounding (oracle.drift_kind): a rounded float feature at most one unit in the last
    decimal the pandas twin rounds it to (4; overage_usd_28d 2), checked against the twin's own gold.
    A label, route, churned, date or count that differs, a NaN on one side or more than one unit is not,
    and neither is any drift in a PIT feature: invariant #2 recomputes those from the edges exactly, so a
    cent of overage_usd_28d drift fails the contract and the drift record must not call it rounding."""
    _, gold = twin
    assert set(oracle.GOLD_FLOAT_DECIMALS) == set(spec.NUMERIC_FEATURES) - spec.INT_FEATURES
    for f, d in oracle.GOLD_FLOAT_DECIMALS.items():
        x = gold[f].astype(float).to_numpy()
        assert abs(x - x.round(d)).max() < 10.0 ** -(d + 6), f          # the twin rounds f to d decimals
        assert abs(x - x.round(d - 1)).max() > 10.0 ** -(d + 6), f      # ... and not to fewer
    k = oracle.drift_kind
    assert k("accept_rate_change", 0.7139 + 1e-4, 0.7139) == k("accept_rate_change", 0.7138, 0.7139) == "rounding"
    assert k("accept_rate_change", 0.7139 + 2e-4, 0.7139) == "beyond rounding"
    assert k("engagement_trend", 6.7143, 1.7143) == "beyond rounding"                     # the verifier's L4
    # the PIT features (invariant #2): overage_usd_28d is the one rounded float among them
    assert set(oracle.PIT_FEATURES) & set(oracle.GOLD_FLOAT_DECIMALS) == {"overage_usd_28d"}
    assert k("overage_usd_28d", 12.35, 12.34) == k("overage_usd_28d", 114.23, 114.22) == "beyond rounding"
    assert k("overage_usd_28d", 12.36, 12.34) == "beyond rounding"
    for f in oracle.PIT_FEATURES:
        assert k(f, 1.0, 1.0 + 1e-12) == "beyond rounding", f
    for column, a, b in (("limit_hits_14d", 4, 3), ("churned", 1, 0), ("outcome", "voluntary_lapse", "renewed"),
                         ("route", "dunning", "model"), ("as_of", "2026-07-20", "2026-07-21"),
                         ("engagement_trend", float("nan"), 1.0), ("engagement_trend", None, 1.0)):
        assert k(column, a, b) == "beyond rounding", column
    g2 = drifted(gold, DRIFT)
    g2.loc[g2["user_id"] == "sub_00002", "route"] = "dunning"
    d = oracle.gold_drift(oracle.twin_gold_content(g2), oracle.twin_gold_content(gold))
    assert d["beyond_rounding"] == 1 and d["beyond_rounding_columns"] == {"route": 1}
    assert [(c["subscription_id"], c["kind"]) for c in d["cell_list"]] == [
        ("sub_00001", "rounding"), ("sub_00002", "beyond rounding"), ("sub_maya", "rounding")]
    assert d["beyond_rounding_list"] == [c for c in d["cell_list"] if c["kind"] != "rounding"]


def _flip_a_label(gold: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """The verifier's L1: a renewed model renewal turned into a voluntary lapse (outcome and churned)."""
    g = gold.copy()
    sub = g.loc[(g["route"] == "model") & (g["outcome"] == "renewed"), "user_id"].sort_values().iloc[0]
    g.loc[g["user_id"] == sub, ["outcome", "churned"]] = ["voluntary_lapse", 1]
    return g, {"outcome": 1, "churned": 1}


def _shift_a_feature(gold: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """The verifier's L4: engagement_trend (a SIMILAR_TO feature no PIT invariant recomputes) +5.0 on 30 rows."""
    g = gold.copy()
    subs = g["user_id"].sort_values().iloc[:30]
    g.loc[g["user_id"].isin(subs), "engagement_trend"] += 5.0
    return g, {"engagement_trend": 30}


@pytest.mark.slow
@pytest.mark.parametrize("change", [_flip_a_label, _shift_a_feature], ids=["label_flip", "feature_shift"])
def test_a_gold_drift_beyond_rounding_fails_strict(fake, twin, exports, tmp_path, change):
    """A lakehouse whose gold flips a label or shifts a feature by far more than rounding broke no
    structural invariant (no rule recomputes the label or engagement_trend from the graph), but it is
    not the gold the graph is specified on: beside the two rounding cells (info), the cells beyond
    rounding are a warning that names them, strict fails and nothing can be promoted."""
    silver, gold = twin
    bad, want = change(drifted(gold, DRIFT))
    root = tmp_path / "g"
    bdir, _ = build_from(fake, FakeLake(silver, bad), root, exports)
    rc, out, rec = check(bdir, "--strict")
    assert rc == 1 and rec["status"] == "fail", out[-3000:]
    flags = flagged(out)
    assert any(f.startswith("  WARN  gold drift beyond rounding vs the pandas twin on data/sample/churn/fixtures/"
                            "tiny: ") for f in flags), flags
    assert all("gold drift beyond rounding" in f or "fatal with --strict" in f or f.startswith("Graph contract FAILED")
               for f in flags), flags                                       # nothing else broke
    drift = rec["summary"]["gold_drift"]
    assert drift["beyond_rounding_columns"] == want and drift["beyond_rounding"] == sum(want.values())
    assert "pandas twin (rounding)" in out and "pandas twin (beyond rounding)" in out
    assert "Graph contract OK" not in out
    with pytest.raises(RuntimeError, match="no strict contract pass"):
        store.promote(root=root)
    rc, out, rec = check(bdir)                                              # a plain check passes, with the warning
    assert rc == 0 and len(rec["warnings"]) == 1 and "beyond rounding (warned)" in out


@pytest.mark.slow
def test_the_builder_rss_of_an_in_process_build_is_a_note_not_a_warning(fake, twin, drift_build, tmp_path):
    """build_from_iceberg ran inside pytest here, so the manifest's builder max RSS is pytest's peak so
    far (ru_maxrss), late in a full run above the 512 MiB soft limit. That is reported as a note, which
    --strict never counts; the strict pass stands."""
    fake.lake = FakeLake(twin[0], drifted(twin[1], DRIFT))
    big = _copy(drift_build["bdir"], tmp_path / "rss")
    man = mf.read_manifest(big)
    man["builder"] = {**man["builder"], "max_rss_bytes": 900 * 2**20, "max_rss_mib": 900.0, "rss_over_soft_limit": True}
    mf.write_manifest(big, man)
    rc, out, rec = check(big, "--strict")
    assert rc == 0 and flagged(out) == [] and rec["warnings"] == [], flagged(out)
    assert "note  above the soft limit (reported, not gated): builder max RSS 900.0 MiB > 512 MiB" in out
    assert "the host's peak for an in-process build" in out


@pytest.mark.slow
def test_events_that_are_not_the_bronzes_fail_strict(fake, twin, exports, tmp_path):
    """The lakehouse's silver lost a ticket the bronze has (dated after its renewal's as_of, so no gold
    feature moves): the gold drift no longer explains the build, the golden counts move outside
    what a feature drift can move, strict fails."""
    silver, gold = twin
    asof = gold.set_index("user_id")["feature_as_of"].map(pd.Timestamp)
    tk = silver["tickets"]
    late = tk[tk["created_date"] > tk["subscription_id"].map(asof)]
    assert len(late)
    lost = {**silver, "tickets": tk.drop(index=late.index[0]).reset_index(drop=True)}
    bdir, _ = build_from(fake, FakeLake(lost, drifted(gold, DRIFT)), tmp_path / "g", exports)
    rc, out, _ = check(bdir, "--strict")
    assert rc == 1
    assert "WARN  the build is not the builder's graph of data/sample/churn/fixtures/tiny's events" in out
    assert "golden value(s) of tiny.json differ that the drift cannot move (only SIMILAR_TO features drifted: " \
           "accept_rate_change)" in out and "counts.nodes.Ticket: golden 33 != 32" in out


@pytest.mark.slow
def test_pins_that_no_longer_hold_fail_and_an_unreachable_catalog_warns(fake, twin, drift_build, tmp_path):
    silver, gold = twin
    lake = FakeLake(silver, drifted(gold, DRIFT))
    fake.lake = lake
    # a tag moved since the build: provenance unavailable, an error with or without --strict
    lake.moved = True
    rc, out, _ = check(_copy(drift_build["bdir"], tmp_path / "moved"))
    assert rc == 1 and "FAIL  the Iceberg pins of this build no longer hold: provenance unavailable: tag " \
                       "graph_0123456789ab on gold.churn_renewal_features points at snapshot 1" in out
    lake.moved = False
    # other data under the same pins: the rebuild gives other bytes
    fake.lake = FakeLake(silver, drifted(gold, {**DRIFT, "sub_00003": 1e-4}))
    rc, out, _ = check(_copy(drift_build["bdir"], tmp_path / "changed"))
    assert rc == 1 and "re-read the 11 inputs at graph_0123456789ab" in out and "files differ: [" in out
    fake.lake = lake
    # recorded pins that are not the snapshots the build read: inconsistent provenance (and stale)
    odd = _copy(drift_build["bdir"], tmp_path / "odd")
    man = mf.read_manifest(odd)
    man["iceberg"]["identity_payload"]["iceberg_inputs"][GOLD] += 1
    mf.write_manifest(odd, man)
    rc, out, _ = check(odd)
    assert rc == 1 and "the identity pins are not the snapshot ids of the input tables read" in out
    assert "FAIL  the recorded identity payload (iceberg.identity_payload) does not hash to business_build_id" in out
    assert "WARN  stale build" in out
    # no reachable catalog: the determinism check cannot run, a warning (strict fails, a plain check passes)
    lake.reachable = False
    away = _copy(drift_build["bdir"], tmp_path / "away")
    rc, out, rec = check(away, "--strict")
    assert rc == 1 and "WARN  Iceberg determinism check skipped: provenance unavailable: cannot reach" in out
    assert rec["status"] == "fail" and "fatal with --strict" in out
    rc, out, rec = check(away)
    assert rc == 0 and rec["status"] == "pass" and not rec["strict"]


def test_without_pyiceberg_or_a_catalog_the_iceberg_determinism_check_warns(drift_build, tmp_path, monkeypatch):
    """The real catalog side: in the core venv pyiceberg is absent; with it, the SQLite catalog the
    manifest recorded does not exist here. Either way the pins cannot be re-read: a warning."""
    for name, fn in REAL.items():
        monkeypatch.setattr(ice, name, fn)
    away = _copy(drift_build["bdir"], tmp_path / "g")
    rc, out, _ = check(away, "--strict", "--no-ladybug")
    assert rc == 1 and "WARN  Iceberg determinism check skipped: " in out
    assert ("pyiceberg is not installed here" in out) != ("SQLite catalog /nonexistent/lakehouse/catalog.db does not "
                                                          "exist" in out)


def test_iceberg_provenance_must_be_complete(drift_build, tmp_path):
    rep = C.Report()
    man = mf.read_manifest(drift_build["bdir"])
    with contextlib.redirect_stdout(io.StringIO()):
        C.check_iceberg_provenance(rep, man)
        assert rep.errors == []
        tables = dict(man["iceberg"]["tables"])
        tables.pop(GOLD)
        C.check_iceberg_provenance(rep, {**man, "iceberg": {**man["iceberg"], "tables": tables, "tag": "main"}})
        C.check_iceberg_provenance(rep, {**man, "iceberg": {**man["iceberg"], "identity": "bronze (byte-identical "
                                                                                          "to the local path)"}})
        C.check_iceberg_provenance(rep, {**man, "iceberg": {**man["iceberg"], "twin": {"ok": False}}})
    assert len(rep.errors) == 3
    assert "tag 'main' is not graph_<12 hex>" in rep.errors[0]
    assert "lakehouse.gold.churn_renewal_features" in rep.errors[0]
    assert "with identity pins" in rep.errors[1] and "twin check" in rep.errors[2]


# --------------------------------------------------------------------------- a real local Iceberg build
def _load_parity():
    mspec = importlib.util.spec_from_file_location("check_graph_parity_harness_c",
                                                   REPO / "scripts/check_graph_parity.py")
    mod = importlib.util.module_from_spec(mspec)
    prev, sys.dont_write_bytecode = sys.dont_write_bytecode, True
    try:
        mspec.loader.exec_module(mod)
    finally:
        sys.dont_write_bytecode = prev
    return mod


def _spark_skip() -> str | None:
    if importlib.util.find_spec("pyiceberg") is None:
        return "pyiceberg is in requirements-graph-spark*.txt (.venv-graph-spark)"
    return _load_parity().skip_reason(iceberg=True)


SPARK_SKIP = _spark_skip()


@pytest.mark.slow
@pytest.mark.skipif(bool(SPARK_SKIP) and os.environ.get("GRAPH_REQUIRE_SPARK") != "1",
                    reason=f"spark harness unavailable: {SPARK_SKIP}")
def test_a_real_local_iceberg_build_passes_strict_promotes_and_fails_when_broken(exports, tmp_path):
    """PySpark 3.5 + Iceberg JdbcCatalog(SQLite) `lakehouse` + file warehouse: the tiny medallion, then
    gold drifted in two cells (UPDATE, as Spark vs pandas rounding would), the graph job publishes and
    tags it, build_from_iceberg reads it by tag + snapshot: the CLI contract (re-reading the SQLite
    catalog the manifest recorded) passes --strict with the drift as info and promote takes the build.
    Then a PIT feature broken in the lakehouse fails strict, and a moved tag fails the first build."""
    if SPARK_SKIP:
        pytest.fail(f"GRAPH_REQUIRE_SPARK=1 but the spark harness is unavailable: {SPARK_SKIP}")
    h = _load_parity()
    lake = tmp_path / "lake"
    spark = h.build_session(lake / "spark", lake / "catalog.db", lake / "warehouse",
                            app_name="pytest_graph_contract_iceberg", driver_memory="1g")
    uri, wh = f"sqlite:///{lake / 'catalog.db'}", f"file://{lake / 'warehouse'}"
    try:
        h.publish_local(spark, TINY)
        ids = ", ".join(f"'{u}'" for u in DRIFT)
        spark.sql(f"UPDATE lakehouse.gold.churn_renewal_features SET accept_rate_change = accept_rate_change + 0.0001 "
                  f"WHERE user_id IN ({ids})")
        drifted_pub = h.graph_job().publish(spark)
        assert drifted_pub["status"] == "published"
        root = tmp_path / "g"
        bdir, man = ice.build_from_iceberg("default", root, catalog_uri=uri, warehouse=wh, sample_dir=TINY,
                                           export_dir=exports, **QUIET)
        assert man["iceberg"]["identity"] == "iceberg inputs" and man["iceberg"]["tag"] == drifted_pub["tag"]
        p = run_script("check_graph_contract.py", "--build", str(bdir), "--strict")
        out = p.stdout + p.stderr
        assert p.returncode == 0, out[-4000:]
        assert "info  gold drift vs the pandas twin on data/sample/churn/fixtures/tiny: 2 cell(s) differ " \
               "(accept_rate_change 2)" in out and "golden tiny (derived)" in out
        assert f"re-read the 11 inputs at {drifted_pub['tag']}" in out and flagged(out) == [], flagged(out)
        assert store.promote(root=root) == bdir

        spark.sql("UPDATE lakehouse.gold.churn_renewal_features SET limit_hits_14d = limit_hits_14d + 1 "
                  "WHERE user_id = 'sub_00002'")
        broken = h.graph_job().publish(spark)
        bdir2, _ = ice.build_from_iceberg("default", tmp_path / "g2", catalog_uri=uri, warehouse=wh, sample_dir=TINY,
                                          export_dir=exports, **QUIET)
        p = run_script("check_graph_contract.py", "--build", str(bdir2), "--strict")
        assert p.returncode == 1 and "FAIL  #2 PIT parity mismatches limit_hits_14d" in p.stdout, p.stdout[-3000:]
        assert broken["tag"] in p.stdout

        newest = int(spark.sql("SELECT snapshot_id FROM lakehouse.gold.churn_renewal_features.refs "
                               "WHERE name = 'main'").collect()[0]["snapshot_id"])
        spark.sql(f"ALTER TABLE lakehouse.gold.churn_renewal_features REPLACE TAG {drifted_pub['tag']} "
                  f"AS OF VERSION {newest}")
        p = run_script("check_graph_contract.py", "--build", str(bdir), "--strict")
        assert p.returncode == 1 and "the Iceberg pins of this build no longer hold: provenance unavailable: tag " \
                                     f"{drifted_pub['tag']} on gold.churn_renewal_features points at snapshot" \
                                     in p.stdout, p.stdout[-3000:]
    finally:
        spark.stop()
