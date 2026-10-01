"""The four lineage tools: the binding interface, input validation, deterministic ordered output.

  lineage_trace(ctx, target, direction="upstream", max_depth=6)
  lineage_pit(ctx, feature=None)
  lineage_guards(ctx, column=None)
  lineage_unused(ctx, layer="silver", domain="churn")      each -> (data: dict, caveats: list[str])
"""
from __future__ import annotations

import inspect
import json
import types

import pytest
from test_lineage_build import core_build  # noqa: F401 (session fixture shared by the lineage tests)

from lakehouse_graph import spec as gspec
from lakehouse_graph import store
from lakehouse_graph.lineage import oracle, spec, tools

GOLD = "gold.churn_renewal_features."
BRONZE_HIT_AT = "bronze.churn_limit_events_raw.hit_at"


@pytest.fixture(scope="module")
def ctx(core_build):  # noqa: F811 (the imported fixture)
    with tools.LineageContext(core_build[0]) as c:
        yield c


# --------------------------------------------------------------------------- interface
def test_signatures_are_the_binding_interface():
    def params(fn):
        return [(p.name, p.default) for p in inspect.signature(fn).parameters.values()]

    assert params(tools.lineage_trace) == [("ctx", inspect.Parameter.empty), ("target", inspect.Parameter.empty),
                                           ("direction", "upstream"), ("max_depth", 6)]
    assert params(tools.lineage_pit) == [("ctx", inspect.Parameter.empty), ("feature", None)]
    assert params(tools.lineage_guards) == [("ctx", inspect.Parameter.empty), ("column", None)]
    assert params(tools.lineage_unused) == [("ctx", inspect.Parameter.empty), ("layer", "silver"),
                                            ("domain", "churn")]
    assert set(tools.TOOLS) == {"lineage_trace", "lineage_pit", "lineage_guards", "lineage_unused"}


def test_every_tool_returns_data_and_caveats_that_are_plain_json(ctx):
    for _key, tool, args in spec.QUESTIONS + spec.EXTRA_QUESTIONS:
        out = tools.TOOLS[tool](ctx, **args)
        assert isinstance(out, tuple) and len(out) == 2
        data, caveats = out
        assert isinstance(data, dict) and isinstance(caveats, list) and all(isinstance(c, str) for c in caveats)
        assert json.loads(json.dumps(data)) == data       # no dates, sets or numpy scalars
        assert tools.TOOLS[tool](ctx, **args) == (data, caveats)   # deterministic


def test_context_is_read_only_capped_and_tools_work_with_any_object_exposing_the_two_attributes(core_build):  # noqa: F811
    with tools.LineageContext(core_build[0]) as c:
        assert c.buffer_pool_mb == store.SERVE_BUFFER_POOL_MB == 128
        with pytest.raises(RuntimeError):
            c.lineage_conn.execute("CREATE (:`Job` {id: 'job:nope'})")
        duck = types.SimpleNamespace(lineage_conn=c.lineage_conn, build_dir=c.build_dir)
        assert tools.lineage_unused(duck)[0]["summary"]["unused"] == 9


def test_missing_lineage_database_is_a_clear_error(tmp_path):
    with tools.LineageContext(tmp_path) as c:
        assert c.lineage_conn is None
        with pytest.raises(tools.LineageUnavailable, match="make lineage-local"):
            tools.lineage_pit(c)
    with pytest.raises(tools.LineageUnavailable):
        tools.lineage_trace(types.SimpleNamespace(build_dir=tmp_path), BRONZE_HIT_AT)


# --------------------------------------------------------------------------- validation
@pytest.mark.parametrize("bad", [
    "limit_hits_14d", "gold.limit_hits_14d", "Gold.churn_renewal_features.limit_hits_14d",
    "gold.churn_renewal_features.limit_hits_14d; MATCH (n) DETACH DELETE n", "graph.nodes_Renewal.renewal_id",
    "gold.churn_renewal_features.Limit_Hits", "gold..x", "", None, 42, ["gold.a.b"], "gold.churn renewal.x",
    "silver.churn_limit_events.hit_at' OR '1'='1", "gold.churn_renewal_features.limit_hits_14d\n",
    "\ngold.churn_renewal_features.limit_hits_14d", " gold.churn_renewal_features.limit_hits_14d"])
def test_malformed_column_ref_is_rejected_before_any_query(ctx, bad):
    with pytest.raises(ValueError, match="invalid column reference"):
        tools.lineage_trace(ctx, bad)
    if bad not in ("", None):   # "" / None mean "no column" for lineage_guards (the unguarded list)
        with pytest.raises(ValueError, match="invalid column reference"):
            tools.lineage_guards(ctx, bad)


def test_unknown_column_ref_lists_close_matches(ctx):
    with pytest.raises(ValueError) as e:
        tools.lineage_trace(ctx, GOLD + "limit_hits_14")
    assert "unknown column" in str(e.value) and GOLD + "limit_hits_14d" in str(e.value)
    with pytest.raises(ValueError) as e:
        tools.lineage_guards(ctx, "silver.churn_limit_events.nothing_like_it")
    assert "silver.churn_limit_events.hit_date" in str(e.value)       # the table's own columns
    with pytest.raises(ValueError) as e:
        tools.lineage_trace(ctx, "gold.no_such_table.no_such_column")
    assert "Known tables" in str(e.value) or "Close matches" in str(e.value)


def test_unknown_feature_is_rejected_with_a_suggestion(ctx):
    with pytest.raises(ValueError) as e:
        tools.lineage_pit(ctx, "limit_hits")
    assert "unknown feature" in str(e.value) and "Did you mean limit_hits_14d" in str(e.value)
    for bad in ("churned", "city", "user_id", GOLD + "limit_hits_14d", 7):
        with pytest.raises(ValueError, match="unknown feature"):
            tools.lineage_pit(ctx, bad)


@pytest.mark.parametrize("bad", [0, 7, -1, 100, True, False, 2.0, "3", "six"])
def test_max_depth_bounds(ctx, bad):
    with pytest.raises(ValueError, match="max_depth"):
        tools.lineage_trace(ctx, BRONZE_HIT_AT, "downstream", bad)


def test_direction_layer_and_domain_are_closed_lists(ctx):
    with pytest.raises(ValueError, match="invalid direction 'sideways'"):
        tools.lineage_trace(ctx, BRONZE_HIT_AT, "sideways")
    with pytest.raises(ValueError, match="Did you mean downstream"):
        tools.lineage_trace(ctx, BRONZE_HIT_AT, "downstrem")
    for kwargs in ({"layer": "gold"}, {"layer": "source"}, {"domain": "graph"}, {"layer": "nil"}):
        with pytest.raises(ValueError, match="invalid (layer|domain)"):
            tools.lineage_unused(ctx, **kwargs)
    # no "close match" for something that is not a string (str(3) / str(None) looked close to "bronze")
    for kwargs in ({"layer": 3}, {"domain": ["churn"]}, {"layer": ["None"]}):
        with pytest.raises(ValueError) as e:
            tools.lineage_unused(ctx, **kwargs)
        assert "expected one of" in str(e.value) and "Did you mean" not in str(e.value), str(e.value)
    with pytest.raises(ValueError, match="Did you mean bronze"):
        tools.lineage_unused(ctx, layer="bronz")
    with pytest.raises(ValueError) as e:
        tools.lineage_pit(ctx, 5)
    assert "unknown feature 5" in str(e.value) and "Did you mean" not in str(e.value)


def test_junk_optional_arguments_mean_absent(ctx):
    """None, "", "null", "None": the argument was left out, so the documented default applies (never an
    error, and never a "Did you mean bronze?" for the string "None")."""
    unused, trace = tools.lineage_unused(ctx), tools.lineage_trace(ctx, BRONZE_HIT_AT)
    down = tools.lineage_trace(ctx, BRONZE_HIT_AT, "downstream")
    assert unused[0]["layer"] == "silver" and unused[0]["domain"] == "churn" and trace[0]["direction"] == "upstream"
    for junk in (None, "", "null", "None", " NULL "):
        assert tools.lineage_pit(ctx, junk) == tools.lineage_pit(ctx)
        assert tools.lineage_guards(ctx, junk) == tools.lineage_guards(ctx)
        assert tools.lineage_unused(ctx, layer=junk) == tools.lineage_unused(ctx, domain=junk) == unused
        assert tools.lineage_unused(ctx, junk, junk) == unused
        assert tools.lineage_trace(ctx, BRONZE_HIT_AT, direction=junk) == trace
        assert tools.lineage_trace(ctx, BRONZE_HIT_AT, "downstream", max_depth=junk) == down


# --------------------------------------------------------------------------- lineage_trace
def test_q15_downstream_from_bronze_hit_at_has_the_two_branches(ctx):
    data, caveats = tools.lineage_trace(ctx, BRONZE_HIT_AT, direction="downstream")
    assert data["target"] == BRONZE_HIT_AT and data["direction"] == "downstream" and data["max_depth"] == 6
    dead, live = data["summary"]["branches"]
    assert dead == {"via": "silver.churn_limit_events.hit_at", "transform": "IDENTITY", "gold": [], "exports": [],
                    "graph_elements": ["ge:edge:HIT_LIMIT", "ge:node:LimitHit"], "dead_end": True}
    assert live["via"] == "silver.churn_limit_events.hit_date" and "to_date" in live["transform"]
    assert live["gold"] == [GOLD + "limit_hits_14d"] and live["dead_end"] is False
    assert live["exports"] == [f"export.{name}.limit_hits_14d" for name in (
        "churn_renewals_audit", "churn_user_features", "hero_inference_record")]
    hop = next(e for e in data["edges"] if e["to"] == GOLD + "limit_hits_14d")
    assert (hop["depth"], hop["rel"], hop["from"], hop["roles"], hop["cte"], hop["window"]) == \
        (2, "DERIVED_FROM", "silver.churn_limit_events.hit_date", "WINDOW_BOUND", "hits", "(as_of-14, as_of]")
    r = data["reached"]
    assert r["columns"]["gold"] == [GOLD + "limit_hits_14d"] and len(r["columns"]["export"]) == 3
    assert {"contract:gold_parity", "contract:churn_export", "contract:graph_contract"} <= set(r["contracts"])
    assert {"assert:gold_parity#values", "assert:churn_export#range:limit_hits_14d"} <= set(r["assertions"])
    assert r["consumers"] == ["repo:github.com/santoshshinde2012/retention-radar"]      # the radar interface
    assert any("Dead end" in c and "silver.churn_limit_events.hit_at" in c for c in caveats)
    assert any("retention-radar contract is not in this build" in c for c in caveats)
    assert data["summary"]["stopped_at_max_depth"] is False


def test_silver_hit_at_has_no_gold_descendant(ctx):
    data, caveats = tools.lineage_trace(ctx, "silver.churn_limit_events.hit_at", "downstream")
    assert data["reached"]["columns"] == {} and data["summary"]["branches"] == []
    assert {e["rel"] for e in data["edges"]} == {"SOURCED_FROM"}          # only the graph's LimitHit ids read it
    assert any("No gold or export column is derived" in c for c in caveats)


def test_trace_edges_are_ordered_by_depth_then_ids(ctx):
    for target, direction in ((BRONZE_HIT_AT, "downstream"), (GOLD + "allowance_used_pct", "upstream"),
                              ("silver.churn_usage_daily.subscription_id", "downstream")):
        data, _ = tools.lineage_trace(ctx, target, direction)
        keys = [(e["depth"], e["rel"], e["from"], e["to"], e["cte"] or "", e["roles"] or "") for e in data["edges"]]
        assert keys == sorted(keys) and len(keys) == data["summary"]["edges"] > 0
        assert set(data["edges"][0]) == {"depth", "rel", "from", "to", "roles", "cte", "window", "transform"}
        assert data["edges"][0]["depth"] == 1 and data["edges"][0]["from"] == target


def test_upstream_trace_reaches_the_source_csv_columns_and_the_parameters(ctx):
    data, _ = tools.lineage_trace(ctx, GOLD + "allowance_used_pct")
    assert data["direction"] == "upstream"
    assert data["summary"]["sources"] == [
        "source.daily_usage.activity_date", "source.daily_usage.agent_requests", "source.daily_usage.subscription_id",
        "source.pricing_changes.effective_date", "source.subscription_snapshots.plan_tier",
        "source.subscription_snapshots.snapshot_date"]
    assert data["reached"]["parameters"] == ["param:allowance.pro", "param:allowance.pro_plus",
                                             "param:allowance.ultra", "param:cap_cut"]
    first = {(e["to"], e["roles"], e["window"]) for e in data["edges"]
             if e["depth"] == 1 and e["rel"] == "DERIVED_FROM"}
    assert ("silver.churn_usage_daily.agent_requests", "VALUE", "(as_of-28, as_of]") in first
    assert ("silver.churn_pricing_changes.effective_date", "WINDOW_BOUND", "(-inf, as_of]") in first
    assert ("silver.churn_subscription_snapshots.snapshot_date", "ANCHOR", None) in first
    count, _ = tools.lineage_trace(ctx, GOLD + "limit_hits_14d")
    rows = [e for e in count["edges"] if e["rel"] == "COUNTS_ROWS_OF"]
    assert [(e["to"], e["window"]) for e in rows] == [("silver.churn_limit_events", "(as_of-14, as_of]")]
    assert count["reached"]["datasets"] == ["silver.churn_limit_events"]
    source, caveats = tools.lineage_trace(ctx, "source.limit_events.hit_at")
    assert source["edges"] == [] and any("no upstream column" in c for c in caveats)


def test_trace_shows_what_an_unbounded_reference_read_is_matched_to(ctx):
    data, _ = tools.lineage_trace(ctx, GOLD + "incident_exposed_28d")
    incidents = {e["to"]: e["window"] for e in data["edges"] if e["depth"] == 1 and "churn_incidents" in e["to"]}
    assert incidents == {f"silver.churn_incidents.{c}": "unbounded, matched to event rows in (as_of-28, as_of]"
                         for c in ("starts_on", "ends_on")}
    down, _ = tools.lineage_trace(ctx, "silver.churn_incidents.starts_on", "downstream")
    hop = next(e for e in down["edges"] if e["to"] == GOLD + "incident_exposed_28d")
    assert hop["window"] == "unbounded, matched to event rows in (as_of-28, as_of]" and hop["cte"] == "derived:i"
    keys = [(e["depth"], e["rel"], e["from"], e["to"], e["cte"] or "", e["roles"] or "", e["window"] or "",
             e["transform"] or "") for e in data["edges"]]
    assert keys == sorted(keys) and len(set(keys)) == len(keys)       # a total order: no two rows tie


def test_trace_puts_summary_and_reached_before_the_edge_rows_and_flags_a_large_answer(ctx):
    small, small_caveats = tools.lineage_trace(ctx, BRONZE_HIT_AT, "downstream")
    assert list(small) == ["target", "direction", "max_depth", "summary", "reached", "edges"]
    assert small["summary"]["edges"] <= spec.TRACE_LARGE_EDGES and not any("Large answer" in c for c in small_caveats)
    big, caveats = tools.lineage_trace(ctx, "silver.churn_subscription_snapshots.snapshot_date", "downstream")
    assert list(big) == list(small) and big["summary"]["edges"] == len(big["edges"]) > spec.TRACE_LARGE_EDGES == 200
    assert any(f"Large answer ({len(big['edges'])} edge rows" in c and "smaller max_depth" in c for c in caveats)
    head = {k: v for k, v in big.items() if k != "edges"}
    assert len(json.dumps(head)) < len(json.dumps(big)) / 4       # the rows are the bulk; the answer is up front
    counts = big["summary"]["reached_counts"]                     # readable even if the lists are cut as well
    assert counts == {"export_columns": len(big["reached"]["columns"]["export"]),
                      "gold_columns": len(big["reached"]["columns"]["gold"]),
                      **{k: len(v) for k, v in big["reached"].items() if k != "columns" and v}}
    assert sum(v for k, v in counts.items() if k.endswith("_columns")) == big["summary"]["columns"]
    assert list(big["summary"])[:5] == ["edges", "columns", "reached_counts", "max_depth_reached",
                                        "stopped_at_max_depth"]
    shallow, shallow_caveats = tools.lineage_trace(ctx, "silver.churn_subscription_snapshots.snapshot_date",
                                                   "downstream", 1)
    assert shallow["summary"]["edges"] < big["summary"]["edges"] and shallow["edges"] == \
        [e for e in big["edges"] if e["depth"] == 1]
    assert any("stopped at max_depth" in c for c in shallow_caveats)


def test_max_depth_truncates_and_says_so(ctx):
    full, _ = tools.lineage_trace(ctx, BRONZE_HIT_AT, "downstream", 6)
    for depth in (1, 2, 3):
        data, caveats = tools.lineage_trace(ctx, BRONZE_HIT_AT, "downstream", depth)
        assert data["edges"] == [e for e in full["edges"] if e["depth"] <= depth]
        assert data["summary"]["max_depth_reached"] == depth
        assert data["summary"]["stopped_at_max_depth"] is (depth < 3)     # the last column hop is at depth 3
        assert any("stopped at max_depth" in c for c in caveats) is (depth < 3)
    one, _ = tools.lineage_trace(ctx, BRONZE_HIT_AT, "downstream", 1)
    assert [e["to"] for e in one["edges"]] == ["silver.churn_limit_events.hit_at",
                                               "silver.churn_limit_events.hit_date"]


# --------------------------------------------------------------------------- lineage_pit
def test_pit_lists_exactly_the_two_exceptions_by_default(ctx):
    data, caveats = tools.lineage_pit(ctx)
    assert [f["feature"] for f in data["features"]] == ["first_renewal_after_pricing_change", "renewals_completed"]
    assert all(f["pit_status"] == "declared_exception" and f["max_upper_vs_as_of_days"] == 7
               and f["reads_after_as_of"] and not f["declared_leaky"] for f in data["features"])
    completed = data["features"][1]
    assert completed["windows"] == ["(-inf, as_of+7)"]
    assert {"dataset": "silver.churn_invoices", "event_column": "invoice_date", "window": "(-inf, as_of+7)"} \
        in completed["late_sources"]
    # a bound that lives in a CASE keeps its event column too
    assert data["features"][0]["late_sources"] == [{"dataset": "silver.churn_pricing_changes",
                                                    "event_column": "effective_date", "window": "[as_of-23, as_of+7)"}]
    assert any("silver.churn_pricing_changes.effective_date in [as_of-23, as_of+7)" in c for c in caveats)
    assert all(f["unbounded_reads"] == [] for f in data["features"])
    assert data["summary"] == {"features": 22, "compliant": 20, "declared_exception": 2,
                               "exceptions": ["first_renewal_after_pricing_change", "renewals_completed"]}
    assert data["rule"]["id"] == "pit:features_le_as_of" and data["rule"]["as_of_offset_days"] == -7
    assert sum("declared exception" in c for c in caveats) == 2


def test_pit_for_one_feature_and_for_all_22(ctx):
    data, caveats = tools.lineage_pit(ctx, "limit_hits_14d")
    assert data["features"] == [{"feature": "limit_hits_14d", "pit_status": "compliant",
                                 "max_upper_vs_as_of_days": 0, "reads_after_as_of": False, "declared_leaky": False,
                                 "windows": ["(as_of-14, as_of]"], "late_sources": [], "unbounded_reads": []}]
    assert len(caveats) == 1
    unbounded = {}
    for feature in gspec.GOLD_FEATURES:
        row = tools.lineage_pit(ctx, feature)[0]["features"][0]
        assert row["pit_status"] == gspec.FEATURE_CARDS[feature]["pit_status"], feature
        assert set(row) == {"feature", "pit_status", "max_upper_vs_as_of_days", "reads_after_as_of",
                            "declared_leaky", "windows", "late_sources", "unbounded_reads"}
        if row["unbounded_reads"]:
            unbounded[feature] = row["unbounded_reads"]
    assert list(unbounded) == ["incident_exposed_28d"]      # the one read with no time bound of its own


def test_pit_shows_the_reference_table_a_feature_reads_without_a_time_bound(ctx):
    data, caveats = tools.lineage_pit(ctx, "incident_exposed_28d")
    row = data["features"][0]
    assert (row["pit_status"], row["max_upper_vs_as_of_days"], row["windows"]) == ("compliant", 0,
                                                                                   ["(as_of-28, as_of]"])
    assert row["unbounded_reads"] == [{
        "dataset": "silver.churn_incidents", "columns": ["ends_on", "starts_on"],
        "matched_window": "(as_of-28, as_of]",
        "reference_data": spec.GLOBAL_DIMENSION_TABLES["lakehouse.silver.churn_incidents"]}]
    assert len(caveats) == 2 and "silver.churn_incidents is read without a time bound of its own" in caveats[1] \
        and "matched to event rows in (as_of-28, as_of]" in caveats[1]


# --------------------------------------------------------------------------- lineage_guards / unused
def test_guards_for_a_gold_column_include_the_checks_on_its_export_copies(ctx):
    data, caveats = tools.lineage_guards(ctx, GOLD + "limit_hits_14d")
    rows = {(a["contract"], a["kind"]): a for a in data["assertions"]}
    assert set(rows) == {("churn_export", "not_null"), ("churn_export", "range"), ("gold_parity", "parity_values"),
                         ("graph_contract", "pit_parity")}
    rng = rows[("churn_export", "range")]
    assert (rng["min"], rng["max"], rng["severity"]) == (0.0, 60.0, "warn (error with --strict)")
    assert rng["checked_column"] == "export.churn_user_features.limit_hits_14d" and rng["source_line"] > 0
    assert rng["source"].startswith("scripts/check_churn_export.py:") and rng["same_rule_as"] == []
    assert rows[("gold_parity", "parity_values")]["checked_column"] == GOLD + "limit_hits_14d"
    assert [(a["contract"], a["kind"], a["checked_column"], a["assertion"]) for a in data["assertions"]] == \
        sorted((a["contract"], a["kind"], a["checked_column"], a["assertion"]) for a in data["assertions"])
    assert data["summary"]["value_checks"] == 4 and data["summary"]["severity_differs"] == []
    assert any("fail only with --strict" in c for c in caveats) and any("No retention-radar rule" in c for c in caveats)
    export_only, _ = tools.lineage_guards(ctx, "export.churn_user_features.limit_hits_14d")
    assert {a["kind"] for a in export_only["assertions"]} == {"not_null", "range"}
    city, city_caveats = tools.lineage_guards(ctx, GOLD + "city")
    assert {a["kind"] for a in city["assertions"]} == {"no_leak"} and city["summary"]["value_checks"] == 0
    assert any("none checks its values" in c for c in city_caveats)


def test_guards_on_a_column_of_an_unchecked_layer_says_where_the_checks_are(core_build, ctx):  # noqa: F811
    t = oracle.load_tables(core_build[0])
    for ref in ("silver.churn_limit_events.hit_at", "silver.churn_usage_daily.agent_requests",
                "bronze.churn_limit_events_raw.hit_at", "source.limit_events.hit_at"):
        data, caveats = tools.lineage_guards(ctx, ref)
        assert data["assertions"] == [] and data["column"] == ref
        layer = ref.split(".", 1)[0]
        assert caveats == [
            "No assertion checks this column.",
            f"Column checks in this build attach to export and gold columns, not to {layer} columns: "
            f"lineage_trace(target='{ref}', direction='downstream') lists the columns that read this one and the "
            f"assertions on them."], caveats
        assert (data, caveats) == oracle.lineage_guards(t, ref)
    # ... and the pointer is right: downstream of silver agent_requests there are assertions
    trace, _ = tools.lineage_trace(ctx, "silver.churn_usage_daily.agent_requests", direction="downstream")
    assert trace["reached"]["assertions"]
    # a gold or export column without a check gets no such pointer (its layer does carry checks)
    for ref in (GOLD + "city", GOLD + "built_at"):
        assert not any("attach to" in c for c in tools.lineage_guards(ctx, ref)[1]), ref


def test_guards_without_a_column_lists_the_unguarded_gold_columns(ctx):
    data, caveats = tools.lineage_guards(ctx)
    assert data["unguarded"] == [GOLD + c for c in ("feature_as_of", "renewal_date", "city", "built_at")]
    assert data["dataset"] == "gold.churn_renewal_features" and len(caveats) == 1


def test_unused_columns_per_layer_and_domain(ctx):
    silver, caveats = tools.lineage_unused(ctx)
    assert len(silver["columns"]) == 9 and silver["columns"] == sorted(silver["columns"])
    assert {"silver.churn_limit_events.hit_at", "silver.churn_invoices.amount_usd",
            "silver.churn_limit_events.limit_type"} <= set(silver["columns"])
    assert silver["also_read_by_graph"]["silver.churn_limit_events.limit_type"] == ["ge:node:LimitHit"]
    assert any("business graph reads 8 of them" in c for c in caveats)
    bronze, bronze_caveats = tools.lineage_unused(ctx, "bronze", "churn")
    assert len(bronze["columns"]) == 20 and all(c.rsplit(".", 1)[1] in ("_source_file", "_ingested_at")
                                                for c in bronze["columns"])
    assert bronze["read_by"] == "silver" and any("dropped before silver" in c for c in bronze_caveats)
    retail, _ = tools.lineage_unused(ctx, layer="silver", domain="retail")
    assert "silver.orders.order_id" in retail["columns"] and "silver.orders.amount" not in retail["columns"]
    # the retail silver SQL keeps every bronze column; only the smoke-test table has no reader at all
    assert tools.lineage_unused(ctx, "bronze", "retail")[0]["columns"] == [
        "bronze.smoke_demo.id", "bronze.smoke_demo.note", "bronze.smoke_demo.created_at"]


def test_tools_equal_the_python_oracle_for_every_feature_and_gold_column(core_build, ctx):  # noqa: F811
    t = oracle.load_tables(core_build[0])
    for feature in gspec.GOLD_FEATURES:
        assert tools.lineage_pit(ctx, feature) == oracle.lineage_pit(t, feature)
    for col in [c for c in t.by_label["DataColumn"] if c["ref"]]:
        if col["layer"] in ("gold", "bronze"):
            for direction in spec.TRACE_DIRECTIONS:
                assert tools.lineage_trace(ctx, col["ref"], direction) == \
                    oracle.lineage_trace(t, col["ref"], direction), (col["ref"], direction)
        if col["layer"] == "gold":
            assert tools.lineage_guards(ctx, col["ref"]) == oracle.lineage_guards(t, col["ref"]), col["ref"]
    for depth in range(1, spec.MAX_TRACE_DEPTH + 1):
        assert tools.lineage_trace(ctx, "source.limit_events.hit_at", "downstream", depth) == \
            oracle.lineage_trace(t, "source.limit_events.hit_at", "downstream", depth), depth
        assert tools.lineage_trace(ctx, "export.churn_user_features.allowance_used_pct", "upstream", depth) == \
            oracle.lineage_trace(t, "export.churn_user_features.allowance_used_pct", "upstream", depth), depth
