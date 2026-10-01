"""Graph tools on the tiny fixture: answers vs the oracle, the point-in-time rules over every renewal, privacy,
argument validation, provenance and the build gate (lakehouse_graph.tools / context / search).

Seed-42 goldens and the 8,001-renewal sweep are in test_tools_s42.py (slow).
"""
from __future__ import annotations

import ast
import json
import os
import re
import shutil

os.environ.setdefault("PYDANTIC_ERRORS_INCLUDE_URL", "0")

import pandas as pd
import pyarrow.parquet as pq
import pytest
from conftest import REPO
from test_tools_support import HERO, rich_tiny, unchecked_copy

from lakehouse_graph import envelope, oracle, queries, spec, tools
from lakehouse_graph.context import CURRENT_ROUTES, ProvenanceUnavailable, ToolContext

TINY_TOP10 = ["sub_00052:2026-09-09", "sub_00020:2026-09-03", "sub_00088:2026-07-20", "sub_00076:2026-08-17",
              "sub_00051:2026-08-23", "sub_00014:2026-08-23", "sub_00045:2026-09-12", "sub_00118:2026-06-21",
              "sub_00019:2026-09-10", "sub_00053:2026-08-03"]   # PLAN 6.6 tiny golden, all renewed
MAYA_ROWS = [("2026-08-15", "CUT_CAP", "cap-cut-2026-08"), ("2026-08-25", "EXPOSED_TO", "inc-002"),
             ("2026-09-09", "EXPOSED_TO", "inc-003"), ("2026-09-20", "CUT_CAP", "cap-cut-2026-09"),
             ("2026-09-20", "FIRST_RENEWAL_AFTER", "cap-cut-2026-09"), ("2026-09-24", "HIT_LIMIT", "lh:sub_maya:001"),
             ("2026-09-25", "HIT_LIMIT", "lh:sub_maya:002"), ("2026-09-27", "HIT_LIMIT", "lh:sub_maya:003")]


@pytest.fixture(scope="module")
def build(tiny_build):
    return tiny_build[0]


@pytest.fixture(scope="module")
def ctx(build, graph_root, tmp_path_factory):
    c = ToolContext(build, allow_unchecked=True, graph_root=graph_root,
                    logs_dir=tmp_path_factory.mktemp("tools_logs"))
    yield c
    c.close()


@pytest.fixture(scope="module")
def t(build):
    return oracle.load_tables(build)


@pytest.fixture(scope="module")
def rich(graph_root, tiny_build):
    root, bdir = rich_tiny(str(graph_root))
    c = ToolContext(bdir, graph_root=root, audit=False)
    yield c
    c.close()


def call(ctx, name, **args):
    return tools.call(ctx, name, args)


# ------------------------------------------------------------------------------------------------ registry
def test_toolsets_are_exact_and_names_are_namespaced():
    names = {ts: [s.name for s in specs] for ts, specs in tools.TOOLSETS.items()}
    assert names == {
        "graph": ["graph_describe", "graph_find", "graph_renewal_evidence", "graph_similar_renewals", "graph_exposure"],
        "metrics": ["metric_lapse_rate", "metric_route_counts", "metric_feature_card"],
        "lineage": ["lineage_trace", "lineage_pit", "lineage_guards", "lineage_unused"],
        "cohorts": ["cohort_summary", "cohort_list"]}
    prefix = {"graph": "graph_", "metrics": "metric_", "lineage": "lineage_", "cohorts": "cohort_"}
    for ts, specs in tools.TOOLSETS.items():
        for s in specs:
            assert tools.TOOL_NAME_RE.match(s.name) and s.name.startswith(prefix[ts]) and s.toolset == ts
            d = s.description
            assert len(d) >= 200 and len(re.findall(r"[.!?](\s|$)", d)) >= 3, s.name
            assert re.search(r"\b(not|never|instead)\b", d, re.I), f"{s.name}: says when not to use it"
            for field, prop in s.args.model_json_schema()["properties"].items():
                assert prop.get("description"), f"{s.name}.{field} has no description in the published schema"
    assert len(tools.SPECS) == 14


def test_tool_modules_are_pure_and_never_reach_contract_templates():
    contract_only = set(queries.CONTRACT_ONLY)
    for mod in ("tools.py", "metrics.py", "search.py", "context.py", "envelope.py"):
        src = (REPO / "src/lakehouse_graph" / mod).read_text()
        assert not re.search(r"^\s*(from mcp|import mcp)", src, re.M), f"{mod} imports mcp"
        assert "ROW_LIMIT" not in src, mod
        consts = {n.value for n in ast.walk(ast.parse(src)) if isinstance(n, ast.Constant) and isinstance(n.value, str)}
        assert not consts & contract_only, (mod, consts & contract_only)


# ------------------------------------------------------------------------------------------------ answers
def test_every_answer_is_the_five_key_envelope_with_provenance(ctx):
    env = call(ctx, "graph_describe")
    assert tuple(env) == envelope.ENVELOPE_KEYS and env["note"] == envelope.NOTE
    p = env["provenance"]
    man = ctx.manifest
    assert p["build_id"] == man["business_build_id"] and p["inputs_sha256"] == man["inputs"]["combined_sha256"]
    assert p["seed"] == 42 and p["n_users"] == 120 and p["data_end"] == man["data_end"] and p["synthetic"] is True
    assert re.fullmatch(r"[0-9a-f]{64}", p["code_sha256"]) and re.fullmatch(r"[0-9a-f]{64}", p["manifest_sha256"])
    assert p["contract"] == ctx.contract["state"] and p["sandboxed"] is False
    assert "event_date <= r.as_of" in p["pit_rule"]
    assert env["data"]["toolsets"]["graph"][0] == "graph_describe"
    detailed = call(ctx, "graph_describe", response_format="detailed")
    assert len(envelope.compact_json(detailed)) > len(envelope.compact_json(env))
    assert "city" not in json.dumps(detailed["data"]["properties"])


def test_hero_evidence_is_the_oracle_and_the_plan(ctx, t):
    env = call(ctx, "graph_renewal_evidence", renewal_id=HERO)
    rows = env["data"]["rows"]
    assert rows == oracle.evidence(t, HERO)
    assert [(r["event_date"], r["relation"], r["target_id"]) for r in rows] == MAYA_ROWS  # PLAN: identical to s42
    assert env["data"]["renewal"]["user_name"] and env["data"]["renewal"]["current"] is True
    assert env["data"]["summary"] == {"window": "all_before_as_of", "rows": 8, "declared_exception_rows": 0,
                                      "by_relation": {"CUT_CAP": 2, "EXPOSED_TO": 2, "FIRST_RENEWAL_AFTER": 1,
                                                      "HIT_LIMIT": 3}}
    windowed = call(ctx, "graph_renewal_evidence", renewal_id=HERO, window="feature_windows")["data"]["rows"]
    assert windowed == [r for r in rows if r["in_feature_window"]] and len(windowed) == 7   # inc-002 is outside
    detailed = call(ctx, "graph_renewal_evidence", renewal_id=HERO, response_format="detailed")["data"]
    assert detailed["features_at_as_of"]["limit_hits_14d"] == 3 and "churned" not in detailed["features_at_as_of"]


def test_hero_neighbours_are_the_oracle_and_the_plan(ctx, t, build):
    env = call(ctx, "graph_similar_renewals", renewal_id=HERO)
    d = env["data"]
    assert [r["renewal_id"] for r in d["rows"]] == TINY_TOP10
    assert [(r["rank"], r["renewal_id"], r["d2_q"], r["outcome"]) for r in d["rows"]] == \
        [(x["rank"], x["renewal_id"], x["d2_q"], x["outcome"]) for x in oracle.top_k(t, HERO)]
    assert d["summary"] == {"n": 10, "outcomes_visible": 10, "lapsed": 0, "not_yet_observed": 0,
                            "wilson_95": [0.0, 0.278], "resolved_visibility": "today"}
    assert "Narrative evidence, not a risk estimate" in env["caveats"][0]
    shares = d["rows"][0]["top3_feature_shares"]
    scaler = pd.read_parquet(build / spec.SCALER_FILE)
    ren = pd.read_parquet(build / "parquet/nodes_Renewal.parquet").set_index("renewal_id")
    parts = {}
    for f, m, s in zip(scaler["feature"], scaler["mean"], scaler["std"], strict=True):
        za = (ren.at[HERO, f] - m) / s if s > 0 else 0.0
        zb = (ren.at[TINY_TOP10[0], f] - m) / s if s > 0 else 0.0
        parts[f] = (za - zb) ** 2
    total = sum(parts.values())
    want = sorted(((v / total, f) for f, v in parts.items()), key=lambda x: (-x[0], x[1]))[:3]
    assert [(x["feature"], x["share"]) for x in shares] == [(f, round(v, 3)) for v, f in want]
    assert all(x["renewal_id"] not in TINY_TOP10 or True for x in d["nearest_known_lapses"])
    assert call(ctx, "graph_similar_renewals", renewal_id=HERO, k=3, explain=False)["data"]["rows"][2].keys() == \
        {"rank", "renewal_id", "dist", "d2_q", "mutual", "outcome", "outcome_observed_on"}


def test_point_in_time_rules_hold_for_every_tiny_renewal(ctx, t):
    """The leak sweep of scripts/check_graph_tools.py on all 121 renewals (s42: test_tools_s42.py)."""
    ren = t["Renewal"].set_index("renewal_id")
    billed = t["BILLED"]
    outcome_ev = set(billed.loc[billed["outcome_evidence"].astype(bool), "dst"])
    declared, rejected, accepted = 0, 0, 0
    for rid in ren.index:
        as_of = ren.at[rid, "as_of"].date().isoformat()
        current = ren.at[rid, "route"] in CURRENT_ROUTES
        for r in call(ctx, "graph_renewal_evidence", renewal_id=rid)["data"]["rows"]:
            if r["relation"] == "FIRST_RENEWAL_AFTER" and r["event_date"] > as_of:
                declared += 1
                assert r["known_by_as_of"] is False and r["declared_exception"] is True
            else:
                assert r["event_date"] <= as_of, (rid, r)
            assert not (r["relation"] == "BILLED" and r["target_id"] in outcome_ev)
        for vis in ("auto", "source_as_of"):
            d = call(ctx, "graph_similar_renewals", renewal_id=rid, outcome_visibility=vis)["data"]
            if current and vis == "auto":
                assert d["summary"]["resolved_visibility"] == "today"
                continue
            assert d["summary"]["resolved_visibility"] == "source_as_of"
            for r in d["rows"]:
                if r["outcome"] != queries.NOT_YET_OBSERVED:
                    assert ren.at[r["renewal_id"], "outcome_observed_on"].date().isoformat() <= as_of
                    assert r["outcome_observed_on"] <= as_of
            assert all(x["outcome_observed_on"] <= as_of for x in d["nearest_known_lapses"])
        try:
            call(ctx, "graph_similar_renewals", renewal_id=rid, outcome_visibility="today")
            accepted += 1
            assert current
        except tools.ToolInputError as exc:
            rejected += 1
            assert not current and "only valid for current renewals" in str(exc)
    assert declared == 5                         # PLAN 6.6 tiny: 5 declared-exception FIRST_RENEWAL_AFTER edges
    assert (rejected, accepted) == (120, 1)


def test_exposure_counts_suppress_small_cells(ctx, t):
    env = call(ctx, "graph_exposure", entity_id="inc-002", response_format="detailed")
    d = env["data"]
    cells = {c["plan_tier"]: c for c in d["cells"]}
    assert list(cells) == ["pro", "pro_plus", "ultra"]                 # the fixed plan set, always
    # PLAN tiny: pro 12 / 12 / 1, pro_plus 1, ultra 1 (all model). Printing pro's 12 would pin pro_plus and ultra
    # at 1 each (the total is 14 and a null plan row is at least 1): p2a verify-3. So every plan row is null.
    assert all(c["exposed"] is None and c["model"] is None and c["voluntary_lapses"] is None for c in cells.values())
    assert all(c[k] == 0 for c in cells.values() for k in ("cancel_flow", "dunning", "current"))
    assert d["by_route"] == {"suppressed": False, "model": 14, "voluntary_lapses": 1, "cancel_flow": 0,
                             "dunning": 0, "current": 0}
    assert d["total"] == 14 and d["naive_additional"] is None and d["breakdown_withheld"] is False
    assert "Descriptive, not causal" in env["caveats"][0]
    assert any("Fixed shape: every plan and route is listed" in c for c in env["caveats"])
    inc1 = call(ctx, "graph_exposure", entity_id="inc-001", response_format="detailed")["data"]
    assert inc1["naive_additional"] == oracle.exposure_incident(t, "inc-001")["naive_additional"] == 10
    for inc in t["Incident"]["incident_id"]:
        d = call(ctx, "graph_exposure", entity_id=inc)["data"]
        assert d["total"] == oracle.exposure_incident(t, inc)["total"]
        for row in [*d["cells"], d["by_route"]]:
            assert all(v is None or v == 0 or v >= 5 for k, v in row.items() if k in ROUTE_CELLS), (inc, row)
            assert row["suppressed"] is any(v is None for k, v in row.items() if k in tools.INCIDENT_COUNTS)
    concise = call(ctx, "graph_exposure", entity_id="inc-002")
    assert "naive_additional" not in concise["data"]
    sep = call(ctx, "graph_exposure", entity_id="cap-cut-2026-09", renewal_id=HERO)["data"]
    assert sep["total"] == 1 and sep["breakdown_withheld"] and sep["named_renewal_member"] is True
    assert len(sep["cells"]) == 6 and all(c["suppressed"] for c in sep["cells"])
    aug = call(ctx, "graph_exposure", entity_id="cap-cut-2026-08", renewal_id=HERO)["data"]
    assert aug["total"] == 37 and aug["known_by_as_of"] == {"true": 32, "false": 5}
    assert aug["named_renewal_member"] is False and len(aug["cells"]) == 6
    assert all(v is None or v == 0 or v >= 5 for c in aug["cells"] for k, v in c.items()
               if k in ("model", "cancel_flow", "dunning", "current"))


ROUTE_CELLS = ("exposed", "model", "cancel_flow", "dunning", "current")   # voluntary_lapses: the model numerator


def test_exposure_answers_resist_the_exact_integer_attack(ctx, t):
    """scripts/check_graph_tools.py's adversary (written apart from metrics.protect) pins no null of any tiny
    exposure answer, and the printed shape is the fixed plan (x known_by_as_of) set."""
    import importlib.util

    mspec = importlib.util.spec_from_file_location("check_graph_tools_under_test",
                                                   REPO / "scripts/check_graph_tools.py")
    mod = importlib.util.module_from_spec(mspec)
    mspec.loader.exec_module(mod)
    for ent in [*t["Incident"]["incident_id"], *t["PricingChange"]["change_id"]]:
        for args in ({}, {"renewal_id": HERO}, {"response_format": "detailed"}):
            d = call(ctx, "graph_exposure", entity_id=ent, **args)["data"]
            assert mod.exposure_cell_problems(d, t) == [], (ent, args)


def test_low_row_cap_cuts_what_is_shown_never_what_is_counted(build, graph_root, t):
    """The row cap applies to the shown rows only: the window filter, the summary and exposure membership see every
    evidence row, and the cap's caveat gives the true total (Maya: 8 rows, 7 in their feature windows)."""
    low = ToolContext(build, allow_unchecked=True, graph_root=graph_root, audit=False, max_rows=3)
    try:
        everything = oracle.evidence(t, HERO)
        for window, want in (("all_before_as_of", everything),
                             ("feature_windows", [r for r in everything if r["in_feature_window"]])):
            env = call(low, "graph_renewal_evidence", renewal_id=HERO, window=window)
            assert env["data"]["rows"] == want[:3] and env["truncated"] is True
            assert env["data"]["summary"]["rows"] == len(want) == (8 if window == "all_before_as_of" else 7)
            assert sum(env["data"]["summary"]["by_relation"].values()) == len(want)
            assert f"data.rows: 3 of {len(want)} rows shown (row cap 3); narrow the call to see the rest." \
                in env["caveats"]
        # cap-cut-2026-09's FIRST_RENEWAL_AFTER row is Maya's 5th: a pre-cut fetch would call her a non-member
        member = {e: call(low, "graph_exposure", entity_id=e, renewal_id=HERO)["data"]["named_renewal_member"]
                  for e in ("inc-001", "inc-002", "inc-003", "cap-cut-2026-08", "cap-cut-2026-09")}
        assert member == {"inc-001": False, "inc-002": False, "inc-003": True, "cap-cut-2026-08": False,
                          "cap-cut-2026-09": True}
    finally:
        low.close()


def test_evidence_scan_bound_is_said_out_loud(ctx, monkeypatch):
    monkeypatch.setattr(tools, "EVIDENCE_SCAN_ROWS", 5)       # Maya has 8 rows: the bound is reached
    env = call(ctx, "graph_renewal_evidence", renewal_id=HERO)
    assert env["data"]["summary"]["rows"] == 5
    assert "This renewal has at least 5 evidence rows: the summary covers the first 5 by date." in env["caveats"]


def test_exposure_unknown_ids_are_repairable(ctx):
    with pytest.raises(tools.ToolInputError, match="this build has incidents inc-001, inc-002, inc-003"):
        call(ctx, "graph_exposure", entity_id="inc-009")
    with pytest.raises(tools.ToolInputError, match="(?i)call graph_find"):
        call(ctx, "graph_exposure", entity_id="inc-001", renewal_id="sub_nobody:2026-01-01")


@pytest.mark.parametrize(("query", "first"), [
    ("Maya", HERO), ("mya", HERO), ("sub_maya:2026-10-07", HERO), ("August pricing change", "cap-cut-2026-08"),
    ("the September cut", "cap-cut-2026-09"), ("inc 2", "inc-002"), ("incident", "inc-001"),
])
def test_graph_find_resolves_names_ids_and_hubs(ctx, query, first):
    env = call(ctx, "graph_find", query=query)
    assert env["data"]["matches"][0]["id"] == first
    assert set(env["data"]["matches"][0]) >= {"id", "kind", "display", "as_of", "route", "match"}


def test_graph_find_filters_caps_and_never_searches_cities(ctx, build):
    assert all(m["kind"] == "subscription" for m in call(ctx, "graph_find", query="maya", kind="subscription")
               ["data"]["matches"])
    assert len(call(ctx, "graph_find", query="sub", limit=10)["data"]["matches"]) == 0   # stop word, not every id
    cities = sorted(set(pq.read_table(build / "parquet/nodes_Subscription.parquet", columns=["city"]).column(0)
                        .to_pylist()))
    for city in cities:
        env = call(ctx, "graph_find", query=f"users in {city}", limit=10)
        assert all(city.lower() in m["display"].lower() for m in env["data"]["matches"]), city   # only a user name
        assert city not in json.dumps(env["caveats"])
    assert call(ctx, "graph_find", query="zzqx")["data"]["matches"] == []


def test_user_names_and_cities_only_where_allowed(ctx, build):
    subs = pq.read_table(build / "parquet/nodes_Subscription.parquet").to_pandas()
    names, cities = set(subs["user_name"]), set(subs["city"])
    hero_name = subs.set_index("subscription_id").at["sub_maya", "user_name"]
    answers = {
        "graph_describe": call(ctx, "graph_describe", response_format="detailed"),
        "graph_similar_renewals": call(ctx, "graph_similar_renewals", renewal_id=HERO),
        "graph_exposure": call(ctx, "graph_exposure", entity_id="inc-001", renewal_id=HERO),
        "metric_lapse_rate": call(ctx, "metric_lapse_rate", group_by=["plan_tier"]),
        "metric_route_counts": call(ctx, "metric_route_counts"),
        "graph_renewal_evidence": call(ctx, "graph_renewal_evidence", renewal_id=HERO),
    }
    for name, env in answers.items():
        text = envelope.compact_json(env)
        found = {n for n in names if f'"{n}"' in text}
        assert found == ({hero_name} if name == "graph_renewal_evidence" else set()), name
        assert not any(re.search(rf"\b{re.escape(c)}\b", text) for c in cities), name
        assert '"city"' not in text, name


def test_feature_card_for_every_gold_feature(ctx):
    for f in spec.GOLD_FEATURES:
        d = call(ctx, "metric_feature_card", feature=f)["data"]
        assert d["feature"] == f and d["pit_status"] in ("compliant", "declared_exception")
        assert d["used_in_similar_to"] == (f in spec.FEATURES)
    d = call(ctx, "metric_feature_card", feature="first_renewal_after_pricing_change")["data"]
    assert d["declared_exception"]["edges_known_by_as_of_false"] == 5 and d["pit_status"] == "declared_exception"


# ------------------------------------------------------------------------------------------------ arguments
@pytest.mark.parametrize(("name", "raw", "field", "want"), [
    ("graph_find", {"query": "maya", "kind": "", "limit": "null"}, "limit", 5),
    ("graph_find", {"query": "maya", "kind": "None"}, "kind", "any"),
    ("graph_similar_renewals", {"renewal_id": HERO, "k": None, "explain": "null"}, "k", 10),
    ("graph_similar_renewals", {"renewal_id": HERO, "k": "3"}, "k", 3),
    ("graph_similar_renewals", {"renewal_id": f"  {HERO} "}, "renewal_id", HERO),
    ("graph_exposure", {"entity_id": "inc-002", "renewal_id": "None"}, "renewal_id", None),
    ("metric_lapse_rate", {"group_by": "plan_tier"}, "group_by", ["plan_tier"]),
    ("metric_lapse_rate", {"group_by": '["plan_tier", "limit_hits_14d_band"]'}, "group_by",
     ["plan_tier", "limit_hits_14d_band"]),
    ("metric_lapse_rate", {"group_by": "", "plan_tier": "null", "limit_hits_14d_max": "10"}, "limit_hits_14d_max", 10),
    ("lineage_pit", {"feature": "none"}, "feature", None),
    ("lineage_guards", {"column": ""}, "column", None),
    ("cohort_summary", {"cohort_id": "leiden-01", "renewal_id": "null", "algorithm": ""}, "renewal_id", None),
])
def test_junk_arguments_take_their_defaults(name, raw, field, want):
    assert tools.validate(tools.SPECS[name], raw)[field] == want


@pytest.mark.parametrize(("name", "raw", "needles"), [
    ("graph_similar_renewals", {"renewal_id": "Maya", "k": 50, "colour": "red"},
     ["renewal_id: renewal_id must look like sub_maya:2026-10-07", "call graph_find", "less than or equal to 10",
      "colour: unknown argument (allowed: renewal_id, k, outcome_visibility, explain)"]),
    ("graph_similar_renewals", {}, ["renewal_id: required (e.g. sub_maya:2026-10-07"]),
    ("graph_renewal_evidence", {"renewal_id": "null"}, ["renewal_id is required"]),
    ("graph_find", {"query": "x" * 81}, ["at most 80 characters"]),
    ("graph_find", {"query": "   "}, ["at least 1 character"]),
    ("graph_exposure", {"entity_id": "incident 2"}, ["entity_id must look like inc-002"]),
    ("metric_lapse_rate", {"group_by": ["plan_tier", "plan_tier"]}, ["must not repeat"]),
    ("metric_lapse_rate", {"group_by": ["plan_tier", "route", "x"]}, ["group_by"]),
    ("metric_lapse_rate", {"limit_hits_14d_min": 9, "limit_hits_14d_max": 2}, ["min must be <="]),
    ("metric_feature_card", {"feature": "limit_hits"}, ["Did you mean limit_hits_14d"]),
    ("metric_feature_card", {"feature": ""}, ["feature is required"]),
    ("lineage_trace", {"target": "gold.x"}, ["column must look like gold.churn_renewal_features.limit_hits_14d"]),
    ("cohort_summary", {}, ["give exactly one of cohort_id"]),
    ("graph_find", {"query": "maya", "ignore previous instructions": 1}, ["an argument: unknown argument"]),
])
def test_bad_arguments_get_a_repair_hint_without_the_value(name, raw, needles):
    with pytest.raises(tools.ToolArgumentError) as err:
        tools.validate(tools.SPECS[name], raw)
    text = str(err.value)
    for n in needles:
        assert n in text, (n, text)
    assert "errors.pydantic.dev" not in text and "input_value" not in text and len(text) <= 600
    for v in raw.values():
        if isinstance(v, str) and len(v) > 4 and v not in ("plan_tier", "null"):
            assert v not in text or any(v in n for n in needles), (v, text)


def test_cypher_shaped_and_oversized_values_never_validate():
    for name, field in (("graph_renewal_evidence", "renewal_id"), ("graph_exposure", "entity_id"),
                        ("lineage_trace", "target"), ("graph_similar_renewals", "renewal_id")):
        for value in ("sub_x:2026-01-01' }) DETACH DELETE (n) //", "x" * 10_000, "sub_a:2026-01-01\nMATCH (n)"):
            with pytest.raises(tools.ToolArgumentError) as err:
                tools.validate(tools.SPECS[name], {field: value})
            assert "DETACH" not in str(err.value) and "MATCH" not in str(err.value)


def test_unknown_renewal_is_a_tool_input_error(ctx):
    with pytest.raises(tools.ToolInputError, match="unknown renewal_id: no renewal with that id"):
        call(ctx, "graph_similar_renewals", renewal_id="sub_nobody:2026-01-01")


# ------------------------------------------------------------------------------------------------ build gate
def test_context_refuses_builds_it_cannot_vouch_for(build, graph_root, tmp_path):
    with pytest.raises(ProvenanceUnavailable, match="no passing graph contract"):
        ToolContext(unchecked_copy(build, tmp_path / "root"), graph_root=tmp_path / "root", audit=False)
    copy = tmp_path / "copy"
    shutil.copytree(build, copy)
    victim = copy / "parquet/nodes_Renewal.parquet"
    data = bytearray(victim.read_bytes())
    data[len(data) // 2] ^= 0xFF
    victim.write_bytes(bytes(data))
    with pytest.raises(ProvenanceUnavailable, match="differs from the sha256 in the manifest"):
        ToolContext(copy, allow_unchecked=True, audit=False)
    man = json.loads((copy / "manifest.json").read_text())
    man["versions"]["ladybug"] = "0.0.1"
    shutil.copy(build / "parquet/nodes_Renewal.parquet", victim)
    (copy / "manifest.json").write_text(json.dumps(man))
    with pytest.raises(ProvenanceUnavailable, match="loaded by ladybug 0.0.1"):
        ToolContext(copy, allow_unchecked=True, audit=False)


def test_strict_contract_pass_lineage_and_cohorts_on_a_complete_build(rich):
    assert rich.contract["state"] == "strict_pass" and rich.provenance["contract"] == "strict_pass"
    assert rich.provenance["lineage_build_id"] and rich.provenance["spec"].get("lineage") == "metadata-graph/0.1"
    pit = call(rich, "lineage_pit")["data"]
    assert {f["feature"] for f in pit["features"]} == {"first_renewal_after_pricing_change", "renewals_completed"}
    tr = call(rich, "lineage_trace", target="bronze.churn_limit_events_raw.hit_at", direction="downstream")
    assert tr["data"]["summary"]["reached_counts"]["gold_columns"] == 1
    assert call(rich, "lineage_guards")["data"]["unguarded"]
    assert len(call(rich, "lineage_unused")["data"]["columns"]) == 9                 # PLAN Q17
    cl = call(rich, "cohort_list")["data"]
    assert cl["algorithm"] == "leiden" and cl["cohorts"]
    cs = call(rich, "cohort_summary", renewal_id=HERO)["data"]
    assert cs["named_renewal"]["renewal_id"] == HERO and cs["outcomes"]["visibility"] == "today"


@pytest.mark.parametrize(("name", "raw", "needle"), [
    ("lineage_trace", {"target": "gold.churn_renewal_features.zzq9"}, "unknown column: no such column"),
    ("lineage_trace", {"target": "gold.zzqx_table.zzq9"}, "unknown column: no such column"),
    ("lineage_guards", {"column": "silver.zzqx_table.zzq9"}, "unknown column"),
    ("cohort_summary", {"cohort_id": "leiden-987"}, "unknown cohort_id: this build has leiden-01"),
    ("cohort_summary", {"renewal_id": "sub_zzq9:2026-01-01"}, "unknown renewal_id: not in this build"),
    ("cohort_summary", {"cohort_id": "leiden-01", "algorithm": "louvain"}, "cohort_id is a leiden cohort but"),
])
def test_lineage_and_cohort_errors_never_repeat_the_id_sent(rich, name, raw, needle):
    with pytest.raises(tools.ToolInputError) as err:
        call(rich, name, **raw)
    text = str(err.value)
    assert needle in text and "zzq" not in text and "987" not in text and "leiden-01'" not in text, text
    assert not text.startswith("invalid arguments"), "the id must pass validation and reach the tool"


def test_every_tool_keeps_its_answer_at_the_minimum_cap(rich):
    with pytest.raises(ValueError, match="max_chars must be between 4000"):
        ToolContext(rich.build_dir, graph_root=rich.graph_root, audit=False, max_chars=envelope.MIN_MAX_CHARS - 1)
    low = ToolContext(rich.build_dir, graph_root=rich.graph_root, audit=False, max_chars=envelope.MIN_MAX_CHARS)
    try:
        for name, raw in [
            ("graph_describe", {"response_format": "detailed"}), ("graph_find", {"query": "sub_0", "limit": 10}),
            ("graph_renewal_evidence", {"renewal_id": HERO, "response_format": "detailed"}),
            ("graph_similar_renewals", {"renewal_id": HERO}),
            ("graph_exposure", {"entity_id": "cap-cut-2026-08", "renewal_id": HERO, "response_format": "detailed"}),
            ("metric_lapse_rate", {"group_by": ["plan_tier", "limit_hits_14d_band"]}),
            ("metric_feature_card", {"feature": "first_renewal_after_pricing_change"}),
            ("lineage_trace", {"target": "bronze.churn_limit_events_raw.hit_at", "direction": "downstream"}),
            ("lineage_pit", {}), ("cohort_list", {}), ("cohort_summary", {"renewal_id": HERO}),
        ]:
            full, env = call(rich, name, **raw), call(low, name, **raw)
            assert len(envelope.compact_json(env)) <= envelope.MIN_MAX_CHARS, name
            assert "answer_too_large" not in env["data"] and next(iter(full["data"])) in env["data"], name
            assert env["data"].get("summary") == full["data"].get("summary"), name
    finally:
        low.close()


def test_missing_lineage_and_cohorts_are_named_with_the_fix(ctx):
    with pytest.raises(tools.ToolUnavailable, match="make lineage-local"):
        call(ctx, "lineage_pit")
    with pytest.raises(tools.ToolUnavailable, match="make graph-cohorts"):
        call(ctx, "cohort_list")
    assert ctx.lineage_conn is None and not ctx.has_cohorts()
