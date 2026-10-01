"""Tier-0 mini repo contract checks: 0 errors and exactly the 2 known warnings at this commit."""
from __future__ import annotations

import ast
import importlib.util
import json
import shutil

import pytest
from conftest import REPO, run_script

from lakehouse_graph import spec


def _load():
    path = REPO / "scripts/check_repo_contracts.py"
    mspec = importlib.util.spec_from_file_location("check_repo_contracts", path)
    mod = importlib.util.module_from_spec(mspec)
    mspec.loader.exec_module(mod)
    return mod


def _literal(rel: str, name: str):
    tree = ast.parse((REPO / rel).read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == name for t in node.targets):
            return ast.literal_eval(node.value)
    raise KeyError(name)


@pytest.fixture()
def repo_copy(tmp_path):
    """The files the checks read, copied so a test can plant drift without touching the repo."""
    root = tmp_path / "repo"
    for rel in ("sql/churn", "src/jobs/churn", "src/jobs/retail"):
        shutil.copytree(REPO / rel, root / rel)
    for rel in ("scripts/build_churn_gold_local.py", "scripts/generate_churn_sample.py",
                "scripts/check_churn_export.py", "src/lakehouse_graph/spec.py"):
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(REPO / rel, root / rel)
    return root


def _patch(root, rel, old, new):
    p = root / rel
    text = p.read_text()
    assert old in text, f"{old!r} not in {rel}"
    p.write_text(text.replace(old, new, 1))


def test_zero_errors_and_exactly_two_warnings():
    res = _load().check(REPO)
    assert res["errors"] == []
    assert [code for code, _ in res["warnings"]] == ["appname-retail", "leaky-dangling"]
    retail, leaky = (msg for _, msg in res["warnings"])
    assert "05_query_timetravel.py" in retail and "05_query_and_timetravel" in retail
    assert "cancel_at_period_end" in leaky
    assert res["facts"]["allowance"] == {"pro": 550, "pro_plus": 1650, "ultra": 11000}
    assert res["facts"]["cap_cut"] == 0.83 and res["facts"]["as_of_offset_days"] == 7
    assert res["facts"]["train_columns"] == 25


def test_cli_exit_codes_and_summary():
    p = run_script("check_repo_contracts.py")
    assert p.returncode == 0 and "Repo contracts OK: 0 errors, 2 warnings" in p.stdout
    for phrase in ("pro 550 / pro_plus 1,650 / ultra 11,000", "cap-cut multiplier: 0.83", "T-7",
                   "churn appName == churn_<stem> for 4 jobs"):
        assert phrase in p.stdout, phrase
    p = run_script("check_repo_contracts.py", "--strict")
    assert p.returncode == 1 and "warnings are fatal with --strict" in p.stderr
    p = run_script("check_repo_contracts.py", "--stict")    # a typo is an error, not a silent plain run
    assert p.returncode == 2 and "unrecognized arguments: --stict" in p.stderr
    p = run_script("check_repo_contracts.py", "--json")
    assert p.returncode == 0 and len(json.loads(p.stdout)["warnings"]) == 2


def test_missing_or_broken_source_is_an_error_not_a_traceback(repo_copy):
    (repo_copy / "sql/churn/gold_renewal_features.sql").unlink()
    (repo_copy / "src/jobs/churn/02_transform_silver.py").write_text("def broken(:\n")
    res = _load().check(repo_copy)
    assert [code for code, _ in res["errors"]] == ["unreadable", "unreadable"] and res["warnings"] == []
    assert "sql/churn/gold_renewal_features.sql" in res["errors"][0][1]
    assert "02_transform_silver.py" in res["errors"][1][1] and "SyntaxError" in res["errors"][1][1]


def test_copy_of_the_repo_gives_the_same_result(repo_copy):
    res = _load().check(repo_copy)
    assert res["errors"] == [] and len(res["warnings"]) == 2


@pytest.mark.parametrize("rel, old, new, code", [
    ("scripts/generate_churn_sample.py", "CAP_CUT = 0.83", "CAP_CUT = 0.85", "constants-cap-cut"),
    ("sql/churn/gold_renewal_features.sql", "WHEN 'pro_plus' THEN 1650", "WHEN 'pro_plus' THEN 1600",
     "constants-allowance"),
    ("scripts/build_churn_gold_local.py", '"ultra": 11000', '"ultra": 12000', "constants-allowance"),
    ("sql/churn/gold_renewal_features.sql", "date_sub(current_period_end, 7)", "date_sub(current_period_end, 14)",
     "constants-t7"),
    ("scripts/build_churn_gold_local.py", 'snaps["current_period_end"] - pd.Timedelta(days=7)',
     'snaps["current_period_end"] - pd.Timedelta(days=6)', "constants-t7"),
    ("scripts/generate_churn_sample.py", '"pro": 20.0', '"pro": 25.0', "constants-price"),
    ("src/jobs/churn/02_transform_silver.py", 'appName("churn_02_transform_silver")', 'appName("07_silver")',
     "appname-churn"),
    ("scripts/check_churn_export.py", '"limit_hits_14d", "cheap_model_share_28d"',
     '"cheap_model_share_28d", "limit_hits_14d"', "columns-train"),
])
def test_planted_drift_is_an_error(repo_copy, rel, old, new, code):
    _patch(repo_copy, rel, old, new)
    res = _load().check(repo_copy)
    assert code in [c for c, _ in res["errors"]], res["errors"]


def test_fixing_the_two_known_warnings_clears_them(repo_copy):
    _patch(repo_copy, "scripts/check_churn_export.py", ', "cancel_at_period_end"}', "}")
    _patch(repo_copy, "src/jobs/retail/05_query_timetravel.py", "05_query_and_timetravel", "05_query_timetravel")
    res = _load().check(repo_copy)
    assert res["errors"] == [] and res["warnings"] == []


def test_feature_cards_agree_with_the_export_contract():
    """spec.FEATURE_CARDS ranges mirror check_churn_export RANGES / BINARY (the radar schema)."""
    ranges = {c: [lo, hi] for c, lo, hi in _literal("scripts/check_churn_export.py", "RANGES")}
    binary = _literal("scripts/check_churn_export.py", "BINARY")
    train = _literal("scripts/check_churn_export.py", "TRAIN_COLUMNS")
    assert train[2:-1] == spec.GOLD_FEATURES
    for f, card in spec.FEATURE_CARDS.items():
        want = ranges.get(f, [0, 1] if f in binary else None)
        assert card["contract_range"] == want, f
        assert card["used_in_similar_to"] == (f in spec.FEATURES)
    assert spec.PLAN_PRICE_USD == _literal("scripts/generate_churn_sample.py", "PRICE")
    # the windows the graph filters on are the windows the gold SQL uses
    sql = (REPO / "sql/churn/gold_renewal_features.sql").read_text()
    for rel, col in (("HIT_LIMIT", "l.hit_date"), ("OPENED", "t.created_date"), ("CHARGED_OVERAGE", "c.charged_at"),
                     ("EXPOSED_TO", "u.activity_date")):
        days = spec.PIT_WINDOWS[rel].days
        assert f"{col} > date_sub(r.as_of, {days}) AND {col} <= r.as_of" in sql, rel
    assert f"date_sub(r.renewal_date, {spec.FIRST_RENEWAL_WINDOW_DAYS})" in sql
