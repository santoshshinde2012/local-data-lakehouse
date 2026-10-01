"""The eval's deterministic merge gate (PLAN 9.2): recorded episodes reproduce their expected grades with no model.

evals/replay/*.jsonl hold episodes recorded on the tiny build (pass, fail, router refusal, a silently truncated
context, a tool call written as text). For each one:

* re-grade (no model, no server): the case is re-materialised from its raw spec on a fresh tiny build (the oracle
  references give the same question and expected values) and the recorded answer is graded again: the grade must be
  identical, check by check;
* live replay (pydantic-ai; no model): the recorded model turns are re-emitted through the harness
  (agent.replay_model) against a live MCP server started by scripts/graph_mcp.sh: status, output, tool calls, tool
  results (data and caveats), flags and grade must be identical;
* negative controls: a changed answer or a changed tool result is caught, so the gate can fail.
"""
from __future__ import annotations

import asyncio
import copy
import importlib.util
import json
import sys

import pytest
from conftest import REPO
from test_tools_support import rich_tiny


def _load():
    spec = importlib.util.spec_from_file_location("graph_eval", REPO / "scripts" / "graph_eval.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["graph_eval"] = mod
    spec.loader.exec_module(mod)
    return mod


ge = _load()
FIXTURES = ge.load_fixtures()
HAS_PAI = importlib.util.find_spec("pydantic_ai") is not None and importlib.util.find_spec("duckdb") is not None
needs_pai = pytest.mark.skipif(not HAS_PAI, reason="pydantic-ai / duckdb not installed (eval venv)")
IDS = [f"{fx['_file']}" for fx in FIXTURES]


def test_fixtures_cover_every_required_class():
    labels = {fx["label"] for fx in FIXTURES}
    assert {"pass", "fail", "text_tool_call", "truncated_context"} <= labels, labels
    by = {fx["label"]: fx for fx in FIXTURES}
    assert by["pass"]["expected"]["grade"]["passed"] is True
    assert by["fail"]["expected"]["grade"]["passed"] is False
    assert by["text_tool_call"]["episode"]["flags"]["text_tool_call_turns"] >= 1
    assert by["text_tool_call"]["note"].startswith("SCRIPTED"), "a scripted fixture says so"
    assert by["truncated_context"]["episode"]["flags"]["truncated_context"] == 1
    assert by["truncated_context"]["expected"]["grade"]["invalid"] is True
    for fx in FIXTURES:
        assert fx["schema"] == ge.FIXTURE_SCHEMA and fx["build_profile"] == "tiny"
        assert "expected" not in json.dumps(fx["case_raw"]), "the raw case holds oracle references only"


@pytest.mark.parametrize("fx", FIXTURES, ids=IDS)
def test_regrade_reproduces_the_expected_grade(fx, graph_root, tiny_build):
    root, build = rich_tiny(str(graph_root))
    r = ge.regrade_fixture(fx, build)
    assert r["question_equal"], "the oracle references give the same question"
    assert r["expected_values_equal"], "the oracle references give the same expected values"
    assert r["grade_equal"], (r["grade"], fx["expected"]["grade"])


@needs_pai
@pytest.mark.slow
@pytest.mark.parametrize("fx", FIXTURES, ids=IDS)
def test_live_replay_through_the_harness_is_identical(fx, graph_root, tiny_build, tmp_path):
    root, build = rich_tiny(str(graph_root))
    r = asyncio.run(ge.replay_live(fx, build, root, logs_dir=tmp_path, graph_py=sys.executable))
    assert r["identical"], r["diff"]


def test_negative_controls_the_gate_can_fail(graph_root, tiny_build):
    root, build = rich_tiny(str(graph_root))
    fx = copy.deepcopy(next(f for f in FIXTURES if f["label"] == "pass"))
    fx["answer"] = "I do not know."
    assert ge.regrade_fixture(fx, build)["grade_equal"] is False
    calls = (fx.get("episode") or {}).get("tool_calls") or []
    if calls and calls[0].get("result"):
        changed = json.loads(calls[0]["result"])
        changed["data"] = {"tampered": True}
        assert ge.stable_result(json.dumps(changed)) != ge.stable_result(calls[0]["result"])
    env = {"data": {"build_id": "x", "n": 1}, "provenance": {"commit": "a"}, "caveats": ["c"], "truncated": False}
    other = {**env, "data": {"build_id": "y", "n": 1}, "provenance": {"commit": "b"}}
    assert ge.stable_result(json.dumps(env)) == ge.stable_result(json.dumps(other)), "provenance is volatile"
