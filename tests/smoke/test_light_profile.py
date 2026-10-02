"""T2: smoke test of a running `light` (or `full`) Compose stack on localhost.

    make test-t2        # up-light --wait, then this file

Skips when nothing answers on LAKEKEEPER_URL (default http://localhost:8181) unless
LDL_REQUIRE_STACK=1, which turns the skip into a failure (CI).
"""
from __future__ import annotations

import json
import os
import subprocess

import light_demo
import pytest
from support.ldl import REPO, env_example, http_json, reachable

import lakehouse_client as lc

BASE = os.environ.get("LAKEKEEPER_URL", "http://localhost:8181").rstrip("/")
ENV = env_example()

if not reachable(f"{BASE}/health"):
    if os.environ.get("LDL_REQUIRE_STACK") == "1":
        raise RuntimeError(f"LDL_REQUIRE_STACK=1 but {BASE}/health does not answer (make up-light)")
    pytest.skip(f"no stack on {BASE} (make up-light)", allow_module_level=True)


def _compose(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", "compose", *args], cwd=REPO, capture_output=True, text=True)


def test_services_are_healthy_and_init_completed():
    p = _compose("--profile", "*", "ps", "-a", "--format", "json")
    assert p.returncode == 0, p.stderr
    rows = [json.loads(line) for line in p.stdout.splitlines() if line.strip()]
    state = {r["Service"]: (r["State"], r.get("Health", ""), r.get("ExitCode")) for r in rows}
    for svc in ("postgres", "lakekeeper", "objectstore", "lakehouse-init"):
        assert state[svc][:2] == ("running", "healthy"), state
    assert state["lakekeeper-migrate"][0] == "exited" and state["lakekeeper-migrate"][2] == 0, state


def test_warehouse_vends_credentials_through_sts():
    _, body = http_json(f"{BASE}/management/v1/warehouse")
    wh = {w["name"]: w for w in body["warehouses"]}[ENV["LAKEKEEPER_WAREHOUSE"]]
    prof = wh["storage-profile"]
    assert prof["sts-enabled"] is True and prof["path-style-access"] is True
    assert prof["endpoint"].rstrip("/") == ENV["S3_ENDPOINT"]


def test_init_is_idempotent():
    p = _compose("--profile", "light", "run", "--rm", "--no-deps", "lakehouse-init", "--once")
    assert p.returncode == 0, p.stdout + p.stderr
    assert "exists" in p.stdout and "ready" in p.stdout


def test_light_retail_demo_meets_the_contract():
    out = light_demo.retail()
    assert (out["bronze"], out["silver"]) == (22, 19)


def test_light_churn_twin_round_trips(monkeypatch):
    monkeypatch.setenv("CHURN_SAMPLE_DIR", str(REPO / "data/sample/churn/fixtures/tiny"))
    out = light_demo.churn()
    assert out["renewals"] == 121 and out["routes"]["score_today"] == 1


def test_three_engines_read_one_snapshot():
    t = lc.catalog().load_table("gold.daily_order_metrics")
    sid = t.current_snapshot().snapshot_id
    ice = sorted(t.scan(snapshot_id=sid).to_arrow().column("orders").to_pylist())
    con = lc.duckdb_connect()
    duck = sorted(r[0] for r in con.sql(f"SELECT orders FROM lakehouse.gold.daily_order_metrics AT (VERSION => {sid})").fetchall())
    pol = sorted(lc.polars_scan(t, snapshot_id=sid).collect()["orders"].to_list())
    assert ice == duck == pol == [9, 10]
    assert t.io.properties.get("s3.session-token"), "vended, short-lived credentials"
    assert t.io.properties.get("s3.access-key-id") != ENV["S3_ACCESS_KEY"]
