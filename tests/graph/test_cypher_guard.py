"""Guarded raw Cypher (lakehouse_graph.cypher_guard, PLAN Later A): the guard's deny list and limits, the EXPLAIN
repair message, the graph_cypher tool on the tiny evidence graph, the start gate (sandbox only), the MCP server in
process, and -- on macOS -- the real sandbox stopping a guard bypass (scripts/graph_sandbox_check.py --cypher).
"""
from __future__ import annotations

import json
import os
import subprocess
import sys

os.environ.setdefault("PYDANTIC_ERRORS_INCLUDE_URL", "0")

import anyio
import pytest
from conftest import REPO
from mcp import Client
from test_tools_support import LAUNCH, evidence_tiny, launcher_env

from lakehouse_graph import cypher_guard as cg
from lakehouse_graph import envelope, mcp_server, tools
from lakehouse_graph.context import ProvenanceUnavailable, ToolTimeout


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(scope="module")
def ev(graph_root, tiny_build):
    return evidence_tiny(str(graph_root))


def _no_exit(why: str) -> None:
    raise AssertionError(f"the watchdog fired inside the test process: {why}")


@pytest.fixture(scope="module")
def ctx(ev, tmp_path_factory):
    c = cg.CypherContext(ev[1], logs_dir=tmp_path_factory.mktemp("cypher_logs"), graph_root=ev[0], on_kill=_no_exit)
    yield c
    c.close()


def run(ctx, query: str) -> dict:
    return cg.call(ctx, "graph_cypher", {"query": query})


# ------------------------------------------------------------------------------------------------ the deny list
EXT = "IN" + "STALL"     # spelled in parts only to keep this file greppable like product code
REFUSED = {
    # file and database statements
    "load from": "LOAD FROM '/etc/hosts' RETURN *",
    "load from mid-query": "MATCH (r:Renewal) WITH r LOAD FROM 'x.csv' RETURN *",
    "copy to": "COPY (MATCH (r:Renewal) RETURN r.renewal_id) TO '/tmp/x.csv'",
    "copy from": "COPY Renewal FROM 'x.parquet'",
    "export": "EXPORT DATABASE '/tmp/x'",
    "import": "IMPORT DATABASE '/tmp/x'",
    "attach": "ATTACH 'other.lbdb' AS o (dbtype lbug)",
    "detach": "DETACH o",
    "use": "USE o",
    # extensions
    "install": f"{EXT} json",
    "uninstall": f"UN{EXT} json",
    "load extension": "LOAD EXTENSION algo",
    "update extension": "UPDATE EXTENSION json",
    "install mid-query": f"MATCH (n) {EXT} json RETURN n",
    # calls, settings and functions
    "call setting": "CALL timeout=0",
    "call threads": "CALL threads=64",
    "call other function": "CALL current_setting('timeout') RETURN *",
    "call read_parquet": "CALL read_parquet('x.parquet') RETURN *",
    "call project_graph": "CALL project_graph('g', ['Renewal'], []) RETURN *",
    "file function": "MATCH (r:Renewal) RETURN read_csv('x') LIMIT 1",
    "setting function": "RETURN current_setting('timeout') AS t",
    # writes and schema
    "create node": "CREATE (:Plan {plan_tier: 'zz'})",
    "create mid-query": "MATCH (r:Renewal) CREATE (r)-[:ON_PLAN]->(:Plan {plan_tier: 'zz'}) RETURN r",
    "merge": "MERGE (p:Plan {plan_tier: 'zz'}) RETURN p",
    "set": "MATCH (r:Renewal) SET r.as_of = date('2026-01-01') RETURN r",
    "delete": "MATCH (r:Renewal) DELETE r",
    "detach delete": "MATCH (r:Renewal) DETACH DELETE r",
    "remove": "MATCH (r:Renewal) REMOVE r.as_of RETURN r",
    "drop": "DROP TABLE Renewal",
    "alter": "ALTER TABLE Renewal ADD x INT64",
    "create table": "CREATE NODE TABLE X(id STRING, PRIMARY KEY(id))",
    "create macro": "CREATE MACRO m(x) AS x + 1",
    "foreach": "MATCH (r:Renewal) FOREACH (x IN [1] | SET r.as_of = date('2026-01-01')) RETURN r",
    "transaction": "BEGIN TRANSACTION",
    "commit": "COMMIT",
    "checkpoint": "CHECKPOINT",
    "explain": "EXPLAIN MATCH (r:Renewal) RETURN r",
    "profile": "PROFILE MATCH (r:Renewal) RETURN r",
    "union": "MATCH (r:Renewal) RETURN r.renewal_id UNION MATCH (p:Plan) RETURN p.plan_tier",
    # several statements
    "two statements": "MATCH (n) RETURN n LIMIT 1; MATCH (m) RETURN m",
    "hidden second statement": "MATCH (n) RETURN n LIMIT 1;/* */CREATE (:Plan {plan_tier: 'zz'})",
    # paths and URLs in strings
    "absolute path": "MATCH (r:Renewal) WHERE r.renewal_id = '/etc/passwd' RETURN r",
    "relative path": "MATCH (r:Renewal) WHERE r.renewal_id = '../graph.lbdb' RETURN r",
    "home path": "MATCH (r:Renewal) WHERE r.renewal_id = '~/.ssh/id_ed25519' RETURN r",
    "url": "MATCH (r:Renewal) WHERE r.renewal_id = 'https://example.com/x' RETURN r",
    "windows path": "MATCH (r:Renewal) WHERE r.renewal_id = 'C:\\\\x' RETURN r",
    "file name": "MATCH (r:Renewal) WHERE r.renewal_id = 'nodes_Renewal.parquet' RETURN r",
    # labels (none exist in the evidence graph; the guard says so before the binder does)
    "label churned": "MATCH (r:Renewal) RETURN r.churned LIMIT 1",
    "label outcome": "MATCH (r:Renewal) WHERE r.outcome = 'voluntary_lapse' RETURN count(*)",
    "label route": "MATCH (r:Renewal) RETURN r.route, count(*)",
    "label observed": "MATCH (r:Renewal) RETURN max(r.outcome_observed_on)",
    "label reference": "MATCH (r:Renewal) WHERE r.is_reference RETURN count(*)",
    # unbounded or long paths
    "unbounded path": "MATCH (a:Subscription)-[*]->(b) RETURN count(*)",
    "open upper bound": "MATCH (a:Subscription)-[e:HIT_LIMIT*1..]->(b) RETURN count(*)",
    "five hops": "MATCH (a)-[*1..5]-(b) RETURN count(*)",
    "five exact": "MATCH (a)-[*5]-(b) RETURN count(*)",
    "shortest unbounded": "MATCH (a:Renewal)-[* SHORTEST]-(b:Renewal) RETURN count(*)",
    "shortest long": "MATCH (a:Renewal)-[* ALL SHORTEST 1..9]-(b:Renewal) RETURN count(*)",
    "incoming unbounded": "MATCH (a:Renewal)<-[*]-(b) RETURN count(*)",
    # limits and shapes
    "parameter": "MATCH (r:Renewal) RETURN r.renewal_id LIMIT $k",
    "expression limit": "MATCH (r:Renewal) RETURN r.renewal_id LIMIT 1 + 1",
    "no return": "MATCH (r:Renewal)",
    "control character": "MATCH (r:Renewal) RETURN count(*) AS n\x00",
    "bidi character": "MATCH (r:Renewal) RETURN count(*) AS n \u202e",
    "too long": "MATCH (r:Renewal) RETURN count(*) AS n " + " " * cg.MAX_QUERY_CHARS,
    "empty": "   ",
    "unterminated string": "MATCH (r:Renewal) WHERE r.renewal_id = 'x RETURN r",
    "unterminated comment": "MATCH (r:Renewal) RETURN r /* x",
}


@pytest.mark.parametrize("name", sorted(REFUSED))
def test_every_deny_list_item_is_refused_before_the_engine(name):
    with pytest.raises(cg.CypherRefused, match="refused by the Cypher guard") as e:
        cg.guard(REFUSED[name])
    assert e.value.outcome == "refused"


def test_the_deny_list_covers_the_plan_items():
    words = set(cg.DENY)
    for item in ("LOAD", "COPY", "EXPORT", "IMPORT", "ATTACH", "DETACH", EXT, "UN" + EXT, "UPDATE", "CREATE",
                 "MERGE", "SET", "DELETE", "DROP", "ALTER", "USE"):
        assert item in words, item
    assert cg.ALLOWED_CALLS == ("show_tables", "table_info", "show_connection")
    assert (cg.MAX_ROWS, cg.MAX_HOPS, cg.TIMEOUT_MS, cg.MAX_QUERY_CHARS) == (200, 4, 5000, 20_000)
    m = cg.MEASURED_TEXT_TO_CYPHER        # measured before any small-model use (PLAN Later A): below the gate, off
    assert max(m["json_schema_output"], m["thinking_on"]) / m["questions"] < 0.8 and not m["enabled_for_small_models"]


@pytest.mark.parametrize(("query", "statement"), [
    ("MATCH (r:Renewal) RETURN count(r) AS n", "MATCH (r:Renewal) RETURN count(r) AS n LIMIT 201"),
    ("MATCH (r:Renewal) RETURN r.renewal_id ORDER BY r.renewal_id LIMIT 5;",
     "MATCH (r:Renewal) RETURN r.renewal_id ORDER BY r.renewal_id LIMIT 5"),
    ("MATCH (r:Renewal) RETURN r.renewal_id LIMIT 5000", "MATCH (r:Renewal) RETURN r.renewal_id LIMIT 200"),
    ("MATCH (r:Renewal) WITH r LIMIT 9000 RETURN r.as_of SKIP 3", "MATCH (r:Renewal) WITH r LIMIT 9000 RETURN "
                                                                 "r.as_of SKIP 3 LIMIT 201"),
    ("CALL show_tables() RETURN *", "CALL show_tables() RETURN * LIMIT 201"),
    ("MATCH (a:Subscription)-[*1..4]->(b) RETURN count(*) AS n", "MATCH (a:Subscription)-[*1..4]->(b) RETURN "
                                                                 "count(*) AS n LIMIT 201"),
    ("MATCH (a:Renewal)-[e* SHORTEST 1..3]-(b:Renewal) RETURN count(*)",
     "MATCH (a:Renewal)-[e* SHORTEST 1..3]-(b:Renewal) RETURN count(*) LIMIT 201"),
    ("MATCH (a)-[*..2]-(b) RETURN count(*)", "MATCH (a)-[*..2]-(b) RETURN count(*) LIMIT 201"),
    ("MATCH (r:Renewal) // a note\nRETURN count(*) AS n", "MATCH (r:Renewal)          \nRETURN count(*) AS n "
                                                          "LIMIT 201"),
    ("MATCH (r:Renewal) WHERE EXISTS { MATCH (r)-[:ON_PLAN]->(:Plan) RETURN r } RETURN count(*) AS n",
     "MATCH (r:Renewal) WHERE EXISTS { MATCH (r)-[:ON_PLAN]->(:Plan) RETURN r } RETURN count(*) AS n LIMIT 201"),
    ("MATCH (s:Subscription {subscription_id: 'sub_santosh'}) RETURN s.started_at, 'sub_santosh:2026-10-07' AS id",
     "MATCH (s:Subscription {subscription_id: 'sub_santosh'}) RETURN s.started_at, 'sub_santosh:2026-10-07' AS id "
     "LIMIT 201"),
])
def test_read_only_statements_pass_with_a_forced_limit(query, statement):
    g = cg.guard(query)
    assert g.statement == statement and g.limit <= cg.MAX_ROWS + 1
    assert bool(g.notes) == ("5000" in query)


def test_tokens_keep_strings_names_and_ranges_apart():
    toks = cg.tokenize("MATCH (`set`:Renewal) WHERE x.load = 'CREATE \\'x\\'' RETURN 1..3, 1.5 // DELETE")
    texts = [t.text for t in toks]
    assert "set" in texts and toks[2].kind == "ident"               # a back-ticked name is not a keyword
    assert "CREATE \\'x\\'" in texts                                  # a string is not a keyword either
    assert ["1", "..", "3"] == texts[texts.index("RETURN") + 1:texts.index("RETURN") + 4] and "1.5" in texts
    assert "DELETE" not in texts                                     # comments are dropped
    assert cg.guard("MATCH (`set`:Renewal) RETURN `set`.as_of").statement.endswith("LIMIT 201")
    assert cg.guard("MATCH (r:Renewal) WHERE r.plan_tier = 'CREATE' RETURN count(*)").limit == 201


# ------------------------------------------------------------------------------------------------ the tool
def test_the_tool_answers_from_the_evidence_graph_only(ctx):
    env = run(ctx, "MATCH (r:Renewal) RETURN count(r) AS n")
    assert set(env) == set(envelope.ENVELOPE_KEYS) and env["data"]["rows"] == [{"n": 121}]
    assert env["provenance"]["evidence_db_sha256"] == ctx.record["db"]["sha256"]
    assert env["provenance"]["sandboxed"] is False and "label-free" in env["provenance"]["pit_rule"]
    env = run(ctx, "MATCH (r:Renewal {renewal_id: 'sub_santosh:2026-10-07'})<-[:HAS_RENEWAL]-(s:Subscription)"
                   "-[e:HIT_LIMIT]->(h:LimitHit) RETURN e.event_date AS d, h.event_id AS id ORDER BY d")
    assert [r["d"] for r in env["data"]["rows"]] == sorted(r["d"] for r in env["data"]["rows"])
    flagged = run(ctx, "MATCH (r:Renewal)-[f:FIRST_RENEWAL_AFTER]->(p:PricingChange) WHERE f.declared_exception "
                       "RETURN count(*) AS n, min(f.known_by_as_of) AS known")["data"]["rows"]
    assert flagged == [{"n": 5, "known": False}]                      # PLAN 6.6 tiny: 5, all flagged
    late = run(ctx, "MATCH (s:Subscription)-[:HAS_RENEWAL]->(r:Renewal), (s)-[e:HIT_LIMIT|OPENED|EXPOSED_TO|BILLED"
                    "|CHARGED_OVERAGE|CHANGED_OVERAGE]->(x) WHERE e.event_date > r.as_of RETURN count(e) AS n")
    assert late["data"]["rows"] == [{"n": 0}]                         # physically cut at as_of


def test_the_row_cap_and_the_limit_rewrite_are_said(ctx):
    env = run(ctx, "MATCH (r:Renewal) RETURN r.renewal_id AS id ORDER BY id")
    assert env["data"]["row_count"] == 121 and env["data"]["more_rows"] is False
    env = run(ctx, "MATCH (s:Subscription)-[e]->(x) RETURN e.event_date AS d ORDER BY d")
    assert env["data"]["row_count"] == 200 and env["data"]["more_rows"] is True
    assert any("More than 200 rows matched" in c for c in env["caveats"])
    env = run(ctx, "MATCH (r:Renewal) RETURN r.renewal_id AS id ORDER BY id LIMIT 500")
    assert env["data"]["row_count"] == 121 and any("lowered to 200" in c for c in env["caveats"])


def test_binder_errors_come_back_with_the_nearest_schema_names(ctx):
    for query, near in (("MATCH (r:Renewl) RETURN r LIMIT 1", "Renewal"),
                        ("MATCH (r:Renewal) RETURN r.limit_hit_14d LIMIT 1", "limit_hits_14d"),
                        ("MATCH (s:Subscription)-[:HIT_LIMT]->(h) RETURN h LIMIT 1", "HIT_LIMIT"),
                        ("MATCH (s:Subscription) RETURN s.user_name LIMIT 1", "subscription_id")):
        with pytest.raises(cg.CypherRefused) as e:
            run(ctx, query)
        text = str(e.value)
        assert text.startswith("the statement does not bind: Binder exception") and near in text, text
        assert "node labels: " in text and "FIRST_RENEWAL_AFTER(Renewal->PricingChange)" in text
    hint = cg.repair_hint("Binder exception: Cannot find property outcome for r.", ctx.catalog)
    assert "label property: the evidence graph has none" in hint


def test_time_and_memory_limits_hold(ctx):
    heavy = "MATCH (a)-[*1..4]-(b) WITH a, b MATCH (c)-[*1..4]-(d) WHERE a.renewal_id < c.renewal_id RETURN count(*)"
    t0 = __import__("time").monotonic()
    with pytest.raises((ToolTimeout, cg.CypherRefused)) as e:
        run(ctx, heavy)
    took = __import__("time").monotonic() - t0
    assert took < 15, took                                   # the 5 s engine timeout (or the 128 MB pool) stops it
    assert "timed out" in str(e.value) or "memory" in str(e.value)
    assert run(ctx, "MATCH (r:Renewal) RETURN count(r) AS n")["data"]["rows"] == [{"n": 121}]   # still usable


def test_the_watchdog_fires_on_time_and_never_on_a_finished_query():
    class Conn:
        interrupted = 0

        def interrupt(self):
            Conn.interrupted += 1

    fired: list[str] = []
    with cg.Watchdog(Conn(), timeout_s=0.2, on_kill=fired.append) as w:
        __import__("time").sleep(1.6)                  # a "query" that ignores the interrupt
    assert w.fired == "the query ran past its time limit" and fired == [w.fired] and Conn.interrupted == 1
    fired.clear()
    with cg.Watchdog(Conn(), timeout_s=5, on_kill=fired.append) as w:
        pass
    assert w.fired is None and fired == []


def test_the_watchdog_ends_the_server_when_the_engine_does_not_stop(ev):
    """Measured on ladybug 0.21.1: UNWIND range(1, 300000000) ignores the 5 s timeout and conn.interrupt() and
    grows memory outside the buffer pool. The watchdog interrupts it, then ends the process (exit 75)."""
    code = ("import sys; from lakehouse_graph import cypher_guard as cg; "
            f"c = cg.CypherContext({str(ev[1])!r}, audit=False); "
            "cg.call(c, 'graph_cypher', {'query': 'UNWIND range(1, 300000000) AS x RETURN sum(x) AS s'}); "
            "print('NOT STOPPED')")
    t0 = __import__("time").monotonic()
    p = subprocess.run([sys.executable, "-c", code], cwd=REPO, env={**os.environ, "PYTHONPATH": str(REPO / "src")},
                       capture_output=True, text=True, timeout=60, check=False)
    took = __import__("time").monotonic() - t0
    assert p.returncode == cg.EXIT_WATCHDOG and "NOT STOPPED" not in p.stdout, (p.returncode, p.stderr[-300:])
    assert "graph_cypher watchdog:" in p.stderr and took < 30


def test_every_call_is_audited_without_the_query(ev, tmp_path):
    logs = tmp_path / "logs"
    logs.mkdir()
    c = cg.CypherContext(ev[1], logs_dir=logs, graph_root=ev[0], on_kill=_no_exit)
    try:
        run(c, "MATCH (r:Renewal) RETURN count(r) AS canaryzz")
        for q in ("LOAD FROM '/etc/hosts' RETURN *", "MATCH (r:Renewl) RETURN r LIMIT 1"):
            with pytest.raises(cg.CypherRefused):
                run(c, q)
        with pytest.raises(tools.ToolArgumentError, match="unknown argument"):
            cg.call(c, "graph_cypher", {"query": "RETURN 1", "colour": "x"})
    finally:
        c.close()
    lines = [json.loads(x) for f in logs.glob("audit-*.jsonl") for x in f.read_text().splitlines()]
    assert [x["outcome"] for x in lines] == ["ok", "refused", "refused", "invalid_arguments"]
    assert all(tuple(x) == envelope.AUDIT_FIELDS and x["tool"] == "graph_cypher" for x in lines)
    raw = "".join(f.read_text() for f in logs.glob("audit-*.jsonl"))
    assert "canaryzz" not in raw and "/etc/hosts" not in raw and lines[0]["args_key"] == "process"


def test_the_context_refuses_an_evidence_graph_it_cannot_vouch_for(ev, tmp_path):
    import shutil

    bad = tmp_path / "tiny" / "builds" / ev[1].name
    shutil.copytree(ev[1], bad)
    with open(bad / "evidence.lbdb", "ab") as f:
        f.write(b"\0")
    with pytest.raises(ProvenanceUnavailable, match="differs from the sha256"):
        cg.CypherContext(bad)
    (bad / "evidence.json").unlink()
    with pytest.raises(ProvenanceUnavailable, match="build_evidence_graph"):
        cg.CypherContext(bad)


# ------------------------------------------------------------------------------------------------ the gate
def test_the_gate_refuses_outside_the_sandbox(monkeypatch, ev):
    monkeypatch.delenv(mcp_server.UNSANDBOXED_CYPHER_ENV, raising=False)
    if sys.platform == "darwin":
        sandboxed, refusal = mcp_server.cypher_gate(ev[1])
        assert not sandboxed and "evidence-only macOS sandbox" in refusal
        assert "GRAPH_SANDBOXED is not 1" in refusal and "is readable" in refusal
        monkeypatch.setenv("GRAPH_SANDBOXED", "1")                  # the marker alone is never trusted
        sandboxed, refusal = mcp_server.cypher_gate(ev[1])
        assert not sandboxed and "not sandboxed (sandbox_check)" in refusal
    monkeypatch.setattr(mcp_server.sys, "platform", "linux")
    sandboxed, refusal = mcp_server.cypher_gate(ev[1])
    assert not sandboxed and mcp_server.UNSANDBOXED_CYPHER_ENV in refusal
    monkeypatch.setattr(mcp_server.sys, "platform", "freebsd14")
    assert "not supported" in mcp_server.cypher_gate(ev[1])[1]


def test_the_linux_override_prints_a_loud_banner(monkeypatch, capsys, ev):
    monkeypatch.setattr(mcp_server.sys, "platform", "linux")
    monkeypatch.setenv(mcp_server.UNSANDBOXED_CYPHER_ENV, "1")
    assert mcp_server.cypher_gate(ev[1]) == (False, "")
    err = capsys.readouterr().err
    assert "GUARDED RAW CYPHER WITHOUT AN OS SANDBOX" in err and err.count("#" * 100) == 2


def _server(*args, env=None, timeout=60):
    return subprocess.run([sys.executable, "-m", "lakehouse_graph.mcp_server", *args], cwd=REPO,
                          env={**os.environ, "PYTHONPATH": str(REPO / "src"), **(env or {})}, capture_output=True,
                          text=True, stdin=subprocess.DEVNULL, timeout=timeout, check=False)


def test_the_server_never_offers_cypher_by_default(ev):
    listed = anyio.run(mcp_server.build_server(list(tools.TOOLSETS), None).list_tools)
    assert "graph_cypher" not in [t.name for t in listed] and "graph_cypher" not in tools.SPECS
    p = _server("--toolset", "cypher", env={"GRAPH_ROOT": str(ev[0])})
    assert p.returncode == 2 and "only with --enable-cypher" in p.stderr and p.stdout == ""
    p = _server("--enable-cypher", "--toolset", "graph", env={"GRAPH_ROOT": str(ev[0])})
    assert p.returncode == 2 and "cypher toolset alone" in p.stderr
    p = _server("--enable-cypher", "--build", str(ev[1]), env={"GRAPH_ROOT": str(ev[0])})
    assert p.returncode == 3 and "refusing to start" in p.stderr and p.stdout == ""
    with pytest.raises(ValueError, match="served alone"):
        mcp_server.build_server(["graph", "cypher"], None)


@pytest.mark.anyio
async def test_the_cypher_server_in_process(ctx):
    server = mcp_server.build_server(["cypher"], ctx, timeout_s=10.0)
    async with Client(server) as c:
        listed = (await c.list_tools()).tools
        assert [t.name for t in listed] == ["graph_cypher"]
        t = listed[0]
        assert t.annotations.read_only_hint is True and t.annotations.destructive_hint is False
        assert t.input_schema["additionalProperties"] is False and t.input_schema["required"] == ["query"]
        assert "FIRST_RENEWAL_AFTER Renewal->PricingChange" in t.description and len(t.description) < 2500
        r = await c.call_tool("graph_cypher", {"query": "MATCH (p:Plan) RETURN p.plan_tier AS tier ORDER BY tier"})
        assert not r.is_error and json.loads(r.content[0].text) == r.structured_content
        assert [x["tier"] for x in r.structured_content["data"]["rows"]] == ["pro", "pro_plus", "ultra"]
        r = await c.call_tool("graph_cypher", {"query": "CALL timeout=0"})
        assert r.is_error and "refused by the Cypher guard" in r.content[0].text
        uris = sorted(str(x.uri) for x in (await c.list_resources()).resources)
        assert "graph://evidence-schema" in uris


# ------------------------------------------------------------------------------------------------ the OS layer
@pytest.mark.skipif(sys.platform != "darwin", reason="macOS sandbox")
def test_launcher_serves_cypher_only_in_its_evidence_only_sandbox(ev):
    p = subprocess.run([str(LAUNCH), "--enable-cypher", "--print-tools"], env=launcher_env(ev[0]),
                       capture_output=True, text=True, timeout=60, check=False, stdin=subprocess.DEVNULL)
    assert p.returncode == 0 and [t["name"] for t in json.loads(p.stdout)] == ["graph_cypher"], p.stderr
    p = subprocess.run([str(LAUNCH), "--enable-cypher", "--build", str(ev[1])],
                       env=launcher_env(ev[0], GRAPH_SANDBOX="0"), capture_output=True, text=True, timeout=60,
                       check=False, stdin=subprocess.DEVNULL)
    assert p.returncode == 64 and "only under the macOS sandbox" in p.stderr


@pytest.mark.skipif(sys.platform != "darwin", reason="macOS sandbox")
def test_the_sandbox_stops_a_guard_bypass(ev, tmp_path):
    """scripts/graph_sandbox_check.py --cypher-only --control: inside the cypher server's profile, with the engine
    opened directly (no guard), every read of a label-bearing file, every write outside the logs dir, the network
    and the extensions are denied by the OS; unsandboxed, the same label reads succeed; the MCP sessions answer,
    refuse the deny list and leave no unexpected denial."""
    report = tmp_path / "report.json"
    p = subprocess.run([sys.executable, str(REPO / "scripts/graph_sandbox_check.py"), "--cypher-only", "--control",
                        "--graph-root", str(ev[0]), "--build", str(ev[1]), "--json", str(report)],
                       capture_output=True, text=True, timeout=600, check=False, cwd=REPO,
                       env={**os.environ, "GRAPH_PY": sys.executable})
    assert p.returncode == 0, p.stdout[-3000:]
    doc = json.loads(report.read_text())
    labels = [r for r in doc["cypher_probe"]["results"] if r["group"] == "labels"]
    assert len(labels) == 6 and all(r["outcome"] == "denied" for r in labels)
    control = [r for r in doc["cypher_control"]["results"] if r["group"] == "labels"]
    assert len(control) == 6 and all(r["outcome"] == "allowed" for r in control)
