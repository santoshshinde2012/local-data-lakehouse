"""The Cytoscape.js evidence / lineage views: structure, determinism, point in time, escaping.

Pure tests use Maya's golden rows (PLAN 6.6, seed 42) in the evidence tool's row shape; the
build-backed ones read the session tiny / inject builds (read only) and a scratch build whose
bronze carries an HTML-hostile user_name. No browser is needed (the browser proofs are in the
ui-viz research; the page script is the one proven there).
"""
from __future__ import annotations

import base64
import hashlib
import json
import re
import shutil
import subprocess
import sys
from html.parser import HTMLParser
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest
from conftest import REPO
from test_lineage_build import core_build  # noqa: F401 (session fixture shared by the lineage tests)

from lakehouse_graph import build, oracle, queries, spec, viz

LIB = viz.load_vendored()
GOLD_REF = "gold.churn_renewal_features.limit_hits_14d"


def _quiet(*_args) -> None:
    return None


def run(script: str, *args: str) -> subprocess.CompletedProcess:
    """A repo script with this interpreter, bounded (a hung child must not hang the suite)."""
    return subprocess.run([sys.executable, str(REPO / "scripts" / script), *args], capture_output=True, text=True,
                          cwd=REPO, check=False, timeout=300)


# --------------------------------------------------------------------------- HTML helpers
class Doc(HTMLParser):
    """What the tests need: inline script / style bodies, meta tags, attributes by id, text."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.scripts: list[tuple[dict, str]] = []
        self.styles: list[str] = []
        self.meta: list[dict] = []
        self.by_id: dict[str, dict] = {}
        self.tags: list[str] = []
        self._cur: tuple[str, dict] | None = None
        self._buf: list[str] = []
        self.text: list[str] = []
        self.html_attrs: dict = {}

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        self.tags.append(tag)
        if tag == "html":
            self.html_attrs = a
        if "id" in a:
            self.by_id[a["id"]] = {"tag": tag, **a}
        if tag == "meta":
            self.meta.append(a)
        if tag in ("script", "style"):
            self._cur, self._buf = (tag, a), []

    def handle_endtag(self, tag):
        if self._cur and tag == self._cur[0]:
            body = "".join(self._buf)
            if tag == "script":
                self.scripts.append((self._cur[1], body))
            else:
                self.styles.append(body)
            self._cur = None

    def handle_data(self, data):
        if self._cur:
            self._buf.append(data)
        else:
            self.text.append(data)


def parse(html_text: str) -> Doc:
    doc = Doc()
    doc.feed(html_text)
    doc.close()
    return doc


def sha(text: str) -> str:
    return "'sha256-" + base64.b64encode(hashlib.sha256(text.encode()).digest()).decode() + "'"


def csp_of(doc: Doc) -> dict[str, list[str]]:
    content = next(m["content"] for m in doc.meta if m.get("http-equiv") == "Content-Security-Policy")
    return {part.split()[0]: part.split()[1:] for part in content.split(";") if part.strip()}


def payload_of(page: str) -> dict:
    return json.loads(next(body for attrs, body in parse(page).scripts if attrs.get("id") == "graph-data"))


def outside_data(page: str) -> str:
    """The page without the inlined library and without the JSON data element."""
    doc = parse(page)
    data = next(body for attrs, body in doc.scripts if attrs.get("id") == "graph-data")
    return page.replace(LIB, "").replace(data, "")


# --------------------------------------------------------------------------- Maya fixture (PLAN 6.6, seed 42)
MAYA_FOCUS = {"renewal_id": "sub_maya:2026-10-07", "subscription_id": "sub_maya", "as_of": "2026-09-30",
              "renewal_date": "2026-10-07", "plan_tier": "pro", "route": "score_today", "user_name": "Maya",
              "visibility": "today", "build_id": "abcdef012345"}


def _ev(day, rel, target, detail=None, feature=None, window=True, known=True, exception=False):
    return {"event_date": day, "relation": rel, "target_id": target, "detail": detail, "feeds_feature": feature,
            "in_feature_window": window, "known_by_as_of": known, "declared_exception": exception}


MAYA_EVIDENCE = [
    _ev("2026-08-15", "CUT_CAP", "cap-cut-2026-08", "via plan pro", "allowance_used_pct"),
    _ev("2026-08-25", "EXPOSED_TO", "inc-002", None, "incident_exposed_28d", window=False),
    _ev("2026-09-09", "EXPOSED_TO", "inc-003", None, "incident_exposed_28d"),
    _ev("2026-09-20", "CUT_CAP", "cap-cut-2026-09", "via plan pro", "allowance_used_pct"),
    _ev("2026-09-20", "FIRST_RENEWAL_AFTER", "cap-cut-2026-09", None, "first_renewal_after_pricing_change"),
    _ev("2026-09-24", "HIT_LIMIT", "lh:sub_maya:001", "weekly", "limit_hits_14d"),
    _ev("2026-09-25", "HIT_LIMIT", "lh:sub_maya:002", "weekly", "limit_hits_14d"),
    _ev("2026-09-27", "HIT_LIMIT", "lh:sub_maya:003", "weekly", "limit_hits_14d"),
]
MAYA_NEIGHBOURS = [
    {"rank": 1, "renewal_id": "sub_07200:2026-08-17", "d2_q": 5099099315, "outcome": "voluntary_lapse"},
    {"rank": 2, "renewal_id": "sub_06614:2026-08-21", "d2_q": 5593304415, "outcome": "renewed"},
    {"rank": 3, "renewal_id": "sub_01541:2026-08-16", "d2_q": 5616850275, "outcome": "renewed"},
    {"rank": 4, "renewal_id": "sub_04760:2026-09-09", "d2_q": 6300541441, "outcome": "renewed"},
    {"rank": 5, "renewal_id": "sub_01355:2026-09-05", "d2_q": 6604184020, "outcome": "voluntary_lapse"},
    {"rank": 6, "renewal_id": "sub_05564:2026-08-20", "d2_q": 7250182869, "outcome": "renewed"},
    {"rank": 7, "renewal_id": "sub_01475:2026-08-19", "d2_q": 7362875678, "outcome": "renewed"},
    {"rank": 8, "renewal_id": "sub_04856:2026-08-22", "d2_q": 7371318997, "outcome": "renewed"},
    {"rank": 9, "renewal_id": "sub_01888:2026-08-23", "d2_q": 7506448249, "outcome": "renewed"},
    {"rank": 10, "renewal_id": "sub_00228:2026-08-03", "d2_q": 7713654324, "outcome": "renewed"},
]
MAYA_HUBS = [{"id": "inc-002", "note": "837 renewals exposed"}, {"id": "cap-cut-2026-09", "note": "cap x0.83"}]
LINEAGE_EDGES = [
    {"from": "silver.churn_limit_events.hit_date", "to": GOLD_REF, "rel": "DERIVED_FROM", "roles": "WINDOW_BOUND"},
    {"from": "bronze.churn_limit_events_raw.hit_at", "to": "silver.churn_limit_events.hit_date", "rel": "DERIVED_FROM",
     "roles": "VALUE"},
    {"from": "silver.churn_limit_events.subscription_id", "to": GOLD_REF, "rel": "DERIVED_FROM", "roles": "JOIN_KEY"},
    {"from": "bronze.churn_limit_events_raw.subscription_id", "to": "silver.churn_limit_events.subscription_id",
     "rel": "DERIVED_FROM", "roles": "VALUE"},
]


def maya() -> viz.Graph:
    return viz.build_evidence_graph(MAYA_FOCUS, MAYA_EVIDENCE, MAYA_NEIGHBOURS, MAYA_HUBS)


# --------------------------------------------------------------------------- vendored library
def test_vendored_library_is_the_pinned_release_with_its_licence():
    raw = viz.CYTOSCAPE_FILE.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == viz.CYTOSCAPE_SHA256
    assert "sha384-" + base64.b64encode(hashlib.sha384(raw).digest()).decode() == viz.CYTOSCAPE_SRI
    assert b"Permission is hereby granted" in raw[:1200]            # the MIT notice travels with the file
    assert f'"{viz.CYTOSCAPE_VERSION}"'.encode() in raw
    assert b"</script" not in raw.lower() and b"<!--" not in raw
    licence = (viz.VENDOR_DIR / "cytoscape.LICENSE").read_text()
    assert "The Cytoscape Consortium" in licence and "Permission is hereby granted" in licence
    readme = (viz.VENDOR_DIR / "README.md").read_text()
    assert viz.CYTOSCAPE_SHA256 in readme and viz.CYTOSCAPE_SRI in readme and viz.CYTOSCAPE_VERSION in readme


def test_load_vendored_refuses_other_bytes(tmp_path):
    bad = tmp_path / "cytoscape.min.js"
    bad.write_text("alert(1)")
    with pytest.raises(ValueError, match="sha256"):
        viz.load_vendored(bad)


def test_render_html_inlines_only_the_pinned_library():
    """The page names 3.34.3 (licence notice, generator meta, injected-style hash): other bytes are refused."""
    for other in ("alert(1)", LIB + "\n", LIB.replace("3.34.3", "3.34.4")):
        with pytest.raises(ValueError, match=r"not the pinned Cytoscape\.js 3\.34\.3"):
            viz.render_html(maya(), cytoscape_js=other)


def test_licence_notice_is_in_every_page():
    for mode in ("inline", "cdn"):
        page = viz.render_html(maya(), cytoscape_js=LIB if mode == "inline" else None, js_mode=mode)
        assert viz.LICENCE_NOTICE in page
        assert "Drawn with Cytoscape.js 3.34.3 (MIT, \u00a9 The Cytoscape Consortium)" in page
        gen = next(m for m in parse(page).meta if m.get("name") == "generator")
        assert "cytoscape 3.34.3 (MIT)" in gen["content"]
    assert "Permission is hereby granted" in viz.render_html(maya(), cytoscape_js=LIB)   # inline: full notice


# --------------------------------------------------------------------------- determinism and structure
def test_deterministic_bytes_and_input_order_independence():
    a = viz.render_html(maya(), cytoscape_js=LIB)
    b = viz.render_html(viz.build_evidence_graph(MAYA_FOCUS, list(reversed(MAYA_EVIDENCE)),
                                                 list(reversed(MAYA_NEIGHBOURS)), list(reversed(MAYA_HUBS))),
                        cytoscape_js=LIB)
    assert a == b and a.encode() == viz.render_html(maya(), cytoscape_js=LIB).encode()
    rest = a.replace(LIB, "")
    assert not re.search(r"\d{2}:\d{2}:\d{2}", rest)                    # no clock time
    assert not re.search(r"/(Users|home|tmp|private)/", rest)          # no absolute path


def test_structure_counts_and_golden_positions():
    g = maya()
    kinds: dict[str, int] = {}
    for n in g.nodes:
        kinds[n["kind"]] = kinds.get(n["kind"], 0) + 1
    assert kinds == {"FocusRenewal": 1, "Renewal": 10, "LimitHit": 3, "Subscription": 1, "Plan": 1,
                     "Incident": 2, "PricingChange": 2, "Marker": 8}
    rel = sorted(e["relation"] for e in g.edges)
    assert rel.count("SIMILAR_TO") == 10 and rel.count("HIT_LIMIT") == 3 and rel.count("EXPOSED_TO") == 2
    assert rel.count("CUT_CAP") == 2 and rel.count("FIRST_RENEWAL_AFTER") == 1 and rel.count("HAS_RENEWAL") == 1
    pos = {n["id"]: (n["x"], n["y"]) for n in g.nodes}
    as_of_x = pos["axis:asof:top"][0]
    for n in g.nodes:                          # point-in-time picture: every event is left of (or on) as_of
        if n["kind"] in viz.EVENT_LANES or n["kind"] in ("Incident", "PricingChange"):
            assert n["x"] <= as_of_x, n["id"]
    assert pos["sub_maya:2026-10-07"][0] > as_of_x
    assert all(isinstance(v, int) for xy in pos.values() for v in xy)
    assert len(g.rows) == 20                   # HAS_RENEWAL + ON_PLAN + 8 evidence + 10 neighbours
    assert "abcdef012345" in g.summary and "2 lapsed" in g.summary and "not a risk estimate" in g.summary
    assert ": 8 evidence rows on or before as_of 2026-09-30, and its 10 most similar renewals (" in g.summary
    assert "declared exception" not in g.summary            # Maya's FIRST_RENEWAL_AFTER (09-20) is before as_of


def test_payload_is_valid_cytoscape_json_and_matches_graph():
    page = viz.render_html(maya(), cytoscape_js=LIB)
    attrs, body = next(s for s in parse(page).scripts if s[0].get("id") == "graph-data")
    assert attrs["type"] == "application/json"
    payload = json.loads(body)
    assert payload["spec"] == viz.VIZ_SPEC and payload["options"]["layout"]["name"] == "preset"
    ids = [n["data"]["id"] for n in payload["elements"]["nodes"]]
    assert ids == sorted(ids) and len(ids) == len(set(ids))
    for e in payload["elements"]["edges"]:
        assert e["data"]["source"] in set(ids) and e["data"]["target"] in set(ids)
    assert all(set(n["position"]) == {"x", "y"} for n in payload["elements"]["nodes"])
    assert "layout:{name:\"preset\"}" in viz._APP_JS                   # never omitted, never 'null'


def test_csp_hashes_cover_exactly_the_inline_code():
    doc = parse(viz.render_html(maya(), cytoscape_js=LIB))
    csp = csp_of(doc)
    assert csp["default-src"] == ["'none'"]
    executable = [body for attrs, body in doc.scripts if attrs.get("type") != "application/json"]
    assert len(executable) == 2 and csp["script-src"] == [sha(b) for b in executable]
    assert csp["style-src"] == [sha(doc.styles[0]), sha(viz.CYTOSCAPE_INJECTED_STYLE)]
    everything = " ".join(v for values in csp.values() for v in values)
    assert "'unsafe-inline'" not in everything and "'unsafe-eval'" not in everything
    assert "connect-src" not in csp and csp["img-src"] == ["data:"]
    # the injected-style constant must be what this library version really inserts
    assert 's="__________cytoscape_container"' in LIB and 'u.textContent="."+s+" { position: relative; }"' in LIB


def test_cdn_mode_is_pinned_with_sri_and_relative_mode_has_no_integrity():
    cdn = parse(viz.render_html(maya(), js_mode="cdn"))
    tag = next(a for a, _ in cdn.scripts if a.get("src"))
    assert tag["src"] == f"https://cdn.jsdelivr.net/npm/cytoscape@{viz.CYTOSCAPE_VERSION}/dist/cytoscape.min.js"
    assert tag["integrity"] == viz.CYTOSCAPE_SRI and tag["crossorigin"] == "anonymous"
    script_src = csp_of(cdn)["script-src"]
    assert script_src[0] == viz.CYTOSCAPE_CDN == tag["src"] and len(script_src) == 2    # that one file, not the host
    assert "https://cdn.jsdelivr.net" not in script_src and not any(s.endswith("/") for s in script_src)
    rel = parse(viz.render_html(maya(), js_mode="relative"))
    tag = next(a for a, _ in rel.scripts if a.get("src"))
    assert tag["src"] == "cytoscape.min.js" and "integrity" not in tag
    assert len(viz.render_html(maya(), js_mode="cdn").encode()) < 40_000
    inline = viz.render_html(maya(), cytoscape_js=LIB)
    assert "src=" not in "".join(t for t in re.findall(r"<script[^>]*>", inline))   # inline: no network at all


def test_accessibility_contract():
    page = viz.render_html(maya(), cytoscape_js=LIB)
    doc = parse(page)
    assert doc.html_attrs["lang"] == "en"
    cy = doc.by_id["cy"]
    assert cy["role"] == "img" and cy["aria-labelledby"] == "viz-title"
    assert set(cy["aria-describedby"].split()) == {"viz-summary", "viz-table-caption"}
    assert doc.by_id["cy-detail"]["aria-live"] == "polite"
    for bid in ("btn-fit", "btn-in", "btn-out"):
        assert doc.by_id[bid]["tag"] == "button"
    assert doc.by_id["btn-in"]["aria-label"] == "Zoom in"
    assert page.count("<th scope=\"col\">") == 4 and "<caption" in page
    assert page.count("<tr><td>") == len(maya().rows) == 20            # every edge also exists as text
    assert "<noscript>" in page
    text = " ".join(doc.text)
    for item in ("Renewal being explained (1)", "Similar renewal (neighbour) (10)", "Limit hit (3)"):
        assert item in text


def test_theme_attribute_and_palette_tokens():
    for theme in viz.THEMES:
        assert parse(viz.render_html(maya(), cytoscape_js=LIB, theme=theme)).html_attrs["data-theme"] == theme
    css = parse(viz.render_html(maya(), cytoscape_js=LIB)).styles[0]
    assert "prefers-color-scheme:dark" in css and ':root[data-theme="dark"]' in css
    for mode in ("light", "dark"):
        for colour in viz.PALETTES[mode].values():
            assert colour in css
    with pytest.raises(ValueError, match="theme"):
        viz.render_html(maya(), cytoscape_js=LIB, theme="blue")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="cose"):
        viz.render_html(maya(), cytoscape_js=LIB, layout="cose")  # type: ignore[arg-type]


def test_estimate_height_tracks_rows():
    g = maya()
    assert viz.estimate_height(g, height=520, table_open=False) == 520 + 330 + 40
    assert viz.estimate_height(g, height=520, table_open=True) == 520 + 330 + 70 + 30 * 20


# --------------------------------------------------------------------------- escaping
HOSTILE = "</script><script>window.__xss=1</script><img src=x onerror=window.__xss=2> &amp;'\"<!--"


def hostile_graph() -> viz.Graph:
    focus = {**MAYA_FOCUS, "plan_tier": HOSTILE, "user_name": HOSTILE}
    evidence = [_ev("2026-09-24", "HIT_LIMIT", HOSTILE, detail=HOSTILE)]
    neighbours = [{"rank": 1, "renewal_id": "sub_x:2026-08-17", "outcome": HOSTILE, "d2_q": HOSTILE}]
    g = viz.build_evidence_graph(focus, evidence, neighbours, [{"id": "inc-002", "note": HOSTILE}])
    return viz.Graph(g.nodes, g.edges, g.rows, HOSTILE, HOSTILE)


def test_hostile_strings_cannot_leave_their_context():
    page = viz.render_html(hostile_graph(), cytoscape_js=LIB)
    doc = parse(page)
    assert len(doc.scripts) == 3 and doc.tags.count("img") == 0          # only the three scripts we wrote
    executable = [body for attrs, body in doc.scripts if attrs.get("type") != "application/json"]
    assert csp_of(doc)["script-src"] == [sha(b) for b in executable]
    data = next(body for attrs, body in doc.scripts if attrs.get("id") == "graph-data")
    assert "<" not in data and ">" not in data and "&" not in data
    labels = [n["data"]["label"] for n in json.loads(data)["elements"]["nodes"]]
    assert any("</script><script>window.__xss=1</script>" in lab for lab in labels)   # round-trips as text
    rest = outside_data(page)
    assert "<script>window.__xss=1</script>" not in rest and "<img" not in rest and "<!--" not in rest
    assert "&lt;/script&gt;&lt;script&gt;window.__xss=1&lt;/script&gt;" in rest


def test_plural():
    assert (viz.plural(0, "row"), viz.plural(1, "row"), viz.plural(2, "row")) == ("0 rows", "1 row", "2 rows")
    assert viz.plural(1, "renewal was", "renewals were") == "1 renewal was"
    assert viz.plural(3, "renewal was", "renewals were") == "3 renewals were"


def test_clean_drops_control_and_format_characters_and_caps_length():
    assert viz.clean("a\nb\rc\td\u2028e") == "a b c d e"
    assert viz.clean("x\u202ey\u200bz\x00\x1b") == "xyz"                       # bidi override, zero-width, C0
    long = viz.clean("x" * 500)
    assert len(long) == 200 and long.endswith("\u2026")
    assert viz.label("a\nb", "c") == "a b\nc"                                # only label() adds line breaks


# --------------------------------------------------------------------------- point in time
def test_post_as_of_evidence_is_refused():
    late = _ev("2026-10-01", "HIT_LIMIT", "lh:sub_maya:004", "weekly", "limit_hits_14d")
    with pytest.raises(ValueError, match="after as_of"):
        viz.build_evidence_graph(MAYA_FOCUS, [*MAYA_EVIDENCE, late])
    outcome = _ev("2026-09-29", "BILLED", "bill:sub_maya:001", "invoice_paid")
    with pytest.raises(ValueError, match="outcome evidence"):
        viz.build_evidence_graph(MAYA_FOCUS, [outcome])
    undeclared = _ev("2026-10-02", "FIRST_RENEWAL_AFTER", "cap-cut-2026-10", known=False, exception=False)
    with pytest.raises(ValueError, match="declared_exception"):
        viz.build_evidence_graph(MAYA_FOCUS, [undeclared])
    ok = viz.build_evidence_graph(MAYA_FOCUS, [_ev("2026-09-29", "BILLED", "bill:sub_maya:001", "cancel_scheduled")])
    assert any(n["kind"] == "BillingEvent" for n in ok.nodes)


def test_declared_exception_is_the_only_node_right_of_the_as_of_line():
    focus = {**MAYA_FOCUS, "renewal_id": "sub_00003:2026-08-20", "subscription_id": "sub_00003",
             "as_of": "2026-08-13", "renewal_date": "2026-08-20", "route": "model", "visibility": "source_as_of"}
    exc = _ev("2026-08-15", "FIRST_RENEWAL_AFTER", "cap-cut-2026-08", None, "first_renewal_after_pricing_change",
              known=False, exception=True)
    g = viz.build_evidence_graph(focus, [_ev("2026-08-10", "HIT_LIMIT", "lh:sub_00003:001"), exc])
    pos = {n["id"]: n for n in g.nodes}
    as_of_x = pos["axis:asof:top"]["x"]
    right = [n["id"] for n in g.nodes if n["kind"] not in ("FocusRenewal", "Marker", "Plan") and n["x"] > as_of_x]
    assert right == ["cap-cut-2026-08"]
    edge = next(e for e in g.edges if e["relation"] == "FIRST_RENEWAL_AFTER")
    assert edge["exception"] is True and edge["label"] == "FIRST_RENEWAL_AFTER (declared exception)"
    row = next(r for r in g.rows if r["relation"] == "FIRST_RENEWAL_AFTER")
    assert "declared exception (gold rule)" in row["note"] and "not known by as_of" in row["note"]
    # the exception is counted once, after as_of, never among the rows "on or before" it
    assert ": 1 evidence row on or before as_of 2026-08-13 plus 1 declared exception after it (" in g.summary
    two = viz.build_evidence_graph(focus, [exc, {**exc, "target_id": "cap-cut-2026-08b"}])
    assert ": 0 evidence rows on or before as_of 2026-08-13 plus 2 declared exceptions after it (" in two.summary


def test_historical_source_never_draws_a_later_neighbour_outcome():
    focus = {**MAYA_FOCUS, "route": "model", "visibility": "source_as_of"}
    late = [{"rank": 1, "renewal_id": "sub_x:2026-10-20", "outcome": "voluntary_lapse",
             "outcome_observed_on": "2026-10-15"}]
    with pytest.raises(ValueError, match="after the source's as_of"):
        viz.build_evidence_graph(focus, MAYA_EVIDENCE, late)
    masked = [{"rank": 1, "renewal_id": "sub_x:2026-10-20", "outcome": queries.NOT_YET_OBSERVED,
               "outcome_observed_on": None}]
    g = viz.build_evidence_graph(focus, MAYA_EVIDENCE, masked)
    assert next(n for n in g.nodes if n["id"] == "sub_x:2026-10-20")["outcome"] == queries.NOT_YET_OBSERVED
    assert "1 not yet observed" in g.summary and "at the source as_of" in g.summary


def test_parallel_evidence_rows_collapse_to_one_counted_edge():
    ev = [_ev(d, "EXPOSED_TO", "inc-002", window=False) for d in ("2026-08-25", "2026-08-26", "2026-08-27")]
    g = viz.build_evidence_graph(MAYA_FOCUS, ev)
    edge = next(e for e in g.edges if e["relation"] == "EXPOSED_TO")
    assert (edge["count"], edge["first"], edge["last"], edge["label"]) == (3, "2026-08-25", "2026-08-27",
                                                                           "EXPOSED_TO \u00d73")
    assert len([r for r in g.rows if r["relation"] == "EXPOSED_TO"]) == 3     # the table keeps every row


def test_validation_rejects_bad_graphs():
    g = maya()
    with pytest.raises(ValueError, match="dangling"):
        viz.render_html(viz.Graph(g.nodes[:3], g.edges, g.rows, g.title, g.summary), cytoscape_js=LIB)
    with pytest.raises(ValueError, match="unknown evidence relation"):
        viz.build_evidence_graph(MAYA_FOCUS, [_ev("2026-09-01", "DROP_TABLE", "x")])
    with pytest.raises(ValueError, match="needs cytoscape_js"):
        viz.render_html(g)
    for bad in ("sub_maya", "sub_maya:2026-10-07 ", "SUB_X:2026-01-01", None, 42, "sub_x:2026-1-1"):
        with pytest.raises(ValueError, match="invalid renewal id"):
            viz.check_renewal_id(bad)


# --------------------------------------------------------------------------- lineage layout
def test_layered_lineage_layout_is_a_dag_left_to_right():
    g = viz.build_layered_graph(LINEAGE_EDGES, GOLD_REF, title="t", summary="s")
    x = {n["id"]: n["x"] for n in g.nodes}
    for e in g.edges:                                   # data flows left to right
        assert x[e["source"]] < x[e["target"]]
    assert next(n for n in g.nodes if n["id"] == GOLD_REF)["kind"] == "TraceTarget"
    assert x[GOLD_REF] == max(x.values())
    # a cycle (not expected in lineage) falls back to the trace depth instead of failing
    cyc = viz.build_layered_graph([{"from": "a", "to": "b", "depth": 1}, {"from": "b", "to": "a", "depth": 2}],
                                  "a", title="t", summary="s")
    assert {n["id"] for n in cyc.nodes} == {"a", "b"}


# --------------------------------------------------------------------------- real builds (read only)
@pytest.fixture(scope="module")
def tiny_ctx(tiny_build):
    with viz.VizContext(tiny_build[0]) as ctx:
        yield ctx


def _post_as_of_targets(bdir: Path) -> dict[str, set[str]]:
    """Per renewal: every event node id dated after its as_of, and every BILLED outcome-evidence id."""
    t = oracle.load_tables(bdir)
    ren = t["Renewal"].set_index("subscription_id")
    out: dict[str, set[str]] = {}
    for rel in spec.EVENT_RELATIONS:
        e = t[rel]
        e = e.assign(renewal_id=e["src"].map(ren["renewal_id"]), as_of=e["src"].map(ren["as_of"]))
        hidden = e["event_date"] > e["as_of"]
        if rel == "BILLED":
            hidden |= e["outcome_evidence"]
        for rid, dst in zip(e.loc[hidden, "renewal_id"], e.loc[hidden, "dst"], strict=True):
            if rel != "EXPOSED_TO":             # incident ids are hubs; their pre-as_of days may be drawn
                out.setdefault(rid, set()).add(dst)
    return out


def test_evidence_data_is_exactly_what_the_tool_templates_serve(tiny_build, tiny_ctx):
    bdir, _ = tiny_build
    t = oracle.load_tables(bdir)
    rid = oracle.hero_renewal(t)
    data = viz.evidence_data(tiny_ctx, rid)
    assert data["evidence"] == queries.evidence(tiny_ctx.graph_conn, rid) == oracle.evidence(t, rid)
    visible = queries.fetch(tiny_ctx.graph_conn, "similar_top_k_visible", {"renewal_id": rid, "k": 10, "today": True})
    assert [(n["rank"], n["renewal_id"], n["outcome"]) for n in data["neighbours"]] == \
        [(n["rank"], n["renewal_id"], n["outcome"]) for n in visible]
    f = data["focus"]
    assert f["visibility"] == "today" and f["route"] == "score_today" and "city" not in f
    assert f["build_id"] == tiny_build[1]["business_build_id"] and f["user_name"]
    assert not data["truncated"] and f"plan:{f['plan_tier']}" in {h["id"] for h in data["hubs"]}
    # the shared header row carries the city; the view drops it (never drawn, never in the data JSON)
    head = queries.fetch(tiny_ctx.graph_conn, "renewal_header", {"renewal_id": rid})[0]
    cities = set(pq.read_table(bdir / "parquet/nodes_Subscription.parquet", columns=["city"]).column(0).to_pylist())
    page, _ = viz.evidence_view(tiny_ctx, rid, js_mode="cdn")         # no inlined library text to search
    assert head["city"] in cities and not [c for c in cities if c in page]


def test_no_post_as_of_or_outcome_evidence_in_any_tiny_view(tiny_build, tiny_ctx):
    """Every one of the 121 renewals: nothing dated after its as_of (except the flagged declared
    exception) and no BILLED outcome evidence reaches the page, although the graph holds both."""
    bdir, _ = tiny_build
    hidden = _post_as_of_targets(bdir)
    assert sum(len(v) for v in hidden.values()) > 100                  # the leak surface is really there
    renewals = pq.read_table(bdir / "parquet/nodes_Renewal.parquet", columns=["renewal_id"]).column(0).to_pylist()
    exceptions = historical_with_exception = 0
    for rid in sorted(renewals):
        data = viz.evidence_data(tiny_ctx, rid)
        as_of = data["focus"]["as_of"]
        after = 0
        for r in data["evidence"]:
            if r["event_date"] > as_of:
                assert r["relation"] == "FIRST_RENEWAL_AFTER" and r["declared_exception"], (rid, r)
                after += 1
            assert not (r["relation"] == "BILLED" and r["detail"] != "cancel_scheduled"), (rid, r)
        exceptions += after
        historical_with_exception += bool(after and data["focus"]["visibility"] == "source_as_of")
        graph = viz.build_evidence_graph(data["focus"], data["evidence"], data["neighbours"], data["hubs"])
        # the summary counts what is drawn on each side of the as_of line
        said = (f": {viz.plural(len(data['evidence']) - after, 'evidence row')} on or before as_of {as_of}"
                + (f" plus {viz.plural(after, 'declared exception')} after it (" if after else ", and its "))
        assert said in graph.summary, (rid, graph.summary)
        for h in data["hubs"]:
            assert not re.search(r"\b1 (renewals|days)\b", h["note"]), h
        page = viz.render_html(graph, js_mode="cdn")
        leaked = sorted(x for x in hidden.get(rid, ()) if x in page)
        assert not leaked, (rid, leaked)
    assert exceptions > 0 and historical_with_exception > 0            # tiny has declared exceptions too


def test_historical_views_mask_outcomes_observed_after_their_as_of(tiny_build, tiny_ctx):
    t = oracle.load_tables(tiny_build[0])
    r = t["Renewal"].set_index("renewal_id")
    rid = "sub_00000:2026-07-27"
    data = viz.evidence_data(tiny_ctx, rid)
    assert data["focus"]["visibility"] == "source_as_of"
    as_of = r.at[rid, "as_of"]
    masked = 0
    for n in data["neighbours"]:
        observed = r.at[n["renewal_id"], "outcome_observed_on"]
        if pd.isna(observed) or observed > as_of:
            assert n["outcome"] == queries.NOT_YET_OBSERVED and n["outcome_observed_on"] is None
            masked += 1
        else:
            assert n["outcome"] == r.at[n["renewal_id"], "outcome"]
    assert masked > 0
    with pytest.raises(ValueError, match="only valid for current sources"):
        viz.evidence_data(tiny_ctx, rid, outcome_visibility="today")


def test_unknown_renewal_is_a_clear_error(tiny_ctx):
    with pytest.raises(viz.UnknownRenewal, match="unknown renewal"):
        viz.evidence_data(tiny_ctx, "sub_nobody:2026-10-07")
    with pytest.raises(ValueError, match="invalid renewal id"):
        viz.evidence_data(tiny_ctx, "sub_maya:2026-10-07' OR 1=1")


def test_evidence_view_is_deterministic_and_embeddable(tiny_ctx):
    page, height = viz.evidence_view(tiny_ctx, "sub_maya:2026-10-07")
    again, _ = viz.evidence_view(tiny_ctx, "sub_maya:2026-10-07")
    assert page == again and isinstance(height, int) and height > 560
    embedded, h2 = viz.evidence_view(tiny_ctx, "sub_maya:2026-10-07", theme="dark", embed=True)
    assert payload_of(embedded)["options"]["wheelZoom"] is False and payload_of(page)["options"]["wheelZoom"] is True
    assert "<details open>" in page and "<details>" in embedded and h2 < height
    assert parse(embedded).html_attrs["data-theme"] == "dark"


def test_evidence_rows_over_the_cap_are_cut_and_said_so(tiny_ctx, monkeypatch):
    """Over EVIDENCE_ROWS (the envelope's 200-row cap) the earliest rows are drawn, the page says the
    rest were cut, and the truncated Graph keeps everything else (legend note, table head)."""
    rid = "sub_maya:2026-10-07"
    full = viz.evidence_data(tiny_ctx, rid)
    page_full, _ = viz.evidence_view(tiny_ctx, rid)
    assert not full["truncated"] and len(full["evidence"]) > 3 and "Only the first" not in page_full
    monkeypatch.setattr(viz, "EVIDENCE_ROWS", 3)
    cut = viz.evidence_data(tiny_ctx, rid)
    assert cut["truncated"] and cut["evidence"] == full["evidence"][:3]               # the earliest rows, by date
    page, height = viz.evidence_view(tiny_ctx, rid)
    doc = parse(page)
    summary = " ".join(doc.text)
    assert "Only the first 3 evidence rows (by date) are drawn." in summary
    assert ": 3 evidence rows on or before as_of " in summary
    n_rows = 2 + 3 + len(cut["neighbours"])                  # HAS_RENEWAL + ON_PLAN + 3 evidence + neighbours
    assert page.count("<tr><td>") == n_rows and height == viz.estimate_height(
        viz.build_evidence_graph(cut["focus"], cut["evidence"], cut["neighbours"], cut["hubs"]))
    assert page.count("<th scope=\"col\">") == 4 and "<th scope=\"col\">Date</th>" in page
    assert "vertical dashed line = as_of" in summary                                  # legend note kept
    dropped = {r["target_id"] for r in full["evidence"][3:]} - {r["target_id"] for r in cut["evidence"]}
    assert dropped and not any(f'"id":"{t}"' in page for t in dropped)               # cut rows are not drawn


def test_lineage_view_without_lineage_tables_is_a_clear_error(tiny_build, tmp_path):
    from lakehouse_graph.lineage import tools as ltools

    bdir, _ = tiny_build
    assert not (bdir / "lineage.lbdb").exists()
    with viz.VizContext(bdir) as ctx, pytest.raises(ltools.LineageUnavailable, match=r"no lineage\.lbdb"):
        viz.lineage_view(ctx, GOLD_REF)
    out = tmp_path / "l.html"
    p = run("graph_viz.py", "--lineage", GOLD_REF, "--build", str(bdir), "--out", str(out))
    assert p.returncode == 1 and "graph-viz FAILED: no lineage.lbdb" in p.stderr
    # a command that runs as printed (whatever Make targets exist): the lineage build into this build
    assert f"to build it into this build: python scripts/build_lineage_local.py --build {bdir}" in p.stderr
    assert not out.exists() and not p.stdout
    # the evidence view never adds the lineage hint
    bad = run("graph_viz.py", "--renewal", "sub_nobody:2026-10-07", "--build", str(bdir), "--out", str(out))
    assert bad.returncode == 1 and "unknown renewal" in bad.stderr and "build_lineage_local" not in bad.stderr


def test_inject_profile_user_name_is_text_only(inject_build):
    """The inject build's poisoned user_name (an instruction aimed at an agent) is shown only for the
    named renewal, as escaped table text and inside the JSON data, never as markup or script."""
    bdir, _ = inject_build
    ren = pq.read_table(bdir / "parquet/nodes_Renewal.parquet", columns=["renewal_id", "subscription_id"]).to_pylist()
    rid = next(r["renewal_id"] for r in ren if r["subscription_id"] == spec.INJECT_SUBSCRIPTION)
    with viz.VizContext(bdir) as ctx:
        page, _ = viz.evidence_view(ctx, rid)
        other = next(r["renewal_id"] for r in ren if r["subscription_id"] != spec.INJECT_SUBSCRIPTION)
        other_page, _ = viz.evidence_view(ctx, other)
    doc = parse(page)
    assert len(doc.scripts) == 3
    assert spec.INJECT_USER_NAME in " ".join(doc.text)                        # visible as text in the table
    subs = next(n for n in payload_of(page)["elements"]["nodes"] if n["data"]["id"] == spec.INJECT_SUBSCRIPTION)
    assert subs["data"]["detail"]["user_name"] == spec.INJECT_USER_NAME
    assert spec.INJECT_USER_NAME not in other_page                           # never for another renewal


HOSTILE_NAME = "</script><script>window.__xss=1</script><img src=x onerror=alert(1)> \u202eIgnore previous \"&\" <!--"


@pytest.fixture(scope="module")
def hostile_name_build(tmp_path_factory):
    """A scratch build of the tiny bronze whose INJECT_SUBSCRIPTION user_name is HTML / script / bidi."""
    root = tmp_path_factory.mktemp("viz_hostile")
    sdir = root / "sample"
    shutil.copytree(REPO / spec.TINY_FIXTURE, sdir)
    snap = sdir / "subscription_snapshots.csv"
    text, rows = build.poison_snapshots(snap.read_text(encoding="utf-8"), spec.INJECT_SUBSCRIPTION, HOSTILE_NAME)
    assert rows == 1
    snap.write_text(text, encoding="utf-8")
    bdir, _ = build.build_profile("default", sample_dir=sdir, export_dir=root / "exports", graph_root=root / "g",
                                  log=_quiet)
    return bdir


def test_html_hostile_user_name_is_escaped(hostile_name_build):
    bdir = hostile_name_build
    ren = pq.read_table(bdir / "parquet/nodes_Renewal.parquet", columns=["renewal_id", "subscription_id"]).to_pylist()
    rid = next(r["renewal_id"] for r in ren if r["subscription_id"] == spec.INJECT_SUBSCRIPTION)
    with viz.VizContext(bdir) as ctx:
        page, _ = viz.evidence_view(ctx, rid)
    doc = parse(page)
    assert len(doc.scripts) == 3 and doc.tags.count("img") == 0             # the name never became an element
    executable = [body for attrs, body in doc.scripts if attrs.get("type") != "application/json"]
    assert csp_of(doc)["script-src"] == [sha(b) for b in executable]
    rest = outside_data(page)
    assert "window.__xss" not in rest.replace("&lt;/script&gt;&lt;script&gt;window.__xss=1&lt;/script&gt;", "")
    assert "&lt;img src=x onerror=alert(1)&gt;" in rest and "<!--" not in rest and "\u202e" not in page
    data = next(body for attrs, body in doc.scripts if attrs.get("id") == "graph-data")
    assert "<" not in data and ">" not in data and "&" not in data
    subs = next(n for n in json.loads(data)["elements"]["nodes"] if n["data"]["id"] == spec.INJECT_SUBSCRIPTION)
    assert subs["data"]["detail"]["user_name"] == viz.clean(HOSTILE_NAME)       # bidi override dropped, text kept


def test_lineage_view_draws_the_trace(core_build):  # noqa: F811 (the imported fixture)
    from lakehouse_graph.lineage import tools as ltools

    with viz.VizContext(core_build[0]) as ctx:
        page, height = viz.lineage_view(ctx, GOLD_REF)
        assert viz.lineage_view(ctx, GOLD_REF)[0] == page                  # deterministic
        data, _ = ltools.lineage_trace(ctx, GOLD_REF)
        down, _ = viz.lineage_view(ctx, "bronze.churn_limit_events_raw.hit_at", direction="downstream")
        with pytest.raises(ValueError, match="unknown column"):
            viz.lineage_view(ctx, "gold.churn_renewal_features.nope")
    payload = payload_of(page)
    ids = {n["data"]["id"] for n in payload["elements"]["nodes"]}
    assert "bronze.churn_limit_events_raw.hit_at" in ids and GOLD_REF in ids
    pos = {n["data"]["id"]: n["position"]["x"] for n in payload["elements"]["nodes"]}
    for e in payload["elements"]["edges"]:
        assert pos[e["data"]["source"]] < pos[e["data"]["target"]]         # upstream on the left
    assert page.count("<tr><td>") == len(data["edges"]) and height > 320
    kinds = {n["data"]["kind"] for n in payload_of(down)["elements"]["nodes"]}
    assert {"TraceTarget", "DataColumn", "Assertion", "Contract", "GraphElement"} <= kinds
    assert "Code-derived" in page


def test_graph_viz_cli_writes_a_deterministic_standalone_file(tiny_build, tmp_path):
    bdir, man = tiny_build
    a = run("graph_viz.py", "--renewal", "sub_maya:2026-10-07", "--build", str(bdir), "--graph-root", str(tmp_path))
    assert a.returncode == 0, a.stderr
    info = json.loads(a.stdout.strip().splitlines()[-1])
    out = Path(info["path"])
    assert out == tmp_path / "viz" / man["business_build_id"] / "sub_maya_2026-10-07.html"
    body = out.read_bytes()
    assert info["bytes"] == len(body) and info["sha256"] == hashlib.sha256(body).hexdigest()
    b = run("graph_viz.py", "--renewal", "sub_maya:2026-10-07", "--build", str(bdir), "--out", str(tmp_path / "b.html"))
    assert b.returncode == 0 and (tmp_path / "b.html").read_bytes() == body          # same input -> same bytes
    assert LIB.encode() in body                                                      # standalone: library inlined
    bad = run("graph_viz.py", "--renewal", "sub_nobody:2026-10-07", "--build", str(bdir), "--out", str(tmp_path / "x"))
    assert bad.returncode == 1 and "unknown renewal" in bad.stderr and not (tmp_path / "x").exists()
    bad = run("graph_viz.py", "--renewal", "maya", "--build", str(bdir))
    assert bad.returncode == 1 and "invalid renewal id" in bad.stderr
    missing = run("graph_viz.py", "--renewal", "sub_maya:2026-10-07", "--graph-root", str(tmp_path / "empty"))
    assert missing.returncode == 1 and "make graph-promote" in missing.stderr


def test_graph_viz_cli_lineage_view(core_build, tmp_path):  # noqa: F811 (the imported fixture)
    p = run("graph_viz.py", "--lineage", GOLD_REF, "--build", str(core_build[0]), "--out", str(tmp_path / "l.html"),
            "--theme", "dark")
    assert p.returncode == 0, p.stderr
    page = (tmp_path / "l.html").read_text(encoding="utf-8")
    assert parse(page).html_attrs["data-theme"] == "dark" and "Lineage (upstream)" in page


@pytest.mark.slow
def test_maya_view_on_s42_matches_the_plan(s42_build):
    bdir, _ = s42_build
    with viz.VizContext(bdir) as ctx:
        data = viz.evidence_data(ctx, "sub_maya:2026-10-07")
        page, _ = viz.evidence_view(ctx, "sub_maya:2026-10-07")
    assert [(r["event_date"], r["relation"], r["target_id"]) for r in data["evidence"]] == [
        (r["event_date"], r["relation"].split(" ")[0], r["target_id"]) for r in MAYA_EVIDENCE]
    assert [(n["rank"], n["renewal_id"], n["d2_q"], n["outcome"]) for n in data["neighbours"]] == [
        (n["rank"], n["renewal_id"], n["d2_q"], n["outcome"]) for n in MAYA_NEIGHBOURS]
    assert "8 evidence rows" in page and "2 lapsed" in page
