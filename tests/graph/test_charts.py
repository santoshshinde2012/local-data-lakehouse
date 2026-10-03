# ruff: noqa: S314 (xml.etree parses SVG this module generated itself, never untrusted input)
"""Charts, Mermaid diagrams and the docs pages that embed them (src/lakehouse_graph/charts.py, scripts/graph_charts.py).

* every figure is deterministic (same input, same bytes), drawn in a light and a dark variant, valid SVG with a
  <title> and <desc>, and keeps text in the ink tokens (never a series colour);
* the numbers come from the inputs: a planted value shows up in the SVG and in the table, and moves the mark;
* Mermaid text covers the spec's labels and edge types and escapes labels;
* the generated regions of the docs are replaced between their markers and nowhere else;
* the docs pages: every embedded image and relative link exists, every anchor resolves, every <picture> has alt
  text, a dark source and a table next to it, no absolute home path or scratch path leaks, the TEACHING-ONLY
  banner is in agent.md.
"""
from __future__ import annotations

import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))

from lakehouse_graph import charts, spec  # noqa: E402 (after the sys.path line above)

DOCS = REPO / "docs" / "graph"
NS = "{http://www.w3.org/2000/svg}"
NOTE = "Source: planted test data."

# ----------------------------------------------------------------------------- planted inputs
COMPOSITION = {"nodes": {"Renewal": 12345, "Plan": 3}, "edges": {"SIMILAR_TO": 54321, "ON_PLAN": 12345}, "note": NOTE}
LEAK = {"by_type": {"HIT_LIMIT": {"edges": 900, "post_as_of": 321, "renewals": 77},
                    "FIRST_RENEWAL_AFTER": {"edges": 50, "post_as_of": 7, "renewals": 7},
                    "OPENED": {"edges": 40, "post_as_of": 0, "renewals": 0}}, "note": NOTE}
NAIVE = {"renewals": 999, "features": [{"feature": "limit_hits_14d", "window": "(as_of-14, as_of]", "pit_mismatches": 0,
                                        "naive_wrong": 4321}], "note": NOTE}
TIMELINE = {"renewal_id": "sub_x:2026-10-07", "label": "sub_x", "as_of": "2026-09-30", "renewal_date": "2026-10-07",
            "windows": {"HIT_LIMIT": 14, "EXPOSED_TO": 28}, "gold_rule_days": 30, "hidden_after_as_of": 5,
            "rows": [{"event_date": "2026-09-24", "relation": "HIT_LIMIT", "target_id": "lh:sub_x:001",
                      "feeds_feature": "limit_hits_14d", "in_feature_window": True, "known_by_as_of": True,
                      "declared_exception": False, "detail": "weekly"},
                     {"event_date": "2026-08-25", "relation": "EXPOSED_TO", "target_id": "inc-777",
                      "feeds_feature": "incident_exposed_28d", "in_feature_window": False, "known_by_as_of": True,
                      "declared_exception": False, "detail": None},
                     {"event_date": "2026-10-02", "relation": "FIRST_RENEWAL_AFTER", "target_id": "cap-cut-2026-10",
                      "feeds_feature": "first_renewal_after_pricing_change", "in_feature_window": True,
                      "known_by_as_of": False, "declared_exception": True, "detail": None}], "note": NOTE}
NEIGHBOURS = {"renewal_id": "sub_x:2026-10-07", "label": "sub_x", "as_of": "2026-09-30", "visibility": "today",
              "neighbours": [{"rank": i, "renewal_id": f"sub_{i:05d}:2026-08-0{i % 9 + 1}", "dist": 2.0 + i / 10,
                              "d2_q": int((2.0 + i / 10) ** 2 * 1e9),
                              "outcome": "voluntary_lapse" if i in (2, 7) else "renewed"} for i in range(1, 11)],
              "n": 10, "lapsed": 2, "wilson": [0.0567, 0.5098], "note": NOTE}
EXPOSURE = {"entity_id": "inc-777", "naive_additional": 4242, "window_days": 28, "note": NOTE,
            "by_plan": {"pro": {"exposed": 700, "renewed": 600, "voluntary_lapse": 50, "cancel_flow": 30, "dunning": 20,
                                "other": 0},
                        "ultra": {"exposed": 9, "renewed": 8, "voluntary_lapse": 1, "cancel_flow": 0, "dunning": 0,
                                  "other": 0}}}
LAPSE = {"groups": [{"label": "all plans", "without": {"n": 1000, "lapses": 63, "wilson": [0.05, 0.08]},
                     "with": {"n": 500, "lapses": 77, "wilson": [0.12, 0.19]}}], "note": NOTE}
COHORTS = {"algorithm": "leiden", "modularity": 0.81, "library": "networkx 3.7", "overall": {"n": 1000, "lapses": 70},
           "cohorts": [{"cohort_id": "leiden-01", "plan": "pro", "n": 400, "lapses": 33, "wilson": [0.06, 0.11],
                        "suppressed": False, "name": "x"},
                       {"cohort_id": "leiden-02", "plan": "ultra", "n": 60, "lapses": 9, "wilson": [0.08, 0.26],
                        "suppressed": False, "name": "y"},
                       {"cohort_id": "leiden-03", "plan": "withheld", "n": None, "lapses": None, "wilson": None,
                        "suppressed": True, "name": ""}], "note": NOTE}
LATENCY = {"mode": "stdio", "gate_ms": 50, "calls": 20, "note": NOTE,
           "tools": [{"tool": "graph_find", "toolset": "graph", "p50": 1.5, "p95": 2.25},
                     {"tool": "metric_route_counts", "toolset": "metrics", "p50": 2.0, "p95": 37.75}]}
EVAL = {"model": "test-model", "arms": ["H", "R"], "shapes": ["graph", "metric"], "note": NOTE,
        "pass3": {"H": {"graph": [9, 22], "metric": [3, 6]}, "R": {"graph": [17, 22], "metric": [4, 6]}}}
LEAKAGE = {"variants": [{"label": "self-inclusive", "single_feature": 0.8525, "lr": 0.8655},
                        {"label": "temporally safe", "single_feature": 0.5487, "lr": None}], "note": NOTE}

FIGURES = {
    "composition": lambda: charts.composition_figure(COMPOSITION),
    "leak": lambda: charts.leak_surface_figure(LEAK),
    "naive": lambda: charts.naive_pit_figure(NAIVE),
    "timeline": lambda: charts.timeline_figure(TIMELINE),
    "neighbours": lambda: charts.neighbours_figure(NEIGHBOURS),
    "exposure": lambda: charts.exposure_figure(EXPOSURE),
    "lapse": lambda: charts.lapse_rate_figure(LAPSE),
    "cohorts": lambda: charts.cohorts_figure(COHORTS),
    "latency": lambda: charts.latency_figure(LATENCY),
    "eval": lambda: charts.eval_figure(EVAL),
    "leakage": lambda: charts.leakage_figure(LEAKAGE),
}


def _texts(svg: str) -> list[ET.Element]:
    return list(ET.fromstring(svg).iter(f"{NS}text"))


# ----------------------------------------------------------------------------- determinism, themes, structure
@pytest.mark.parametrize("name", sorted(FIGURES))
def test_every_figure_is_deterministic_valid_svg_in_both_themes(name):
    a, b = FIGURES[name](), FIGURES[name]()
    for theme in charts.THEMES:
        s1, s2 = a.svg(theme), b.svg(theme)
        assert s1 == s2, "same input must give the same bytes"
        root = ET.fromstring(s1)
        kids = list(root)
        assert kids[0].tag == f"{NS}title" and kids[0].text, "the first child is a non-empty <title>"
        assert kids[1].tag == f"{NS}desc" and kids[1].text, "the second child is a non-empty <desc>"
        assert root.get("role") == "img"
        assert f'fill="{theme.surface}"' in s1, "the surface of the theme is drawn"
        assert not re.search(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}", s1), "no timestamps in the output"
    assert a.svg(charts.LIGHT) != a.svg(charts.DARK)


@pytest.mark.parametrize("name", sorted(FIGURES))
def test_text_wears_ink_tokens_never_a_series_colour(name):
    fig = FIGURES[name]()
    for theme in charts.THEMES:
        allowed = {theme.ink, theme.ink2, theme.muted, "#0b0b0b", "#ffffff"}
        for t in _texts(fig.svg(theme)):
            assert t.get("fill") in allowed, f"text {t.text!r} has fill {t.get('fill')}"


@pytest.mark.parametrize("name", sorted(FIGURES))
def test_every_figure_has_a_table_alt_text_and_a_picture_with_both_themes(name):
    fig = FIGURES[name]()
    md = fig.markdown("img/")
    assert '<source media="(prefers-color-scheme: dark)"' in md
    assert f'srcset="img/{fig.name}-dark.svg"' in md and f'src="img/{fig.name}-light.svg"' in md
    assert re.search(r'alt="[^"]{20,}"', md), "alt text says what the chart shows"
    assert fig.rows and all(len(r) == len(fig.headers) for r in fig.rows)
    assert "\n| " in md and "|---" in md, "the same numbers as a markdown table"


def test_write_figure_emits_light_and_dark_files_with_identical_bytes_on_rewrite(tmp_path):
    fig = FIGURES["composition"]()
    paths = charts.write_figure(fig, tmp_path)
    assert [p.name for p in paths] == ["graph-composition-light.svg", "graph-composition-dark.svg"]
    first = [p.read_bytes() for p in paths]
    charts.write_figure(FIGURES["composition"](), tmp_path)
    assert [p.read_bytes() for p in paths] == first


def test_palette_is_the_documented_fixed_order():
    assert charts.LIGHT.series[:4] == ("#2a78d6", "#eb6834", "#1baf7a", "#eda100")
    assert charts.DARK.series[:4] == ("#3987e5", "#d95926", "#199e70", "#c98500")
    assert (charts.LIGHT.surface, charts.DARK.surface) == ("#fcfcfb", "#1a1a19")
    # the validated palette record exists next to the results
    assert (DOCS / "results" / "palette-validation.md").is_file()


# ----------------------------------------------------------------------------- numbers come from the inputs
def _bar_length(svg: str, label: str) -> float:
    """Length of the bar in the <g> whose <title> starts with ``label`` (from its path: M x0 ... H / arc end x)."""
    root = ET.fromstring(svg)
    for g in root.iter(f"{NS}g"):
        title = g.find(f"{NS}title")
        if title is not None and title.text.startswith(label):
            d = g.find(f"{NS}path").get("d")
            x0 = float(re.match(r"M([\d.]+) ", d).group(1))
            xs = [float(x) for x in re.findall(r"H([\d.]+)", d)]
            xs += [float(x) for x in re.findall(r"A[\d.]+ [\d.]+ 0 0 1 ([\d.]+)", d)]
            return max(xs) - x0
    raise AssertionError(f"no mark titled {label!r}")


def test_a_planted_count_is_printed_and_sizes_its_bar():
    fig = charts.composition_figure(COMPOSITION)
    svg = fig.svg(charts.LIGHT)
    assert "12,345" in svg and "54,321" in svg
    assert ["edge", "SIMILAR_TO", "54,321"] in fig.rows
    # bars are proportional within a panel: 12,345 / 54,321 of the longest edge bar
    ratio = _bar_length(svg, "ON_PLAN: 12,345") / _bar_length(svg, "SIMILAR_TO: 54,321")
    assert ratio == pytest.approx(12345 / 54321, rel=0.02)
    moved = charts.composition_figure({**COMPOSITION, "edges": {"SIMILAR_TO": 54321, "ON_PLAN": 27000}})
    assert _bar_length(moved.svg(charts.LIGHT), "ON_PLAN: 27,000") > _bar_length(svg, "ON_PLAN: 12,345")


@pytest.mark.parametrize("name,in_svg,in_table", [
    ("leak", "321 of 900 after as_of", "| 321 |"), ("naive", "4,321 wrong", "| 4,321 |"),
    ("timeline", "inc-777", "| inc-777 |"), ("neighbours", "sub_00007", "sub_00007:"),
    ("exposure", "700 exposed", "| 4,242 |"), ("lapse", "77/500", "| 500 | 77 |"), ("cohorts", "leiden-02", "| leiden-02 |"),
    ("latency", "37.8", "| 37.75 |"), ("eval", "17/22", "| 17/22 |"), ("leakage", "0.853", "| 0.8525 |"),
])
def test_planted_values_reach_the_svg_and_the_table(name, in_svg, in_table):
    fig = FIGURES[name]()
    assert in_svg in _all_text(fig.svg(charts.LIGHT)), f"{in_svg!r} missing from the {name} SVG"
    assert in_table in fig.table_md(), f"{in_table!r} missing from the {name} table"


@pytest.mark.parametrize("name", sorted(FIGURES))
def test_image_text_stays_short_and_the_detail_goes_to_the_caption(name):
    """A title, one subtitle line, a short legend, one footer of at most 8 words; no paragraph."""
    fig = FIGURES[name]()
    texts = [t.text or "" for t in _texts(fig.svg(charts.LIGHT))]
    assert len(fig.title) <= 60, fig.title
    sub = texts[1]
    assert len(charts.wrap(sub, charts.W - 2 * charts.PAD, 12)) == 1 and len(sub) <= 90, sub
    assert all(len(t.split()) <= 8 for t in texts if t.startswith("Graph build"))
    assert not any("Source:" in t or "regenerate" in t for t in texts), "the long note is not drawn"
    assert all(len(t) <= 90 for t in texts), [t for t in texts if len(t) > 90]
    assert fig.caption and fig.caption in fig.markdown() and fig.note in fig.markdown()


def test_the_drawn_footer_is_the_build_id_and_seed_only():
    man = {"business_build_id": "e2b501f9dbe9", "profile": "tiny", "seed": 42, "n_users": 120,
           "data_end": "2026-09-30", "spec": {"graph": "renewal-graph/1"}, "commit": "abc1234", "dirty": False}
    note = charts.build_note(man, "Rows as graph_renewal_evidence returns them.")
    assert charts.short_footer(note) == "Graph build e2b501f9dbe9, seed 42"
    fig = charts.timeline_figure({**TIMELINE, "note": note})
    texts = [t.text or "" for t in _texts(fig.svg(charts.LIGHT))]
    assert texts[-1] == "Graph build e2b501f9dbe9, seed 42" and "abc1234" not in " ".join(texts)
    assert "commit abc1234" in fig.markdown(), "the full provenance stays in the doc caption"


def _all_text(svg: str) -> str:
    """Every visible text of an SVG, wrapped lines joined by a space."""
    return " ".join(t.text or "" for t in _texts(svg))


def test_withheld_cohorts_are_listed_never_drawn():
    fig = charts.cohorts_figure(COHORTS)
    svg = fig.svg(charts.LIGHT)
    assert "Withheld (small cells): leiden-03" in fig.caption
    assert not any(g.find(f"{NS}title") is not None and g.find(f"{NS}title").text.startswith("leiden-03")
                   for g in ET.fromstring(svg).iter(f"{NS}g"))
    assert ["leiden-03", "withheld", "-", "-", "-", "-", "small cell (or its complement)"] in fig.rows


def test_declared_exception_and_hidden_events_are_named_on_the_timeline():
    fig = charts.timeline_figure(TIMELINE)
    text = _all_text(fig.svg(charts.LIGHT))
    assert "after as_of (exception)" in text and "cap-cut-2026-10" in text
    assert "declared exception" in fig.caption and "(5 exist for this renewal)" in fig.caption
    assert fig.caption in fig.markdown(), "the detail is the caption under the image"


def test_historical_neighbours_without_visible_outcomes_are_shown_as_not_yet_observed():
    d = {**NEIGHBOURS, "visibility": "source_as_of",
         "neighbours": [{**NEIGHBOURS["neighbours"][0], "outcome": "not_yet_observed"}, *NEIGHBOURS["neighbours"][1:]]}
    fig = charts.neighbours_figure(d)
    assert "not yet observed" in _all_text(fig.svg(charts.DARK))
    assert "outcomes visible as of 2026-09-30" in fig.caption


def _marks(svg: str, prefix: str) -> list[ET.Element]:
    """The <g> marks whose <title> starts with ``prefix``."""
    return [g for g in ET.fromstring(svg).iter(f"{NS}g")
            if g.find(f"{NS}title") is not None and g.find(f"{NS}title").text.startswith(prefix)]


def test_a_zero_is_a_hollow_ring_on_the_baseline_never_a_bar():
    svg = charts.naive_pit_figure(NAIVE).svg(charts.LIGHT)
    (zero,) = _marks(svg, "limit_hits_14d: point in time 0 wrong")
    assert zero.find(f"{NS}path") is None, "no stub bar for a zero"
    ring = zero.find(f"{NS}circle")
    assert ring.get("fill") == charts.LIGHT.surface and ring.get("stroke") == charts.LIGHT.series[0]
    (bar,) = _marks(svg, "limit_hits_14d: naive 4,321 wrong")
    assert bar.find(f"{NS}path") is not None and bar.find(f"{NS}circle") is None
    ev = charts.eval_figure({**EVAL, "pass3": {"H": {"graph": [0, 22], "metric": [3, 6]},
                                               "R": {"graph": [17, 22], "metric": [4, 6]}}}).svg(charts.DARK)
    (z,) = _marks(ev, "graph, arm H: 0/22")
    assert z.find(f"{NS}path") is None and z.find(f"{NS}circle") is not None


def test_latency_draws_p50_and_p95_as_separate_bars_so_neither_hides_the_other():
    svg = charts.latency_figure(LATENCY).svg(charts.LIGHT)
    p50 = _bar_length(svg, "metric_route_counts p50: 2.0 ms")
    p95 = _bar_length(svg, "metric_route_counts p95: 37.75 ms")
    assert p95 / p50 == pytest.approx(37.75 / 2.0, rel=0.02)
    # equal p50 and p95: two bars of the same length on different rows
    same = charts.latency_figure({**LATENCY, "tools": [{"tool": "graph_find", "toolset": "graph", "p50": 3.0,
                                                        "p95": 3.0}]}).svg(charts.DARK)
    (a,), (b,) = _marks(same, "graph_find p50"), _marks(same, "graph_find p95")
    ya = float(re.search(r"M[\d.]+ ([\d.]+)", a.find(f"{NS}path").get("d")).group(1))
    yb = float(re.search(r"M[\d.]+ ([\d.]+)", b.find(f"{NS}path").get("d")).group(1))
    assert yb - ya >= 8 + 2, "the p95 bar sits below the p50 bar with a 2 px gap"


def test_close_markers_on_a_timeline_lane_are_dodged_apart():
    assert charts.dodge([10, 16, 40, 44, 47, 100]) == [-5.5, 5.5, -5.5, 5.5, -5.5, 0.0]
    assert charts.dodge([10, 30, 50]) == [0.0, 0.0, 0.0]
    rows = [{**TIMELINE["rows"][0], "event_date": "2026-09-24", "target_id": "lh:sub_x:001"},
            {**TIMELINE["rows"][0], "event_date": "2026-09-25", "target_id": "lh:sub_x:002"}]
    svg = charts.timeline_figure({**TIMELINE, "rows": rows}).svg(charts.LIGHT)
    centres = []
    for g in _marks(svg, "2026-09-2"):
        cs = g.findall(f"{NS}circle")
        centres.append((float(cs[-1].get("cx")), float(cs[-1].get("cy"))))
    (x1, y1), (x2, y2) = centres
    assert ((x1 - x2) ** 2 + (y1 - y2) ** 2) ** 0.5 >= 11, "fills (r 4) plus their 2 px surface rings never touch"


def test_a_cumulative_feature_window_is_shaded_from_the_start_to_as_of():
    row = {"event_date": "2026-08-15", "relation": "CUT_CAP", "target_id": "cap-cut-2026-08",
           "feeds_feature": "allowance_used_pct", "in_feature_window": True, "known_by_as_of": True,
           "declared_exception": False, "detail": None}
    with_band = charts.timeline_figure({**TIMELINE, "rows": [*TIMELINE["rows"], row], "cumulative": ["CUT_CAP"]})
    without = charts.timeline_figure({**TIMELINE, "rows": [*TIMELINE["rows"], row]})
    assert "all before as_of" in _all_text(with_band.svg(charts.LIGHT))
    assert "all before as_of" not in _all_text(without.svg(charts.LIGHT))


def test_headline_table_numbers_come_from_the_manifest_contract_and_bench():
    inv = {"parity_mismatches": dict.fromkeys(("a", "b", "c", "d", "e", "f"), 0),
           "naive_mismatches": {"limit_hits_14d": 1111, "incident_exposed_28d": 222, "support_tickets_90d": 33},
           "leak_surface": {"post_as_of_total": 4444, "event_edges": 55555}}
    man = {"profile": "s42", "business_build_id": "feedbeef0001", "commit": "abc1234", "dirty": True,
           "counts": {"total_nodes": 12345, "total_edges": 67890},
           "builder": {"seconds": 4.26, "max_rss_mib": 301.4}, "ladybug": {"load_s": 1.04}}
    lat = {"mode": "stdio", "calls": 20,
           "tools": [{"tool": "graph_find", "toolset": "graph", "p50": 1, "p95": 16.73},
                     {"tool": "cohort_list", "toolset": "cohorts", "p50": 1, "p95": 25.6}]}
    text = charts.headline_table([{"profile": "s42", "manifest": man, "contract": {"status": "pass", "strict": True,
                                                                                   "summary": {"invariants": inv}}},
                                  {"profile": "tiny", "manifest": {**man, "profile": "tiny"}, "contract": None}],
                                 lat, bench_profile="s42")
    for want in ("| Nodes / edges | 12,345 / 67,890 | 12,345 / 67,890 |", "| pass (strict) | - |",
                 "0 mismatches | - |", "1,111 / 222 / 33 | - |", "4,444 of 55,555 | - |", "4.3 s + 1.0 s, 301 MiB",
                 "graph tools at most 16.7 ms; all 2 tools at most 25.6 ms | not measured |", "20 calls each",
                 "feedbeef0001", "Seed 42 (`s42`)", "Tiny fixture (`tiny`)"):
        assert want in text, want


def test_nice_scale_and_text_width_are_sane():
    assert charts.nice_scale(1165) == (1250, 250)
    assert charts.nice_scale(0.42)[0] == pytest.approx(0.5)
    assert charts.nice_scale(0) == (1.0, 0.2)
    assert charts.text_width("SIMILAR_TO", 12) > charts.text_width("Plan", 12) > 0


# ----------------------------------------------------------------------------- Mermaid
def test_mermaid_schema_lists_every_label_and_edge_type_with_counts():
    counts = {"nodes": {k: i + 1 for i, k in enumerate(spec.NODE_SCHEMA)},
              "edges": {k: 1000 + i for i, k in enumerate(spec.EDGE_SCHEMA)}}
    text = charts.mermaid_schema(counts)
    assert text.startswith(charts.MERMAID_INIT + "\nflowchart LR\n")
    assert charts.MERMAID_CLASSDEFS in text and f"  class {','.join(spec.NODE_SCHEMA)}," in text
    for label in spec.NODE_SCHEMA:
        assert f'{label}["{label}<br/>' in text
    loops = 0
    for i, (rel, e) in enumerate(spec.EDGE_SCHEMA.items()):
        if e.src == e.dst:   # drawn as a note: a self-loop's label lands on its neighbours' labels
            loops += 1
            assert f'{e.src}_{rel}{{{{"{rel} {1000 + i:,}<br/>{e.src} to another {e.dst}"}}}}' in text
            assert f"  {e.src} -.- {e.src}_{rel}\n" in text
            assert f"{e.src} -->|\"{rel}" not in text
        else:
            assert f'{e.src} -->|"{rel} {1000 + i:,}"| {e.dst}' in text
    assert loops == 1, "SIMILAR_TO is the one Renewal -> Renewal edge type"


def test_mermaid_er_matches_the_spec():
    text = charts.mermaid_er()
    assert text.startswith(charts.MERMAID_INIT + "\nerDiagram\n")
    for label, n in spec.NODE_SCHEMA.items():
        assert f"  {label} {{" in text
        assert f" {n.key} PK" in text
    for rel, e in spec.EDGE_SCHEMA.items():
        assert re.search(rf"  {e.src} \S+ {e.dst} : {rel}\n", text), rel
    assert f"features_{len(spec.NUMERIC_FEATURES)}" in text


def test_mermaid_lineage_flows_upstream_to_downstream_and_escapes_labels():
    trace = {"target": "gold.t.f", "direction": "upstream", "edges": [
        {"depth": 1, "rel": "DERIVED_FROM", "from": "gold.t.f", "to": "silver.s.c", "roles": "VALUE", "window": None,
         "transform": "COUNT(*) AS `f`"},
        {"depth": 1, "rel": "COUNTS_ROWS_OF", "from": "gold.t.f", "to": "silver.s", "roles": None,
         "window": "(as_of-14, as_of]", "transform": None}]}
    text = charts.mermaid_lineage(trace)
    assert text.startswith(charts.MERMAID_INIT + "\nflowchart LR\n") and text.rstrip().endswith(" data")
    assert 'subgraph silver["silver"]' in text and 'subgraph gold["gold"]' in text
    assert '"s (table)"' in text
    ids = {m.group(2): m.group(1) for m in re.finditer(r'(n\d+)\["([^"]+)"\]', text)}
    assert f'{ids["s.c"]} -->|"VALUE as f"| {ids["t.f"]}' in text
    assert "COUNTS_ROWS_OF (as_of-14, as_of]" in text
    assert "`" not in text
    assert charts.mermaid_label('a "b" <c> `d` |e') == "a #quot;b#quot; #lt;c#gt; d #124;e"


# ----------------------------------------------------------------------------- generated regions
def test_fill_regions_replaces_only_between_the_markers():
    text = ("intro\n<!-- graph-evidence:begin figure:a -->\nold\n<!-- graph-evidence:end figure:a -->\n"
            "middle\n<!-- graph-evidence:begin mermaid:b -->\n<!-- graph-evidence:end mermaid:b -->\nend\n")
    new, filled = charts.fill_regions(text, {"figure:a": "NEW A", "mermaid:b": "NEW B", "figure:absent": "x"})
    assert filled == ["figure:a", "mermaid:b"]
    assert new.startswith("intro\n") and "\nmiddle\n" in new and new.endswith("end\n")
    assert "old" not in new and "NEW A" in new and "NEW B" in new
    assert charts.fill_regions(new, {"figure:a": "NEW A", "mermaid:b": "NEW B"})[0] == new, "idempotent"
    assert charts.region_names(text) == ["figure:a", "mermaid:b"]
    with pytest.raises(ValueError, match="no end marker"):
        charts.fill_regions("<!-- graph-evidence:begin x -->\n", {"x": "y"})


# ----------------------------------------------------------------------------- real builds
def test_figures_from_the_tiny_build(tiny_build):
    bdir, man = tiny_build
    figs = {f.name: f for f in charts.figures_from_build(bdir)}
    assert {"graph-composition", "leak-surface", "naive-vs-pit", "santosh-timeline", "santosh-neighbours",
            "inc-002-exposure", "lapse-first-after-cut"} <= set(figs)
    assert ["total", "nodes", "616"] in figs["graph-composition"].rows
    assert ["total", "edges", "1,949"] in figs["graph-composition"].rows
    naive = {r[0]: r[3] for r in figs["naive-vs-pit"].rows}
    assert naive == {"limit_hits_14d": "16", "incident_exposed_28d": "12", "support_tickets_90d": "4"}
    assert len(figs["santosh-timeline"].rows) == 8, "the hero's 8 evidence rows (tiny = seed 42)"
    assert man["business_build_id"] in figs["leak-surface"].note


def test_graph_charts_cli_writes_both_themes_and_fills_a_page(tiny_build, tmp_path):
    bdir, _ = tiny_build
    docs = tmp_path / "docs"
    docs.mkdir()
    page = docs / "page.md"
    regions = "".join(f"<!-- graph-evidence:begin {n} -->\n<!-- graph-evidence:end {n} -->\n"
                      for n in ("figure:naive-vs-pit", "figure:eval-pass3", "summary:headline"))
    page.write_text("x\n" + regions, encoding="utf-8")
    args = ["--build", str(bdir), "--img-dir", str(tmp_path / "img"), "--mermaid-dir", str(tmp_path / "mmd"),
            "--docs-dir", str(docs)]
    p = subprocess.run([sys.executable, str(REPO / "scripts/graph_charts.py"), *args], capture_output=True, text=True,
                       cwd=REPO, timeout=300, check=False)
    assert p.returncode == 0, p.stderr
    for name in ("naive-vs-pit", "santosh-timeline", "graph-composition"):
        assert (tmp_path / "img" / f"{name}-light.svg").is_file() and (tmp_path / "img" / f"{name}-dark.svg").is_file()
    assert (tmp_path / "mmd" / "schema.mmd").is_file() and (tmp_path / "mmd" / "er.mmd").is_file()
    text = page.read_text(encoding="utf-8")
    assert 'srcset="img/naive-vs-pit-dark.svg"' in text and "| limit_hits_14d |" in text
    assert "Pending final run." in text, "an absent input leaves a pending note, not an empty region"
    assert "| Nodes / edges | 616 / 1,949 |" in text, "the headline table of the given build"
    assert not (tmp_path / "img" / "tool-latency-light.svg").exists(), "no tiny bench: no latency chart"
    again = subprocess.run([sys.executable, str(REPO / "scripts/graph_charts.py"), *args, "--check"],
                           capture_output=True, text=True, cwd=REPO, timeout=300, check=False)
    assert again.returncode == 0, again.stdout + again.stderr


def test_graph_charts_without_a_build_skips_with_a_relative_path(tmp_path):
    root = REPO / "tests" / "graph" / "no-such-graph-root"   # inside the repo: shown repo-relative
    p = subprocess.run([sys.executable, str(REPO / "scripts/graph_charts.py"), "--profile", "s42", "--graph-root",
                        str(root), "--img-dir", str(tmp_path / "img"), "--mermaid-dir", str(tmp_path / "mmd")],
                       capture_output=True, text=True, cwd=REPO, timeout=120, check=False)
    assert p.returncode == 3, p.stdout + p.stderr
    assert "graph_charts SKIPPED: no graph build at tests/graph/no-such-graph-root/s42" in p.stderr
    assert str(REPO) not in p.stdout + p.stderr and not (tmp_path / "img").exists()


@pytest.mark.slow
def test_seed_42_charts_carry_the_plan_numbers(s42_build):
    bdir, _ = s42_build
    figs = {f.name: f for f in charts.figures_from_build(bdir)}
    rows = {f: [" ".join(r) for r in figs[f].rows] for f in figs}
    assert ["total", "nodes", "40,204"] in figs["graph-composition"].rows
    assert {r[0]: r[3] for r in figs["naive-vs-pit"].rows} == {
        "limit_hits_14d": "1,165", "incident_exposed_28d": "681", "support_tickets_90d": "114"}
    assert ["total", "34,348", "19,486", "14,862", "", ""] in figs["leak-surface"].rows
    expo = {r[0]: r[1] for r in figs["inc-002-exposure"].rows}
    assert (expo["pro"], expo["pro_plus"], expo["ultra"]) == ("606", "185", "46")
    assert "329" in figs["inc-002-exposure"].svg(charts.LIGHT)
    assert any("2 of 10 lapsed, Wilson 95% [0.057, 0.510]" in r for r in rows["santosh-neighbours"])
    lapse = {(r[0], r[1]): (r[2], r[3]) for r in figs["lapse-first-after-cut"].rows}
    assert lapse[("all plans", "first after a cut")] == ("2,292", "227")
    assert lapse[("pro", "not first after a cut")] == ("4,016", "275")


# ----------------------------------------------------------------------------- the docs pages
PAGES = [*sorted(DOCS.glob("*.md")), REPO / "docs/demo/graph-e2e.excerpt.md"]
RESULTS = sorted((DOCS / "results").glob("*.md"))


def _slug(heading: str) -> str:
    """GitHub's heading anchor: lowercase, drop punctuation but - and _, spaces to -."""
    h = re.sub(r"[`*]", "", heading.strip().lower())
    h = re.sub(r"[^\w\- ]", "", h)
    return h.replace(" ", "-")


def _anchors(path: Path) -> set[str]:
    text = re.sub(r"```.*?```", "", path.read_text(encoding="utf-8"), flags=re.S)
    return {_slug(m.group(1)) for m in re.finditer(r"^#{1,6} (.+)$", text, flags=re.M)}


def test_the_docs_pages_exist():
    names = {p.name for p in DOCS.glob("*.md")}
    assert {"README.md", "architecture.md", "data-model.md", "agent.md", "lineage.md", "lakehouse-twin.md",
            "evaluation.md", "operations.md", "research.md"} <= names
    assert (REPO / "docs/demo/graph-e2e.excerpt.md").is_file()


@pytest.mark.parametrize("page", PAGES + RESULTS, ids=lambda p: p.name)
def test_every_embedded_image_link_and_anchor_resolves(page):
    text = page.read_text(encoding="utf-8")
    body = re.sub(r"```.*?```", "", text, flags=re.S)
    refs = re.findall(r'(?:srcset|src)="([^"]+)"', body) + re.findall(r"\]\(([^)\s]+)\)", body)
    for ref in refs:
        if re.match(r"[a-z]+://", ref) or ref.startswith("mailto:"):
            continue
        target, _, anchor = ref.partition("#")
        path = (page.parent / target).resolve() if target else page
        assert path.exists(), f"{page.name}: {ref} does not exist"
        if anchor and path.suffix == ".md":
            assert anchor in _anchors(path), f"{page.name}: anchor #{anchor} not found in {path.name}"


@pytest.mark.parametrize("page", PAGES, ids=lambda p: p.name)
def test_every_picture_has_alt_text_a_dark_variant_and_a_table(page):
    text = page.read_text(encoding="utf-8")
    for m in re.finditer(r"<picture>(.*?)</picture>(.*?)(?=<picture>|<!-- graph-evidence:end|\Z)", text, flags=re.S):
        pic, after = m.group(1), m.group(2)
        assert 'media="(prefers-color-scheme: dark)"' in pic and "-dark.svg" in pic
        assert re.search(r'alt="[^"]{20,}"', pic), "alt text"
        assert re.search(r"^\|.*\|\s*$\n^\|[-:|]+\|\s*$", after, flags=re.M), "a table next to the chart"


@pytest.mark.parametrize("page", PAGES + RESULTS + sorted((DOCS / "results").glob("*.json")), ids=lambda p: p.name)
def test_no_home_path_scratch_path_or_credential_in_the_docs(page):
    text = page.read_text(encoding="utf-8")
    assert not re.search(r"/Users/|/home/[a-z]", text), "absolute home path"
    assert ".graph-work" not in text, "scratch path"
    assert "minioadmin" not in text
    assert not re.search(r"(?i)(password|secret|token)\s*[=:]\s*(?!<redacted>)[\w-]{4,}", text), "credential"


def test_mermaid_blocks_are_well_formed():
    for page in PAGES:
        for block in re.findall(r"```mermaid\n(.*?)```", page.read_text(encoding="utf-8"), flags=re.S):
            init, first = block.splitlines()[:2]
            assert init == charts.MERMAID_INIT, f"{page.name}: the shared palette init line comes first"
            assert first in ("flowchart LR", "flowchart TD", "flowchart TB", "erDiagram"), f"{page.name}: {first}"
            assert block.count("subgraph ") == len(re.findall(r"^\s*end\s*$", block, flags=re.M))
            assert block.count('"') % 2 == 0, f"{page.name}: unbalanced quotes"


def test_agent_page_has_the_teaching_only_banner_and_the_threat_model():
    text = (DOCS / "agent.md").read_text(encoding="utf-8")
    assert "**TEACHING-ONLY. NOT PRODUCTION.**" in text
    for phrase in ("`read_only` is not a sandbox", "lethal trifecta", "What `read_only` does not protect",
                   "What the sandbox does not do", "Approving `.mcp.json` runs repo code"):
        assert phrase in text, phrase


def test_honesty_notes_are_published():
    text = " ".join(p.read_text(encoding="utf-8") for p in PAGES)
    for phrase in ("no predictive lift", "not a risk estimate", "descriptive, not causal", "labels, not structure",
                   "not fully open source", "macOS arm64", "Linux"):
        assert phrase.lower() in text.lower(), phrase


def test_generated_regions_are_known_and_filled_with_existing_charts():
    names = {f"figure:{n}" for n in ("graph-composition", "leak-surface", "naive-vs-pit", "santosh-timeline",
                                     "santosh-neighbours", "inc-002-exposure", "lapse-first-after-cut",
                                     "cohort-lapse-rates", "tool-latency", "eval-pass3", "leakage-aucs")}
    names |= {"mermaid:schema", "mermaid:er", "mermaid:lineage-limit-hits-14d", "results:checks", "summary:headline"}
    seen = set()
    for page in PAGES:
        regions = charts.region_names(page.read_text(encoding="utf-8"))
        assert set(regions) <= names, f"{page.name}: unknown region {set(regions) - names}"
        seen |= set(regions)
    assert seen == names, f"regions not placed in any page: {names - seen}"
