"""Deterministic standalone Cytoscape.js HTML views of a renewal's evidence (and of a lineage trace).

One self-contained HTML file per view, built with the standard library only (no template
engine, no network): Cytoscape.js 3.34.3 (MIT) is vendored in ``vendor/cytoscape.min.js``,
checked against its pinned sha256 and inlined, so the page opens offline from ``file://``
or inside an iframe. The same inputs always give byte-identical HTML:

  * rows are sorted explicitly; JSON is dumped with sort_keys, fixed separators, ensure_ascii;
  * every position is computed here (Cytoscape ``preset`` layout, integer pixels);
  * nothing time-, host- or path-dependent is written (no clock, no absolute path).

Views
  evidence_view(ctx, renewal_id)   the renewal's point-in-time evidence as a timeline (x = event
                                   date, a dashed vertical line at as_of), its 10 SIMILAR_TO
                                   neighbours with outcomes under the tool's outcome_visibility
                                   rule, and the shared hubs (incidents, pricing changes, plan)
  lineage_view(ctx, column)        a lineage_trace answer as a layered DAG (data flows left to right)

What is drawn is exactly what the tools serve (``evidence_data`` reads the tool templates of
queries.py: ``queries.evidence`` for the evidence rows, ``similar_top_k_visible`` for the
neighbours): never a Subscription event after as_of, never BILLED outcome evidence, never a
neighbour outcome observed after a historical source's as_of. ``check_point_in_time`` enforces
it again on whatever rows reach the builder: the one row allowed right of the as_of line is a
FIRST_RENEWAL_AFTER edge flagged ``declared_exception`` (the gold rule; drawn dotted and
labelled). ``user_name`` is shown for the named renewal only (as the evidence tool does); city
is never drawn (the shared ``renewal_header`` row carries it and ``evidence_data`` drops it: the
focus dict keeps an explicit list of fields).

Safety of the HTML (the page may be embedded same-origin, e.g. in the local UI):
  * graph data travels as one JSON document in <script type="application/json">, with '<', '>'
    and '&' written as \\u escapes, so no value can close the element or open a comment;
  * every other dynamic string goes through html.escape; the page script writes text with
    textContent only; canvas labels do not interpret markup;
  * strings are cleaned like the tool envelope (control / format / unassigned characters out,
    200-character cap);
  * a Content-Security-Policy <meta> allows only the hashed inline scripts and styles
    (default-src 'none'; no unsafe-inline, no unsafe-eval, no connect-src); the opt-in CDN mode
    adds exactly one source, the pinned library's full URL (with SRI), never the whole host;
  * render_html inlines only the pinned library bytes (sha256-checked, like load_vendored).

CLI: python scripts/graph_viz.py --renewal <id> | --lineage <ColumnRef> (Makefile alias: make
graph-viz RENEWAL=...). Tests: tests/graph/test_viz.py.
"""
from __future__ import annotations

import base64
import contextlib
import hashlib
import html
import json
import re
import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path
from typing import Any, Literal

from . import manifest as mf
from . import queries, spec, store

VIZ_SPEC = "renewal-graph-viz/1"
CYTOSCAPE_VERSION = "3.34.3"
# sha384 of dist/cytoscape.min.js from the npm tarball (identical bytes on jsDelivr, unpkg, cdnjs).
CYTOSCAPE_SRI = "sha384-qPKQxl9uMXOw7vSTUDAnpUilhLuulovw6P5Z4db4bqxW5VhumS7przEmHX0iM0Oc"
CYTOSCAPE_SHA256 = "5f3b5b529546d5af1fc5628590af033b74511a5b6f789f5f4682845863228b91"
CYTOSCAPE_CDN = f"https://cdn.jsdelivr.net/npm/cytoscape@{CYTOSCAPE_VERSION}/dist/cytoscape.min.js"
# Text of the <style id="__________cytoscape_stylesheet"> element the 3.34.3 renderer injects.
CYTOSCAPE_INJECTED_STYLE = ".__________cytoscape_container { position: relative; }"
VENDOR_DIR = Path(__file__).resolve().parent / "vendor"
CYTOSCAPE_FILE = VENDOR_DIR / "cytoscape.min.js"
LICENCE_NOTICE = f"Drawn with Cytoscape.js {CYTOSCAPE_VERSION} (MIT, \u00a9 The Cytoscape Consortium)"

JsMode = Literal["inline", "cdn", "relative"]
Theme = Literal["auto", "light", "dark"]
Layout = Literal["preset", "concentric", "breadthfirst", "grid", "circle"]  # all deterministic; never cose
THEMES = ("auto", "light", "dark")
LAYOUTS = ("preset", "concentric", "breadthfirst", "grid", "circle")
RENEWAL_ID_RE = re.compile(r"^sub_[a-z0-9_]+:\d{4}-\d{2}-\d{2}$")   # the tools' RenewalId
CURRENT_ROUTES = ("score_today", "pending")    # sources whose as_of is the current T-7
VISIBILITIES = ("auto", "today", "source_as_of")
EVIDENCE_ROWS = 200                             # the envelope's row cap; one more is fetched to detect a cut
NON_OUTCOME_BILLING = "cancel_scheduled"        # the only BILLED event that is not outcome evidence (spec)

# ---------------------------------------------------------------------------------------------
# Visual encoding. Identity is carried by SHAPE + LABEL; colour only groups kinds into three
# families (the palette validates all-pairs for exactly three slots in light and dark).
# ---------------------------------------------------------------------------------------------
GROUPS = ("renewal", "event", "context")
KINDS: dict[str, dict[str, Any]] = {
    # kind:            group,     cytoscape shape,     size, legend text
    "FocusRenewal":  {"group": "renewal", "shape": "star", "size": 46, "legend": "Renewal being explained"},
    "Renewal":       {"group": "renewal", "shape": "ellipse", "size": 26, "legend": "Similar renewal (neighbour)"},
    "LimitHit":      {"group": "event", "shape": "triangle", "size": 24, "legend": "Limit hit"},
    "OverageChange": {"group": "event", "shape": "diamond", "size": 24, "legend": "Overage setting change"},
    "OverageCharge": {"group": "event", "shape": "pentagon", "size": 24, "legend": "Overage charge"},
    "Ticket":        {"group": "event", "shape": "rectangle", "size": 22, "legend": "Support ticket"},
    "BillingEvent":  {"group": "event", "shape": "hexagon", "size": 24, "legend": "Billing event"},
    "Subscription":  {"group": "context", "shape": "round-rectangle", "size": 30, "legend": "Subscription"},
    "Plan":          {"group": "context", "shape": "barrel", "size": 30, "legend": "Plan"},
    "Incident":      {"group": "context", "shape": "octagon", "size": 30, "legend": "Incident (shared hub)"},
    "PricingChange": {"group": "context", "shape": "round-tag", "size": 30, "legend": "Pricing change (shared hub)"},
    # lineage trace view
    "TraceTarget":   {"group": "renewal", "shape": "star", "size": 40, "legend": "Traced column"},
    "DataColumn":    {"group": "context", "shape": "round-rectangle", "size": 26, "legend": "Column"},
    "Dataset":       {"group": "context", "shape": "barrel", "size": 28, "legend": "Dataset (row count)"},
    "Parameter":     {"group": "context", "shape": "diamond", "size": 24, "legend": "Parameter"},
    "Assertion":     {"group": "event", "shape": "triangle", "size": 24, "legend": "Check (assertion)"},
    "Contract":      {"group": "event", "shape": "hexagon", "size": 26, "legend": "Contract"},
    "GraphElement":  {"group": "renewal", "shape": "ellipse", "size": 24, "legend": "Graph element"},
    "Metric":        {"group": "renewal", "shape": "pentagon", "size": 24, "legend": "Metric"},
    "DownstreamRepo": {"group": "renewal", "shape": "octagon", "size": 28, "legend": "Consumer repository"},
    "Marker":        {"group": "axis", "shape": "rectangle", "size": 5, "legend": ""},
}
EVENT_LANES = ("LimitHit", "OverageChange", "OverageCharge", "Ticket", "BillingEvent")
RELATION_TARGET_KIND = {
    "HIT_LIMIT": "LimitHit", "CHANGED_OVERAGE": "OverageChange", "CHARGED_OVERAGE": "OverageCharge",
    "OPENED": "Ticket", "BILLED": "BillingEvent", "EXPOSED_TO": "Incident",
    "FIRST_RENEWAL_AFTER": "PricingChange", "CUT_CAP": "PricingChange",
}
# lineage_trace "reached" buckets -> node kind (columns are DataColumn)
TRACE_BUCKET_KIND = {"datasets": "Dataset", "assertions": "Assertion", "contracts": "Contract",
                     "graph_elements": "GraphElement", "metrics": "Metric", "parameters": "Parameter",
                     "consumers": "DownstreamRepo"}

PALETTES: dict[str, dict[str, str]] = {
    "light": {"surface": "#fcfcfb", "page": "#f9f9f7", "ink": "#0b0b0b", "ink2": "#52514e", "muted": "#898781",
              "grid": "#e1e0d9", "axis": "#c3c2b7", "renewal": "#2a78d6", "event": "#eb6834", "context": "#1baf7a"},
    "dark": {"surface": "#1a1a19", "page": "#0d0d0d", "ink": "#ffffff", "ink2": "#c3c2b7", "muted": "#898781",
             "grid": "#2c2c2a", "axis": "#383835", "renewal": "#3987e5", "event": "#d95926", "context": "#199e70"},
}

_MAX_STR = 200
_DROP_CATEGORIES = frozenset({"Cc", "Cf", "Cs", "Co", "Cn"})  # control, format (bidi, zero-width), unassigned
_ELLIPSIS = "\u2026"


class UnknownRenewal(ValueError):
    """The renewal id is well formed but not in this build."""


# ---------------------------------------------------------------------------------------------
# small pure helpers
# ---------------------------------------------------------------------------------------------
def clean(value: Any) -> str:
    """One line of plain text: line breaks become spaces, control / format / unassigned
    characters are dropped (the tool envelope's rule), then a 200-character cap."""
    text = re.sub(r"[\r\n\t\u2028\u2029]", " ", str(value))
    text = "".join(ch for ch in text if unicodedata.category(ch) not in _DROP_CATEGORIES)
    return text if len(text) <= _MAX_STR else text[: _MAX_STR - 1] + _ELLIPSIS


def label(*parts: Any) -> str:
    """A canvas label: cleaned parts, one per line (only this function adds line breaks)."""
    return "\n".join(clean(p) for p in parts if p not in (None, ""))


def script_json(obj: Any) -> str:
    """JSON that is safe inside a <script> element (cannot terminate it, cannot start a comment)."""
    text = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
    return text.replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")


def csp_hash(text: str) -> str:
    return "'sha256-" + base64.b64encode(hashlib.sha256(text.encode("utf-8")).digest()).decode() + "'"


def plural(n: int, one: str, many: str | None = None) -> str:
    """'1 renewal', '2 renewals' ('many' when the plural is irregular)."""
    return f"{n} {one if n == 1 else (many or one + 's')}"


def _d(value: str | date) -> date:
    return value if isinstance(value, date) else date.fromisoformat(str(value)[:10])


def _iso(value: Any) -> str | None:
    return None if value is None else _d(value).isoformat()


@dataclass(frozen=True)
class Graph:
    nodes: list[dict[str, Any]]
    edges: list[dict[str, Any]]
    rows: list[dict[str, str]]          # the text alternative (table), already ordered
    title: str
    summary: str
    legend_note: str = ""               # how to read lines / borders in THIS view (plain text)
    table_head: tuple[str, str, str, str] = ("Date", "Relation", "Target", "Note")


# ---------------------------------------------------------------------------------------------
# Point in time: what may be drawn at all
# ---------------------------------------------------------------------------------------------
def check_point_in_time(focus: dict[str, Any], evidence: Iterable[dict[str, Any]],
                        neighbours: Iterable[dict[str, Any]] = ()) -> None:
    """ValueError unless the rows are what the evidence / neighbour tools may serve.

    Evidence: no row after as_of except FIRST_RENEWAL_AFTER flagged declared_exception (the gold
    rule); a BILLED row must be the one non-outcome billing event (cancel_scheduled). Neighbours,
    when ``focus["visibility"]`` is ``source_as_of``: a revealed outcome must have been observed
    on or before the source's as_of.
    """
    as_of = _d(focus["as_of"])
    for r in evidence:
        rel = str(r["relation"]).split(" ")[0]
        day = _d(r["event_date"])
        if rel == "FIRST_RENEWAL_AFTER":
            if day > as_of and not r.get("declared_exception"):
                raise ValueError(f"FIRST_RENEWAL_AFTER {r['target_id']!r} on {day} is after as_of {as_of} but not "
                                 f"flagged declared_exception: refusing to draw it")
            continue
        if day > as_of:
            raise ValueError(f"evidence row {rel} {r['target_id']!r} on {day} is after as_of {as_of}: the view never "
                             f"draws post-as_of events")
        if rel == "BILLED" and r.get("detail") != NON_OUTCOME_BILLING:
            raise ValueError(f"BILLED row {r['target_id']!r} ({r.get('detail')!r}) is outcome evidence: only "
                             f"{NON_OUTCOME_BILLING} on or before as_of may be drawn")
    if focus.get("visibility") == "source_as_of":
        for n in neighbours:
            outcome = n.get("outcome") or queries.NOT_YET_OBSERVED
            if outcome == queries.NOT_YET_OBSERVED:
                continue
            seen = n.get("outcome_observed_on")
            if seen is None or _d(seen) > as_of:
                raise ValueError(f"neighbour {n['renewal_id']!r} shows outcome {outcome!r} observed {seen} after "
                                 f"the source's as_of {as_of}: refusing to draw it")


# ---------------------------------------------------------------------------------------------
# Deterministic layouts computed in Python (Cytoscape only draws them: layout {name: "preset"})
# ---------------------------------------------------------------------------------------------
def build_evidence_graph(
    focus: dict[str, Any],
    evidence: Iterable[dict[str, Any]],
    neighbours: Iterable[dict[str, Any]] = (),
    hubs: Iterable[dict[str, Any]] = (),
    *,
    timeline_px: int = 620,
) -> Graph:
    """One renewal, its point-in-time evidence rows, up to 10 neighbours, shared hubs.

    focus:      {renewal_id, subscription_id, as_of, renewal_date, plan_tier?, route?, user_name?,
                 build_id?, visibility?}
    evidence:   rows of the evidence tool: {event_date, relation, target_id, detail?, feeds_feature?,
                in_feature_window?, known_by_as_of?, declared_exception?}
    neighbours: rows of the neighbour tool: {rank, renewal_id, d2_q?, dist?, outcome, outcome_observed_on?}
    hubs:       optional facts per hub id: {id, note}

    Layout = a timeline: x is the event date, the vertical dashed line is as_of (T-7); nothing that
    belongs to the subscription is drawn right of that line (check_point_in_time); lanes (y)
    separate event kinds.
    """
    evidence, neighbours = list(evidence), list(neighbours)
    check_point_in_time(focus, evidence, neighbours)
    rid, sid = clean(focus["renewal_id"]), clean(focus["subscription_id"])
    as_of, renewal_date = _d(focus["as_of"]), _d(focus["renewal_date"])
    ev = sorted(({**r, "event_date": _d(r["event_date"]).isoformat(), "relation": clean(r["relation"]),
                  "target_id": clean(r["target_id"])} for r in evidence),
                key=lambda r: (r["event_date"], r["relation"], r["target_id"]))
    nb = sorted(({**n, "rank": int(n["rank"]), "renewal_id": clean(n["renewal_id"])} for n in neighbours),
                key=lambda n: (n["rank"], n["renewal_id"]))
    hub_notes = {clean(h["id"]): clean(h.get("note", "")) for h in hubs}

    t0 = min([_d(r["event_date"]) for r in ev] + [as_of])
    span = max((renewal_date - t0).days, 1)
    ppd = max(4.0, min(24.0, timeline_px / span))

    def x_of(day: date) -> int:
        return round((day - t0).days * ppd)

    nodes: dict[str, dict[str, Any]] = {}
    edges: dict[tuple[str, str, str], dict[str, Any]] = {}
    rows: list[dict[str, str]] = []

    def add_node(nid: str, kind: str, text: str, x: int, y: int, **detail: Any) -> None:
        if nid in nodes:
            return
        kspec = KINDS[kind]
        nodes[nid] = {"id": nid, "kind": kind, "group": kspec["group"], "shape": kspec["shape"], "size": kspec["size"],
                      "label": text, "x": int(x), "y": int(y),
                      "detail": {k: clean(v) for k, v in sorted(detail.items()) if v not in (None, "")}}

    def add_edge(src: str, rel: str, dst: str, when: str = "", *, exception: bool = False,
                 text: str | None = None) -> None:
        key = (src, rel, dst)
        if key in edges:                      # parallel rows (one per usage day) collapse to one edge
            e = edges[key]
            e["count"] += 1
            e["first"], e["last"] = min(e["first"], when), max(e["last"], when)
            e["label"] = f"{rel} \u00d7{e['count']}"
            return
        edges[key] = {"id": f"e:{src}|{rel}|{dst}", "source": src, "target": dst, "relation": rel,
                      "label": clean(text if text is not None else rel), "exception": bool(exception),
                      "count": 1, "first": when, "last": when}

    # Rows, top to bottom: shared hubs (2 staggered rows) / the subscription lifeline (= time axis,
    # drawn as the HAS_RENEWAL edge with weekly ticks) / one lane per event kind / neighbours.
    y_hub, y_axis, lane_h, sub_h = 40, 200, 60, 46
    placed: dict[str, list[tuple[int, int]]] = {}
    y_cursor = y_axis + 80
    per_kind: dict[str, list[dict[str, Any]]] = {}
    for r in ev:
        kind = RELATION_TARGET_KIND.get(r["relation"].split(" ")[0])
        if kind is None:
            raise ValueError(f"unknown evidence relation: {r['relation']!r}")
        per_kind.setdefault(kind, []).append(r)

    def stagger(lane: str, x: int, min_gap: int = 112, rows_max: int = 4) -> int:
        taken = placed.setdefault(lane, [])
        for sub in range(rows_max):
            if all(not (s == sub and abs(x - px) < min_gap) for px, s in taken):
                taken.append((x, sub))
                return sub
        taken.append((x, rows_max - 1))
        return rows_max - 1

    for kind in EVENT_LANES:
        if kind not in per_kind:
            continue
        depth = 0
        for r in per_kind[kind]:
            sub = stagger(kind, x_of(_d(r["event_date"])))
            depth = max(depth, sub)
            add_node(r["target_id"], kind, label(r["target_id"], r["event_date"]),
                     x_of(_d(r["event_date"])), y_cursor + sub * sub_h,
                     event_date=r["event_date"], relation=r["relation"], detail=r.get("detail"),
                     feeds_feature=r.get("feeds_feature"), in_feature_window=r.get("in_feature_window"))
        y_cursor += lane_h + depth * sub_h

    # --- hubs on the top lane at their (first) date -------------------------------------------
    for kind in ("Incident", "PricingChange"):
        for r in per_kind.get(kind, []):
            if r["target_id"] in nodes:
                continue
            sub = stagger("hub", x_of(_d(r["event_date"])), min_gap=120, rows_max=2)
            add_node(r["target_id"], kind, label(r["target_id"], r["event_date"]),
                     x_of(_d(r["event_date"])), y_hub + sub * sub_h,
                     event_date=r["event_date"], note=hub_notes.get(r["target_id"]))

    # --- axis: weekly ticks counted back from as_of, and the as_of cut line ---------------------
    x_asof, x_renewal = x_of(as_of), x_of(renewal_date)
    week = 1
    while (as_of - t0).days - 7 * week >= 0:
        tick_x = round(((as_of - t0).days - 7 * week) * ppd)
        tick_day = date.fromordinal(as_of.toordinal() - 7 * week)
        add_node(f"axis:tick:{tick_day.isoformat()}", "Marker", tick_day.isoformat()[5:], tick_x, y_axis)
        week += 1
    add_node("axis:asof:top", "Marker", f"as_of {as_of.isoformat()} (T-7): nothing after this line is used",
             x_asof, -14)
    y_bottom = max(y_cursor - lane_h + 50, y_axis + 110)
    add_node("axis:asof:bottom", "Marker", "", x_asof, y_bottom)
    add_edge("axis:asof:top", "_asof", "axis:asof:bottom", text="")

    # --- subscription (left end of the lifeline), focus renewal (right end), plan --------------
    x_sub, x_focus, y_focus = -110, x_renewal, y_axis
    user = focus.get("user_name")
    add_node(sid, "Subscription", label(sid, user), x_sub, y_axis, subscription_id=sid, user_name=user)
    add_node(rid, "FocusRenewal", label(rid, f"as_of {as_of.isoformat()}"), x_focus, y_focus,
             as_of=as_of.isoformat(), renewal_date=renewal_date.isoformat(),
             plan_tier=focus.get("plan_tier"), route=focus.get("route"))
    plan = clean(focus.get("plan_tier") or "")
    add_edge(sid, "HAS_RENEWAL", rid, text="")        # the lifeline; named in the legend instead
    who = f"user {clean(user)}; " if user not in (None, "") else ""
    rows.append({"date": "", "relation": "HAS_RENEWAL", "target": f"{sid} \u2192 {rid}",
                 "note": who + "the horizontal lifeline; x position = event date"})
    if plan:
        add_node(f"plan:{plan}", "Plan", label(f"plan {plan}"), x_focus, y_hub, plan_tier=plan,
                 note=hub_notes.get(f"plan:{plan}"))
        add_edge(rid, "ON_PLAN", f"plan:{plan}")
        rows.append({"date": "", "relation": "ON_PLAN", "target": f"plan:{plan}", "note": "as of as_of"})

    for r in ev:
        rel = r["relation"].split(" ")[0]
        exc = bool(r.get("declared_exception"))
        if rel == "CUT_CAP":
            if plan:
                add_edge(r["target_id"], "CUT_CAP", f"plan:{plan}", r["event_date"])
        elif rel == "FIRST_RENEWAL_AFTER":
            add_edge(rid, rel, r["target_id"], r["event_date"], exception=exc,
                     text=rel + (" (declared exception)" if exc else ""))
        else:
            add_edge(sid, rel, r["target_id"], r["event_date"], exception=exc)
        note = [clean(r["detail"])] if r.get("detail") not in (None, "") else []
        if r.get("feeds_feature"):
            note.append(f"feeds {clean(r['feeds_feature'])}")
        if r.get("in_feature_window") is False:
            note.append("outside the feature window")
        if exc:
            note.append("declared exception (gold rule)")
        if r.get("known_by_as_of") is False:
            note.append("not known by as_of")
        rows.append({"date": r["event_date"], "relation": r["relation"], "target": r["target_id"],
                     "note": "; ".join(note)})

    # --- neighbours: 5 x 2 grid under the timeline, rank order ---------------------------------
    # Brick pattern (second row shifted half a column) so the fan of SIMILAR_TO edges passes between
    # the first-row nodes instead of through their labels.
    cols, col_w, row_h = 5, 165, 92
    y_nb = y_bottom + 80
    grid_w = (cols - 0.5) * col_w
    x_nb0 = round((x_sub + x_focus) / 2 - grid_w / 2)
    for i, n in enumerate(nb):
        outcome = clean(n.get("outcome") or queries.NOT_YET_OBSERVED)
        brick = col_w // 2 if (i // cols) % 2 else 0
        nx, ny = x_nb0 + (i % cols) * col_w + brick, y_nb + (i // cols) * row_h
        add_node(n["renewal_id"], "Renewal", label(f"#{n['rank']} {n['renewal_id']}", outcome), nx, ny,
                 rank=n["rank"], outcome=outcome, d2_q=n.get("d2_q"), dist=n.get("dist"),
                 outcome_observed_on=_iso(n.get("outcome_observed_on")))
        nodes[n["renewal_id"]]["outcome"] = outcome
        add_edge(rid, "SIMILAR_TO", n["renewal_id"], text=f"#{n['rank']}")
        rows.append({"date": "", "relation": f"SIMILAR_TO rank {n['rank']}", "target": n["renewal_id"],
                     "note": f"outcome {outcome}"
                     + (f"; d2_q {clean(n['d2_q'])}" if n.get("d2_q") is not None else "")})

    # Counted by date, not by flag: check_point_in_time already proved every row after as_of is a
    # flagged FIRST_RENEWAL_AFTER, so "after" is exactly the declared exceptions and "on or before"
    # is everything else.
    n_after = sum(1 for r in ev if _d(r["event_date"]) > as_of)
    n_before, n_nb = len(ev) - n_after, len(nb)
    lapsed = sum(1 for n in nb if "lapse" in str(n.get("outcome", "")))
    hidden = sum(1 for n in nb if (n.get("outcome") or queries.NOT_YET_OBSERVED) == queries.NOT_YET_OBSERVED)
    visibility = focus.get("visibility")
    build_id = focus.get("build_id")
    summary = (f"Point-in-time evidence for renewal {rid}"
               + (f" (build {clean(build_id)})" if build_id else "")
               + f": {plural(n_before, 'evidence row')} on or before as_of {as_of.isoformat()}"
               + (f" plus {plural(n_after, 'declared exception')} after it (FIRST_RENEWAL_AFTER, gold rule)"
                  if n_after else "")
               + f", and its {plural(n_nb, 'most similar renewal')} ({lapsed} lapsed"
               + (f", {hidden} not yet observed" if hidden else "")
               + (f"; outcomes visible {'today' if visibility == 'today' else 'at the source as_of'}"
                  if visibility else "")
               + "). Narrative evidence, not a risk estimate. The table below lists every edge drawn.")
    return Graph(sorted(nodes.values(), key=lambda n: n["id"]),
                 sorted(edges.values(), key=lambda e: e["id"]), rows,
                 f"Evidence graph for {rid}", summary,
                 "horizontal line = subscription lifeline (HAS_RENEWAL), left to right in time; vertical dashed "
                 "line = as_of; double border = lapsed; dashed border = outcome not yet observed; dotted edge = "
                 "declared exception")


def build_layered_graph(edges_in: Iterable[dict[str, Any]], root: str, *, title: str, summary: str,
                        kinds: dict[str, str] | None = None, col_w: int = 260, row_h: int = 64,
                        legend_note: str = "arrows point from a source to what is derived from it; left = upstream",
                        table_head: tuple[str, str, str, str] = ("Depth", "Relation", "Edge", "Detail")) -> Graph:
    """A small DAG (a lineage trace): longest-path layering, columns = layer, rows = sorted ids.

    edges_in: {from, to, rel?, roles?, note?, depth?} meaning data flows ``from`` -> ``to``.
    kinds:    node id -> KINDS key (default DataColumn; ``root`` is drawn as TraceTarget).
    A cycle (not expected in lineage) falls back to the trace depth given on the edges.
    """
    es = sorted(({"from": clean(e["from"]), "to": clean(e["to"]), "rel": clean(e.get("rel") or "DERIVED_FROM"),
                  "roles": clean(e.get("roles") or ""), "note": clean(e.get("note") or ""),
                  "depth": int(e.get("depth") or 0)} for e in edges_in),
                key=lambda e: (e["depth"], e["rel"], e["from"], e["to"], e["roles"], e["note"]))
    root = clean(root)
    kinds = {clean(k): v for k, v in (kinds or {}).items()}
    ids = sorted({e["from"] for e in es} | {e["to"] for e in es} | {root})
    layer = {i: 0 for i in ids}
    for _ in range(len(ids) + 1):                 # longest path from the sources along the flow
        changed = False
        for e in es:
            if e["from"] != e["to"] and layer[e["to"]] < layer[e["from"]] + 1:
                layer[e["to"]] = layer[e["from"]] + 1
                changed = True
        if not changed:
            break
    else:                                         # a cycle: use the trace depth instead
        depth = {root: 0}
        for e in es:
            for end in (e["from"], e["to"]):
                if end != root:
                    depth[end] = min(depth.get(end, e["depth"]), e["depth"])
        upstream = any(e["to"] == root for e in es)
        top = max(depth.values(), default=0)
        layer = {i: (top - depth.get(i, 0)) if upstream else depth.get(i, 0) for i in ids}
    layers: dict[int, list[str]] = {}
    for i in ids:
        layers.setdefault(layer[i], []).append(i)
    nodes = []
    for lay, members in sorted(layers.items()):
        for row, nid in enumerate(members):
            kind = "TraceTarget" if nid == root else kinds.get(nid, "DataColumn")
            kspec = KINDS[kind]
            text = label(*nid.split(".", 1)) if len(nid) > 26 else nid
            nodes.append({"id": nid, "kind": kind, "group": kspec["group"], "shape": kspec["shape"],
                          "size": kspec["size"], "label": text, "x": lay * col_w,
                          "y": round((row - (len(members) - 1) / 2) * row_h), "detail": {"layer": str(lay)}})
    edges, seen = [], set()
    for e in es:
        eid = f"e:{e['from']}|{e['rel']}|{e['to']}"
        if eid in seen:                            # one drawn edge per (from, rel, to); the table keeps every row
            continue
        seen.add(eid)
        edges.append({"id": eid, "source": e["from"], "target": e["to"], "relation": e["rel"],
                      "label": e["rel"] + (f" [{e['roles']}]" if e["roles"] else ""), "exception": False,
                      "count": 1, "first": "", "last": ""})
    rows = [{"date": str(e["depth"]) if e["depth"] else "", "relation": e["rel"] + (f" [{e['roles']}]" if e["roles"]
                                                                                   else ""),
             "target": f"{e['from']} \u2192 {e['to']}", "note": e["note"]} for e in es]
    return Graph(sorted(nodes, key=lambda n: n["id"]), sorted(edges, key=lambda e: e["id"]), rows, title, summary,
                 legend_note, table_head)


def lineage_graph(trace: dict[str, Any], caveats: Iterable[str] = (), *, provenance: str = "") -> Graph:
    """A lineage_trace answer (data, caveats) as a layered DAG in which data flows left to right.

    Trace rows go outward from the target (``from`` is the nearer node): upstream rows are drawn
    reversed (source -> derived), downstream rows as they are (column -> reader / check / consumer).
    """
    target, direction = trace["target"], trace["direction"]
    kinds: dict[str, str] = {}
    for layer_cols in trace["reached"].get("columns", {}).values():
        kinds.update({c: "DataColumn" for c in layer_cols})
    for bucket, kind in TRACE_BUCKET_KIND.items():
        kinds.update({x: kind for x in trace["reached"].get(bucket, [])})
    edges = []
    for r in trace["edges"]:
        a, b = (r["to"], r["from"]) if direction == "upstream" else (r["from"], r["to"])
        note = "; ".join(clean(x) for x in (r.get("cte") and f"cte {r['cte']}", r.get("window"), r.get("transform"))
                         if x)
        edges.append({"from": a, "to": b, "rel": r["rel"], "roles": r.get("roles"), "note": note,
                      "depth": r["depth"]})
    s = trace.get("summary", {})
    summary = (f"Code-derived {direction} lineage of {target}" + (f" ({clean(provenance)})" if provenance else "")
               + f": {s.get('edges', len(edges))} edges, {s.get('columns', 0)} columns, max depth "
               f"{s.get('max_depth_reached', 0)}." + "".join(f" {clean(c)}" for c in caveats))
    return build_layered_graph(edges, target, kinds=kinds, title=f"Lineage ({direction}) of {target}",
                               summary=summary)


def validate(graph: Graph) -> None:
    ids = [n["id"] for n in graph.nodes]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate node ids")
    known = set(ids)
    for n in graph.nodes:
        if n["kind"] not in KINDS:
            raise ValueError(f"unknown node kind {n['kind']!r}")
    for e in graph.edges:
        if e["source"] not in known or e["target"] not in known:
            raise ValueError(f"dangling edge {e['id']!r}")
    if len({e["id"] for e in graph.edges}) != len(graph.edges):
        raise ValueError("duplicate edge ids")


# ---------------------------------------------------------------------------------------------
# HTML (no template engine: fixed fragments + html.escape)
# ---------------------------------------------------------------------------------------------
def _css() -> str:
    def block(p: dict[str, str]) -> str:
        return "".join(f"--{k}:{v};" for k, v in sorted(p.items()))
    light, dark = block(PALETTES["light"]), block(PALETTES["dark"])
    return (
        ":root{color-scheme:light;" + light + "}"
        "@media (prefers-color-scheme:dark){:root:where(:not([data-theme=\"light\"])){color-scheme:dark;" + dark + "}}"
        ":root[data-theme=\"dark\"]{color-scheme:dark;" + dark + "}"
        "*{box-sizing:border-box}"
        "body{margin:0;background:var(--page);color:var(--ink);"
        "font:14px/1.45 system-ui,-apple-system,\"Segoe UI\",sans-serif}"
        ".viz-root{max-width:1100px;margin:0 auto;padding:12px 16px 20px}"
        "h1{font-size:16px;margin:0 0 4px}"
        "p{margin:4px 0;color:var(--ink2)}"
        "#cy{position:relative;width:100%;height:__HEIGHT__px;background:var(--surface);"
        "border:1px solid var(--axis);border-radius:6px;margin:8px 0}"
        ".toolbar{display:flex;gap:6px;align-items:center;flex-wrap:wrap}"
        "button{font:inherit;color:var(--ink);background:var(--surface);border:1px solid var(--axis);"
        "border-radius:4px;padding:2px 10px;cursor:pointer}"
        "button:focus-visible,summary:focus-visible{outline:2px solid var(--renewal);outline-offset:2px}"
        "#cy-detail{min-height:1.5em;color:var(--ink)}"
        ".legend{list-style:none;display:flex;flex-wrap:wrap;gap:4px 14px;padding:0;margin:6px 0}"
        ".legend li{display:flex;align-items:center;gap:5px;color:var(--ink2)}"
        ".legend svg{width:16px;height:16px;flex:none}"
        ".g-renewal{fill:var(--renewal)}.g-event{fill:var(--event)}.g-context{fill:var(--context)}"
        "table{border-collapse:collapse;width:100%;margin-top:6px;font-variant-numeric:tabular-nums}"
        "caption{text-align:left;color:var(--ink2);padding:2px 0}"
        "th,td{text-align:left;padding:3px 8px;border-bottom:1px solid var(--grid);vertical-align:top;"
        "overflow-wrap:anywhere}"
        "th{color:var(--ink2);font-weight:600}"
        "details{margin-top:8px}summary{cursor:pointer}"
        ".notice{font-size:12px;color:var(--ink2)}"      # muted is 3.4:1 on the light page: not for text
    )


# Tiny SVG glyphs for the legend so shape (not colour) carries identity there too.
_GLYPH = {
    "star": "<polygon points=\"8,1 10,6 15,6 11,9.5 12.5,15 8,11.5 3.5,15 5,9.5 1,6 6,6\"/>",
    "ellipse": "<circle cx=\"8\" cy=\"8\" r=\"6.5\"/>",
    "triangle": "<polygon points=\"8,1.5 15,14.5 1,14.5\"/>",
    "diamond": "<polygon points=\"8,1 15,8 8,15 1,8\"/>",
    "pentagon": "<polygon points=\"8,1 15,6.3 12.3,14.5 3.7,14.5 1,6.3\"/>",
    "rectangle": "<rect x=\"2\" y=\"2\" width=\"12\" height=\"12\"/>",
    "hexagon": "<polygon points=\"4.5,1.9 11.5,1.9 15,8 11.5,14.1 4.5,14.1 1,8\"/>",
    "round-rectangle": "<rect x=\"1.5\" y=\"3\" width=\"13\" height=\"10\" rx=\"3\"/>",
    "barrel": "<rect x=\"2\" y=\"2.5\" width=\"12\" height=\"11\" rx=\"5\" ry=\"2.5\"/>",
    "octagon": "<polygon points=\"5,1 11,1 15,5 15,11 11,15 5,15 1,11 1,5\"/>",
    "round-tag": "<polygon points=\"1.5,3 10.5,3 15,8 10.5,13 1.5,13\"/>",
}

# The page script (browser-proven in the ui-viz research: zero CSP violations, textContent only).
_APP_JS = r"""(function(){
"use strict";
var root=document.documentElement;
var status={ready:false,errors:[],csp:[],mode:null,nodes:0,edges:0,positions:null};
function publish(){root.setAttribute("data-cy-status",JSON.stringify(status));}
document.addEventListener("securitypolicyviolation",function(e){
status.csp.push(e.violatedDirective+" "+(e.blockedURI||"inline"));publish();});
window.addEventListener("error",function(e){status.errors.push(String(e.message));publish();});
var detail=document.getElementById("cy-detail");
var payload;
try{payload=JSON.parse(document.getElementById("graph-data").textContent);}
catch(err){status.errors.push("bad graph data");publish();return;}
if(typeof cytoscape!=="function"){status.errors.push("cytoscape not loaded");publish();
detail.textContent="The graph library did not load. The table below has the same content.";return;}
function mode(){var t=root.getAttribute("data-theme");if(t==="light"||t==="dark"){return t;}
return (window.matchMedia&&window.matchMedia("(prefers-color-scheme: dark)").matches)?"dark":"light";}
function styleFor(p){return [
{selector:"node",style:{"shape":"data(shape)","width":"data(size)","height":"data(size)","label":"data(label)",
"color":p.ink,"font-size":12,
"font-family":"system-ui, -apple-system, Segoe UI, sans-serif","text-wrap":"wrap","text-max-width":150,
"text-valign":"bottom","text-halign":"center","text-margin-y":4,"text-background-color":p.surface,
"text-background-opacity":0.85,"text-background-padding":2,"text-background-shape":"roundrectangle",
"border-width":2,"border-color":p.surface,"min-zoomed-font-size":5}},
{selector:"node[group = 'renewal']",style:{"background-color":p.renewal}},
{selector:"node[group = 'event']",style:{"background-color":p.event}},
{selector:"node[group = 'context']",style:{"background-color":p.context}},
{selector:"node[kind = 'FocusRenewal']",style:{"font-weight":700,"border-color":p.ink,"border-width":2}},
{selector:"node[outcome *= 'lapse']",style:{"border-color":p.ink,"border-width":3,"border-style":"double"}},
{selector:"node[outcome = 'not_yet_observed']",style:{"border-color":p.ink2,"border-width":2,"border-style":"dashed"}},
{selector:"node[group = 'axis']",style:{"background-color":p.muted,"border-width":0,"color":p.ink2,"font-size":11,
"text-valign":"top","text-margin-y":-4,"text-max-width":400,"text-background-opacity":0}},
{selector:"edge",style:{"width":1.5,"line-color":p.muted,"target-arrow-color":p.muted,"target-arrow-shape":"triangle",
"arrow-scale":0.9,"curve-style":"bezier","label":"data(label)","font-size":10,"color":p.ink2,
"text-rotation":"autorotate","text-background-color":p.surface,"text-background-opacity":0.85,"text-background-padding":1}},
{selector:"edge[relation = 'SIMILAR_TO']",style:{"line-style":"dashed","width":1,"opacity":0.7}},
{selector:"edge[?exception]",style:{"line-style":"dotted","width":2.5,"line-color":p.ink,"target-arrow-color":p.ink,"color":p.ink}},
{selector:"edge[relation = 'HAS_RENEWAL']",style:{"width":2.5,"curve-style":"straight","text-margin-y":-9}},
{selector:"edge[relation = '_asof']",style:{"target-arrow-shape":"none","line-color":p.ink2,
"line-style":"dashed","width":1.5,"curve-style":"straight"}},
{selector:".dim",style:{"opacity":0.25}},
{selector:"node:selected",style:{"border-color":p.ink,"border-width":4,"border-style":"solid"}}
];}
var opts=payload.options;
var cy=cytoscape({container:document.getElementById("cy"),elements:payload.elements,layout:{name:"preset"},
style:styleFor(payload.palettes[mode()]),autoungrabify:true,boxSelectionEnabled:false,
userZoomingEnabled:!!opts.wheelZoom,userPanningEnabled:true,minZoom:0.2,maxZoom:3});
function applyTheme(){status.mode=mode();
cy.style().fromJson(styleFor(payload.palettes[status.mode])).update();publish();}
if(window.matchMedia){var mq=window.matchMedia("(prefers-color-scheme: dark)");
if(mq.addEventListener){mq.addEventListener("change",applyTheme);}}
function snapshot(){status.nodes=cy.nodes().length;status.edges=cy.edges().length;
status.positions=cy.nodes().map(function(n){return [n.id(),Math.round(n.position("x")),
Math.round(n.position("y"))];}).sort();
status.zoom=Math.round(cy.zoom()*1000)/1000;status.mode=mode();status.ready=true;publish();}
if(opts.layout.name==="preset"){cy.fit(undefined,24);snapshot();}
else{var layout=cy.layout(opts.layout);
layout.one("layoutstop",function(){cy.fit(undefined,24);snapshot();});layout.run();}
function show(n){var d=n.data();var parts=[d.kind+" "+d.id];var k=Object.keys(d.detail||{}).sort();
for(var i=0;i<k.length;i++){parts.push(k[i]+": "+d.detail[k[i]]);}detail.textContent=parts.join(" \u00b7 ");}
cy.on("tap","node[group != 'axis']",function(evt){var n=evt.target;cy.elements().addClass("dim");
n.closedNeighborhood().removeClass("dim");show(n);});
cy.on("tap",function(evt){if(evt.target===cy){cy.elements().removeClass("dim");detail.textContent=opts.hint;}});
function bind(id,fn){var b=document.getElementById(id);if(b){b.addEventListener("click",fn);}}
function zoomBy(f){cy.zoom({level:cy.zoom()*f,renderedPosition:{x:cy.width()/2,y:cy.height()/2}});}
bind("btn-fit",function(){cy.fit(undefined,24);});
bind("btn-in",function(){zoomBy(1.25);});
bind("btn-out",function(){zoomBy(0.8);});
window.addEventListener("resize",function(){cy.resize();cy.fit(undefined,24);});
window.__cy=cy;
})();"""


def _legend(graph: Graph) -> str:
    counts: dict[str, int] = {}
    for n in graph.nodes:
        if n["kind"] != "Marker":
            counts[n["kind"]] = counts.get(n["kind"], 0) + 1
    items = []
    for kind, kspec in KINDS.items():            # fixed order: never depends on the data
        if kind in counts:
            items.append(
                "<li><svg viewBox=\"0 0 16 16\" aria-hidden=\"true\" focusable=\"false\" class=\"g-"
                + kspec["group"] + "\">" + _GLYPH[kspec["shape"]] + "</svg>"
                + html.escape(f"{kspec['legend']} ({counts[kind]})") + "</li>")
    if graph.legend_note:
        items.append("<li>" + html.escape(graph.legend_note) + "</li>")
    return "<ul class=\"legend\" aria-label=\"Legend: node shapes\">" + "".join(items) + "</ul>"


def estimate_height(graph: Graph, *, height: int = 560, table_open: bool = True) -> int:
    """Pixel height for an iframe (Streamlit: st.iframe(html, height=<int>)).

    Never use st.iframe(height="content") with this page: Streamlit then appends its own inline
    measuring <script> to the srcdoc, which the page's CSP blocks (the iframe collapses to 150 px).
    The 330 px of chrome include 40 px of slack for a narrow (centred) column, where the legend
    wraps; the iframe scrolls, so an estimate is enough.
    """
    chrome = 330
    table = 70 + 30 * len(graph.rows) if table_open else 40
    return height + chrome + table


def _table(graph: Graph, table_open: bool = True) -> str:
    head = "<tr>" + "".join(f"<th scope=\"col\">{html.escape(h)}</th>" for h in graph.table_head) + "</tr>"
    body = "".join(
        "<tr><td>" + html.escape(r["date"]) + "</td><td>" + html.escape(r["relation"]) + "</td><td>"
        + html.escape(r["target"]) + "</td><td>" + html.escape(r["note"]) + "</td></tr>" for r in graph.rows)
    return ("<details" + (" open" if table_open else "") + "><summary>Same content as a table ("
            + str(len(graph.rows)) + " rows)</summary>"
            "<table><caption id=\"viz-table-caption\">Every edge drawn above, in the order the tool returned it."
            "</caption><thead>" + head + "</thead><tbody>" + body + "</tbody></table></details>")


def render_html(
    graph: Graph,
    *,
    cytoscape_js: str | None = None,
    js_mode: JsMode = "inline",
    theme: Theme = "auto",
    layout: Layout = "preset",
    height: int = 560,
    wheel_zoom: bool = False,
    relative_src: str = "cytoscape.min.js",
    table_open: bool = True,
) -> str:
    """Return one self-contained HTML document (UTF-8 text). Deterministic for fixed inputs.

    js_mode: ``inline`` (default: the vendored library inside the page, works offline),
    ``cdn`` (opt-in: the pinned jsDelivr URL with SRI, and the CSP allows only that URL; needs
    the network), ``relative`` (a ``<script src>`` next to the file; no SRI is possible on file://).
    """
    validate(graph)
    if theme not in THEMES:
        raise ValueError(f"theme {theme!r}: expected one of {', '.join(THEMES)}")
    if layout not in LAYOUTS:
        raise ValueError(f"layout {layout!r}: expected one of {', '.join(LAYOUTS)} (cose is not deterministic)")
    layout_opts: dict[str, Any] = {"name": layout, "animate": False, "fit": True, "padding": 24}
    if layout == "concentric":
        layout_opts.update({"minNodeSpacing": 30})
    elif layout == "breadthfirst":
        layout_opts.update({"directed": True, "spacingFactor": 1.1})
    elements = {
        "nodes": [{"data": {k: v for k, v in n.items() if k not in ("x", "y")},
                   "position": {"x": n["x"], "y": n["y"]}} for n in graph.nodes],
        "edges": [{"data": e} for e in graph.edges],
    }
    hint = "Select a node to see its properties."
    payload = {"spec": VIZ_SPEC, "elements": elements, "palettes": PALETTES,
               "options": {"layout": layout_opts, "wheelZoom": bool(wheel_zoom), "hint": hint}}
    data_json = script_json(payload)
    css = _css().replace("__HEIGHT__", str(int(height)))
    app_js = _APP_JS

    if js_mode == "inline":
        if cytoscape_js is None:
            raise ValueError("js_mode='inline' needs cytoscape_js (the vendored dist/cytoscape.min.js text: "
                             "viz.load_vendored())")
        # Only the pinned release: the licence notice, the generator meta and the injected-style CSP hash
        # all name 3.34.3, and those bytes are tested to contain no "</script" or "<!--".
        got = hashlib.sha256(cytoscape_js.encode("utf-8")).hexdigest()
        if got != CYTOSCAPE_SHA256:
            raise ValueError(f"cytoscape_js sha256 {got} is not the pinned Cytoscape.js {CYTOSCAPE_VERSION} "
                             f"({CYTOSCAPE_SHA256}): pass viz.load_vendored()")
        lib_tag = "<script>" + cytoscape_js + "</script>"
        script_src = " ".join([csp_hash(cytoscape_js), csp_hash(app_js)])
    elif js_mode == "cdn":
        lib_tag = ("<script src=\"" + html.escape(CYTOSCAPE_CDN, quote=True) + "\" integrity=\"" + CYTOSCAPE_SRI
                   + "\" crossorigin=\"anonymous\" referrerpolicy=\"no-referrer\"></script>")
        # The full file URL (a CSP path without a trailing '/' matches that one file), not the host: no
        # other script on cdn.jsdelivr.net may run even if something injected a tag for it.
        script_src = CYTOSCAPE_CDN + " " + csp_hash(app_js)
    elif js_mode == "relative":
        # No integrity attribute here: Chrome refuses SRI on file:// subresources ("requires the
        # request to be CORS enabled", measured), which is exactly where a relative src is used.
        lib_tag = "<script src=\"" + html.escape(relative_src, quote=True) + "\"></script>"
        script_src = "'self' " + csp_hash(app_js)
    else:
        raise ValueError(f"js_mode {js_mode!r}: expected inline, cdn or relative")
    # Cytoscape's canvas renderer inserts ONE <style> element at init (its container rule). Without its
    # hash the browser blocks it and logs a CSP violation (measured); the text is fixed in the library.
    csp = ("default-src 'none'; script-src " + script_src + "; style-src " + csp_hash(css) + " "
           + csp_hash(CYTOSCAPE_INJECTED_STYLE) + "; img-src data:; base-uri 'none'; form-action 'none'")

    esc = html.escape
    parts = [
        "<!doctype html>\n",
        "<html lang=\"en\" data-theme=\"" + theme + "\">\n<head>\n",
        "<meta charset=\"utf-8\">\n",
        "<meta http-equiv=\"Content-Security-Policy\" content=\"" + esc(csp, quote=True) + "\">\n",
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n",
        "<meta name=\"generator\" content=\""
        + esc(f"lakehouse_graph.viz {VIZ_SPEC}; cytoscape {CYTOSCAPE_VERSION} (MIT)", quote=True) + "\">\n",
        "<title>" + esc(graph.title) + "</title>\n",
        "<style>" + css + "</style>\n</head>\n<body>\n<main class=\"viz-root\">\n",
        "<h1 id=\"viz-title\">" + esc(graph.title) + "</h1>\n",
        "<p id=\"viz-summary\">" + esc(graph.summary) + "</p>\n",
        "<div class=\"toolbar\" role=\"group\" aria-label=\"Graph view controls\">"
        "<button type=\"button\" id=\"btn-fit\">Fit</button>"
        "<button type=\"button\" id=\"btn-in\" aria-label=\"Zoom in\">+</button>"
        "<button type=\"button\" id=\"btn-out\" aria-label=\"Zoom out\">\u2212</button></div>\n",
        "<div id=\"cy\" role=\"img\" aria-labelledby=\"viz-title\" aria-describedby=\"viz-summary viz-table-caption\">"
        "</div>\n",
        "<p id=\"cy-detail\" role=\"status\" aria-live=\"polite\">" + esc(hint) + "</p>\n",
        _legend(graph) + "\n",
        _table(graph, table_open) + "\n",
        "<noscript><p>JavaScript is off: the table above is the full content of this view.</p></noscript>\n",
        "<p class=\"notice\">" + esc(LICENCE_NOTICE) + ". Generated offline; this page makes no network requests"
        + (" except the pinned library file" if js_mode == "cdn" else "") + ".</p>\n",
        "</main>\n",
        "<script type=\"application/json\" id=\"graph-data\">" + data_json + "</script>\n",
        lib_tag + "\n",
        "<script>" + app_js + "</script>\n",
        "</body>\n</html>\n",
    ]
    return "".join(parts)


def load_vendored(path: str | Path | None = None) -> str:
    """Read the vendored library and refuse a file whose bytes are not the pinned release."""
    raw = Path(path or CYTOSCAPE_FILE).read_bytes()
    got = hashlib.sha256(raw).hexdigest()
    if got != CYTOSCAPE_SHA256:
        raise ValueError(f"cytoscape.min.js sha256 {got} != pinned {CYTOSCAPE_SHA256} (see vendor/README.md)")
    return raw.decode("utf-8")


# ---------------------------------------------------------------------------------------------
# Data path: what the tools serve, from a build directory
# ---------------------------------------------------------------------------------------------
class VizContext:
    """Minimal context: a build directory with lazily opened read-only Ladybug connections.

    Any object with ``build_dir`` and a ``graph_conn`` (or ``conn``) attribute works too (the MCP
    server's ToolContext); ``lineage_conn`` is only needed by lineage_view.
    """

    def __init__(self, build_dir: str | Path, buffer_pool_mb: int = store.SERVE_BUFFER_POOL_MB):
        self.build_dir = Path(build_dir)
        self.buffer_pool_mb = buffer_pool_mb
        self._open: dict[str, tuple] = {}

    def _get(self, name: str, file: str, required: bool):
        if name not in self._open:
            path = self.build_dir / file
            if not path.exists():
                if required:
                    raise FileNotFoundError(f"{path} not found: run make graph-build (or pass --build <dir>)")
                return None
            self._open[name] = store.open_readonly(path, buffer_pool_mb=self.buffer_pool_mb)
        return self._open[name][1]

    @property
    def graph_conn(self):
        return self._get("graph", store.DB_FILE, required=True)

    @property
    def lineage_conn(self):
        return self._get("lineage", "lineage.lbdb", required=False)

    def close(self) -> None:
        for db, conn in self._open.values():
            conn.close()
            db.close()
        self._open.clear()

    def __enter__(self) -> VizContext:
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def _graph_conn(ctx):
    for name in ("graph_conn", "conn"):
        conn = getattr(ctx, name, None)
        if conn is not None:
            return conn
    raise ValueError("the context has no graph connection (graph_conn / conn): use viz.VizContext(build_dir)")


def check_renewal_id(renewal_id: Any) -> str:
    if not isinstance(renewal_id, str) or not RENEWAL_ID_RE.fullmatch(renewal_id):
        raise ValueError(f"invalid renewal id {renewal_id!r}: expected sub_<id>:<YYYY-MM-DD>, "
                         f"for example sub_santosh:2026-10-07")
    return renewal_id


def _read_rows(path: Path, columns: list[str], key: str | None = None, values: Iterable[str] = ()) -> list[dict]:
    import pyarrow.parquet as pq

    wanted = sorted(set(values))
    filters = [(key, "in", wanted)] if key else None
    if key and not wanted:
        return []
    return pq.read_table(path, columns=columns, filters=filters).to_pylist()


def _hub_notes(build_dir: Path, conn, evidence: list[dict], plan: str | None) -> list[dict]:
    """Facts of the shared hubs the evidence touches (public calendar: dates, multipliers, prices)
    and their population totals as of today (a count of edges to a global event, no outcome)."""
    pdir = build_dir / "parquet"
    inc_ids = sorted({r["target_id"] for r in evidence if r["relation"] == "EXPOSED_TO"})
    pc_ids = sorted({r["target_id"] for r in evidence if r["relation"] in ("CUT_CAP", "FIRST_RENEWAL_AFTER")})
    hubs = []
    for row in _read_rows(pdir / "nodes_Incident.parquet", ["incident_id", "starts_on", "ends_on", "days"],
                          "incident_id", inc_ids):
        exposed = sum(int(x["exposed"]) for x in queries.fetch(conn, "exposure_incident_by_plan",
                                                               {"incident_id": row["incident_id"]}))
        hubs.append({"id": row["incident_id"],
                     "note": f"incident {_iso(row['starts_on'])} to {_iso(row['ends_on'])} "
                             f"({plural(int(row['days']), 'day')}); {plural(exposed, 'renewal')} exposed inside "
                             f"their 28-day feature window (population total, as of today)"})
    for row in _read_rows(pdir / "nodes_PricingChange.parquet", ["change_id", "effective_date", "description",
                                                                 "cap_multiplier"], "change_id", pc_ids):
        first = sum(int(x["renewals"]) for x in queries.fetch(conn, "exposure_pricing_change",
                                                              {"change_id": row["change_id"]}))
        hubs.append({"id": row["change_id"],
                     "note": f"effective {_iso(row['effective_date'])}; cap x{row['cap_multiplier']:g}; "
                             f"{clean(row['description'])}; {plural(first, 'renewal was', 'renewals were')} the "
                             f"first after it (gold rule, population total)"})
    for row in _read_rows(pdir / "nodes_Plan.parquet", ["plan_tier", "price_usd", "base_allowance_28d"],
                          "plan_tier", [plan] if plan else []):
        hubs.append({"id": f"plan:{row['plan_tier']}",
                     "note": f"list price {row['price_usd']:g} USD / month; base allowance "
                             f"{row['base_allowance_28d']:,} agent requests / 28 days"})
    return sorted(hubs, key=lambda h: h["id"])


def _user_name(build_dir: Path, subscription_id: str) -> str | None:
    rows = _read_rows(build_dir / "parquet" / "nodes_Subscription.parquet", ["subscription_id", "user_name"],
                      "subscription_id", [subscription_id])
    return rows[0]["user_name"] if rows else None


def evidence_data(ctx, renewal_id: str, *, k: int = spec.K, outcome_visibility: str = "auto") -> dict:
    """The rows the evidence view draws, read through the tool templates of queries.py.

    Returns {focus, evidence, neighbours, hubs, truncated}. ``outcome_visibility`` follows the
    neighbour tool: ``auto`` = ``today`` only for a current source (route score_today / pending),
    else ``source_as_of``; an explicit ``today`` on a historical source is a ValueError. Raises
    UnknownRenewal (a ValueError) for an id that is not in the build.
    """
    rid = check_renewal_id(renewal_id)
    if outcome_visibility not in VISIBILITIES:
        raise ValueError(f"outcome_visibility {outcome_visibility!r}: expected one of {', '.join(VISIBILITIES)}")
    if isinstance(k, bool) or not isinstance(k, int) or not 1 <= k <= spec.K:
        raise ValueError(f"k {k!r}: expected an integer from 1 to {spec.K}")
    conn = _graph_conn(ctx)
    build_dir = Path(ctx.build_dir)
    head = queries.fetch(conn, "renewal_header", {"renewal_id": rid})
    if not head:
        raise UnknownRenewal(f"unknown renewal {rid!r}: not in the build {build_dir.name} (find ids with "
                             f"graph_find, or look at parquet/nodes_Renewal.parquet)")
    h = head[0]
    current = h["route"] in CURRENT_ROUTES
    if outcome_visibility == "today" and not current:
        raise ValueError("outcome_visibility='today' is only valid for current sources (route score_today or "
                         f"pending); {rid} is historical (route {h['route']})")
    today = current if outcome_visibility == "auto" else outcome_visibility == "today"
    ev = queries.evidence(conn, rid, limit=EVIDENCE_ROWS + 1)
    nb = queries.fetch(conn, "similar_top_k_visible", {"renewal_id": rid, "k": k, "today": today})
    man = mf.read_manifest(build_dir)
    focus = {"renewal_id": rid, "subscription_id": h["subscription_id"], "plan_tier": h["plan_tier"],
             "as_of": _iso(h["as_of"]), "renewal_date": _iso(h["renewal_date"]), "route": h["route"],
             "user_name": _user_name(build_dir, h["subscription_id"]),
             "visibility": "today" if today else "source_as_of", "build_id": man.get("business_build_id")}
    neighbours = [{"rank": int(n["rank"]), "renewal_id": n["renewal_id"], "d2_q": int(n["d2_q"]),
                   "dist": round(float(n["dist"]), 4), "outcome": n["outcome"],
                   "outcome_observed_on": _iso(n["outcome_observed_on"])} for n in nb]
    evidence = ev[:EVIDENCE_ROWS]
    check_point_in_time(focus, evidence, neighbours)
    return {"focus": focus, "evidence": evidence, "neighbours": neighbours,
            "hubs": _hub_notes(build_dir, conn, evidence, h["plan_tier"]), "truncated": len(ev) > EVIDENCE_ROWS}


def evidence_view(ctx, renewal_id: str, *, theme: Theme = "auto", embed: bool = False, js_mode: JsMode = "inline",
                  cytoscape_js: str | None = None, outcome_visibility: str = "auto") -> tuple[str, int]:
    """(html, height) of a renewal's evidence view. ``embed=True`` for an iframe (no wheel zoom,
    table closed); standalone files get wheel zoom and an open table."""
    data = evidence_data(ctx, renewal_id, outcome_visibility=outcome_visibility)
    graph = build_evidence_graph(data["focus"], data["evidence"], data["neighbours"], data["hubs"])
    if data["truncated"]:
        graph = replace(graph, summary=graph.summary + f" Only the first {EVIDENCE_ROWS} evidence rows (by date) "
                                                       f"are drawn.")
    lib = cytoscape_js if cytoscape_js is not None or js_mode != "inline" else load_vendored()
    page = render_html(graph, cytoscape_js=lib, js_mode=js_mode, theme=theme, wheel_zoom=not embed,
                       table_open=not embed)
    return page, estimate_height(graph, table_open=not embed)


def lineage_view(ctx, column: str, *, direction: str = "upstream", max_depth: int = 6, theme: Theme = "auto",
                 embed: bool = False, js_mode: JsMode = "inline", cytoscape_js: str | None = None) -> tuple[str, int]:
    """(html, height) of a lineage_trace answer for a ColumnRef (needs <build_dir>/lineage.lbdb)."""
    from .lineage import tools as ltools  # lazily: the evidence view needs no lineage code

    data, caveats = ltools.lineage_trace(ctx, column, direction, max_depth)
    lid = None
    with contextlib.suppress(OSError, ValueError):
        lid = json.loads((Path(ctx.build_dir) / "lineage" / "manifest.json").read_text()).get("lineage_build_id")
    graph = lineage_graph(data, caveats, provenance=f"lineage build {lid}" if lid else "")
    lib = cytoscape_js if cytoscape_js is not None or js_mode != "inline" else load_vendored()
    height = max(320, min(900, 120 + 70 * max((sum(1 for n in graph.nodes if n["x"] == x)
                                                for x in {n["x"] for n in graph.nodes}), default=1)))
    page = render_html(graph, cytoscape_js=lib, js_mode=js_mode, theme=theme, height=height, wheel_zoom=not embed,
                       table_open=not embed)
    return page, estimate_height(graph, height=height, table_open=not embed)


def output_name(renewal_id: str) -> str:
    """File name of a renewal's view: ':' is not portable in file names."""
    return check_renewal_id(renewal_id).replace(":", "_") + ".html"


__all__ = [
    "CYTOSCAPE_INJECTED_STYLE",
    "CYTOSCAPE_SHA256",
    "CYTOSCAPE_SRI",
    "CYTOSCAPE_VERSION",
    "VIZ_SPEC",
    "Graph",
    "UnknownRenewal",
    "VizContext",
    "build_evidence_graph",
    "build_layered_graph",
    "check_point_in_time",
    "csp_hash",
    "estimate_height",
    "evidence_data",
    "evidence_view",
    "lineage_graph",
    "lineage_view",
    "load_vendored",
    "plural",
    "render_html",
    "script_json",
]
