"""Deterministic SVG charts, a static graph renderer and Mermaid diagrams for docs/graph (no plotting library).

Every figure is drawn twice, once per theme, by a small hand-written SVG writer:

  <name>-light.svg   chart surface #fcfcfb, ink #0b0b0b
  <name>-dark.svg    chart surface #1a1a19, ink #ffffff

so a markdown page can switch with ``<picture><source media="(prefers-color-scheme: dark)" ...>``.
The same input gives the same bytes: no timestamps, no random ids, rows sorted explicitly, every
coordinate rounded to 2 decimals. Text never wears a series colour (it uses the ink tokens); the
categorical hues come from one fixed palette in a fixed order (slot 1 blue, 2 orange, 3 aqua,
4 yellow), never cycled, validated light and dark with the dataviz palette validator (adjacent pairs
for up to 4 series, all pairs for the 3-slot dot plot). Marks are thin: bars at most 18 px with a
4 px rounded data end and a 2 px surface gap between stacked segments, 2 px whiskers, markers with
a 2 px surface ring, one hairline axis, no dual axis. Each SVG has a <title> and a <desc>, and each
mark a <title> (a tooltip when the file is opened directly); every figure also carries the same
numbers as a markdown table, which the docs print next to the picture.

The module has three layers:

  1. the writer and the figure builders: ``*_figure(data) -> Figure`` take plain dicts / lists and
     never read files, so a test can plant a number and find it in the SVG;
  2. the data extractors: ``*_data(...)`` read one graph build (Parquet through the pandas oracle,
     cohorts.parquet, lineage Parquet) or a results JSON. Nothing is typed in: every number on a
     chart comes from those inputs;
  3. Mermaid text (graph schema with counts, the entity-relationship schema from spec.py, the
     upstream lineage of one column) and ``fill_regions`` for the generated regions of the docs.

scripts/graph_charts.py is the CLI; scripts/graph_evidence.py calls it after running the checks.
"""
from __future__ import annotations

import html
import math
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

# --------------------------------------------------------------------------- themes (dataviz reference palette)
FONT = "system-ui, -apple-system, 'Segoe UI', sans-serif"


@dataclass(frozen=True)
class Theme:
    name: str
    surface: str
    ink: str
    ink2: str
    muted: str
    grid: str
    axis: str
    band: str
    series: tuple[str, ...]


LIGHT = Theme("light", surface="#fcfcfb", ink="#0b0b0b", ink2="#52514e", muted="#898781", grid="#e1e0d9",
              axis="#c3c2b7", band="#f0efec",
              series=("#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"))
DARK = Theme("dark", surface="#1a1a19", ink="#ffffff", ink2="#c3c2b7", muted="#898781", grid="#2c2c2a",
             axis="#383835", band="#2c2c2a",
             series=("#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300", "#9085e9", "#e66767"))
THEMES = (LIGHT, DARK)

W = 760          # every figure is 760 px wide (GitHub scales it down on narrow screens)
PAD = 24
BAR = 16         # bar thickness (<= 24 px)
ROUND = 4        # rounded data end
GAP = 2          # surface gap between stacked segments

# Approximate advance widths (Helvetica AFM, 1/1000 em) for layout; system-ui is a little wider (x 1.06).
_W = {**dict.fromkeys("0123456789", 556), " ": 278, "!": 278, '"': 355, "#": 556, "$": 556, "%": 889, "&": 667,
      "'": 191, "(": 333, ")": 333, "*": 389, "+": 584, ",": 278, "-": 333, ".": 278, "/": 278, ":": 278,
      ";": 278, "<": 584, "=": 584, ">": 584, "?": 556, "@": 1015, "[": 278, "]": 278, "_": 556, "|": 260,
      "A": 667, "B": 667, "C": 722, "D": 722, "E": 667, "F": 611, "G": 778, "H": 722, "I": 278, "J": 500,
      "K": 667, "L": 556, "M": 833, "N": 722, "O": 778, "P": 667, "Q": 778, "R": 722, "S": 667, "T": 611,
      "U": 722, "V": 667, "W": 944, "X": 667, "Y": 667, "Z": 611, "a": 556, "b": 556, "c": 500, "d": 556,
      "e": 556, "f": 278, "g": 556, "h": 556, "i": 222, "j": 222, "k": 500, "l": 222, "m": 833, "n": 556,
      "o": 556, "p": 556, "q": 556, "r": 333, "s": 500, "t": 278, "u": 556, "v": 500, "w": 722, "x": 500,
      "y": 500, "z": 500, "·": 278, "≤": 584, "≥": 584, "→": 1000, "–": 556}  # noqa: RUF001 (an en dash width)


def text_width(s: str, size: float, bold: bool = False) -> float:
    """Estimated rendered width in px (layout only; deterministic)."""
    w = sum(_W.get(ch, 600) for ch in s) * size / 1000 * 1.06
    return w * 1.05 if bold else w


def wrap(text: str, width: float, size: float) -> list[str]:
    """Greedy word wrap to ``width`` px."""
    lines: list[str] = []
    cur = ""
    for word in text.split():
        cand = f"{cur} {word}".strip()
        if cur and text_width(cand, size) > width:
            lines.append(cur)
            cur = word
        else:
            cur = cand
    if cur:
        lines.append(cur)
    return lines or [""]


def _n(v: float) -> str:
    """A coordinate: at most 2 decimals, no trailing zeros, never '-0'."""
    s = f"{round(float(v), 2):.2f}".rstrip("0").rstrip(".")
    return "0" if s in ("-0", "") else s


def fmt_int(v) -> str:
    return f"{int(v):,}"


def fmt_pct(v: float, digits: int = 1) -> str:
    return f"{100 * v:.{digits}f}%"


def _esc(s) -> str:
    return html.escape(str(s), quote=True)


def nice_scale(vmax: float, target: int = 5) -> tuple[float, float]:
    """(axis max, tick step) with steps 1 / 2 / 2.5 / 5 x 10^k."""
    if vmax <= 0:
        return 1.0, 0.2
    raw = vmax / target
    mag = 10 ** math.floor(math.log10(raw))
    step = next(m * mag for m in (1, 2, 2.5, 5, 10) if raw <= m * mag + 1e-12)
    return step * math.ceil(vmax / step - 1e-9), step


def luminance(hexcolor: str) -> float:
    def ch(c: float) -> float:
        c /= 255
        return c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4
    r, g, b = (int(hexcolor[i:i + 2], 16) for i in (1, 3, 5))
    return 0.2126 * ch(r) + 0.7152 * ch(g) + 0.0722 * ch(b)


def on_fill(fill: str) -> str:
    """Ink for a label set inside a coloured fill (black or white by luminance)."""
    return "#0b0b0b" if luminance(fill) > 0.18 else "#ffffff"


# --------------------------------------------------------------------------- SVG writer
class Svg:
    """A tiny, ordered SVG document (no ids, no timestamps)."""

    def __init__(self, width: float, height: float, theme: Theme, title: str, desc: str):
        self.w, self.h, self.t = width, height, theme
        self.parts: list[str] = [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{_n(width)}" height="{_n(height)}" '
            f'viewBox="0 0 {_n(width)} {_n(height)}" role="img" font-family="{_esc(FONT)}">',
            f"<title>{_esc(title)}</title>",
            f"<desc>{_esc(desc)}</desc>",
            f'<rect width="{_n(width)}" height="{_n(height)}" rx="8" fill="{theme.surface}"/>',
        ]

    def add(self, s: str) -> None:
        self.parts.append(s)

    def open(self, tip: str | None = None) -> None:
        self.add("<g>" + (f"<title>{_esc(tip)}</title>" if tip else ""))

    def close(self) -> None:
        self.add("</g>")

    def rect(self, x, y, w, h, fill, rx: float | None = None, opacity: float | None = None,
             stroke: str | None = None) -> None:
        extra = (f' rx="{_n(rx)}"' if rx else "") + (f' fill-opacity="{_n(opacity)}"' if opacity is not None else "") \
            + (f' stroke="{stroke}" stroke-width="1"' if stroke else "")
        self.add(f'<rect x="{_n(x)}" y="{_n(y)}" width="{_n(max(0, w))}" height="{_n(max(0, h))}" '
                 f'fill="{fill}"{extra}/>')

    def line(self, x1, y1, x2, y2, stroke, width: float = 1, cap: str | None = None) -> None:
        extra = f' stroke-linecap="{cap}"' if cap else ""
        self.add(f'<line x1="{_n(x1)}" y1="{_n(y1)}" x2="{_n(x2)}" y2="{_n(y2)}" stroke="{stroke}" '
                 f'stroke-width="{_n(width)}"{extra}/>')

    def path(self, d: str, fill: str = "none", stroke: str | None = None, width: float | None = None) -> None:
        extra = (f' stroke="{stroke}"' if stroke else "") + (f' stroke-width="{_n(width)}"' if width else "")
        self.add(f'<path d="{d}" fill="{fill}"{extra}/>')

    def circle(self, cx, cy, r, fill, stroke: str | None = None, width: float | None = None) -> None:
        extra = (f' stroke="{stroke}"' if stroke else "") + (f' stroke-width="{_n(width)}"' if width else "")
        self.add(f'<circle cx="{_n(cx)}" cy="{_n(cy)}" r="{_n(r)}" fill="{fill}"{extra}/>')

    def text(self, x, y, s, size: float = 12, fill: str | None = None, anchor: str = "start",
             weight: str | None = None, tabular: bool = False) -> None:
        extra = (f' text-anchor="{anchor}"' if anchor != "start" else "") + \
                (f' font-weight="{weight}"' if weight else "") + \
                (' font-variant-numeric="tabular-nums"' if tabular else "")
        self.add(f'<text x="{_n(x)}" y="{_n(y)}" font-size="{_n(size)}" fill="{fill or self.t.ink}"{extra}>'
                 f"{_esc(s)}</text>")

    def render(self) -> str:
        return "\n".join([*self.parts, "</svg>"]) + "\n"


def hbar_path(x0: float, y: float, length: float, h: float, round_end: bool = True) -> str:
    """A horizontal bar: square at the baseline, 4 px rounded data end."""
    r = min(ROUND, length / 2, h / 2) if round_end else 0
    x1 = x0 + length
    if r <= 0:
        return f"M{_n(x0)} {_n(y)}H{_n(x1)}V{_n(y + h)}H{_n(x0)}Z"
    return (f"M{_n(x0)} {_n(y)}H{_n(x1 - r)}A{_n(r)} {_n(r)} 0 0 1 {_n(x1)} {_n(y + r)}"
            f"V{_n(y + h - r)}A{_n(r)} {_n(r)} 0 0 1 {_n(x1 - r)} {_n(y + h)}H{_n(x0)}Z")


def zero_or_bar(svg: Svg, x0: float, y: float, length: float, h: float, color: str, zero: bool = False) -> float:
    """A bar from the baseline, or for a zero a hollow ring on the baseline (a stub would read as a small
    positive value). Returns the x where a value label may start."""
    if not zero:
        length = max(length, 1.5)
        svg.path(hbar_path(x0, y, length, h, round_end=length > 3), fill=color)
        return x0 + length
    r = h / 2 - 1.5
    svg.circle(x0 + r + 2, y + h / 2, r, svg.t.surface, stroke=color, width=2)
    return x0 + 2 * r + 4


def marker(svg: Svg, shape: str, x: float, y: float, color: str, size: float = 5) -> None:
    """A data marker with a 2 px surface ring: dot, hollow, square or diamond."""
    t = svg.t
    if shape == "dot":
        svg.circle(x, y, size + 2, t.surface)
        svg.circle(x, y, size, color)
    elif shape == "hollow":
        svg.circle(x, y, size + 2, t.surface)
        svg.circle(x, y, size - 1, t.surface, stroke=color, width=2)
    elif shape == "square":
        s = size * 0.95
        svg.rect(x - s - 2, y - s - 2, 2 * s + 4, 2 * s + 4, t.surface, rx=2)
        svg.rect(x - s, y - s, 2 * s, 2 * s, color, rx=1.5)
    elif shape == "diamond":
        s = size * 1.35
        svg.path(f"M{_n(x)} {_n(y - s - 2.5)}L{_n(x + s + 2.5)} {_n(y)}L{_n(x)} {_n(y + s + 2.5)}"
                 f"L{_n(x - s - 2.5)} {_n(y)}Z", fill=t.surface)
        svg.path(f"M{_n(x)} {_n(y - s)}L{_n(x + s)} {_n(y)}L{_n(x)} {_n(y + s)}L{_n(x - s)} {_n(y)}Z", fill=color)
    else:
        raise ValueError(f"unknown marker shape {shape!r}")


@dataclass
class LegendItem:
    label: str
    slot: int | None = None      # categorical slot (0-based); None = ink
    shape: str = "bar"           # bar | dot | hollow | square | diamond | line | band


def _legend_width(items: list[LegendItem]) -> float:
    return sum(18 + text_width(i.label, 11) + 16 for i in items)


def _draw_legend(svg: Svg, x: float, y: float, items: list[LegendItem], max_w: float) -> float:
    """Legend row(s) starting at (x, y = row centre); returns the y after the last row."""
    t = svg.t
    cx, cy = x, y
    for it in items:
        w = 18 + text_width(it.label, 11) + 16
        if cx > x and cx + w > x + max_w:
            cx, cy = x, cy + 18
        color = t.series[it.slot] if it.slot is not None else t.ink2
        if it.shape == "bar":
            svg.rect(cx, cy - 5, 12, 10, color, rx=2)
        elif it.shape == "line":
            svg.line(cx, cy, cx + 12, cy, color, 2, cap="round")
        elif it.shape == "band":
            svg.rect(cx + 0.5, cy - 5.5, 11, 11, t.band, rx=2, stroke=t.axis)
        else:
            marker(svg, it.shape, cx + 6, cy, color, 4)
        svg.text(cx + 18, cy + 4, it.label, 11, t.ink2)
        cx += w
    return cy + 14


@dataclass
class Figure:
    """One chart: drawn per theme, plus the same numbers as a table (docs) and alt text."""
    name: str
    title: str
    alt: str
    headers: list[str]
    rows: list[list[str]]
    note: str
    draw: Callable[[Theme], str] = field(repr=False)
    caption: str = ""

    def svg(self, theme: Theme) -> str:
        return self.draw(theme)

    def table_md(self) -> str:
        head = "| " + " | ".join(self.headers) + " |"
        sep = "|" + "|".join("---:" if _numeric_col(self.rows, i) else "---" for i in range(len(self.headers))) + "|"
        body = ["| " + " | ".join(c.replace("|", "\\|") for c in r) + " |" for r in self.rows]
        return "\n".join([head, sep, *body])

    def markdown(self, img_prefix: str = "img/") -> str:
        alt = self.alt.replace('"', "'")
        out = [
            "<picture>",
            f'  <source media="(prefers-color-scheme: dark)" srcset="{img_prefix}{self.name}-dark.svg">',
            f'  <img src="{img_prefix}{self.name}-light.svg" alt="{_esc(alt)}" width="760">',
            "</picture>",
            "",
        ]
        if self.caption:
            out += [self.caption, ""]
        out += [self.table_md(), "", f"<sub>{self.note}</sub>"]
        return "\n".join(out)


def _numeric_col(rows: list[list[str]], i: int) -> bool:
    vals = [r[i] for r in rows if i < len(r) and r[i] not in ("", "-")]
    if not vals:
        return False
    def isnum(s: str) -> bool:
        s = s.replace(",", "").replace("%", "").replace(" ms", "").strip()
        try:
            float(s)
            return True
        except ValueError:
            return False
    return all(isnum(v) for v in vals)


def _header(svg: Svg, title: str, subtitle: str, legend: list[LegendItem] | None = None) -> float:
    """Title, wrapped subtitle and legend; returns the y where the plot starts."""
    t = svg.t
    svg.text(PAD, PAD + 14, title, 15, t.ink, weight="600")
    y = PAD + 34
    for line in wrap(subtitle, W - 2 * PAD, 12):
        svg.text(PAD, y, line, 12, t.ink2)
        y += 17
    if legend:
        y = _draw_legend(svg, PAD, y + 6, legend, W - 2 * PAD) + 2
    return y + 6


def _header_height(subtitle: str, legend: list[LegendItem] | None = None) -> float:
    n = len(wrap(subtitle, W - 2 * PAD, 12))
    h = PAD + 34 + 17 * n
    if legend:
        rows, cx = 1, 0.0
        for it in legend:
            w = 18 + text_width(it.label, 11) + 16
            if cx > 0 and cx + w > W - 2 * PAD:
                rows, cx = rows + 1, 0.0
            cx += w
        h += 6 + 18 * (rows - 1) + 14 + 2
    return h + 6


def _footer(svg: Svg, y: float, note: str) -> float:
    for line in wrap(note, W - 2 * PAD, 11):
        svg.text(PAD, y, line, 11, svg.t.ink2)
        y += 15
    return y


def _footer_height(note: str) -> float:
    return 15 * len(wrap(note, W - 2 * PAD, 11))


def _x_axis(svg: Svg, x0: float, plot_w: float, y_top: float, y_bot: float, vmax: float, step: float,
            fmt: Callable[[float], str], vmin: float = 0.0) -> None:
    """Hairline vertical gridlines + tick labels under the plot (one axis only)."""
    t = svg.t
    v = vmin
    while v <= vmax + 1e-9:
        x = x0 + (v - vmin) / (vmax - vmin) * plot_w
        svg.line(x, y_top, x, y_bot, t.grid, 1)
        svg.text(x, y_bot + 14, fmt(v), 10.5, t.ink2, anchor="middle", tabular=True)
        v = round(v + step, 10)


# --------------------------------------------------------------------------- 1 graph composition
def composition_figure(d: dict) -> Figure:
    """d: {"nodes": {label: n}, "edges": {type: n}, "note": str}."""
    nodes = sorted(d["nodes"].items(), key=lambda kv: (-kv[1], kv[0]))
    edges = sorted(d["edges"].items(), key=lambda kv: (-kv[1], kv[0]))
    tn, te = sum(v for _, v in nodes), sum(v for _, v in edges)
    title = "Graph composition: nodes and edges by type"
    subtitle = (f"{fmt_int(tn)} nodes and {fmt_int(te)} edges. SIMILAR_TO (k = 10 per renewal) is the largest "
                "edge type; Plan, Incident and PricingChange are small hubs.")
    alt = ("Horizontal bar charts. Nodes by label: " + ", ".join(f"{k} {fmt_int(v)}" for k, v in nodes) +
           f" (total {fmt_int(tn)}). Edges by type: " + ", ".join(f"{k} {fmt_int(v)}" for k, v in edges) +
           f" (total {fmt_int(te)}).")
    rows = [["node", k, fmt_int(v)] for k, v in nodes] + [["edge", k, fmt_int(v)] for k, v in edges]
    rows += [["total", "nodes", fmt_int(tn)], ["total", "edges", fmt_int(te)]]
    row_h = 22

    def draw(theme: Theme) -> str:
        label_w = max(text_width(k, 12) for k, _ in nodes + edges) + 14
        value_w = 64
        plot_w = W - 2 * PAD - label_w - value_w
        x0 = PAD + label_w
        h = _header_height(subtitle) + 2 * 26 + row_h * (len(nodes) + len(edges)) + 24 + _footer_height(d["note"]) + PAD
        svg = Svg(W, h, theme, title, alt)
        y = _header(svg, title, subtitle)
        for heading, items, total in (("Nodes by label", nodes, tn), ("Edges by type", edges, te)):
            svg.text(PAD, y + 14, f"{heading} ({fmt_int(total)})", 12, theme.ink, weight="600")
            y += 26
            vmax = max(v for _, v in items) or 1
            top = y
            for k, v in items:
                length = v / vmax * plot_w
                svg.open(f"{k}: {fmt_int(v)}")
                svg.text(x0 - 10, y + row_h / 2 + 4, k, 12, theme.ink, anchor="end")
                if length > 0:
                    svg.path(hbar_path(x0, y + (row_h - BAR) / 2, max(length, 1.5), BAR), fill=theme.series[0])
                svg.text(x0 + length + 6, y + row_h / 2 + 4, fmt_int(v), 11, theme.ink2, tabular=True)
                svg.close()
                y += row_h
            svg.line(x0, top - 2, x0, y + 2, theme.axis, 1)
            y += 6
        _footer(svg, y + 16, d["note"])
        return svg.render()

    return Figure("graph-composition", title, alt, ["kind", "type", "count"], rows, d["note"], draw)


# --------------------------------------------------------------------------- 2 leak surface
def leak_surface_figure(d: dict) -> Figure:
    """d: {"by_type": {rel: {"edges", "post_as_of", "renewals"}}, "declared_exception": rel, "note": str}."""
    exc = d.get("declared_exception", "FIRST_RENEWAL_AFTER")
    items = sorted(d["by_type"].items(), key=lambda kv: (-kv[1]["edges"], kv[0]))
    total = sum(v["edges"] for _, v in items)
    post = sum(v["post_as_of"] for _, v in items)
    title = "Point-in-time leak surface: event edges dated after their renewal's as_of"
    subtitle = (f"{fmt_int(post)} of {fmt_int(total)} event edges fall after as_of (T-7). The graph keeps them on "
                f"purpose; every tool template filters event_date <= as_of. {exc} edges after as_of are the one "
                "declared exception (gold rule), served flagged.")
    legend = [LegendItem("on or before as_of", 0), LegendItem("after as_of", 1)]
    alt = ("Stacked horizontal bars per event edge type, on or before as_of versus after as_of: " +
           "; ".join(f"{k} {fmt_int(v['post_as_of'])} of {fmt_int(v['edges'])} after as_of "
                     f"({fmt_int(v['renewals'])} renewals)" for k, v in items) + ".")
    rows = [[k, fmt_int(v["edges"]), fmt_int(v["edges"] - v["post_as_of"]), fmt_int(v["post_as_of"]),
             fmt_int(v["renewals"]), "declared exception (gold rule)" if k == exc else ""] for k, v in items]
    rows.append(["total", fmt_int(total), fmt_int(total - post), fmt_int(post), "", ""])
    row_h = 26

    def draw(theme: Theme) -> str:
        label_w = max(text_width(k, 12) for k, _ in items) + 14
        tips = [f"{fmt_int(v['post_as_of'])} of {fmt_int(v['edges'])} after as_of"
                + (f" ({fmt_int(v['renewals'])} renewals)" if v["post_as_of"] else "")
                + (" · declared exception" if k == exc and v["post_as_of"] else "") for k, v in items]
        tip_w = max(text_width(s, 11) for s in tips) + 10
        plot_w = W - 2 * PAD - label_w - tip_w
        x0 = PAD + label_w
        h = _header_height(subtitle, legend) + row_h * len(items) + 30 + _footer_height(d["note"]) + PAD
        svg = Svg(W, h, theme, title, alt)
        y = _header(svg, title, subtitle, legend)
        vmax = max(v["edges"] for _, v in items) or 1
        top = y
        for (k, v), tip in zip(items, tips, strict=True):
            before, after = v["edges"] - v["post_as_of"], v["post_as_of"]
            lb, la = before / vmax * plot_w, after / vmax * plot_w
            by = y + (row_h - BAR) / 2
            svg.open(f"{k}: {fmt_int(before)} on or before as_of, {fmt_int(after)} after")
            svg.text(x0 - 10, y + row_h / 2 + 4, k, 12, theme.ink, anchor="end")
            if lb > 0:
                svg.path(hbar_path(x0, by, lb, BAR, round_end=la <= GAP), fill=theme.series[0])
            if la > GAP:
                svg.path(hbar_path(x0 + lb + (GAP if lb > 0 else 0), by, la - (GAP if lb > 0 else 0), BAR),
                         fill=theme.series[1])
            svg.text(x0 + lb + la + 6, y + row_h / 2 + 4, tip, 11, theme.ink2, tabular=True)
            svg.close()
            y += row_h
        svg.line(x0, top - 2, x0, y + 2, theme.axis, 1)
        _footer(svg, y + 22, d["note"])
        return svg.render()

    return Figure("leak-surface", title, alt,
                  ["edge type", "edges", "on or before as_of", "after as_of", "renewals with one after as_of", "note"],
                  rows, d["note"], draw)


# --------------------------------------------------------------------------- 3 naive vs point in time
def naive_pit_figure(d: dict) -> Figure:
    """d: {"renewals": N, "features": [{"feature", "window", "pit_mismatches", "naive_wrong"}], "note": str}."""
    feats = list(d["features"])
    n = d["renewals"]
    title = "A traversal without the as_of bound gets features wrong"
    subtitle = (f"Renewals (of {fmt_int(n)}) whose feature, recomputed from graph edges, differs from gold. "
                "Point in time: event_date in (as_of-window, as_of]. Naive: event_date > as_of-window, with no "
                "upper bound. The contract requires both numbers exactly. A hollow ring marks a zero.")
    legend = [LegendItem("point in time: event_date in (as_of-window, as_of]", 0),
              LegendItem("naive: no as_of upper bound", 1)]
    alt = ("Grouped horizontal bars per feature: " +
           "; ".join(f"{f['feature']} point in time {fmt_int(f['pit_mismatches'])} wrong, naive "
                     f"{fmt_int(f['naive_wrong'])} wrong" for f in feats) + f", out of {fmt_int(n)} renewals.")
    rows = [[f["feature"], f["window"], fmt_int(f["pit_mismatches"]), fmt_int(f["naive_wrong"])] for f in feats]
    group_h = 2 * 20 + 14

    def draw(theme: Theme) -> str:
        label_w = max(max(text_width(f["feature"], 12), text_width(f["window"], 11)) for f in feats) + 14
        tip_w = 86
        plot_w = W - 2 * PAD - label_w - tip_w
        x0 = PAD + label_w
        h = _header_height(subtitle, legend) + group_h * len(feats) + 24 + _footer_height(d["note"]) + PAD
        svg = Svg(W, h, theme, title, alt)
        y = _header(svg, title, subtitle, legend)
        vmax = max([f["naive_wrong"] for f in feats] + [f["pit_mismatches"] for f in feats]) or 1
        top = y
        for f in feats:
            svg.text(x0 - 10, y + 17, f["feature"], 12, theme.ink, anchor="end")
            svg.text(x0 - 10, y + 33, f["window"], 11, theme.ink2, anchor="end")
            for i, key in enumerate(("pit_mismatches", "naive_wrong")):
                v = f[key]
                length = v / vmax * plot_w
                by = y + 4 + i * 20
                svg.open(f"{f['feature']}: {'point in time' if i == 0 else 'naive'} {fmt_int(v)} wrong")
                end = zero_or_bar(svg, x0, by, length, 14, theme.series[i], zero=v == 0)
                svg.text(end + 6, by + 11, f"{fmt_int(v)} wrong", 11, theme.ink2, tabular=True)
                svg.close()
            y += group_h
        svg.line(x0, top - 2, x0, y - 10, theme.axis, 1)
        _footer(svg, y + 12, d["note"])
        return svg.render()

    return Figure("naive-vs-pit", title, alt,
                  ["feature", "window", "point in time: renewals wrong", "naive: renewals wrong"], rows, d["note"],
                  draw)


# --------------------------------------------------------------------------- 4 evidence timeline
LANE_ORDER = ["CUT_CAP", "EXPOSED_TO", "FIRST_RENEWAL_AFTER", "HIT_LIMIT", "CHANGED_OVERAGE", "CHARGED_OVERAGE",
              "OPENED", "BILLED"]
EVENT_NOUN = {"HIT_LIMIT": "cap hit", "CHANGED_OVERAGE": "overage switch", "CHARGED_OVERAGE": "overage charge",
              "OPENED": "ticket", "BILLED": "billing event"}


def _d(s: str) -> date:
    return date.fromisoformat(str(s)[:10])


def dodge(xs: list[float], min_gap: float = 11.0, step: float = 5.5) -> list[float]:
    """Vertical offsets for markers on one lane: runs of markers closer than ``min_gap`` px alternate
    above and below the lane centre, so neighbours stay apart (y carries no meaning inside a lane)."""
    out: list[float] = []
    run = 0
    for i, x in enumerate(xs):
        run = run + 1 if i and x - xs[i - 1] < min_gap else 0
        out.append(0.0)
        if run == 1:
            out[-2] = -step
        if run:
            out[-1] = step if run % 2 else -step
    return out


def timeline_figure(d: dict) -> Figure:
    """d: {"renewal_id", "as_of", "renewal_date", "rows": [evidence rows], "windows": {rel: days},
    "cumulative": [rel, ...] (feature window = everything on or before as_of), "gold_rule_days": 30,
    "hidden_after_as_of": int, "label": str, "note": str}."""
    rows_in = sorted(d["rows"], key=lambda r: (r["event_date"], r["relation"], r["target_id"]))
    as_of, rdate = _d(d["as_of"]), _d(d["renewal_date"])
    gold_days = d.get("gold_rule_days", 30)
    lanes = [rel for rel in LANE_ORDER if any(r["relation"] == rel for r in rows_in)]
    lanes += sorted({r["relation"] for r in rows_in} - set(lanes))
    windows = {rel: days for rel, days in d.get("windows", {}).items() if rel in lanes and days}
    cumulative = [rel for rel in d.get("cumulative", []) if rel in lanes and rel not in windows]
    feeds = {r["relation"]: r.get("feeds_feature") or "" for r in rows_in}
    who = d.get("label") or d["renewal_id"]
    n_exc = sum(1 for r in rows_in if r.get("declared_exception"))
    hidden = d.get("hidden_after_as_of", 0)
    title = f"What the model could see: {who} at T-7"
    subtitle = (f"{len(rows_in)} evidence rows on or before as_of {as_of.isoformat()} (renewal {rdate.isoformat()}). "
                f"Events after as_of are never served ({fmt_int(hidden)} exist for this renewal). Shaded: each "
                "feature's window. FIRST_RENEWAL_AFTER follows the gold rule, the one declared exception"
                + (f"; {n_exc} row(s) here took effect after as_of." if n_exc
                   else "; here the cut was known by as_of."))
    legend = [LegendItem("inside its feature window", 0, "dot"),
              LegendItem("on or before as_of, outside the window", 0, "hollow"),
              LegendItem("FIRST_RENEWAL_AFTER (gold rule)", 0, "diamond")]
    if n_exc:
        legend.append(LegendItem("declared exception: after as_of", 1, "diamond"))
    legend.append(LegendItem("feature window (ends at as_of)", None, "band"))
    alt = (f"Timeline of {who}'s evidence before as_of {as_of.isoformat()}: " +
           "; ".join(f"{r['event_date']} {r['relation']} {r['target_id']}"
                     f"{' (in window)' if r.get('in_feature_window') else ' (outside window)'}" for r in rows_in) + ".")
    trows = [[r["event_date"], r["relation"], r["target_id"], r.get("feeds_feature") or "",
              "yes" if r.get("in_feature_window") else "no", "yes" if r.get("known_by_as_of") else "no",
              "yes" if r.get("declared_exception") else "no"] for r in rows_in]
    lane_h = 52

    def draw(theme: Theme) -> str:
        label_w = max(max(text_width(rel, 12), text_width(feeds.get(rel, ""), 11)) for rel in lanes) + 16
        x0 = PAD + label_w
        plot_w = W - 2 * PAD - label_w - 8
        starts = [_d(r["event_date"]) for r in rows_in] + [as_of - timedelta(days=w) for w in windows.values()]
        if "FIRST_RENEWAL_AFTER" in lanes:
            starts.append(rdate - timedelta(days=gold_days))
        d0 = min(starts) - timedelta(days=4)
        rl = f"renewal {rdate.isoformat()}"
        d1 = rdate + timedelta(days=3)   # widen until the renewal label fits right of its line
        while (d1 - rdate).days < 40 and (rdate - d0).days / (d1 - d0).days * plot_w + 5 + text_width(rl, 11) > plot_w:
            d1 += timedelta(days=1)
        span = (d1 - d0).days

        def X(day: date) -> float:
            return x0 + (day - d0).days / span * plot_w

        top_pad = 40
        h = (_header_height(subtitle, legend) + top_pad + lane_h * len(lanes) + 34 + _footer_height(d["note"]) + PAD)
        svg = Svg(W, h, theme, title, alt)
        y = _header(svg, title, subtitle, legend) + top_pad
        y_top, y_bot = y, y + lane_h * len(lanes)
        day = d0   # date gridlines: the 1st and the 15th of each month
        while day <= d1:
            if day.day in (1, 15):
                svg.line(X(day), y_top, X(day), y_bot, theme.grid, 1)
                svg.text(X(day), y_bot + 15, day.strftime("%b ") + str(day.day), 10.5, theme.ink2, anchor="middle")
            day += timedelta(days=1)
        half = plot_w / span / 2
        for i, rel in enumerate(lanes):
            ly = y_top + i * lane_h
            band = None
            if rel in windows:
                a = as_of - timedelta(days=windows[rel] - 1)
                band = (X(a) - half, X(as_of) + half, f"{windows[rel]}-day window")
            elif rel in cumulative:   # no start: every event on or before as_of counts (e.g. cuts so far)
                band = (x0 + 1, X(as_of) + half, "window: everything on or before as_of")
            elif rel == "FIRST_RENEWAL_AFTER":
                a = rdate - timedelta(days=gold_days)
                band = (X(a) - half, X(rdate) - half, f"gold rule: the {gold_days} days before renewal")
            if band:
                svg.rect(band[0], ly + 5, band[1] - band[0], lane_h - 10, theme.band, rx=3)
                lw = text_width(band[2], 9.5)
                lx = band[0] + 4 if band[0] + 4 + lw <= x0 + plot_w else x0 + plot_w - lw - 2
                svg.text(lx, ly + 15, band[2], 9.5, theme.ink2)
            svg.text(x0 - 10, ly + lane_h / 2 + 1, rel, 12, theme.ink, anchor="end")
            svg.text(x0 - 10, ly + lane_h / 2 + 15, feeds.get(rel, ""), 11, theme.ink2, anchor="end")
            if i:
                svg.line(x0, ly, x0 + plot_w, ly, theme.grid, 1)
        # as_of and renewal date: labels on two rows above the plot, never past the right edge
        xa, xr = X(as_of), X(rdate)
        svg.line(xa, y_top - 30, xa, y_bot, theme.ink, 1.5)
        svg.text(xa - 5, y_top - 22, f"as_of {as_of.isoformat()} (T-7)", 11, theme.ink, anchor="end", weight="600")
        svg.line(xr, y_top - 12, xr, y_bot, theme.ink2, 1)
        if xr + 5 + text_width(rl, 11) <= W - PAD:
            svg.text(xr + 5, y_top - 4, rl, 11, theme.ink2)
        else:
            svg.text(xr - 5, y_top - 4, rl, 11, theme.ink2, anchor="end")
        for i, rel in enumerate(lanes):
            cy = y_top + i * lane_h + lane_h / 2 + 6
            lane_rows = [r for r in rows_in if r["relation"] == rel]
            dys = dodge([X(_d(r["event_date"])) for r in lane_rows])   # close markers alternate above / below
            for r, dy in zip(lane_rows, dys, strict=True):
                x = X(_d(r["event_date"]))
                exc = bool(r.get("declared_exception"))
                shape = ("diamond" if rel == "FIRST_RENEWAL_AFTER"
                         else "dot" if r.get("in_feature_window") else "hollow")
                svg.open(f"{r['event_date']} {rel} {r['target_id']}" + (" (declared exception)" if exc else ""))
                marker(svg, shape, x, cy + dy, theme.series[1 if exc else 0], 4)
                svg.close()
            if rel not in EVENT_NOUN:   # hub events: label each one beside its marker
                prev_right = -1e9
                for r, dy in zip(lane_rows, dys, strict=True):
                    x = X(_d(r["event_date"]))
                    label = r["target_id"] + (" (after as_of)" if r.get("declared_exception") else "")
                    lw = text_width(label, 10.5)
                    right = x + 10 + lw
                    crosses = x < xa < right + 2 and r["event_date"] <= as_of.isoformat()
                    if crosses or right > x0 + plot_w or x + 10 < prev_right:
                        lx, anchor = x - 10, "end"
                    else:
                        lx, anchor = x + 10, "start"
                    svg.text(lx, cy + dy + 4, label, 10.5, theme.ink2, anchor=anchor)
                    prev_right = max(prev_right, right if anchor == "start" else x)
            elif lane_rows:
                inw = sum(1 for r in lane_rows if r.get("in_feature_window"))
                noun = EVENT_NOUN[rel] + ("" if len(lane_rows) == 1 else "s")
                detail = sorted({r.get("detail") for r in lane_rows if r.get("detail")})
                label = f"{len(lane_rows)} {noun}, {inw} in window" + (f" ({', '.join(detail)})" if detail else "")
                svg.text(X(_d(lane_rows[0]["event_date"])) - 10, cy + 4, label, 10.5, theme.ink2, anchor="end")
        svg.line(x0, y_top, x0, y_bot, theme.axis, 1)
        svg.line(x0, y_bot, x0 + plot_w, y_bot, theme.axis, 1)
        _footer(svg, y_bot + 40, d["note"])
        return svg.render()

    return Figure("maya-timeline", title, alt,
                  ["event_date", "relation", "target", "feeds feature", "in feature window", "known by as_of",
                   "declared exception"], trows, d["note"], draw)


# --------------------------------------------------------------------------- 5 neighbour graph (static render)
def neighbours_figure(d: dict) -> Figure:
    """d: {"renewal_id", "as_of", "visibility", "neighbours": [{"rank","renewal_id","dist","d2_q","outcome"}],
    "lapsed", "n", "wilson": [lo, hi], "label", "note"}."""
    nb = sorted(d["neighbours"], key=lambda r: r["rank"])
    who = d.get("label") or d["renewal_id"]
    lo, hi = d["wilson"]
    title = f"The {len(nb)} renewals most similar to {who}, and how they ended"
    subtitle = (f"{d['lapsed']} of {d['n']} lapsed (Wilson 95% {lo:.3f}-{hi:.3f}); outcomes visible "
                f"{'today (a current renewal)' if d['visibility'] == 'today' else 'as of ' + d['as_of']}. "
                "Edges are labelled by rank; distance from the centre is the SIMILAR_TO distance (feature space, "
                "same plan; the rings start above 0). Narrative evidence, not a risk estimate.")
    legend = [LegendItem("renewed", 0, "dot"), LegendItem("voluntary lapse", 1, "square")]
    if any(r["outcome"] not in ("renewed", "voluntary_lapse") for r in nb):
        legend.append(LegendItem("not yet observed (after the source's as_of)", None, "hollow"))
    alt = (f"Radial graph: {who} at the centre and its {len(nb)} nearest renewals by SIMILAR_TO rank. " +
           "; ".join(f"rank {r['rank']} {r['renewal_id']} distance {r['dist']:.3f} {r['outcome']}" for r in nb) +
           f". {d['lapsed']} of {d['n']} lapsed.")
    rows = [[str(r["rank"]), r["renewal_id"], f"{r['dist']:.4f}", fmt_int(r["d2_q"]), r["outcome"]] for r in nb]
    rows.append(["", "summary", "", "", f"{d['lapsed']} of {d['n']} lapsed, Wilson 95% [{lo:.3f}, {hi:.3f}]"])
    plot_h = 440

    def draw(theme: Theme) -> str:
        h = _header_height(subtitle, legend) + plot_h + _footer_height(d["note"]) + PAD + 8
        svg = Svg(W, h, theme, title, alt)
        y = _header(svg, title, subtitle, legend)
        cx, cy = W / 2, y + plot_h / 2 + 4
        dists = [r["dist"] for r in nb]
        dlo = math.floor(min(dists) * 10) / 10 - 0.1
        dhi = math.ceil(max(dists) * 10) / 10
        r0, r1 = 70.0, 190.0

        def R(dist: float) -> float:
            return r0 + (dist - dlo) / ((dhi - dlo) or 1) * (r1 - r0)

        # distance rings (hairline); their values sit on the free 0-degree ray (ranks are 36 degrees apart
        # starting at -90, so 0 degrees falls between ranks 3 and 4)
        step = 0.2 if dhi - dlo > 0.4 else 0.1
        v = math.ceil(dlo / step - 1e-9) * step
        rings = []
        while v <= dhi + 1e-9:
            if v > dlo + 1e-9:
                rings.append(v)
                svg.circle(cx, cy, R(v), "none", stroke=theme.grid, width=1)
            v = round(v + step, 10)
        for i, v in enumerate(rings):
            label = f"{v:.1f}" + (" distance" if i == len(rings) - 1 else "")
            svg.text(cx + R(v) + 3, cy - 4, label, 9.5, theme.ink2)
        pos = []
        for r in nb:
            ang = math.radians(-90 + (r["rank"] - 1) * 360 / max(len(nb), 1))
            rad = R(r["dist"])
            pos.append((r, ang, cx + rad * math.cos(ang), cy + rad * math.sin(ang)))
        for _r, _ang, x, yy in pos:
            svg.line(cx, cy, x, yy, theme.axis, 1.5)
        for r, _ang, x, yy in pos:   # rank pills on the edges
            px, py = cx + (x - cx) * 0.5, cy + (yy - cy) * 0.5
            svg.circle(px, py, 8.5, theme.surface, stroke=theme.grid, width=1)
            svg.text(px, py + 3.5, str(r["rank"]), 9.5, theme.ink2, anchor="middle", tabular=True)
        for r, ang, x, yy in pos:
            outcome = r["outcome"]
            shape, color = (("square", theme.series[1]) if outcome == "voluntary_lapse" else
                            ("dot", theme.series[0]) if outcome == "renewed" else ("hollow", theme.ink2))
            svg.open(f"rank {r['rank']}: {r['renewal_id']}, distance {r['dist']:.3f}, {outcome}")
            marker(svg, shape, x, yy, color, 6.5)
            svg.close()
            c, s = math.cos(ang), math.sin(ang)
            short = r["renewal_id"].split(":")[0]
            word = {"voluntary_lapse": "lapsed", "renewed": "renewed"}.get(outcome, outcome.replace("_", " "))
            label = f"{short} · {word}"
            if abs(c) < 0.35:
                anchor, lx, ly = "middle", x, yy + (22 if s > 0 else -14)
            else:
                anchor = "start" if c > 0 else "end"
                lx, ly = x + 14 * (1 if c > 0 else -1), yy + 4
            svg.text(lx, ly, label, 11, theme.ink, anchor=anchor)
        # the source renewal
        svg.circle(cx, cy, 12, theme.surface)
        svg.circle(cx, cy, 10, theme.ink)
        sub2 = f"as_of {d['as_of']}"
        bw = max(text_width(who, 11.5, bold=True), text_width(sub2, 10.5)) + 10
        svg.rect(cx - bw / 2, cy + 16, bw, 31, theme.surface, rx=4)
        svg.text(cx, cy + 29, who, 11.5, theme.ink, anchor="middle", weight="600")
        svg.text(cx, cy + 43, sub2, 10.5, theme.ink2, anchor="middle")
        _footer(svg, y + plot_h + 22, d["note"])
        return svg.render()

    return Figure("maya-neighbours", title, alt, ["rank", "renewal", "dist", "d2_q", "outcome"], rows, d["note"], draw)


# --------------------------------------------------------------------------- 6 exposure (stacked by plan)
SEGMENTS = [("renewed", "renewed"), ("voluntary_lapse", "voluntary lapse"), ("cancel_flow", "cancel flow"),
            ("dunning", "dunning"), ("other", "other route")]


def exposure_figure(d: dict) -> Figure:
    """d: {"entity_id", "by_plan": {plan: {"exposed","renewed","voluntary_lapse","cancel_flow","dunning","other"}},
    "naive_additional": int, "window_days": 28, "note"}."""
    plans = [p for p in ("pro", "pro_plus", "ultra") if p in d["by_plan"]] + \
        sorted(set(d["by_plan"]) - {"pro", "pro_plus", "ultra"})
    segs = [(k, lab) for k, lab in SEGMENTS if any(d["by_plan"][p].get(k, 0) for p in plans)]
    total = sum(d["by_plan"][p]["exposed"] for p in plans)
    ent = d["entity_id"]
    title = f"Blast radius of {ent} by plan: who was active inside the feature window"
    subtitle = (f"{fmt_int(total)} renewals were active during {ent} inside their {d.get('window_days', 28)}-day "
                f"window before as_of. A graph without the as_of bound would add {fmt_int(d['naive_additional'])} "
                "more. Descriptive, not causal.")
    legend = [LegendItem(lab, i) for i, (_, lab) in enumerate(segs)]
    alt = (f"Stacked horizontal bars of renewals exposed to {ent} by plan: " +
           "; ".join(f"{p} {fmt_int(d['by_plan'][p]['exposed'])} (" +
                     ", ".join(f"{lab} {fmt_int(d['by_plan'][p].get(k, 0))}" for k, lab in segs) + ")" for p in plans)
           + f". A naive graph would add {fmt_int(d['naive_additional'])}.")
    rows = [[p, fmt_int(d["by_plan"][p]["exposed"])] + [fmt_int(d["by_plan"][p].get(k, 0)) for k, _ in segs]
            for p in plans]
    rows.append(["total", fmt_int(total)] + [fmt_int(sum(d["by_plan"][p].get(k, 0) for p in plans)) for k, _ in segs])
    rows.append(["a graph without the as_of bound would add", fmt_int(d["naive_additional"])] + [""] * len(segs))
    row_h = 34

    def draw(theme: Theme) -> str:
        label_w = max(text_width(p, 12) for p in plans) + 14
        tip_w = 84
        plot_w = W - 2 * PAD - label_w - tip_w
        x0 = PAD + label_w
        vmax = max(d["by_plan"][p]["exposed"] for p in plans) or 1
        # direct series labels above the first bar, on two staggered lines with leader lines
        first = d["by_plan"][plans[0]]
        centres, acc = [], 0.0
        for k, lab in segs:
            w = first.get(k, 0) / vmax * plot_w
            centres.append((x0 + acc + w / 2, lab, w))
            acc += w
        lines: list[list[tuple[float, float]]] = [[], []]
        assign = []
        for x, lab, w in centres:
            tw = text_width(lab, 10.5)
            lx = min(max(x - tw / 2, x0), x0 + plot_w + tip_w - tw)
            lvl = 0 if all(lx > b + 8 or lx + tw < a - 8 for a, b in lines[0]) else 1
            lines[lvl].append((lx, lx + tw))
            assign.append((x, lab, lx, lvl, w))
        lab_h = 34
        h = _header_height(subtitle, legend) + lab_h + row_h * len(plans) + 24 + _footer_height(d["note"]) + PAD
        svg = Svg(W, h, theme, title, alt)
        y = _header(svg, title, subtitle, legend) + lab_h
        for x, lab, lx, lvl, w in assign:
            if w <= 0:
                continue
            ty = y - 22 + lvl * 12
            svg.text(lx, ty, lab, 10.5, theme.ink2)
            svg.line(x, ty + 3, x, y + (row_h - 18) / 2 - 1, theme.axis, 1)
        top = y
        for p in plans:
            v = d["by_plan"][p]
            by = y + (row_h - 18) / 2
            svg.text(x0 - 10, y + row_h / 2 + 4, p, 12, theme.ink, anchor="end")
            acc = 0.0
            nonzero = [k for k, _ in segs if v.get(k, 0) > 0]
            for i, (k, lab) in enumerate(segs):
                val = v.get(k, 0)
                if val <= 0:
                    continue
                w = val / vmax * plot_w
                gap = GAP if acc > 0 else 0
                last = k == nonzero[-1]
                color = theme.series[i]
                svg.open(f"{p} {lab}: {fmt_int(val)}")
                if w - gap > 0.5:
                    svg.path(hbar_path(x0 + acc + gap, by, w - gap, 18, round_end=last), fill=color)
                label = fmt_int(val)
                if w - gap >= text_width(label, 10.5) + 8:
                    svg.text(x0 + acc + gap + (w - gap) / 2, by + 13, label, 10.5, on_fill(color), anchor="middle",
                             tabular=True)
                svg.close()
                acc += w
            svg.text(x0 + acc + 6, y + row_h / 2 + 4, f"{fmt_int(v['exposed'])} exposed", 11, theme.ink2, tabular=True)
            y += row_h
        svg.line(x0, top - 2, x0, y + 2, theme.axis, 1)
        _footer(svg, y + 22, d["note"])
        return svg.render()

    return Figure(f"{ent}-exposure", title, alt, ["plan", "exposed"] + [lab for _, lab in segs], rows, d["note"], draw)


# --------------------------------------------------------------------------- dot + whisker (rates with CI)
@dataclass
class Dot:
    label: str
    group: str
    value: float
    lo: float
    hi: float
    slot: int
    shape: str
    right: str
    tip: str


def _dot_rows(svg: Svg, dots: list[Dot], x0: float, plot_w: float, y: float, vmin: float, vmax: float,
              row_h: float, group_gap: float, label_x: float) -> float:
    t = svg.t

    def X(v: float) -> float:
        return x0 + (v - vmin) / (vmax - vmin) * plot_w

    prev = None
    for dt in dots:
        if prev is not None and dt.group != prev:
            y += group_gap
        if dt.group != prev:
            svg.text(label_x, y + row_h / 2 + 4, dt.group, 12, t.ink, weight="600")
        cy = y + row_h / 2
        if dt.label:
            svg.text(x0 - 10, cy + 4, dt.label, 11, t.ink2, anchor="end")
        color = t.series[dt.slot]
        svg.open(dt.tip)
        svg.line(X(dt.lo), cy, X(dt.hi), cy, color, 2, cap="round")
        marker(svg, dt.shape, X(dt.value), cy, color, 4.5)
        svg.close()
        svg.text(x0 + plot_w + 12, cy + 4, dt.right, 11, t.ink2, tabular=True)
        prev = dt.group
        y += row_h
    return y


def lapse_rate_figure(d: dict) -> Figure:
    """d: {"groups": [{"label", "without": {"n","lapses","wilson"}, "with": {...}}], "note"}."""
    groups = d["groups"]
    title = "Lapse rate of first renewals after a cap cut, by plan"
    first = groups[0]
    subtitle = (f"Model-routed renewals (voluntary lapse label), Wilson 95% intervals. {first['label'].capitalize()}: "
                f"{fmt_pct(first['with']['lapses'] / first['with']['n'])} first after a cut vs "
                f"{fmt_pct(first['without']['lapses'] / first['without']['n'])} otherwise. The generator plants this "
                "association; it is a metric question, not a graph result.")
    legend = [LegendItem("not the first renewal after a cut", 0, "dot"),
              LegendItem("first renewal after a cap cut", 1, "diamond")]
    dots: list[Dot] = []
    rows: list[list[str]] = []
    for g in groups:
        for key, lab, slot, shape in (("without", "not first after a cut", 0, "dot"),
                                      ("with", "first after a cut", 1, "diamond")):
            c = g[key]
            rate = c["lapses"] / c["n"] if c["n"] else 0.0
            lo, hi = c["wilson"]
            right = f"{fmt_pct(rate)}  {fmt_int(c['lapses'])}/{fmt_int(c['n'])}  [{fmt_pct(lo)}, {fmt_pct(hi)}]"
            dots.append(Dot("", g["label"], rate, lo, hi, slot, shape, right, f"{g['label']}, {lab}: {right}"))
            rows.append([g["label"], lab, fmt_int(c["n"]), fmt_int(c["lapses"]), fmt_pct(rate),
                         f"[{fmt_pct(lo)}, {fmt_pct(hi)}]"])
    alt = ("Dot and whisker chart of voluntary-lapse rates with Wilson 95% intervals: " +
           "; ".join(f"{r[0]} {r[1]} {r[4]} {r[5]} (n {r[2]})" for r in rows) + ".")
    row_h, group_gap = 20, 12

    def draw(theme: Theme) -> str:
        label_w = max(text_width(g["label"], 12, bold=True) for g in groups) + 16
        right_w = max(text_width(dt.right, 11) for dt in dots) + 16
        x0 = PAD + label_w
        plot_w = W - 2 * PAD - label_w - right_w
        vmax, step = nice_scale(max(dt.hi for dt in dots))
        plot_h = row_h * len(dots) + group_gap * (len(groups) - 1)
        h = _header_height(subtitle, legend) + plot_h + 30 + _footer_height(d["note"]) + PAD
        svg = Svg(W, h, theme, title, alt)
        y = _header(svg, title, subtitle, legend)
        _x_axis(svg, x0, plot_w, y - 4, y + plot_h + 2, vmax, step, lambda v: fmt_pct(v, 0))
        yb = _dot_rows(svg, dots, x0, plot_w, y, 0.0, vmax, row_h, group_gap, PAD)
        _footer(svg, yb + 34, d["note"])
        return svg.render()

    return Figure("lapse-first-after-cut", title, alt, ["group", "renewals", "n", "lapses", "rate", "Wilson 95%"],
                  rows, d["note"], draw)


PLAN_SLOT = {"pro": 0, "pro_plus": 1, "ultra": 2}


def cohorts_figure(d: dict) -> Figure:
    """d: {"algorithm", "cohorts": [{"cohort_id","plan","n","lapses","wilson","suppressed"}],
    "overall": {"n","lapses"}, "modularity", "library", "note"}."""
    pub = [c for c in d["cohorts"] if not c["suppressed"]]
    withheld = sorted(c["cohort_id"] for c in d["cohorts"] if c["suppressed"])
    pub.sort(key=lambda c: (-(c["lapses"] / c["n"]), c["cohort_id"]))
    overall = d["overall"]["lapses"] / d["overall"]["n"]
    title = f"Feature cohorts ({d['algorithm'].capitalize()}): voluntary-lapse rate with 95% intervals"
    subtitle = (f"{len(d['cohorts'])} cohorts over SIMILAR_TO ({d.get('library', 'networkx')}, modularity "
                f"{d.get('modularity', 0):.4f}), sorted by rate; model renewals only. Cohorts rediscover feature "
                "segments: labels, not structure. Plan purity is 1.0 by construction."
                + (f" Withheld (small cells): {', '.join(withheld)}." if withheld else ""))
    plans = sorted({c["plan"] for c in pub}, key=lambda p: PLAN_SLOT.get(p, 9))
    legend = [LegendItem(p, PLAN_SLOT.get(p, 3), "dot") for p in plans] + \
        [LegendItem(f"all model renewals {fmt_pct(overall)}", None, "line")]
    dots, rows = [], []
    for c in pub:
        rate = c["lapses"] / c["n"]
        lo, hi = c["wilson"]
        right = f"{fmt_pct(rate)}  {fmt_int(c['lapses'])}/{fmt_int(c['n'])}"
        dots.append(Dot(c["plan"], c["cohort_id"], rate, lo, hi, PLAN_SLOT.get(c["plan"], 3), "dot", right,
                        f"{c['cohort_id']} ({c['plan']}): {right} [{fmt_pct(lo)}, {fmt_pct(hi)}]"))
        rows.append([c["cohort_id"], c["plan"], fmt_int(c["n"]), fmt_int(c["lapses"]), fmt_pct(rate),
                     f"[{fmt_pct(lo)}, {fmt_pct(hi)}]", c.get("name", "")])
    rows += [[w, "withheld", "-", "-", "-", "-", "small cell (or its complement)"] for w in withheld]
    alt = (f"Dot plot of {len(pub)} published cohorts sorted by lapse rate with Wilson intervals: " +
           "; ".join(f"{r[0]} {r[1]} {r[4]} (n {r[2]})" for r in rows if r[1] != "withheld") +
           f". Overall model-renewal rate {fmt_pct(overall)}.")
    row_h = 19

    def draw(theme: Theme) -> str:
        label_w = max(text_width(dt.group, 12, bold=True) + text_width(dt.label, 11) for dt in dots) + 26
        right_w = max(text_width(dt.right, 11) for dt in dots) + 16
        x0 = PAD + label_w
        plot_w = W - 2 * PAD - label_w - right_w
        vmax, step = nice_scale(max(dt.hi for dt in dots))
        plot_h = row_h * len(dots)
        h = _header_height(subtitle, legend) + plot_h + 30 + _footer_height(d["note"]) + PAD
        svg = Svg(W, h, theme, title, alt)
        y = _header(svg, title, subtitle, legend)
        _x_axis(svg, x0, plot_w, y - 4, y + plot_h + 2, vmax, step, lambda v: fmt_pct(v, 0))
        xr = x0 + overall / vmax * plot_w
        svg.line(xr, y - 4, xr, y + plot_h + 2, theme.ink2, 1.5)
        yb = _dot_rows(svg, dots, x0, plot_w, y, 0.0, vmax, row_h, 0, PAD)
        _footer(svg, yb + 34, d["note"])
        return svg.render()

    return Figure("cohort-lapse-rates", title, alt, ["cohort", "plan", "n", "lapses", "rate", "Wilson 95%", "name"],
                  rows, d["note"], draw)


# --------------------------------------------------------------------------- tool latency
def latency_figure(d: dict) -> Figure:
    """d: {"mode": "stdio"|"in_process", "tools": [{"tool","toolset","p50","p95"}], "gate_ms": 50,
    "calls": int, "note"}."""
    tools = list(d["tools"])
    gate = d.get("gate_ms", 50)
    mode = "over MCP stdio (client round trip)" if d["mode"] == "stdio" else "in process"
    worst = max(tools, key=lambda r: (r["p95"], r["tool"]))
    gtools = [r for r in tools if r["toolset"] == "graph"]
    gworst = max((r["p95"] for r in gtools), default=0)
    title = "Tool latency: warm p50 and p95 per tool"
    subtitle = (f"{d.get('calls', '?')} warm calls per tool {mode}. Slowest p95: {worst['tool']} "
                f"{worst['p95']:.1f} ms. Graph tools p95 at most {gworst:.1f} ms (gate: below {gate} ms). "
                "One Mac under load: indicative, not a benchmark.")
    # two thin bars per tool (p50 above p95, a 2 px surface gap between them): p95 >= p50 always, so a
    # dot pair would hide the p50 under the p95 marker whenever they are close
    legend = [LegendItem("p50", 0), LegendItem("p95", 1)]
    rows = [[r["toolset"], r["tool"], f"{r['p50']:.2f}", f"{r['p95']:.2f}"] for r in tools]
    rights = [f"{r['p50']:.1f} / {r['p95']:.1f}" for r in tools]
    alt = (f"Paired bar chart of warm tool latency {mode}, p50 and p95 per tool: " +
           "; ".join(f"{r[1]} p50 {r[2]} ms, p95 {r[3]} ms" for r in rows) + ".")
    row_h, group_gap, bar_h, head_h = 24, 10, 8, 14
    right_head = "p50 / p95 ms"

    def draw(theme: Theme) -> str:
        label_w = max(max(text_width(r["toolset"], 12, bold=True), text_width(r["tool"], 11)) for r in tools) + 90
        right_w = max(text_width(s, 11) for s in [*rights, right_head]) + 16
        x0 = PAD + label_w
        plot_w = W - 2 * PAD - label_w - right_w
        top_v = max(r["p95"] for r in tools)
        vmax, step = nice_scale(max(top_v, gate * 1.04) if top_v >= gate * 0.4 else top_v)
        groups = sorted({r["toolset"] for r in tools}, key=lambda g: [r["toolset"] for r in tools].index(g))
        plot_h = row_h * len(tools) + group_gap * (len(groups) - 1)
        h = _header_height(subtitle, legend) + head_h + plot_h + 30 + _footer_height(d["note"]) + PAD
        svg = Svg(W, h, theme, title, alt)
        y = _header(svg, title, subtitle, legend) + head_h
        svg.text(x0 + plot_w + 12, y - 8, right_head, 10.5, theme.ink2)
        _x_axis(svg, x0, plot_w, y - 4, y + plot_h + 2, vmax, step, lambda v: f"{v:g} ms")
        if gate <= vmax:
            xg = x0 + gate / vmax * plot_w
            svg.line(xg, y - 4, xg, y + plot_h + 2, theme.ink2, 1.5)
            svg.text(xg - 4, y + 8, f"{gate} ms gate", 10, theme.ink2, anchor="end")
        prev, yy = None, y
        for r, right in zip(tools, rights, strict=True):
            if prev is not None and r["toolset"] != prev:
                yy += group_gap
            if r["toolset"] != prev:
                svg.text(PAD, yy + row_h / 2 + 4, r["toolset"], 12, theme.ink, weight="600")
            by = yy + (row_h - 2 * bar_h - GAP) / 2
            svg.text(x0 - 10, yy + row_h / 2 + 4, r["tool"], 11, theme.ink2, anchor="end")
            for i, key in enumerate(("p50", "p95")):
                svg.open(f"{r['tool']} {key}: {r[key]} ms")
                zero_or_bar(svg, x0, by + i * (bar_h + GAP), r[key] / vmax * plot_w, bar_h, theme.series[i],
                            zero=r[key] <= 0)
                svg.close()
            svg.text(x0 + plot_w + 12, yy + row_h / 2 + 4, right, 11, theme.ink2, tabular=True)
            prev = r["toolset"]
            yy += row_h
        svg.line(x0, y - 4, x0, yy + 2, theme.axis, 1)
        _footer(svg, yy + 34, d["note"])
        return svg.render()

    return Figure("tool-latency", title, alt, ["toolset", "tool", "p50 ms", "p95 ms"], rows, d["note"], draw)


# --------------------------------------------------------------------------- eval pass^3 (when a report exists)
def eval_figure(d: dict) -> Figure:
    """d: {"model", "arms": [..<=4], "shapes": [...], "pass3": {arm: {shape: [passed, cases]}}, "note"}."""
    arms = list(d["arms"])[:4]
    shapes = list(d["shapes"])
    title = f"Agent eval: pass^3 by arm and question shape ({d.get('model', 'model')})"
    subtitle = ("Share of cases answered correctly in all 3 trials (pass^3). A hollow ring marks a zero. LLM "
                "results gate article claims, never merges.")
    legend = [LegendItem(a, i) for i, a in enumerate(arms)]
    rows = [[s, a, f"{d['pass3'][a][s][0]}/{d['pass3'][a][s][1]}"] for s in shapes for a in arms if s in d["pass3"][a]]
    alt = "Grouped bars of pass^3 by question shape and arm: " + "; ".join(f"{r[0]} {r[1]} {r[2]}" for r in rows) + "."
    bar_h = 12

    def draw(theme: Theme) -> str:
        label_w = max(text_width(s, 12) for s in shapes) + 14
        plot_w = W - 2 * PAD - label_w - 70
        x0 = PAD + label_w
        group_h = bar_h * len(arms) + 2 * (len(arms) - 1) + 14
        h = _header_height(subtitle, legend) + group_h * len(shapes) + 30 + _footer_height(d["note"]) + PAD
        svg = Svg(W, h, theme, title, alt)
        y = _header(svg, title, subtitle, legend)
        _x_axis(svg, x0, plot_w, y - 4, y + group_h * len(shapes) - 12, 1.0, 0.25, lambda v: fmt_pct(v, 0))
        for s in shapes:
            svg.text(x0 - 10, y + group_h / 2, s, 12, theme.ink, anchor="end")
            for i, a in enumerate(arms):
                if s not in d["pass3"][a]:
                    continue
                k, n = d["pass3"][a][s]
                frac = k / n if n else 0.0
                by = y + i * (bar_h + 2)
                svg.open(f"{s}, arm {a}: {k}/{n}")
                end = zero_or_bar(svg, x0, by, frac * plot_w, bar_h, theme.series[i], zero=k == 0)
                svg.text(end + 6, by + 10, f"{k}/{n}", 10.5, theme.ink2, tabular=True)
                svg.close()
            y += group_h
        _footer(svg, y + 26, d["note"])
        return svg.render()

    return Figure("eval-pass3", title, alt, ["shape", "arm", "pass^3"], rows, d["note"], draw)


# --------------------------------------------------------------------------- leakage demo AUCs (when a report exists)
def leakage_figure(d: dict) -> Figure:
    """d: {"variants": [{"label", "single_feature": auc|None, "lr": auc|None}], "note"}."""
    vs = list(d["variants"])
    title = "Leakage demo: AUC of a neighbour lapse-rate feature, by how it was built"
    subtitle = ("A dataset with zero network effect. A self-inclusive or as-of-today neighbour rate looks "
                "predictive; the temporally safe one does not. 0.5 is chance.")
    legend = [LegendItem("single feature", 0, "dot"), LegendItem("logistic regression (all features)", 1, "diamond")]
    rows = [[v["label"], "-" if v.get("single_feature") is None else f"{v['single_feature']:.4f}",
             "-" if v.get("lr") is None else f"{v['lr']:.4f}"] for v in vs]
    alt = "Dot chart of AUCs: " + "; ".join(f"{r[0]} single feature {r[1]}, LR {r[2]}" for r in rows) + "."
    row_h = 24

    def draw(theme: Theme) -> str:
        label_w = max(text_width(v["label"], 12) for v in vs) + 14
        x0 = PAD + label_w
        plot_w = W - 2 * PAD - label_w - 110
        h = _header_height(subtitle, legend) + row_h * len(vs) + 30 + _footer_height(d["note"]) + PAD
        svg = Svg(W, h, theme, title, alt)
        y = _header(svg, title, subtitle, legend)
        _x_axis(svg, x0, plot_w, y - 4, y + row_h * len(vs) + 2, 1.0, 0.1, lambda v: f"{v:.1f}", vmin=0.5)

        def X(v: float) -> float:
            return x0 + (v - 0.5) / 0.5 * plot_w

        for v in vs:
            cy = y + row_h / 2
            svg.text(x0 - 10, cy + 4, v["label"], 12, theme.ink, anchor="end")
            parts = []
            for key, slot, shape in (("single_feature", 0, "dot"), ("lr", 1, "diamond")):
                if v.get(key) is not None:
                    svg.open(f"{v['label']} {key}: {v[key]:.4f}")
                    marker(svg, shape, X(max(0.5, v[key])), cy, theme.series[slot], 4.5)
                    svg.close()
                    parts.append(f"{v[key]:.3f}")
                else:
                    parts.append("-")
            svg.text(x0 + plot_w + 12, cy + 4, " / ".join(parts), 11, theme.ink2, tabular=True)
            y += row_h
        _footer(svg, y + 34, d["note"])
        return svg.render()

    return Figure("leakage-aucs", title, alt, ["variant", "single-feature AUC", "LR AUC"], rows, d["note"], draw)


# --------------------------------------------------------------------------- writing figures
def write_figure(fig: Figure, out_dir: Path) -> list[Path]:
    """Write <name>-light.svg and <name>-dark.svg; returns the paths (bytes depend only on the data)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for theme in THEMES:
        p = out_dir / f"{fig.name}-{theme.name}.svg"
        p.write_text(fig.svg(theme), encoding="utf-8", newline="\n")
        paths.append(p)
    return paths


# =========================================================================== data extraction (reads inputs)
def build_note(man: dict, extra: str = "") -> str:
    """Provenance line under a chart: build id, profile, seed and N, specs, commit."""
    commit = man.get("commit") or "unknown"
    dirty = " (dirty)" if man.get("dirty") else ""
    spec = man.get("spec", {})
    base = (f"Source: graph build {man.get('business_build_id')} · profile {man.get('profile')} · seed "
            f"{man.get('seed')}, N_USERS {man.get('n_users')} ({man.get('seed_n_status', 'declared')}) · data_end "
            f"{man.get('data_end')} · {spec.get('graph', '')} · commit {commit}{dirty} · regenerate with "
            "scripts/graph_evidence.py.")
    return f"{extra} {base}".strip() if extra else base


def load_build(build_dir: str | Path) -> tuple[dict, dict]:
    """(tables, manifest) of one build, through the pandas oracle (Parquet only)."""
    from . import manifest as mf
    from . import oracle

    return oracle.load_tables(build_dir), mf.read_manifest(build_dir)


def composition_data(t: dict, man: dict) -> dict:
    from . import oracle

    c = oracle.counts(t)
    return {"nodes": c["nodes"], "edges": c["edges"], "note": build_note(man)}


def leak_surface_data(t: dict, man: dict) -> dict:
    from . import oracle

    ls = oracle.leak_surface(t)
    return {"by_type": ls["by_type"], "declared_exception": "FIRST_RENEWAL_AFTER",
            "note": build_note(man, "After as_of = event_date > the renewal's as_of; for FIRST_RENEWAL_AFTER, "
                                    "known_by_as_of = false (the cut took effect after T-7).")}


def naive_pit_data(t: dict, man: dict) -> dict:
    from . import oracle, spec

    p = oracle.pit_parity(t)
    feats = []
    for f in ("limit_hits_14d", "incident_exposed_28d", "support_tickets_90d"):
        feats.append({"feature": f, "window": spec.FEATURE_CARDS[f]["window"],
                      "pit_mismatches": p["parity_mismatches"][f], "naive_wrong": p["naive_mismatches"][f]})
    return {"renewals": len(t["Renewal"]), "features": feats,
            "note": build_note(man, "Recomputed for every renewal by the pandas oracle and in Cypher by "
                                    "scripts/check_graph_contract.py (invariants #2 and #3).")}


def _hero(t: dict, renewal_id: str | None) -> str:
    from . import oracle

    rid = renewal_id or oracle.hero_renewal(t)
    if not rid:
        raise ValueError("this build has no hero renewal (sub_maya); pass a renewal id")
    if rid not in set(t["Renewal"]["renewal_id"]):
        raise ValueError(f"unknown renewal {rid!r} in this build")
    return rid


def timeline_data(t: dict, man: dict, renewal_id: str | None = None) -> dict:
    from . import oracle, spec

    rid = _hero(t, renewal_id)
    r = t["Renewal"].set_index("renewal_id").loc[rid]
    windows = {rel: w.days for rel, w in spec.PIT_WINDOWS.items() if w.days}
    # features whose window is "everything on or before as_of" (cuts so far, the latest overage setting)
    cumulative = [rel for rel, w in spec.PIT_WINDOWS.items()
                  if w.days is None and w.feature and rel != "FIRST_RENEWAL_AFTER"]
    sub = rid.split(":")[0]
    return {"renewal_id": rid, "label": sub, "as_of": str(r["as_of"].date()),
            "renewal_date": str(r["renewal_date"].date()), "rows": oracle.evidence(t, rid), "windows": windows,
            "cumulative": cumulative,
            "gold_rule_days": spec.FIRST_RENEWAL_WINDOW_DAYS, "hidden_after_as_of": oracle.hidden_after_as_of(t, rid),
            "note": build_note(man, "Rows as graph_renewal_evidence returns them (oracle.evidence).")}


def neighbours_data(t: dict, man: dict, renewal_id: str | None = None) -> dict:
    from . import cohorts, spec

    rid = _hero(t, renewal_id)
    ren = t["Renewal"].set_index("renewal_id")
    src = ren.loc[rid]
    current = src["route"] in ("score_today", "pending")
    s = t["SIMILAR_TO"]
    s = s[s["src"] == rid].sort_values("rank").head(spec.K)
    nb = []
    for x in s.itertuples():
        dst = ren.loc[x.dst]
        seen = current or (dst["outcome_observed_on"] == dst["outcome_observed_on"]
                           and dst["outcome_observed_on"] <= src["as_of"])
        nb.append({"rank": int(x.rank), "renewal_id": x.dst, "dist": float(x.dist), "d2_q": int(x.d2_q),
                   "outcome": dst["outcome"] if seen else "not_yet_observed"})
    n = sum(1 for r in nb if r["outcome"] != "not_yet_observed")
    k = sum(1 for r in nb if r["outcome"] == "voluntary_lapse")
    return {"renewal_id": rid, "label": rid.split(":")[0], "as_of": str(src["as_of"].date()),
            "visibility": "today" if current else "source_as_of", "neighbours": nb, "n": n, "lapsed": k,
            "wilson": cohorts.wilson(k, n) or [0.0, 0.0],
            "note": build_note(man, f"SIMILAR_TO spec {spec.SIMILAR_TO_SPEC_VERSION}: blocked by plan, 20 features, "
                                    "persisted z-score scaler, rank on floor(d2*1e9 + 0.5) then dst.")}


def exposure_data(t: dict, man: dict, incident_id: str = "inc-002") -> dict:
    from . import oracle

    e = oracle.exposure_incident(t, incident_id)
    if not e["by_plan"]:
        raise ValueError(f"no renewal was exposed to {incident_id!r} in this build")
    by_plan = {}
    for p, v in e["by_plan"].items():
        other = v["exposed"] - v["model"] - v["cancel_flow"] - v["dunning"]
        by_plan[p] = {"exposed": v["exposed"], "renewed": v["model"] - v["voluntary_lapses"],
                      "voluntary_lapse": v["voluntary_lapses"], "cancel_flow": v["cancel_flow"],
                      "dunning": v["dunning"], "other": other}
    return {"entity_id": incident_id, "by_plan": by_plan, "naive_additional": e["naive_additional"],
            "window_days": 28,
            "note": build_note(man, "Exposed = an active usage day during the incident inside (as_of-28, as_of]; "
                                    "renewed and voluntary lapse are model-routed renewals.")}


def lapse_rate_data(t: dict, man: dict) -> dict:
    from . import cohorts

    r = t["Renewal"]
    m = r[r["route"] == "model"].copy()
    m["first_after"] = m["renewal_id"].isin(set(t["FIRST_RENEWAL_AFTER"]["src"]))

    def cell(g) -> dict:
        n, k = len(g), int(g["churned"].sum())
        return {"n": n, "lapses": k, "wilson": cohorts.wilson(k, n) or [0.0, 0.0]}

    groups = [{"label": "all plans", "without": cell(m[~m["first_after"]]), "with": cell(m[m["first_after"]])}]
    for p in sorted(m["plan_tier"].unique(), key=lambda p: PLAN_SLOT.get(p, 9)):
        g = m[m["plan_tier"] == p]
        groups.append({"label": p, "without": cell(g[~g["first_after"]]), "with": cell(g[g["first_after"]])})
    return {"groups": groups,
            "note": build_note(man, "First renewal after a cap cut = a FIRST_RENEWAL_AFTER edge (the gold rule). "
                                    "Same population as metric_lapse_rate (route = model).")}


def cohorts_data(build_dir: str | Path, man: dict, algorithm: str = "leiden") -> dict:
    from . import cohorts, oracle

    data, _ = cohorts.cohort_list(Path(build_dir), algorithm)
    rows = []
    for c in data["cohorts"]:
        plan = c["name"].split(":")[0] if not c["suppressed"] else "withheld"
        rows.append({"cohort_id": c["cohort_id"], "plan": plan, "n": c["model_renewals"],
                     "lapses": c["voluntary_lapses"], "wilson": c["wilson_95"], "suppressed": c["suppressed"],
                     "name": c["name"].split(": ", 1)[-1] if not c["suppressed"] else ""})
    rt = oracle.routes(oracle.load_tables(build_dir))
    model = rt["routes"].get("model", 0)
    return {"algorithm": algorithm, "cohorts": rows, "overall": {"n": model, "lapses": rt["model_lapses"]},
            "modularity": data.get("modularity") or 0.0, "library": data.get("library"),
            "note": build_note(man, f"cohort_list (cohorts/renewal-v1, seed {data.get('seed')}); outside the graph "
                                    "contract; cells under 5 and their complements are withheld.")}


def latency_data(report: dict, note: str = "") -> dict | None:
    """From scripts/check_graph_tools.py --json (its "bench" section); None when no bench ran."""
    bench = report.get("bench") or {}
    mode = "stdio" if bench.get("stdio") else "in_process" if bench.get("in_process") else None
    if not mode:
        return None
    try:
        from . import tools as _tools
        order = {name: (i, s.toolset) for i, (name, s) in enumerate(_tools.SPECS.items())}
    except Exception:  # noqa: BLE001 - the tool registry only orders the rows; fall back to name order
        order = {}
    rows = []
    for name, v in bench[mode].items():
        idx, ts = order.get(name, (999, name.split("_")[0]))
        rows.append((idx, name, ts, float(v["p50"]), float(v["p95"])))
    rows.sort()
    toolset_name = {"metric": "metrics", "cohort": "cohorts"}
    return {"mode": mode, "gate_ms": 50, "calls": report.get("bench_calls"),
            "tools": [{"tool": n, "toolset": toolset_name.get(ts, ts), "p50": p50, "p95": p95}
                      for _, n, ts, p50, p95 in rows],
            "rss_kb": bench.get("rss_kb", {}), "note": note}


def eval_data(report: dict, note: str = "") -> dict | None:
    """From an eval report JSON with {"model", "pass3": {arm: {shape: [passed, cases]}}}; None if absent."""
    p3 = report.get("pass3")
    if not isinstance(p3, dict) or not p3:
        return None
    arms = sorted(p3)
    shapes = sorted({s for a in arms for s in p3[a]})
    return {"model": report.get("model", "model"), "arms": arms, "shapes": shapes, "pass3": p3, "note": note}


def leakage_data(report: dict, note: str = "") -> dict | None:
    """From a leakage demo JSON with {"variants": [{"label", "single_feature", "lr"}]}; None if absent."""
    vs = report.get("variants")
    if not isinstance(vs, list) or not vs:
        return None
    return {"variants": vs, "note": note}


# =========================================================================== Mermaid
_PA_TYPES = {"string": "string", "int64": "int", "double": "float", "bool": "bool", "date32[day]": "date"}
ER_CARDINALITY = {
    "HAS_RENEWAL": "||--|{", "ON_PLAN": "}o--||", "HIT_LIMIT": "||--o{", "CHANGED_OVERAGE": "||--o{",
    "CHARGED_OVERAGE": "||--o{", "OPENED": "||--o{", "BILLED": "||--o{", "EXPOSED_TO": "}o--o{",
    "FIRST_RENEWAL_AFTER": "}o--o{", "CUT_CAP": "}o--|{", "SIMILAR_TO": "}o--o{",
}


# One palette for every Mermaid diagram in the repo (docs/diagrams.md; tests/unit/test_mermaid_diagrams.py
# checks every block against it): the base theme with white clusters, and one classDef per layer.
MERMAID_INIT = ('%%{init: {"theme": "base", "flowchart": {"wrappingWidth": 360}, "themeVariables": {"primaryColor": "'
    '#CCFBF1", "primaryTextColor": "#0F172A", "primaryBorderColor": "#0F766E", "lineColor": "#64748B", "t'
    'extColor": "#0F172A", "edgeLabelBackground": "#FFFFFF", "clusterBkg": "#FFFFFF", "clusterBorder": "#'
    '64748B", "titleColor": "#0F172A", "attributeBackgroundColorOdd": "#FFFFFF", "attributeBackgroundColo'
    'rEven": "#F0FDFA", "relationColor": "#64748B", "relationLabelBackground": "#FFFFFF", "relationLabelC'
    'olor": "#0F172A"}}}%%')
MERMAID_CLASSDEFS = """  classDef storage fill:#DBEAFE,stroke:#1D4ED8,color:#0F172A,stroke-width:1.5px
  classDef catalog fill:#FEF3C7,stroke:#B45309,color:#0F172A,stroke-width:1.5px
  classDef compute fill:#ECFCCB,stroke:#4D7C0F,color:#0F172A,stroke-width:1.5px
  classDef orchestration fill:#FCE7F3,stroke:#BE185D,color:#0F172A,stroke-width:1.5px
  classDef graphlayer fill:#CCFBF1,stroke:#0F766E,color:#0F172A,stroke-width:1.5px
  classDef consumer fill:#FFEDD5,stroke:#C2410C,color:#0F172A,stroke-width:1.5px
  classDef data fill:#F1F5F9,stroke:#475569,color:#0F172A,stroke-width:1.5px"""


def mermaid_label(s: str) -> str:
    """Text safe inside a quoted Mermaid label."""
    return (str(s).replace("`", "").replace('"', "#quot;").replace("<", "#lt;").replace(">", "#gt;")
            .replace("|", "#124;"))


def mermaid_schema(counts: dict) -> str:
    """Node labels and edge types of the business graph with their counts (flowchart).

    An edge type whose both ends have the same label (SIMILAR_TO: a Renewal to another Renewal) is drawn as a
    hexagon note joined to that label by a dotted line: Mermaid lays a self-loop's label on top of the
    neighbouring edge labels."""
    from . import spec

    nodes, edges = counts["nodes"], counts["edges"]
    out = [MERMAID_INIT, "flowchart LR"]
    for label in spec.NODE_SCHEMA:
        out.append(f'  {label}["{label}<br/>{fmt_int(nodes.get(label, 0))}"]')
    loops = []
    for rel, e in spec.EDGE_SCHEMA.items():
        if e.src == e.dst:
            loops.append((rel, e))
            continue
        out.append(f'  {e.src} -->|"{rel} {fmt_int(edges.get(rel, 0))}"| {e.dst}')
    for rel, e in loops:
        out.append(f'  {e.src}_{rel}{{{{"{rel} {fmt_int(edges.get(rel, 0))}<br/>{e.src} to another {e.dst}"}}}}')
        out.append(f"  {e.src} -.- {e.src}_{rel}")
    members = [*spec.NODE_SCHEMA, *(f"{e.src}_{rel}" for rel, e in loops)]
    out += ["", MERMAID_CLASSDEFS, f"  class {','.join(members)} graphlayer"]
    return "\n".join(out) + "\n"


def mermaid_er() -> str:
    """Entity-relationship view of spec.NODE_SCHEMA / EDGE_SCHEMA (columns and types; features collapsed)."""
    from . import spec

    out = [MERMAID_INIT, "erDiagram"]   # entities take the graph-layer colours from the init (primaryColor)
    feats = set(spec.NUMERIC_FEATURES)
    for label, n in spec.NODE_SCHEMA.items():
        out.append(f"  {label} {{")
        added = False
        for col, typ in n.columns:
            if col in feats:
                if not added:
                    out.append(f'    float features_{len(spec.NUMERIC_FEATURES)} "the {len(spec.NUMERIC_FEATURES)} '
                               'numeric gold features"')
                    added = True
                continue
            key = " PK" if col == n.key else ""
            out.append(f"    {_PA_TYPES.get(str(typ), 'string')} {col}{key}")
        out.append("  }")
    for rel, e in spec.EDGE_SCHEMA.items():
        out.append(f"  {e.src} {ER_CARDINALITY.get(rel, '}o--o{')} {e.dst} : {rel}")
    return "\n".join(out) + "\n"


_LAYERS = ("source", "bronze", "silver", "gold", "export")


def mermaid_lineage(trace: dict) -> str:
    """Upstream (or downstream) lineage of one column from lineage_trace data, data flowing left to right."""
    edges = sorted(trace["edges"], key=lambda e: (e["depth"], e["from"], e["to"], e["rel"]))
    names = sorted({e["from"] for e in edges} | {e["to"] for e in edges} | {trace["target"]})
    ids = {n: f"n{i}" for i, n in enumerate(names)}
    out = [MERMAID_INIT, "flowchart LR"]
    by_layer: dict[str, list[str]] = {}
    for n in names:
        layer = n.split(".", 1)[0] if n.split(".", 1)[0] in _LAYERS else "other"
        by_layer.setdefault(layer, []).append(n)
    for layer in [*_LAYERS, "other"]:
        if layer not in by_layer:
            continue
        out.append(f'  subgraph {layer}["{layer}"]')
        for n in by_layer[layer]:
            short = n.split(".", 1)[1] if "." in n else n
            if short.count(".") == 0:   # a dataset (layer.table): a row count reads the whole table
                short += " (table)"
            out.append(f'    {ids[n]}["{mermaid_label(short)}"]')
        out.append("  end")
    for e in edges:
        bits = [x for x in (e.get("roles"), e.get("window")) if x]
        if e["rel"] != "DERIVED_FROM":
            bits.insert(0, e["rel"])
        tr = e.get("transform")
        if tr and tr != "IDENTITY" and e["rel"] == "DERIVED_FROM" and not e.get("window"):
            if " AS " in tr:   # 'x AS `name`': the name it becomes says more than the expression
                tr = "as " + tr.rsplit(" AS ", 1)[-1].replace("`", "")
            bits.append(tr if len(tr) <= 40 else tr[:39] + "...")
        label = " ".join(bits) or "copy"
        src, dst = (e["to"], e["from"]) if trace.get("direction", "upstream") == "upstream" else (e["from"], e["to"])
        out.append(f'  {ids[src]} -->|"{mermaid_label(label)}"| {ids[dst]}')
    out += ["", MERMAID_CLASSDEFS, f"  class {','.join(ids[n] for n in names)} data"]
    return "\n".join(out) + "\n"


def lineage_trace_data(build_dir: str | Path, target: str, direction: str = "upstream") -> dict:
    """lineage_trace from the lineage Parquet (pure-Python oracle; no Ladybug)."""
    from .lineage import oracle as lo

    data, _ = lo.lineage_trace(lo.load_tables(build_dir), target, direction)
    return data


# =========================================================================== generated regions in the docs
BEGIN = "<!-- graph-evidence:begin {} -->"
END = "<!-- graph-evidence:end {} -->"


def fill_regions(text: str, regions: dict[str, str]) -> tuple[str, list[str]]:
    """Replace the content between the begin / end markers of every named region present in ``text``."""
    filled = []
    for name in sorted(regions):
        b, e = BEGIN.format(name), END.format(name)
        i = text.find(b)
        if i < 0:
            continue
        j = text.find(e, i)
        if j < 0:
            raise ValueError(f"region {name!r} has a begin marker but no end marker")
        body = regions[name].strip("\n")
        text = text[: i + len(b)] + "\n" + body + "\n" + text[j:]
        filled.append(name)
    return text, filled


def region_names(text: str) -> list[str]:
    out, i = [], 0
    pre = "<!-- graph-evidence:begin "
    while True:
        i = text.find(pre, i)
        if i < 0:
            return out
        j = text.find(" -->", i)
        out.append(text[i + len(pre): j])
        i = j


PROFILE_LABEL = {"s42": "Seed 42", "tiny": "Tiny fixture", "default": "Default sample"}


def headline_table(cols: list[dict], latency: dict | None = None, bench_profile: str | None = None) -> str:
    """The headline numbers of docs/graph/README.md, from each build's manifest.json and contract.json.

    cols: [{"profile", "manifest": dict, "contract": dict | None}]; latency: latency_data() of the bench
    (drawn in the column of ``bench_profile`` only)."""
    def inv(c: dict | None) -> dict:
        return ((c or {}).get("summary") or {}).get("invariants") or {}

    def cell(c: dict | None, fn: Callable[[dict], str]) -> str:
        try:
            return fn(c) if c else "-"
        except (KeyError, TypeError, ValueError):
            return "-"

    def build_cell(m: dict) -> str:
        b, lb = m.get("builder") or {}, m.get("ladybug") or {}
        if "seconds" not in b:
            return "-"
        load = f" + {lb['load_s']:.1f} s" if "load_s" in lb else ""
        return f"{b['seconds']:.1f} s{load}, {b.get('max_rss_mib', 0):.0f} MiB"

    def tools_cell(col: dict) -> str:
        if not latency or col["profile"] != bench_profile:
            return "not measured"
        g = [r["p95"] for r in latency["tools"] if r["toolset"] == "graph"]
        allp = [r["p95"] for r in latency["tools"]]
        return (f"graph tools at most {max(g):.1f} ms; all {len(allp)} tools at most {max(allp):.1f} ms"
                if g else f"all {len(allp)} tools at most {max(allp):.1f} ms")

    naive_keys = ("limit_hits_14d", "incident_exposed_28d", "support_tickets_90d")
    spec_rows = [
        ("Nodes / edges", lambda col: f"{fmt_int(col['manifest']['counts']['total_nodes'])} / "
                                      f"{fmt_int(col['manifest']['counts']['total_edges'])}"),
        ("Strict contract", lambda col: cell(col["contract"], lambda c: c["status"] + (" (strict)" if c.get("strict")
                                                                                      else " (not strict)"))),
        ("Point-in-time parity (6 features, pandas and Cypher)",
         lambda col: cell(col["contract"], lambda c: f"{sum(inv(c)['parity_mismatches'].values())} mismatches"
                          if len(inv(c)["parity_mismatches"]) == 6 else "-")),
        ("A naive traversal must be wrong (limit hits / incident exposure / tickets)",
         lambda col: cell(col["contract"], lambda c: " / ".join(fmt_int(inv(c)["naive_mismatches"][k])
                                                                for k in naive_keys))),
        ("Event edges after their renewal's as_of (kept on purpose, never served)",
         lambda col: cell(col["contract"], lambda c: f"{fmt_int(inv(c)['leak_surface']['post_as_of_total'])} of "
                                                     f"{fmt_int(inv(c)['leak_surface']['event_edges'])}")),
        ("Build (builder + Ladybug load, max RSS)", lambda col: build_cell(col["manifest"])),
    ]
    if latency:
        calls = latency.get("calls")
        spec_rows.append((f"Tools (warm p95 over MCP stdio{f', {calls} calls each' if calls else ''})"
                          if latency.get("mode") == "stdio" else "Tools (warm p95 in process)", tools_cell))
    heads = [PROFILE_LABEL.get(c["profile"], c["profile"]) + f" (`{c['profile']}`)" for c in cols]
    out = ["| | " + " | ".join(heads) + " |", "|---|" + "---:|" * len(cols)]
    for label, fn in spec_rows:
        out.append(f"| {label} | " + " | ".join(fn(c) for c in cols) + " |")
    ids = ", ".join(f"{c['profile']} build `{c['manifest'].get('business_build_id')}`" for c in cols)
    first = cols[0]["manifest"]
    src = (f"Generated by `scripts/graph_charts.py` from each build's `manifest.json` and `contract.json` ({ids}; "
           f"commit `{first.get('commit') or 'unknown'}`{' with uncommitted changes' if first.get('dirty') else ''})")
    if latency:
        src += f" and `results/tools-bench-{bench_profile}.json`"
    return "\n".join(out) + f"\n\n<sub>{src}.</sub>"


def pending(what: str, how: str) -> str:
    """The text a region holds until its input exists."""
    return f"> **Pending final run.** {what} {how}"


def fence(lang: str, body: str) -> str:
    return f"```{lang}\n{body.rstrip()}\n```"


def figures_from_build(build_dir: str | Path, renewal_id: str | None = None,
                       incident_id: str = "inc-002") -> list[Figure]:
    """Every chart a single build can feed."""
    t, man = load_build(build_dir)
    figs = [composition_figure(composition_data(t, man)), leak_surface_figure(leak_surface_data(t, man)),
            naive_pit_figure(naive_pit_data(t, man)), timeline_figure(timeline_data(t, man, renewal_id)),
            neighbours_figure(neighbours_data(t, man, renewal_id)),
            exposure_figure(exposure_data(t, man, incident_id)), lapse_rate_figure(lapse_rate_data(t, man))]
    if (Path(build_dir) / "cohorts.parquet").is_file():
        figs.append(cohorts_figure(cohorts_data(build_dir, man)))
    return figs


def iter_names(figs: Iterable[Figure]) -> list[str]:
    return [f.name for f in figs]


__all__ = [
    "DARK",
    "LIGHT",
    "THEMES",
    "Figure",
    "Svg",
    "cohorts_figure",
    "composition_figure",
    "eval_figure",
    "exposure_figure",
    "figures_from_build",
    "fill_regions",
    "lapse_rate_figure",
    "latency_figure",
    "leak_surface_figure",
    "leakage_figure",
    "mermaid_er",
    "mermaid_lineage",
    "mermaid_schema",
    "naive_pit_figure",
    "neighbours_figure",
    "nice_scale",
    "pending",
    "region_names",
    "text_width",
    "timeline_figure",
    "write_figure",
]

