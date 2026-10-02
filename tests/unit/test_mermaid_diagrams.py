"""Every Mermaid diagram in the repo (```mermaid blocks in tracked *.md and the *.mmd sources) uses the one
palette in docs/diagrams.md, GitHub-safe syntax and no HTML but <br/>. Rendering itself is checked with
mermaid-cli when a diagram changes (docs/diagrams.md); this test keeps them consistent."""
from __future__ import annotations

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

INIT = ('%%{init: {"theme": "base", "flowchart": {"wrappingWidth": 360}, "themeVariables": {"primaryColor": '
        '"#CCFBF1", "primaryTextColor": "#0F172A", "primaryBorderColor": "#0F766E", "lineColor": "#64748B", '
        '"textColor": "#0F172A", "edgeLabelBackground": "#FFFFFF", "clusterBkg": "#FFFFFF", "clusterBorder": '
        '"#64748B", "titleColor": "#0F172A", "attributeBackgroundColorOdd": "#FFFFFF", '
        '"attributeBackgroundColorEven": "#F0FDFA", "relationColor": "#64748B", "relationLabelBackground": '
        '"#FFFFFF", "relationLabelColor": "#0F172A"}}}%%')
CLASSDEFS = {
    "storage": "fill:#DBEAFE,stroke:#1D4ED8,color:#0F172A,stroke-width:1.5px",
    "catalog": "fill:#FEF3C7,stroke:#B45309,color:#0F172A,stroke-width:1.5px",
    "compute": "fill:#ECFCCB,stroke:#4D7C0F,color:#0F172A,stroke-width:1.5px",
    "orchestration": "fill:#FCE7F3,stroke:#BE185D,color:#0F172A,stroke-width:1.5px",
    "graphlayer": "fill:#CCFBF1,stroke:#0F766E,color:#0F172A,stroke-width:1.5px",
    "consumer": "fill:#FFEDD5,stroke:#C2410C,color:#0F172A,stroke-width:1.5px",
    "data": "fill:#F1F5F9,stroke:#475569,color:#0F172A,stroke-width:1.5px",
}
DASHED = "stroke-dasharray:5 5"


def _diagrams() -> list[tuple[str, str]]:
    files = subprocess.run(["git", "ls-files", "*.md", "*.mmd"], cwd=ROOT, capture_output=True, text=True,
                           check=True).stdout.split()
    out = []
    for f in files:
        text = (ROOT / f).read_text(encoding="utf-8")
        blocks = [text] if f.endswith(".mmd") else re.findall(r"```mermaid\n(.*?)```", text, flags=re.S)
        out += [(f"{f}#{i}", b) for i, b in enumerate(blocks)]
    return out


def _luminance(hex_: str) -> float:
    c = [int(hex_[i:i + 2], 16) / 255 for i in (1, 3, 5)]
    c = [x / 12.92 if x <= 0.03928 else ((x + 0.055) / 1.055) ** 2.4 for x in c]
    return 0.2126 * c[0] + 0.7152 * c[1] + 0.0722 * c[2]


def _contrast(a: str, b: str) -> float:
    la, lb = sorted((_luminance(a), _luminance(b)), reverse=True)
    return (la + 0.05) / (lb + 0.05)


def test_the_palette_meets_wcag_aa():
    for name, spec in CLASSDEFS.items():
        props = dict(p.split(":", 1) for p in spec.split(","))
        assert _contrast(props["color"], props["fill"]) >= 4.5, name
    assert _contrast("#64748B", "#FFFFFF") >= 3 and _contrast("#64748B", "#0D1117") >= 3, "lines, light and dark"


def test_every_diagram_uses_the_shared_palette_and_github_safe_syntax():
    diagrams = _diagrams()
    assert len(diagrams) >= 11
    for where, block in diagrams:
        lines = block.splitlines()
        assert lines[0] == INIT, f"{where}: the shared init line comes first"
        kind = lines[1]
        assert kind in ("flowchart LR", "flowchart TB", "erDiagram"), f"{where}: {kind}"
        assert block.count('"') % 2 == 0, f"{where}: unbalanced quotes"
        assert block.count("subgraph ") == len(re.findall(r"^\s*end\s*$", block, flags=re.M)), where
        html = set(re.findall(r"<[^>]+>", block)) - {"<br/>"}
        assert not html, f"{where}: HTML other than <br/> ({sorted(html)})"
        assert "%%{" not in block[len(INIT):], f"{where}: one directive"
        defs = dict(re.findall(r"^\s*classDef (\w+) (\S+)$", block, flags=re.M))
        if kind == "erDiagram":
            assert not defs, f"{where}: entities take the graph colours from the init"
            continue
        assert defs == CLASSDEFS, f"{where}: classDefs differ from the palette"
        used = set(re.findall(r"^\s*class \S+ (\w+)$", block, flags=re.M))
        assert used and used <= set(CLASSDEFS), f"{where}: {used}"
        for style in re.findall(r"^\s*style \S+ (.+)$", block, flags=re.M):
            assert style == DASHED, f"{where}: style other than a dashed border ({style})"
        assert not re.search(r"-->\|[^\"]", block), f"{where}: unquoted edge label"


def test_the_graph_chart_generator_uses_the_same_palette():
    src = (ROOT / "src/lakehouse_graph/charts.py").read_text(encoding="utf-8")
    joined = "".join(re.findall(r"'([^']*)'", src[src.index("MERMAID_INIT = ("):src.index("MERMAID_CLASSDEFS")]))
    assert joined == INIT
    for name, spec in CLASSDEFS.items():
        assert f"  classDef {name} {spec}" in src, name
