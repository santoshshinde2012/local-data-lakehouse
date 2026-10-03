"""File-naming convention and relative links (see CONTRIBUTING.md, "File names").

Runs scripts/check_docs.py over the tracked files: Markdown and Mermaid files outside the root are
lowercase kebab-case (README.md is the directory-index exception), everything under docs/ and
results/ is lowercase, and every relative link in a tracked .md resolves.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("check_docs", ROOT / "scripts" / "check_docs.py")
check_docs = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(check_docs)


def test_file_names_follow_the_convention():
    assert check_docs.name_problems(check_docs.tracked()) == []


def test_relative_markdown_links_resolve():
    assert check_docs.link_problems(check_docs.tracked()) == []


def test_checker_rejects_bad_names():
    bad = check_docs.name_problems(["docs/USE_CASE.md", "results/Worked Examples.md", "docs/img/Shot.png",
                                    "docs/graph/lineage-limit_hits_14d.mmd"])
    assert len(bad) == 4
    assert check_docs.name_problems(["README.md", "CHANGELOG.md", "docs/README.md", "docs/use-case.md",
                                     "results/churn-e2e.excerpt.md", "results/plots/pr_curve.png",
                                     ".claude/skills/x/SKILL.md"]) == []
