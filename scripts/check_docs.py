#!/usr/bin/env python3
"""Check the repo's file-naming convention and its relative Markdown links (no network).

Naming (CONTRIBUTING.md, "File names"):
  * root files keep their conventional names (README.md, CHANGELOG.md, LICENSE, ...): not checked here;
  * every other tracked .md / .mmd is README.md or lowercase kebab-case (dots allowed between parts,
    e.g. churn-e2e.excerpt.md); tool-required names in ALLOWED_NAMES are the only exceptions;
  * every tracked file under docs/ and results/ is lowercase with no spaces ([a-z0-9._-]).
Links: every relative link or image in a tracked .md resolves to a tracked file or directory, and a
#fragment on a Markdown target matches one of its headings or an explicit anchor.

Usage: python scripts/check_docs.py [--names] [--links]   (both by default; exit 1 on any problem)
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
import unicodedata
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[1]
KEBAB = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*(?:\.[a-z0-9]+(?:-[a-z0-9]+)*)*\.(?:md|mmd)$")
LOWER = re.compile(r"^[a-z0-9._-]+$")
ALLOWED_NAMES = {"README.md", "SKILL.md"}  # directory index; Claude Code requires SKILL.md in a skill folder
LOWER_DIRS = ("docs/", "results/")
LINK = re.compile(r"(?<!\\)!?\[(?:[^\]\\]|\\.)*\]\(\s*<?([^)\s>]+)>?(?:\s+\"[^\"]*\")?\s*\)")
REF = re.compile(r"^\s{0,3}\[[^\]]+\]:\s*<?(\S+?)>?(?:\s+\"[^\"]*\")?\s*$", re.M)


def tracked() -> list[str]:
    out = subprocess.run(["git", "ls-files", "-z"], cwd=ROOT, capture_output=True, check=True).stdout
    return [p for p in out.decode().split("\0") if p and (ROOT / p).exists()]


def name_problems(files: list[str]) -> list[str]:
    bad = []
    for f in files:
        p = PurePosixPath(f)
        if len(p.parts) > 1 and p.suffix in (".md", ".mmd") and p.name not in ALLOWED_NAMES and not KEBAB.match(p.name):
            bad.append(f"{f}: Markdown and Mermaid files outside the root use lowercase kebab-case")
        elif f.startswith(LOWER_DIRS) and not all(LOWER.match(part) for part in p.parts[1:]) and p.name not in ALLOWED_NAMES:
            bad.append(f"{f}: files under docs/ and results/ are lowercase with no spaces")
    return bad


def _slug(heading: str) -> str:
    """GitHub's heading anchor: lowercase, drop punctuation except - and _, spaces to -."""
    h = re.sub(r"<[^>]+>", "", heading)
    h = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", h)
    h = h.replace("`", "").strip().lower()
    h = "".join(c for c in h if c in " -_" or unicodedata.category(c)[0] in "LN")
    return h.replace(" ", "-")


def anchors(text: str) -> set[str]:
    text = re.sub(r"```.*?```", "", text, flags=re.S)
    seen: dict[str, int] = {}
    out = set()
    for m in re.finditer(r"^#{1,6}\s+(.+?)\s*#*\s*$", text, flags=re.M):
        s = _slug(m.group(1))
        n = seen.get(s, 0)
        out.add(s if n == 0 else f"{s}-{n}")
        seen[s] = n + 1
    out |= set(re.findall(r"""<a\s+(?:name|id)=["']([^"']+)["']""", text))
    return out


def link_problems(files: list[str]) -> list[str]:
    files_set = set(files)
    dirs = {str(PurePosixPath(f).parent) for f in files} | {"."}
    for d in list(dirs):
        parts = PurePosixPath(d).parts
        dirs |= {str(PurePosixPath(*parts[:i])) for i in range(1, len(parts))}
    bad = []
    cache: dict[str, set[str]] = {}
    for f in (x for x in files if x.endswith(".md")):
        text = (ROOT / f).read_text(encoding="utf-8")
        body = re.sub(r"```.*?```", "", text, flags=re.S)
        body = re.sub(r"`[^`\n]*`", "", body)
        targets = LINK.findall(body) + REF.findall(body)
        for t in targets:
            if re.match(r"^[a-z][a-z0-9+.-]*:", t, re.I) or t.startswith("//"):
                continue  # URL, mailto:, etc.
            path, _, frag = t.partition("#")
            if path.startswith("/"):
                target = path.lstrip("/")
            elif path:
                target = str(PurePosixPath(f).parent / path)
            else:
                target = f
            norm = str(PurePosixPath(*[p for p in PurePosixPath(target).parts]))
            parts: list[str] = []
            for part in PurePosixPath(norm).parts:
                if part == "..":
                    if parts:
                        parts.pop()
                    else:
                        parts.append("..")
                elif part != ".":
                    parts.append(part)
            norm = "/".join(parts) or "."
            norm = re.sub(r"%20", " ", norm).rstrip("/") or "."
            if norm not in files_set and norm not in dirs:
                bad.append(f"{f}: broken link {t}")
                continue
            if frag and norm.endswith(".md") and norm in files_set:
                if norm not in cache:
                    cache[norm] = anchors((ROOT / norm).read_text(encoding="utf-8"))
                if frag.lower() not in cache[norm] and not frag.startswith(("L", "user-content-")):
                    bad.append(f"{f}: no heading for #{frag} in {norm}")
    return bad


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--names", action="store_true")
    ap.add_argument("--links", action="store_true")
    a = ap.parse_args(argv)
    both = not (a.names or a.links)
    files = tracked()
    problems = (name_problems(files) if a.names or both else []) + (link_problems(files) if a.links or both else [])
    for p in problems:
        print(f"  FAIL  {p}")
    n_md = sum(f.endswith(".md") for f in files)
    print(f"check_docs: {'FAILED' if problems else 'OK'} ({len(problems)} problem(s); {len(files)} tracked files, "
          f"{n_md} Markdown)")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
