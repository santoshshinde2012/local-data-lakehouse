#!/usr/bin/env python3
"""Tier-0 mini repo contract checks: cheap static checks that catch real drift.

Reads the repo's own code with ``ast`` and regular expressions (stdlib only: no pandas,
no Spark, no sqlglot, nothing is imported or executed), so it runs anywhere in < 1 s.

  constants   the plan allowances (550 / 1,650 / 11,000), the cap-cut multiplier (0.83) and
              the T-7 offset must agree across sql/churn/gold_renewal_features.sql, the
              pandas twin (scripts/build_churn_gold_local.py) and the generator
              (scripts/generate_churn_sample.py); plan prices must agree between the
              generator and the graph spec                                          -> error
  columns     the 24-field train contract must be the same list in check_churn_export.py,
              04_export_features.py, the pandas twin and the graph spec             -> error
  appName     churn Spark jobs: appName == "churn_<file stem>"                      -> error
              retail Spark jobs: appName == "<file stem>"                           -> warning
  LEAKY       every name in check_churn_export.LEAKY must exist in some dataset     -> warning
              (until the owner decides whether to drop the name or add the column)
  unreadable  a source file these checks read is missing or does not parse          -> error

Expected at commit 3efe31a: 0 errors, 2 warnings (the dangling LEAKY member
``cancel_at_period_end`` and the retail 05 appName drift).

Usage:
  python scripts/check_repo_contracts.py [--strict] [--json]
"""
from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GOLD_SQL = "sql/churn/gold_renewal_features.sql"
PANDAS_TWIN = "scripts/build_churn_gold_local.py"
GENERATOR = "scripts/generate_churn_sample.py"
EXPORT_CONTRACT = "scripts/check_churn_export.py"
EXPORT_JOB = "src/jobs/churn/04_export_features.py"
BRONZE_JOB = "src/jobs/churn/01_ingest_bronze.py"
SILVER_JOB = "src/jobs/churn/02_transform_silver.py"
GOLD_JOB = "src/jobs/churn/03_publish_gold_features.py"
GRAPH_SPEC = "src/lakehouse_graph/spec.py"
REQUIRED_SOURCES = [GOLD_SQL, PANDAS_TWIN, GENERATOR, EXPORT_CONTRACT, EXPORT_JOB, BRONZE_JOB, SILVER_JOB, GOLD_JOB]


# --------------------------------------------------------------------------- ast helpers
def _tree(root: Path, rel: str) -> ast.Module:
    return ast.parse((root / rel).read_text(encoding="utf-8"), filename=rel)


def _const(tree: ast.Module, name: str):
    """Value of a module-level ``NAME = <literal>`` assignment (None if absent or not literal)."""
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name for t in node.targets):
            try:
                return ast.literal_eval(node.value)
            except ValueError:
                return None
    return None


def _timedelta_days(node: ast.AST) -> int | None:
    """N from the first ``Timedelta(days=N)`` call inside ``node``."""
    for n in ast.walk(node):
        if isinstance(n, ast.Call) and getattr(n.func, "attr", getattr(n.func, "id", "")) == "Timedelta":
            for kw in n.keywords:
                if kw.arg == "days" and isinstance(kw.value, ast.Constant):
                    return int(kw.value.value)
    return None


def _app_name(tree: ast.Module) -> str | None:
    for n in ast.walk(tree):
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "appName" and n.args \
                and isinstance(n.args[0], ast.Constant) and isinstance(n.args[0].value, str):
            return n.args[0].value
    return None


# --------------------------------------------------------------------------- extractors
def sql_constants(root: Path) -> dict:
    sql = (root / GOLD_SQL).read_text(encoding="utf-8")
    case = re.search(r"CASE\s+plan_tier\s+((?:WHEN\s+'\w+'\s+THEN\s+[\d.]+\s+)+)END", sql, re.IGNORECASE)
    whens = re.findall(r"WHEN\s+'(\w+)'\s+THEN\s+([\d.]+)", case.group(1), re.IGNORECASE) if case else []
    allowance = {k: int(float(v)) for k, v in whens} if case else None
    cut = re.search(r"pow\(\s*([\d.]+)\s*,\s*cuts_so_far\s*\)", sql, re.IGNORECASE)
    t7 = re.search(r"snapshot_date\s*=\s*date_sub\(\s*current_period_end\s*,\s*(\d+)\s*\)", sql, re.IGNORECASE)
    return {"allowance": allowance, "cap_cut": float(cut.group(1)) if cut else None,
            "as_of_offset_days": int(t7.group(1)) if t7 else None,
            "aliases": set(re.findall(r"\bAS\s+([a-z_][a-z0-9_]*)", sql, re.IGNORECASE))}


def pandas_constants(root: Path) -> dict:
    tree = _tree(root, PANDAS_TWIN)
    offset = None
    for n in ast.walk(tree):  # snapshot_date == current_period_end - pd.Timedelta(days=7)
        if isinstance(n, ast.BinOp) and isinstance(n.op, ast.Sub) and "current_period_end" in ast.unparse(n.left):
            offset = _timedelta_days(n.right)
            if offset is not None:
                break
    return {"allowance": _const(tree, "ALLOWANCE"), "cap_cut": _const(tree, "CAP_CUT"),
            "as_of_offset_days": offset, "features": _const(tree, "FEATURES")}


def generator_constants(root: Path) -> dict:
    tree = _tree(root, GENERATOR)
    offsets = set()
    for n in ast.walk(tree):  # "snapshot_date": (renewal - pd.Timedelta(days=7))... (the cohort rows)
        if isinstance(n, ast.Dict):
            for k, v in zip(n.keys, n.values, strict=True):
                if isinstance(k, ast.Constant) and k.value == "snapshot_date" and _timedelta_days(v) is not None:
                    offsets.add(_timedelta_days(v))
    return {"allowance": _const(tree, "ALLOWANCE"), "cap_cut": _const(tree, "CAP_CUT"),
            "as_of_offset_days": offsets.pop() if len(offsets) == 1 else None, "price": _const(tree, "PRICE")}


def known_columns(root: Path) -> set[str]:
    """Every column name some churn dataset has: bronze schemas, silver additions, gold SQL
    aliases, the gold job's added columns and the export column lists."""
    cols: set[str] = set()
    bronze = _const(_tree(root, BRONZE_JOB), "BRONZE") or {}
    for _csv, schema in bronze.values():
        cols.update(part.split()[0] for part in schema.split(",") if part.strip())
    for rel in (SILVER_JOB, GOLD_JOB):
        for n in ast.walk(_tree(root, rel)):
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "withColumn" \
                    and n.args and isinstance(n.args[0], ast.Constant):
                cols.add(n.args[0].value)
    cols |= sql_constants(root)["aliases"]
    export = _tree(root, EXPORT_JOB)
    cols.update(_const(export, "TRAIN_COLUMNS") or [])
    cols.update(_const(export, "AUDIT_EXTRA") or [])
    return cols


# --------------------------------------------------------------------------- checks
def unreadable_sources(root: Path) -> list[tuple[str, str]]:
    """One error per source file the checks read that is missing or does not parse."""
    out = []
    paths = [root / rel for rel in REQUIRED_SOURCES]
    paths += sorted((root / "src/jobs/churn").glob("*.py")) + sorted((root / "src/jobs/retail").glob("*.py"))
    if (root / GRAPH_SPEC).is_file():
        paths.append(root / GRAPH_SPEC)
    for path in dict.fromkeys(paths):
        rel = str(path.relative_to(root))
        try:
            text = path.read_text(encoding="utf-8")
            if path.suffix == ".py":
                ast.parse(text, filename=rel)
        except (OSError, UnicodeDecodeError, SyntaxError) as e:
            out.append(("unreadable", f"{rel}: cannot be read ({type(e).__name__}: {e})"))
    return out


def check(root: Path = ROOT) -> dict:
    """Returns {"errors": [(code, msg)], "warnings": [(code, msg)], "ok": [msg], "facts": {...}}."""
    unreadable = unreadable_sources(root)
    if unreadable:
        return {"errors": unreadable, "warnings": [], "ok": [], "facts": {}}
    return _check(root)


def _check(root: Path) -> dict:
    errors: list[tuple[str, str]] = []
    warnings: list[tuple[str, str]] = []
    ok: list[str] = []
    sql, pdc, gen = sql_constants(root), pandas_constants(root), generator_constants(root)
    sources = {GOLD_SQL: sql, PANDAS_TWIN: pdc, GENERATOR: gen}

    def agree(code: str, label: str, key: str, fmt) -> None:
        vals = {rel: src.get(key) for rel, src in sources.items()}
        if any(v is None for v in vals.values()):
            unread = ", ".join(r for r, v in vals.items() if v is None)
            errors.append((code, f"{label}: could not be read from {unread}"))
        elif len({json.dumps(v, sort_keys=True) for v in vals.values()}) != 1:
            errors.append((code, f"{label} disagree: " + "; ".join(f"{r} = {fmt(v)}" for r, v in vals.items())))
        else:
            ok.append(f"{label}: {fmt(next(iter(vals.values())))} agrees in gold SQL, pandas twin and generator")

    agree("constants-allowance", "plan allowances (requests / 28 days)", "allowance",
          lambda a: " / ".join(f"{k} {v:,}" for k, v in a.items()))
    agree("constants-cap-cut", "cap-cut multiplier", "cap_cut", str)
    agree("constants-t7", "as_of offset (T-N days before renewal)", "as_of_offset_days", lambda d: f"T-{d}")

    spec_tree = _tree(root, GRAPH_SPEC) if (root / GRAPH_SPEC).is_file() else None
    if spec_tree is not None:
        price = _const(spec_tree, "PLAN_PRICE_USD")
        if price != gen["price"]:
            msg = f"plan prices disagree: {GRAPH_SPEC} = {price}; {GENERATOR} = {gen['price']}"
            errors.append(("constants-price", msg))
        else:
            ok.append(f"plan prices: {price} agree in generator and graph spec")

    contract = _const(_tree(root, EXPORT_CONTRACT), "TRAIN_COLUMNS")
    export_cols = _const(_tree(root, EXPORT_JOB), "TRAIN_COLUMNS")
    twin_cols = ["user_id", "user_name", *(pdc["features"] or []), "churned"]
    lists = {EXPORT_CONTRACT: contract, EXPORT_JOB: export_cols, PANDAS_TWIN: twin_cols}
    if spec_tree is not None:
        lists[GRAPH_SPEC] = ["user_id", "user_name", *(_const(spec_tree, "GOLD_FEATURES") or []), "churned"]
    if len({json.dumps(v) for v in lists.values()}) != 1:
        errors.append(("columns-train", "the train contract columns differ between " + ", ".join(lists)))
    else:
        ok.append(f"train contract: the same {len(contract)} columns in {len(lists)} places "
                  f"({len(contract) - 3} features + user_id, user_name, churned)")

    churn_ok = 0
    for path in sorted((root / "src/jobs/churn").glob("*.py")):
        name, want = _app_name(ast.parse(path.read_text(encoding="utf-8"))), f"churn_{path.stem}"
        if name != want:
            errors.append(("appname-churn", f"{path.relative_to(root)}: appName {name!r} != {want!r}"))
        else:
            churn_ok += 1
    if churn_ok:
        ok.append(f"churn appName == churn_<stem> for {churn_ok} jobs")
    retail_ok = 0
    for path in sorted((root / "src/jobs/retail").glob("*.py")):
        name = _app_name(ast.parse(path.read_text(encoding="utf-8")))
        if name != path.stem:
            msg = f"{path.relative_to(root)}: appName {name!r} != file stem {path.stem!r} (job-name drift)"
            warnings.append(("appname-retail", msg))
        else:
            retail_ok += 1
    if retail_ok:
        ok.append(f"retail appName == <stem> for {retail_ok} jobs")

    leaky = _const(_tree(root, EXPORT_CONTRACT), "LEAKY") or set()
    dangling = sorted(set(leaky) - known_columns(root))
    for name in dangling:
        msg = (f"{EXPORT_CONTRACT}: LEAKY names '{name}', a column no churn dataset has "
               f"(bronze, silver, gold or export): drop it or add the column")
        warnings.append(("leaky-dangling", msg))
    if not dangling:
        ok.append(f"LEAKY: all {len(leaky)} names exist in some dataset")
    else:
        ok.append(f"LEAKY: {len(leaky) - len(dangling)} of {len(leaky)} names exist in some dataset")
    facts = {"allowance": sql["allowance"], "cap_cut": sql["cap_cut"], "as_of_offset_days": sql["as_of_offset_days"],
             "price": gen["price"], "train_columns": len(contract or []), "leaky": sorted(leaky)}
    return {"errors": errors, "warnings": warnings, "ok": ok, "facts": facts}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Tier-0 mini repo contract checks (constants, columns, appName, LEAKY).")
    ap.add_argument("--strict", action="store_true", help="warnings fail too")
    ap.add_argument("--json", action="store_true", help="print the result document as JSON")
    a = ap.parse_args(argv)
    strict, as_json = a.strict, a.json
    res = check(ROOT)
    if as_json:
        print(json.dumps(res, indent=1, default=list))
    else:
        print("Repo contracts (Tier-0 mini: constants, columns, appName, LEAKY)")
        for m in res["ok"]:
            print(f"  ok    {m}")
        for code, m in res["warnings"]:
            print(f"  WARN  [{code}] {m}")
        for code, m in res["errors"]:
            print(f"  FAIL  [{code}] {m}")
    n_err, n_warn = len(res["errors"]), len(res["warnings"])
    failed = n_err > 0 or (strict and n_warn > 0)
    out = sys.stderr if failed else sys.stdout
    if not as_json:
        print(f"Repo contracts {'FAILED' if failed else 'OK'}: {n_err} errors, {n_warn} warnings"
              f"{' (warnings are fatal with --strict)' if strict and n_warn and not n_err else ''}", file=out)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
