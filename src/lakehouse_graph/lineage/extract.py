"""Tier-0 static extraction: repo code -> plain facts (no graph yet, nothing executed).

Reads, with ``ast`` and regular expressions only (no pyspark import, no job is run):

  jobs / scripts    src/jobs/**/*.py, scripts/*.py: appName, tables read and written
                    (spark.table / writeTo / SQL strings), partition columns of a write,
                    CSV inputs, files written, SQL files executed (SQL_PATH, or SQL_DIR +
                    SQL_FILES), environment knobs, module constants, loop-built table names.
                    A read of ``global_temp.<view>`` in a job that creates global temp views
                    itself is a view of this Spark application, not a lakehouse table: it is
                    dropped (in a job that creates none it stays and is unresolved)
  DAGs              airflow/dags/*.py: dag ids, tasks, what each task runs, task order (a task's
                    arguments are bound to its factory's parameters when airflow/dags defines
                    the factory: spark_submit_task, spark_submit_args_task, graph_exec_task)
  Makefile          targets, prerequisites, scripts / shell pipelines / sub-makes they run
  shell             pipelines/*.sh, scripts/*.sh: job lists, scripts called
  CI                .github/workflows/ci.yml: steps that run a script, a make target or
                    clone another repo (`git clone` / `gh repo clone`: the repository and --branch
                    resolve from literals, from VAR=... assignments and `for` loops earlier in the
                    same run: block, from the env: of the step, job or workflow, or from a
                    ${VAR:-default}, every candidate kept: `if ...; then REF=a; else REF=b; fi` gives
                    both refs; an actions/checkout step with `repository:` is a clone too)
  contracts         scripts/check_churn_export.py (constants + every check with its guard),
                    scripts/check_gold_parity.py (columns, tolerance)
  README            the contract numbers it states (documentation assertions)

``SourceFiles`` records every file that is read, with its sha256: that set is what
``lineage_build_id`` hashes, so the id changes when any extracted file changes.

Robustness rule: a name the extractor recognises but cannot resolve is never dropped.
It is either a hard ``LineageExtractError`` (the churn pipeline itself is unreadable) or an
entry in ``facts["unresolved"]``, which the core lineage contract turns into an error.
"""
from __future__ import annotations

import ast
import fnmatch
import functools
import hashlib
import json
import logging
import re
import shlex
from pathlib import Path

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from . import spec
from .spec import LineageExtractError

logging.getLogger("sqlglot").setLevel(logging.ERROR)   # "falling back to Command" is handled below
WRITE_MODES = ("createOrReplace", "append", "create", "replace", "overwritePartitions", "overwrite")
GLOBAL_TEMP_DB = "global_temp"   # Spark's database of application-scoped global temp views
GLOBAL_VIEW_CALLS = ("createOrReplaceGlobalTempView", "createGlobalTempView")
# Calls that write a .csv / .json file: pandas to_csv(path), path.write_text(...), and the export job's
# helpers _write_csv(path, rows, columns) / _write_text(path, text), whose first argument is the export
# they produce (they write a staged sibling that the job moves onto it once every export is written).
FILE_WRITERS = ("_write_csv", "_write_text", "to_csv", "write_text")
TYPE_MAP = {"STRING": "string", "DATE": "date", "DOUBLE": "double", "INT": "int", "TIMESTAMP": "timestamp"}
REPO_TOKEN = "<repo>"
_ENV_DEFAULT = re.compile(r"\$\{\w+:-([^}]*)\}")
_PY_PATH = re.compile(r"(?<![\w./-])((?:scripts|src)/[\w./-]+\.py)((?:[ \t]+(?:--[\w-]+|[a-z][\w-]*))?)")
_SH_PATH = re.compile(r"(?<![\w.-])(?:\./)?((?:pipelines|scripts)/[\w./-]+\.sh)(?:[ \t]+\"?([\w./-]+)\"?)?")
_MODULE_RUN = re.compile(r"-m[ \t]+([a-z_][\w.]*)")
# `make [VAR=x | -flag ...] target` at a command position (line start, after && ; || ( or `run:`),
# optionally behind VAR=x prefixes: prose such as "make sure" in an echo is not a target.
MAKE_COMMAND = re.compile(r"(?:^|&&|\|\||;|\(|\brun:)[ \t]*(?:[A-Za-z_]\w*=\S+[ \t]+)*make[ \t]+"
                       r"(?:(?:[A-Za-z_]\w*=\S+|-[-\w]+)[ \t]+)*([a-z][\w-]*)", re.M)
SUB_MAKE = re.compile(r"\$\(MAKE\)(?:[ \t]+(?:--?[\w-]+|[A-Za-z_]\w*=\S+))*[ \t]+([a-z][\w-]*)")
_JOB_PATH = re.compile(r"(?:/opt/jobs/|run_job\.sh[ \t]+\"?)([a-z_]+/[\w.-]+\.py)")
# Shell of a CI `run:` block (see shell_clones): `NAME=value` and `for NAME in words` at a command
# position (line start, after ; & | ( or then / else / do, optionally behind export), and the
# `git [-C dir] clone ...` / `gh repo clone ...` commands.
_CMD_START = r"(?:^|[;&|(]|\b(?:then|else|do)\b|\brun:)[ \t]*"
# a command substitution: $( ... ) (two levels of nested parentheses, so $(( arithmetic )) too) or ` ... `
_CMD_SUBST = r"\$\((?:[^()]|\((?:[^()]|\([^()]*\))*\))*\)|`[^`]*`"
# (a value is quoted text, a GitHub `${{ expression }}` (a run-time value, kept as written), a command
# substitution (kept as written: only the step knows its output) or plain characters)
_SH_ASSIGN = re.compile(_CMD_START + r"(?:(?:export|readonly|local|declare)[ \t]+)?([A-Za-z_]\w*)="
                        r"((?:\"(?:[^\"\\]|\\.)*\"|'[^']*'|\$\{\{.*?\}\}|" + _CMD_SUBST +
                        r"|[^\s;&|()<>\"'])*)", re.M)
# `for NAME in w1 w2 ...` (every word is a candidate value of NAME; no `in` = the script's arguments)
_SH_FOR = re.compile(_CMD_START + r"for[ \t]+([A-Za-z_]\w*)(?:[ \t]+in\b([^;\n]*))?", re.M)
_GIT_CLONE = re.compile(_CMD_START + r"git(?:[ \t]+-[\w-]+(?:[ \t]+[^\s-]\S*)?)*?[ \t]+clone\b([^\n;&|]*)", re.M)
# GitHub CLI: gh repo clone <[HOST/]OWNER/REPO | URL> [<directory>] [-- <git clone flags>...]
_GH_CLONE = re.compile(_CMD_START + r"gh[ \t]+repo[ \t]+clone\b([^\n;&|]*)", re.M)
_GH_VALUE_OPTS = frozenset({"-u", "--upstream-remote-name"})
_SH_REF = re.compile(r"\$\{(\w+)((?::?[-=+?])(?:[^{}]|\{[^{}]*\})*)?\}|\$(\w+)")
# git clone options that take a value (the next word, or --opt=value); every other option is a flag
_CLONE_VALUE_OPTS = frozenset({"--depth", "--branch", "-b", "--origin", "-o", "--config", "-c", "--reference",
                               "--reference-if-able", "--separate-git-dir", "--filter", "--jobs", "-j", "--template",
                               "--shallow-since", "--shallow-exclude", "--upload-pack", "-u", "--server-option",
                               "--bundle-uri", "--revision"})
_REMOTE_URL = re.compile(r"^(?:(?:https?|ssh|git)://[^\s$'\"]+|[\w.-]+@[\w.-]+:[\w./~-]+)$")
_GH_REPO = re.compile(r"^(?:([\w-]+(?:\.[\w-]+)+)/)?([\w.-]+)/([\w.-]+)$")   # [HOST/]OWNER/REPO
_RUNTIME_TEXT = re.compile(r"\$\(|`")    # a command substitution left in an expanded word (``\x00`` = a literal $)
# Variables GitHub Actions sets on every runner: a ref built from them is a run-time value, not unresolved.
CI_RUNTIME_VARS = re.compile(r"^(?:GITHUB_\w+|RUNNER_\w+|CI)$")
MAX_SHELL_CANDIDATES = 16


# --------------------------------------------------------------------------- files
class SourceFiles:
    """Read-only view of the repo that remembers every file it read (path -> sha256)."""

    def __init__(self, repo: str | Path):
        self.repo = Path(repo)
        self.read: dict[str, str] = {}

    def exists(self, rel: str) -> bool:
        return (self.repo / rel).is_file()

    def text(self, rel: str) -> str:
        path = self.repo / rel
        try:
            data = path.read_bytes()
        except OSError as e:
            raise LineageExtractError(f"{rel}: cannot be read ({type(e).__name__}: {e})") from e
        self.read[rel] = hashlib.sha256(data).hexdigest()
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError as e:
            raise LineageExtractError(f"{rel}: is not UTF-8 text ({e})") from e

    def tree(self, rel: str) -> ast.Module:
        try:
            return ast.parse(self.text(rel), filename=rel)
        except SyntaxError as e:
            raise LineageExtractError(f"{rel}: does not parse as Python ({e})") from e

    def glob(self, pattern: str) -> list[str]:
        return sorted(str(p.relative_to(self.repo)) for p in self.repo.glob(pattern)
                      if p.is_file() and "__pycache__" not in p.parts)

    def note(self, rel: str, path: str | Path) -> None:
        """Record a file that was read through another channel (an imported module)."""
        self.read[rel] = hashlib.sha256(Path(path).read_bytes()).hexdigest()


def canonical_path(p: str) -> str:
    """``${CHURN_EXPORT_DIR:-/opt/data/export}/x.csv`` -> ``data/export/x.csv`` (repo-relative)."""
    p = _ENV_DEFAULT.sub(r"\1", p).replace(f"{REPO_TOKEN}/", "")
    for mount, rel in spec.MOUNTS.items():
        if p.startswith(mount):
            p = rel + p[len(mount):]
    return p


# --------------------------------------------------------------------------- tiny evaluator
class Env:
    """Module constants, f-strings and simple path arithmetic of one Python file.

    ``os.environ.get("X", d)`` evaluates to ``${X:-d}`` and is remembered as a knob.
    ``ev`` returns None when a name is unknown; ``pattern`` returns the same string with
    ``*`` for the unknown parts (so ``f"lakehouse.gold.graph_{name}"`` stays usable).
    """

    def __init__(self, tree: ast.Module):
        self.vals: dict[str, object] = {}
        self.knobs: dict[str, str] = {}
        self.funcs = {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}
        for n in tree.body:
            if isinstance(n, ast.Assign) and len(n.targets) == 1 and isinstance(n.targets[0], ast.Name):
                v = self.ev(n.value)
                if v is not None:
                    self.vals[n.targets[0].id] = v
        self.locals: dict[str, object] = {}
        self.local_patterns: dict[str, str] = {}   # path = some_dir / "x.csv" -> "*/x.csv"
        for fn in self.funcs.values():
            for n in ast.walk(fn):
                if isinstance(n, ast.Assign) and len(n.targets) == 1 and isinstance(n.targets[0], ast.Name) \
                        and n.targets[0].id not in self.vals:
                    v = self.ev(n.value)
                    if v is not None:
                        self.locals[n.targets[0].id] = v
                    elif (p := self.pattern(n.value)) is not None:
                        self.local_patterns[n.targets[0].id] = p

    def ev(self, node, local: dict | None = None, wild: bool = False):
        local = local or {}
        unknown = "*" if wild else None
        try:
            return ast.literal_eval(node)
        except (ValueError, TypeError, SyntaxError, MemoryError, RecursionError):
            pass
        if isinstance(node, ast.Name):
            if node.id in local:
                return local[node.id]
            return self.vals.get(node.id, getattr(self, "locals", {}).get(node.id, unknown))
        if isinstance(node, ast.Call):
            f = ast.unparse(node.func)
            if f in ("os.environ.get", "os.getenv") and node.args and isinstance(node.args[0], ast.Constant):
                var = node.args[0].value
                d = self.ev(node.args[1], local, wild) if len(node.args) > 1 else ""
                self.knobs[var] = "" if d is None else str(d)
                return f"${{{var}:-{'' if d is None else d}}}"
            if f in ("Path", "str", "int", "float") and node.args:
                return self.ev(node.args[0], local, wild)
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
            a, b = self.ev(node.left, local, wild), self.ev(node.right, local, wild)
            if a is not None and b is not None:
                return f"{a}/{b}"
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            a, b = self.ev(node.left, local, wild), self.ev(node.right, local, wild)
            if isinstance(a, list) and isinstance(b, list):
                return a + b
            if isinstance(a, str) and isinstance(b, str):
                return a + b
        if isinstance(node, ast.Attribute) and node.attr.isupper():
            # other_module.CONSTANT: resolved by the caller through ``local`` (qualified name)
            return local.get(ast.unparse(node), unknown)
        if isinstance(node, ast.List):
            out: list = []
            for e in node.elts:
                if isinstance(e, ast.Starred):
                    inner = self.ev(e.value, local, wild)
                    if not isinstance(inner, list):
                        return unknown
                    out += inner
                else:
                    out.append(self.ev(e, local, wild))
            return out
        if isinstance(node, ast.JoinedStr):
            parts = []
            for v in node.values:
                if isinstance(v, ast.Constant):
                    parts.append(str(v.value))
                else:
                    x = self.ev(v.value, local, wild)
                    if x is None:
                        return None
                    parts.append(str(x))
            return "".join(parts)
        text = ast.unparse(node)
        if text.startswith("Path(__file__).resolve().parents["):
            return REPO_TOKEN
        return unknown

    def pattern(self, node, local: dict | None = None) -> str | None:
        """Like ``ev`` but unknown parts become ``*``; None when nothing literal is left."""
        v = self.ev(node, local, wild=True)
        if not isinstance(v, str) or not v.replace("*", "").strip("/. "):
            return None
        return v


def module_const(tree: ast.Module, name: str):
    """Value of a module-level ``NAME = <literal>`` assignment (None if absent / not literal)."""
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name for t in node.targets):
            try:
                return ast.literal_eval(node.value)
            except (ValueError, TypeError, SyntaxError):
                return None
    return None


def _timedelta_days(node: ast.AST) -> int | None:
    for n in ast.walk(node):
        if isinstance(n, ast.Call) and getattr(n.func, "attr", getattr(n.func, "id", "")) == "Timedelta":
            for kw in n.keywords:
                if kw.arg == "days" and isinstance(kw.value, ast.Constant):
                    return int(kw.value.value)
    return None


def sql_tables(sql: str) -> tuple[set[str], set[tuple[str, str]], dict[str, list]] | None:
    """(reads, writes, ddl) of ``lakehouse.*`` tables in a SQL string; None when it does not
    parse. ``ddl`` maps a created table to its [(column, type)] when the statement declares them."""
    reads: set[str] = set()
    writes: set[tuple[str, str]] = set()
    ddl: dict[str, list] = {}
    try:
        statements = sqlglot.parse(sql, read="spark")
    except SqlglotError:
        return None
    for st in statements:
        if st is None:
            continue
        if isinstance(st, exp.Command):   # unsupported syntax (ALTER TABLE x CREATE TAG ...): names only
            verb = str(st.this).upper()
            for name in re.findall(r"\blakehouse\.(?!system\.)\w+\.\w+", sql if len(statements) == 1 else st.sql()):
                if verb in ("ALTER", "DROP", "TRUNCATE", "MERGE", "UPDATE"):
                    writes.add((name, verb))
                else:
                    reads.add(name)
            continue
        target = None
        if isinstance(st, (exp.Create, exp.Insert, exp.Delete)):
            if isinstance(st, exp.Create) and str(st.args.get("kind", "")).upper() in ("SCHEMA", "DATABASE",
                                                                                         "NAMESPACE"):
                continue
            t = st.this if isinstance(st.this, exp.Table) else st.find(exp.Table)
            if t is not None:
                target = ".".join(p for p in (t.catalog, t.db, t.name) if p)
                writes.add((target, type(st).__name__.upper()))
            if isinstance(st, exp.Create) and isinstance(st.this, exp.Schema) and target:
                ddl[target] = [(c.name, c.args["kind"].sql("spark").lower()) for c in st.this.expressions
                               if isinstance(c, exp.ColumnDef) and c.args.get("kind") is not None]
        ctes = {c.alias for c in st.find_all(exp.CTE)}
        for t in st.find_all(exp.Table):
            parts = [p.name for p in t.parts]
            name = ".".join(parts[:3])
            meta = parts[3] if len(parts) > 3 else None
            if t.args.get("version") is not None:
                meta = "VERSION AS OF"
            if name != target and t.name not in ctes and name.startswith("lakehouse."):
                reads.add(name + (f"#{meta}" if meta else ""))
    return reads, writes, ddl


# --------------------------------------------------------------------------- jobs and scripts
def extract_job(files: SourceFiles, rel: str) -> dict:
    """Static facts about one Python job or script (see the module docstring)."""
    tree = files.tree(rel)
    env = Env(tree)
    strict = rel.startswith("src/jobs/")   # Spark jobs: an unresolved table / file name is an error
    job: dict = {"path": rel, "doc": (ast.get_docstring(tree) or "").split("\n")[0], "app_name": None,
                 "reads": [], "writes": [], "csv_in": [], "files_out": [], "sql_file": None, "sql_substitution": None,
                 "columns_added": [], "knobs": {}, "silver_map": None, "bronze_map": None, "row_filters": {},
                 "consts": {}, "imports": [], "file_refs": [], "unresolved": [], "ddl": {}, "records": {},
                 "kw_calls": [], "partitions": {}, "global_views": [], "sql_files": []}
    sql_reads: list[str] = []

    def add(lst: list, item) -> None:
        if item not in lst:
            lst.append(item)

    def unresolved(node: ast.AST, what: str) -> None:
        if strict:
            add(job["unresolved"], {"where": f"{rel}:{getattr(node, 'lineno', '?')}", "what": what})

    # loop bindings: for k, (a, b) in D.items() / for k, v in fn(...).items()
    loop_binds: list[tuple[ast.For, list[dict]]] = []
    for n in ast.walk(tree):
        if not (isinstance(n, ast.For) and isinstance(n.iter, ast.Call) and isinstance(n.iter.func, ast.Attribute)
                and n.iter.func.attr == "items"):
            continue
        base = n.iter.func.value
        d = env.ev(base) if isinstance(base, ast.Name) else None
        if isinstance(d, dict):
            items = list(d.items())
        else:
            items = None
            if isinstance(base, ast.Name):   # local = fn(...): keys of the dict literal fn returns
                for a in ast.walk(tree):
                    if isinstance(a, ast.Assign) and isinstance(a.targets[0], ast.Name) and a.targets[0].id == base.id \
                            and isinstance(a.value, ast.Call) and isinstance(a.value.func, ast.Name) \
                            and a.value.func.id in env.funcs:
                        ret = [r for r in ast.walk(env.funcs[a.value.func.id]) if isinstance(r, ast.Return)]
                        if ret and isinstance(ret[0].value, ast.Dict) and all(
                                isinstance(k, ast.Constant) for k in ret[0].value.keys):
                            items = [(k.value, None) for k in ret[0].value.keys]
            if items is None:
                continue
        binds = []
        for k, v in items:
            b: dict = {}
            if isinstance(n.target, ast.Tuple) and n.target.elts and isinstance(n.target.elts[0], ast.Name):
                b[n.target.elts[0].id] = k
                if len(n.target.elts) > 1:
                    second = n.target.elts[1]
                    if isinstance(second, ast.Tuple) and isinstance(v, tuple):
                        b.update({e.id: vv for e, vv in zip(second.elts, v, strict=False) if isinstance(e, ast.Name)})
                    elif isinstance(second, ast.Name) and v is not None:
                        b[second.id] = v
            binds.append(b)
        loop_binds.append((n, binds))

    def bindings_for(node: ast.AST) -> list[dict]:
        for loop, binds in loop_binds:
            if any(x is node for x in ast.walk(loop)):
                return binds
        return [{}]

    # reader-function pattern (02 silver_tables(b)): the parameter is called as b("bronze_table")
    lambda_calls: set[int] = set()
    for fname, fn in env.funcs.items():
        ret = [r for r in ast.walk(fn) if isinstance(r, ast.Return) and isinstance(r.value, ast.Dict)]
        if not ret or not fn.args.args:
            continue
        param = fn.args.args[0].arg
        smap: dict[str, dict] = {}
        for k, v in zip(ret[0].value.keys, ret[0].value.values, strict=True):
            calls = [c for c in ast.walk(v) if isinstance(c, ast.Call)]
            bronze = next((c.args[0].value for c in calls if isinstance(c.func, ast.Name) and c.func.id == param
                           and c.args and isinstance(c.args[0], ast.Constant)), None)
            if not isinstance(k, ast.Constant) or bronze is None:
                smap = {}
                break
            withs = {c.args[0].value: ast.unparse(c.args[1]) for c in calls
                     if isinstance(c.func, ast.Attribute) and c.func.attr == "withColumn" and len(c.args) > 1
                     and isinstance(c.args[0], ast.Constant)}
            filt = [ast.unparse(c.args[0]) for c in calls
                    if isinstance(c.func, ast.Attribute) and c.func.attr == "filter" and c.args]
            dedupe = next((env.ev(c.args[1]) for c in calls
                           if isinstance(c.func, ast.Name) and c.func.id == "latest" and len(c.args) > 1), None)
            smap[k.value] = {"bronze": bronze, "with": withs, "filter": filt, "dedupe_keys": dedupe}
        if not smap:
            continue
        job["silver_map"] = smap
        for c in ast.walk(tree):   # fn(lambda t: spark.table(f"lakehouse.bronze.{t}"))
            if isinstance(c, ast.Call) and isinstance(c.func, ast.Name) and c.func.id == fname and c.args \
                    and isinstance(c.args[0], ast.Lambda):
                lam = c.args[0]
                arg = lam.args.args[0].arg
                for tc in ast.walk(lam.body):
                    if isinstance(tc, ast.Call) and isinstance(tc.func, ast.Attribute) and tc.func.attr == "table":
                        lambda_calls.add(id(tc))
                        for info in smap.values():
                            name = env.ev(tc.args[0], {arg: info["bronze"]})
                            if name is None:
                                unresolved(tc, f"table name {ast.unparse(tc.args[0])}")
                            else:
                                add(job["reads"], (name, "spark.table", "input"))

    # df.writeTo(name).using("iceberg").partitionedBy(F.col("label")).createOrReplace(): the last call
    # of the chain is the mode; partitionedBy("x") / partitionedBy(F.col("x")) names the partition columns
    write_mode: dict[int, str] = {}
    partitioned: dict[int, list[str]] = {}
    for n in ast.walk(tree):
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr in WRITE_MODES:
            inner, cols = n.func.value, None
            while isinstance(inner, ast.Call) and isinstance(inner.func, ast.Attribute):
                if inner.func.attr == "partitionedBy":
                    cols = [_column_name(a) or ast.unparse(a) for a in inner.args]
                if inner.func.attr == "writeTo":
                    write_mode[id(inner)] = n.func.attr
                    if cols:
                        partitioned[id(inner)] = cols
                    break
                inner = inner.func.value

    def named(node: ast.AST, arg: ast.AST, binds: dict, what: str) -> str | None:
        """A table / path name: the evaluated value, else a ``*`` pattern, else unresolved."""
        v = env.ev(arg, binds)
        if isinstance(v, str):
            return v
        p = env.pattern(arg, binds)
        if p is None:
            unresolved(node, f"{what} {ast.unparse(arg)}")
        return p

    for n in ast.walk(tree):
        if not isinstance(n, ast.Call):
            continue
        f = n.func
        attr = f.attr if isinstance(f, ast.Attribute) else (f.id if isinstance(f, ast.Name) else None)
        owner = ast.unparse(f.value) if isinstance(f, ast.Attribute) else ""
        for b in bindings_for(n):
            if attr == "appName" and n.args:
                job["app_name"] = env.ev(n.args[0], b)
            elif attr == "table" and owner == "spark" and n.args and id(n) not in lambda_calls:
                v = named(n, n.args[0], b, "table name")
                if v:
                    add(job["reads"], (v, "spark.table", "input"))
            elif attr == "writeTo" and n.args:
                v = named(n, n.args[0], b, "table name")
                if v:
                    add(job["writes"], (v, write_mode.get(id(n), "writeTo")))
                    if id(n) in partitioned:
                        job["partitions"][v] = partitioned[id(n)]
            elif attr in GLOBAL_VIEW_CALLS and n.args:
                v = env.ev(n.args[0], b)
                add(job["global_views"], v if isinstance(v, str) else env.pattern(n.args[0], b) or "*")
            elif attr in ("csv", "parquet", "json") and "read" in owner and n.args:
                v = env.ev(n.args[0], b)
                if v is None:
                    v = env.pattern(n.args[0], b)
                    if v is None:
                        unresolved(n, f"input path {ast.unparse(n.args[0])}")
                for p in (v if isinstance(v, list) else [v]):
                    if p:
                        add(job["csv_in"], p)
            elif attr == "withColumn" and n.args and isinstance(n.args[0], ast.Constant) and len(n.args) > 1:
                add(job["columns_added"], (n.args[0].value, ast.unparse(n.args[1])))
            elif attr == "sql" and owner == "spark" and n.args:
                a = n.args[0]
                if isinstance(a, ast.Call):   # spark.sql(gold_sql()): the SQL file, handled below
                    continue
                if isinstance(a, ast.JoinedStr):   # literal values interpolated into the statement
                    text = "".join(str(v.value) if isinstance(v, ast.Constant) else "0" for v in a.values)
                elif isinstance(a, ast.Constant) and isinstance(a.value, str):
                    text = a.value
                else:
                    unresolved(n, f"SQL statement {ast.unparse(a)[:60]}")
                    continue
                tables = sql_tables(text)
                if tables is None:
                    unresolved(n, f"SQL statement does not parse: {text.strip()[:60]}")
                    continue
                for t in sorted(tables[0]):
                    add(sql_reads, t)
                for t, kind in sorted(tables[1]):
                    add(job["writes"], (t, f"sql:{kind}"))
                job["ddl"].update(tables[2])
            elif attr in FILE_WRITERS:
                target = n.args[0] if attr != "write_text" and n.args else (
                    f.value if attr == "write_text" and isinstance(f, ast.Attribute) else None)
                if target is None:
                    continue
                p = env.ev(target, b)
                if not isinstance(p, str) or not p.rsplit("/", 1)[-1].endswith((".csv", ".json")):
                    continue
                cols = env.ev(n.args[2]) if attr == "_write_csv" and len(n.args) > 2 else None
                rows = ast.unparse(n.args[1]) if attr == "_write_csv" and len(n.args) > 1 else None
                add(job["files_out"], (p, attr, rows, tuple(cols) if isinstance(cols, list) else None))
            elif isinstance(f, ast.Name) and f.id == "_read" and n.args:   # pandas twin: _read("x.csv", dates)
                base, name = env.vals.get("SAMPLE"), env.ev(n.args[0])
                if name:
                    add(job["csv_in"], f"{base}/{name}" if base else name)
            elif isinstance(f, ast.Name) and f.id == "_load" and n.args:   # importlib load of another job by path
                v = env.ev(n.args[0])
                if isinstance(v, str):
                    add(job["imports"], v.replace(f"{REPO_TOKEN}/", ""))
            elif isinstance(f, ast.Attribute) and f.attr.endswith("_sql") and n.keywords and all(
                    isinstance(k.value, ast.Constant) and isinstance(k.value.value, str) for k in n.keywords):
                add(job["kw_calls"], (f.attr, {k.arg: k.value.value for k in n.keywords}))   # gold_sql(silver=...)

    # every `<dir> / "name.csv|json"` mention; the assembler treats the ones not written as reads
    for b in ast.walk(tree):
        if isinstance(b, ast.BinOp) and isinstance(b.op, ast.Div) and isinstance(b.right, ast.Constant) \
                and str(b.right.value).endswith((".csv", ".json")):
            v = env.ev(b)
            add(job["file_refs"], v if isinstance(v, str) else f"*/{b.right.value}")
    # row filters of comprehension-built lists (04: train / hero)
    for a in ast.walk(tree):
        if isinstance(a, ast.Assign) and isinstance(a.targets[0], ast.Name) and isinstance(a.value, ast.ListComp):
            ifs = [ast.unparse(i) for g in a.value.generators for i in g.ifs]
            if ifs:
                job["row_filters"][a.targets[0].id] = " and ".join(ifs)
    # record = {c: ... for c in TRAIN_COLUMNS if c != "churned"}: the keys of a written JSON record
    for a in ast.walk(tree):
        if isinstance(a, ast.Assign) and isinstance(a.targets[0], ast.Name) and isinstance(a.value, ast.DictComp) \
                and len(a.value.generators) == 1:
            gen = a.value.generators[0]
            keys = env.ev(gen.iter)
            if not isinstance(keys, list) or not isinstance(gen.target, ast.Name):
                continue
            simple = True
            for cond in gen.ifs:
                if isinstance(cond, ast.Compare) and len(cond.ops) == 1 and isinstance(cond.left, ast.Name) \
                        and cond.left.id == gen.target.id and isinstance(cond.comparators[0], ast.Constant) \
                        and isinstance(cond.ops[0], (ast.NotEq, ast.Eq)):
                    want = cond.comparators[0].value
                    keys = [k for k in keys if (k != want) == isinstance(cond.ops[0], ast.NotEq)]
                else:
                    simple = False
            if simple:
                job["records"][a.targets[0].id] = keys
    # a SQL file executed through string.Template (03)
    if isinstance(env.vals.get("SQL_PATH"), str):
        job["sql_file"] = env.vals["SQL_PATH"]
        for fn in env.funcs.values():
            with_default = fn.args.args[-len(fn.args.defaults):] if fn.args.defaults else []
            for arg, d in zip(with_default, fn.args.defaults, strict=True):
                if isinstance(d, ast.Constant) and str(d.value).startswith("lakehouse."):
                    job["sql_substitution"] = {arg.arg: d.value}
    # a directory of SQL files executed section by section (the Spark graph job: SQL_DIR + SQL_FILES)
    sql_dir, sql_names = env.vals.get("SQL_DIR"), env.vals.get("SQL_FILES")
    if isinstance(sql_dir, str) and isinstance(sql_names, (list, tuple)):
        job["sql_files"] = [f"{sql_dir}/{name}" for name in sql_names if isinstance(name, str)]
    job["knobs"] = dict(env.knobs)
    job["consts"] = {k: v for k, v in env.vals.items() if k.isupper()}
    if isinstance(env.vals.get("BRONZE"), dict):
        job["bronze_map"] = env.vals["BRONZE"]
    # global_temp.<view>: a view this Spark application binds itself (to a pinned snapshot, a scratch
    # Parquet re-read, ...), whatever lakehouse table it was made from: not a dataset read
    if job["global_views"]:
        job["reads"] = [r for r in job["reads"] if not r[0].startswith(GLOBAL_TEMP_DB + ".")]
    # SQL reads and spark.table reads of a table the job itself writes are verification reads
    written = {w[0] for w in job["writes"]}
    for t in sql_reads:
        add(job["reads"], (t, "spark.sql", "verify" if t in written else "input"))
    for r in list(job["reads"]):
        if r[2] == "input" and r[0] in written and r[1] == "spark.table":
            job["reads"].remove(r)
            add(job["reads"], (r[0], r[1], "verify"))
    return job


def _column_name(node: ast.AST) -> str | None:
    """``"label"`` / ``F.col("label")`` / ``col("label")`` -> ``label``."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Call) and node.args and isinstance(node.args[0], ast.Constant) and (
            (isinstance(node.func, ast.Attribute) and node.func.attr == "col")
            or (isinstance(node.func, ast.Name) and node.func.id == "col")):
        return str(node.args[0].value)
    return None


def job_domain(rel: str) -> str:
    name = rel.lower()
    if "graph" in name or "lineage" in name:
        return "graph"
    if "churn" in name or "gold_parity" in name:
        return "churn"
    if "retail" in name:
        return "retail"
    return "shared"


def job_kind(rel: str) -> str:
    stem = Path(rel).stem
    if rel.startswith("src/jobs/"):
        return "spark"
    if stem.startswith("check_"):
        return "check"
    if stem.startswith("generate_"):
        return "generator"
    if rel.startswith("scripts/"):
        return "script"
    return "module"


# --------------------------------------------------------------------------- bronze / silver schema
def bronze_tables(job01: dict) -> dict[str, dict]:
    """bronze table -> {"csv": file, "columns": [(name, type)]} from 01_ingest_bronze.BRONZE."""
    raw = job01.get("bronze_map")
    if not isinstance(raw, dict) or not raw:
        raise LineageExtractError(f"{spec.BRONZE_JOB}: the BRONZE dict (table -> (csv, 'col TYPE, ...')) is "
                                  f"missing or is no longer a literal")
    out = {}
    for table, value in raw.items():
        try:
            csv_name, schema = value
            cols = [(c.split()[0], TYPE_MAP[c.split()[1].upper()]) for c in schema.split(",") if c.strip()]
        except (ValueError, KeyError, IndexError, AttributeError) as e:
            raise LineageExtractError(f"{spec.BRONZE_JOB}: BRONZE[{table!r}] is not (csv, 'col TYPE, ...') with "
                                      f"known types ({type(e).__name__}: {e})") from e
        out[table] = {"csv": csv_name, "columns": cols}
    return out


def silver_tables(job02: dict, bronze: dict[str, dict]) -> dict[str, dict]:
    """silver table -> {"bronze", "columns": [(name, type)], "derived": {col: (expr, [bronze cols])}, ...}."""
    smap = job02.get("silver_map")
    if not smap:
        raise LineageExtractError(f"{spec.SILVER_JOB}: silver_tables(b) must return a dict literal whose values "
                                  f"read b('<bronze table>')")
    out = {}
    for name, info in smap.items():
        if info["bronze"] not in bronze:
            raise LineageExtractError(f"{spec.SILVER_JOB}: silver table {name} reads bronze table "
                                      f"{info['bronze']!r}, which {spec.BRONZE_JOB} does not define")
        cols = list(bronze[info["bronze"]]["columns"])
        known = {c for c, _ in cols}
        derived = {}
        for col, expr in info["with"].items():
            sources = [s for s in re.findall(r"'(\w+)'", expr) if s in known]
            if not sources:
                raise LineageExtractError(f"{spec.SILVER_JOB}: withColumn({col!r}, {expr}) reads no column of "
                                          f"{info['bronze']}")
            derived[col] = (expr, sources)
            if col not in known:   # a new column (hit_date <- to_date(hit_at))
                cols.append((col, "date" if "to_date" in expr else "string"))
        out[name] = {"bronze": info["bronze"], "columns": cols, "derived": derived, "filter": info["filter"],
                     "dedupe_keys": info["dedupe_keys"]}
    return out


def silver_schema(silver: dict[str, dict]) -> dict:
    """sqlglot schema mapping for the gold SQL: {catalog: {db: {table: {column: type}}}}."""
    catalog, db = spec.SILVER_NAMESPACE.split(".")
    return {catalog: {db: {t: dict(info["columns"]) for t, info in silver.items()}}}


# --------------------------------------------------------------------------- DAGs
def dag_task_factories(files: SourceFiles) -> dict[str, list[str]]:
    """Positional parameter names of every function a file in airflow/dags defines: the task
    helpers (lakehouse_operators.spark_submit_task -> [task_id, job_path],
    lakehouse_graph_operators.graph_exec_task -> [task_id, command, env], ...)."""
    out: dict[str, list[str]] = {}
    for rel in files.glob(spec.DAG_GLOB):
        for n in files.tree(rel).body:
            if isinstance(n, ast.FunctionDef):
                out.setdefault(n.name, [a.arg for a in n.args.posonlyargs + n.args.args])
    return out


def command_text(value) -> str | None:
    """A task argument as command text: a string as is, an argv list / tuple of strings joined
    the way a shell reads it (``["python", "scripts/x.py", "build"]`` -> ``python scripts/x.py build``)."""
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)) and value and all(isinstance(v, str) for v in value):
        return shlex.join(value)
    return None


def spark_job_path(job: str) -> str:
    """The repo file a spark-submit helper runs: ``churn/01_x.py``, ``jobs/churn/01_x.py`` and
    ``/opt/jobs/churn/01_x.py`` all name ``src/jobs/churn/01_x.py`` (the helpers strip the same prefixes)."""
    job = job.lstrip("/")
    for prefix in ("opt/jobs/", "jobs/"):
        if job.startswith(prefix):
            job = job[len(prefix):]
    return f"src/jobs/{job}"


def extract_dags(files: SourceFiles) -> tuple[list[dict], list[dict]]:
    """(dags, unresolved). A task is ``name = factory("task_id", ...)`` inside the file.

    The arguments of a task are bound to the factory's parameter names when a file in
    airflow/dags defines the factory (``params``: name -> command text; unknown factories bind
    positional arguments as ``arg0``, ``arg1``, ...). An argument is a string, an argv list / tuple
    of strings or a module constant of the DAG file holding one; anything else is not evaluated.
    """
    dags, unresolved = [], []
    factories = dag_task_factories(files)
    for rel in files.glob(spec.DAG_GLOB):
        tree = files.tree(rel)
        env = Env(tree)
        dag_id, tags, tasks, edges = None, [], {}, []
        for n in ast.walk(tree):
            if isinstance(n, ast.Call) and ast.unparse(n.func) == "DAG":
                for kw in n.keywords:
                    if kw.arg == "dag_id" and isinstance(kw.value, ast.Constant):
                        dag_id = kw.value.value
                    if kw.arg == "tags":
                        try:
                            tags = list(ast.literal_eval(kw.value))
                        except (ValueError, TypeError):
                            tags = []
        if dag_id is None:
            continue   # a helper module (operators), not a DAG definition
        for n in ast.walk(tree):
            if isinstance(n, ast.Assign) and len(n.targets) == 1 and isinstance(n.targets[0], ast.Name) \
                    and isinstance(n.value, ast.Call) and ast.unparse(n.value.func) != "DAG":
                call = n.value
                factory = ast.unparse(call.func)
                names = factories.get(factory.rsplit(".", 1)[-1], [])
                bound = [(names[i] if i < len(names) else f"arg{i}", a) for i, a in enumerate(call.args)
                         if not isinstance(a, ast.Starred)]
                bound += [(k.arg, k.value) for k in call.keywords if k.arg]
                params = {name: text for name, a in bound if (text := command_text(env.ev(a))) is not None}
                literal = [a.value for a in call.args if isinstance(a, ast.Constant) and isinstance(a.value, str)]
                task_id = params.get("task_id") or (literal[0] if literal else None)
                if task_id is None:
                    continue
                params.pop("task_id", None)
                if "task_id" not in names and literal and params.get("arg0") == task_id:
                    params.pop("arg0")   # an unknown factory's first positional string is its task id
                tasks[n.targets[0].id] = {"task_id": task_id, "factory": factory, "factory_params": names,
                                          "params": params, "args": list(params.values()), "lineno": n.lineno}
            if isinstance(n, ast.Expr) and isinstance(n.value, ast.BinOp) and isinstance(n.value.op, ast.RShift):
                chain: list[list[str]] = []

                def flat(b, chain=chain, rel=rel):
                    if isinstance(b, ast.BinOp) and isinstance(b.op, ast.RShift):
                        flat(b.left)
                        flat(b.right)
                    elif isinstance(b, ast.Name):
                        chain.append([b.id])
                    elif isinstance(b, (ast.List, ast.Tuple)) and all(isinstance(e, ast.Name) for e in b.elts):
                        chain.append([e.id for e in b.elts])
                    else:
                        unresolved.append({"where": f"{rel}:{b.lineno}",
                                           "what": f"task dependency operand {ast.unparse(b)[:60]}"})
                        chain.append([])
                flat(n.value)
                for left, right in zip(chain, chain[1:], strict=False):
                    edges += [(a, b) for a in left for b in right]
        for a, b in edges:
            for var in (a, b):
                if var not in tasks:
                    unresolved.append({"where": rel, "what": f"task variable {var} in a >> chain is not a task"})
        dags.append({"file": rel, "dag_id": dag_id, "tags": tags, "tasks": tasks,
                     "edges": [(tasks[a]["task_id"], tasks[b]["task_id"]) for a, b in edges
                               if a in tasks and b in tasks]})
    return dags, unresolved


# --------------------------------------------------------------------------- Makefile, shell, CI
def command_runs(text: str) -> dict:
    """Scripts, shell pipelines, python modules and job paths a command text runs."""
    return {"py": [(p, a.strip() or None) for p, a in _PY_PATH.findall(text)],
            "sh": [(p, a or None) for p, a in _SH_PATH.findall(text)],
            "modules": _MODULE_RUN.findall(text),
            "jobs": _JOB_PATH.findall(text)}


def extract_make(files: SourceFiles) -> dict[str, dict]:
    text = files.text(spec.MAKEFILE)
    logical = re.sub(r"\\\n", " ", text)
    phony: set[str] = set()
    for line in logical.splitlines():
        if line.startswith(".PHONY:"):
            phony |= set(line.split(":", 1)[1].split())
    targets: dict[str, dict] = {}
    help_text: dict[str, str] = {}
    cur = None
    for line in logical.splitlines():
        m = re.match(r"^([a-zA-Z0-9_.-]+):(?!=)\s*(.*)$", line)
        if m and m.group(1) != ".PHONY":
            cur = m.group(1)
            targets[cur] = {"prereqs": m.group(2).split(), "recipe": []}
        elif line.startswith("\t") and cur:
            targets[cur]["recipe"].append(line.strip())
            h = re.match(r'@echo "\s+make ([a-z0-9-]+)\s+(.*)"', line.strip())
            if h:
                help_text.setdefault(h.group(1), h.group(2).strip())
        elif line.strip() and not line.startswith("#"):
            cur = None
    for name, d in targets.items():
        recipe = " ; ".join(d["recipe"])
        d.update(command_runs(recipe))
        d["phony"] = name in phony
        d["help"] = help_text.get(name, "")
        d["calls_make"] = SUB_MAKE.findall(recipe)
        d["env"] = re.findall(r"(\w+)=\$\$\{\1:-(\w+)\}", recipe)
    return targets


def extract_shell(files: SourceFiles) -> dict[str, dict]:
    out = {}
    for pattern in spec.SHELL_GLOBS:
        for rel in files.glob(pattern):
            text = "\n".join(line for line in files.text(rel).splitlines() if not line.lstrip().startswith("#"))
            m = re.search(r"\bjobs=\((.*?)\)", text, re.S)
            jobs = m.group(1).split() if m else []
            runs = command_runs(text)
            calls = sorted({p for p, _ in runs["sh"]} - {rel})
            out[rel] = {"jobs": jobs + [j for j in runs["jobs"] if j not in jobs], "calls": calls,
                        "py": runs["py"], "make": MAKE_COMMAND.findall(text),
                        "docker_exec": bool(re.search(r"docker (compose )?exec", text))}
    return out


def _unquote(word: str) -> str:
    """A shell word without its quotes. A ``$`` that must stay literal (single quotes, ``\\$``) becomes
    ``\\x00`` so that ``ShellScope.expand`` does not substitute it (it restores the ``$`` afterwards)."""
    out: list[str] = []
    i = 0
    while i < len(word):
        c = word[i]
        if c == "'":
            j = word.find("'", i + 1)
            j = len(word) if j < 0 else j
            out.append(word[i + 1:j].replace("$", "\x00"))
            i = j + 1
        elif c == '"':
            j = i + 1
            while j < len(word) and word[j] != '"':
                if word[j] == "\\" and j + 1 < len(word) and word[j + 1] in '"\\$`':
                    out.append("\x00" if word[j + 1] == "$" else word[j + 1])
                    j += 2
                else:
                    out.append(word[j])
                    j += 1
            i = j + 1
        elif c == "\\" and i + 1 < len(word):
            out.append("\x00" if word[i + 1] == "$" else word[i + 1])
            i += 2
        else:
            out.append(c)
            i += 1
    return "".join(out)


def _sh_split(text: str) -> list[str]:
    """Words of one command line, split on unquoted blanks. Quotes, ``\\`` escapes and ``$( )`` / back-quote
    command substitutions stay inside their word as written (ValueError when one is left open)."""
    words: list[str] = []
    cur: list[str] = []
    stack: list[str] = []   # the open contexts, innermost last: ' " ` or ( (a $( ... ) substitution)
    i = 0
    while i < len(text):
        ch = text[i]
        top = stack[-1] if stack else None
        if top == "'":
            if ch == "'":
                stack.pop()
        elif ch == "\\" and i + 1 < len(text):
            cur.append(text[i:i + 2])
            i += 2
            continue
        elif (ch == top and ch in "\"`") or (ch == ")" and top == "("):   # the open quote / $( ) closes
            stack.pop()
        elif ch == "$" and text[i + 1:i + 2] == "(":
            stack.append("(")
            cur.append("$(")
            i += 2
            continue
        elif ch == "`" or (ch in "'\"" and top != '"'):
            stack.append(ch)
        elif ch == "(" and top == "(":
            stack.append("(")
        elif ch.isspace() and not stack:
            if cur:
                words.append("".join(cur))
                cur = []
            i += 1
            continue
        cur.append(ch)
        i += 1
    if stack:
        raise ValueError("unbalanced $( command substitution" if stack[-1] == "(" else f"unbalanced {stack[-1]} quote")
    if cur:
        words.append("".join(cur))
    return words


def _restore(text: str) -> str:
    """An expanded word with its literal ``$`` back (``_unquote`` hides them as ``\\x00``)."""
    return text.replace("\x00", "$")


class ShellScope:
    """The variables a shell script (a CI ``run:`` block) assigns: resolves ``$NAME`` / ``${NAME}``
    without running anything.

    Every ``NAME=value`` at a command position is recorded with its position (line, column), and so is
    every word of a ``for NAME in w1 w2 ...`` loop (``for NAME`` without ``in`` loops over ``"$@"``). A
    reference may take the value of ANY assignment of that name before it: the evaluator follows no
    control flow, so both branches of ``if ...; then REF=a; else REF=b; fi``, a reassignment and every
    loop word are kept as candidates. ``env`` (the ``env:`` mappings of the workflow, the job and the
    step, the innermost winning) counts as assigned before the script.

    ``${NAME:-word}`` (``${NAME-word}``, ``${NAME:=word}``, ``${NAME=word}``) of a NAME assigned nowhere
    is ``word``, as the shell expands it. A variable the CI runner sets (``GITHUB_*``, ``RUNNER_*``,
    ``CI``: ``${GITHUB_HEAD_REF:-main}``) and any other variable assigned nowhere stay as written, and
    so does a command substitution: those are run-time values, and the caller decides whether that is
    acceptable.
    """

    def __init__(self, lines: list[tuple[int, str]], env: dict[str, str] | None = None):
        self.assigns: list[tuple[tuple[int, int], str, str]] = [
            ((0, i), name, value) for i, (name, value) in enumerate(sorted((env or {}).items()))]
        for lineno, text in lines:
            for m in _SH_ASSIGN.finditer(text):
                self.assigns.append(((lineno, m.start(1)), m.group(1), m.group(2)))
            for m in _SH_FOR.finditer(text):
                if m.group(2) is None:
                    words = ['"$@"']
                else:
                    try:
                        words = _sh_split(m.group(2))
                    except ValueError:
                        words = [m.group(2).strip()]
                self.assigns += [((lineno, m.start(1)), m.group(1), w) for w in words]
        self.assigns.sort(key=lambda a: a[0])   # stable: the words of one loop keep their order

    def assigned(self, name: str, before: tuple[int, int]) -> bool:
        return any(n == name and pos < before for pos, n, _v in self.assigns)

    def _values(self, name: str, before: tuple[int, int], depth: int) -> list[str]:
        """Candidate values of ``name`` at position ``before`` (empty when it is not assigned before it)."""
        out: list[str] = []
        for pos, n, raw in self.assigns:
            if n == name and pos < before:
                for v in self.expand_raw(raw, pos, depth + 1):
                    if v not in out:
                        out.append(v)
        return out[:MAX_SHELL_CANDIDATES]

    @staticmethod
    def _default(m: re.Match) -> str | None:
        """The word of ``${NAME:-word}`` / ``${NAME-word}`` / ``${NAME:=word}`` / ``${NAME=word}``, which the
        shell uses when NAME is unset; None for any other reference and for a NAME the CI runner sets."""
        name, op = m.group(1), m.group(2)
        if not name or not op or CI_RUNTIME_VARS.match(name):
            return None
        mo = re.match(r"^:?[-=]", op)
        return op[mo.end():] if mo else None

    def expand_raw(self, word: str, before: tuple[int, int], depth: int = 0) -> list[str]:
        """``expand`` with every literal ``$`` still ``\\x00`` (so that a ``$(`` left in a candidate is a
        real command substitution, not a quoted one)."""
        text = _unquote(word)
        if depth > 8:
            return [text]
        parts: list[list[str]] = []
        pos = 0
        for m in _SH_REF.finditer(text):
            parts.append([text[pos:m.start()]])
            vals = self._values(m.group(1) or m.group(3), before, depth)
            default = None if vals else self._default(m)
            if default is not None:
                vals = self.expand_raw(default, before, depth + 1)
            parts.append(vals or [m.group(0)])   # ${NAME:-x} when NAME is assigned here: NAME's value
            pos = m.end()
        parts.append([text[pos:]])
        out = [""]
        for alts in parts:
            out = [a + b for a in out for b in alts][:MAX_SHELL_CANDIDATES]
        return out

    def expand(self, word: str, before: tuple[int, int]) -> list[str]:
        """Candidate values of one shell word: quotes removed, assigned variables substituted (every
        combination of their candidates, at most MAX_SHELL_CANDIDATES), the others left as written."""
        return [_restore(v) for v in self.expand_raw(word, before)]

    def unassigned(self, word: str, before: tuple[int, int]) -> list[str]:
        """Variables ``word`` needs that are assigned nowhere before ``before`` (outermost references; a
        ``${NAME:-default}`` whose NAME is assigned nowhere needs what its default needs instead)."""
        out: list[str] = []
        for m in _SH_REF.finditer(_unquote(word)):
            name = m.group(1) or m.group(3)
            if self.assigned(name, before):
                continue
            default = self._default(m)
            out += [name] if default is None else self.unassigned(default, before)
        return list(dict.fromkeys(out))


def _clone_words(words: list[str], value_opts: frozenset[str] = _CLONE_VALUE_OPTS
                 ) -> tuple[dict[str, str | bool], list[str]]:
    """(options, positional words) of ``git clone <words>`` (or of another command whose options that
    take a value are ``value_opts``); redirections are dropped."""
    opts: dict[str, str | bool] = {}
    positional: list[str] = []
    i = 0
    while i < len(words):
        w = words[i]
        if re.match(r"^\d*(?:>>?|<)(?:&\d*)?$", w):        # `> file`, `2>&1`: an operator (and its target)
            i += 1 if "&" in w else 2
            continue
        if re.match(r"^\d*(?:>>?|<)", w):                  # `>/dev/null`: operator and target in one word
            i += 1
            continue
        if w == "--":
            positional += words[i + 1:]
            break
        if w.startswith("-") and len(w) > 1:
            name, eq, value = w.partition("=")
            if name in value_opts:
                if eq:
                    opts[name] = value
                elif i + 1 < len(words):
                    opts[name] = words[i + 1]
                    i += 1
            else:
                opts[name] = True
        else:
            positional.append(w)
        i += 1
    return opts, positional


def normalise_repo_url(url: str) -> str:
    """``https://github.com/o/r.git/`` -> ``https://github.com/o/r`` (the form CLONES / DownstreamRepo keep)."""
    return re.sub(r"(?:\.git)?/*$", "", url)


def repo_key(url: str) -> str:
    """``https://github.com/o/r`` and ``git@github.com:o/r`` -> ``github.com/o/r`` (a DownstreamRepo id)."""
    if "://" in url:
        return url.split("://", 1)[1].split("@", 1)[-1]
    user_host, _, path = url.partition(":")
    return f"{user_host.split('@', 1)[-1]}/{path}"


def _git_repo_urls(value: str) -> list[str] | None:
    """The remote URL ``git clone`` clones for its repository argument (None: not a remote URL)."""
    return [value] if _REMOTE_URL.match(value) else None


def _gh_repo_urls(value: str, hosts: list[str]) -> list[str] | None:
    """Remote URLs ``gh repo clone`` clones for its repository argument: a URL as is, HOST/OWNER/REPO, and
    OWNER/REPO on ``hosts`` (GH_HOST when the script sets it, else github.com). None for a bare REPO (the
    authenticated user's repository: only the run knows the owner) and anything else."""
    if _REMOTE_URL.match(value):
        return [value]
    m = _GH_REPO.match(value)
    if not m:
        return None
    return [f"https://{h}/{m.group(2)}/{m.group(3)}" for h in ([m.group(1)] if m.group(1) else hosts)]


def _not_url_reason(tool: str, raw: str) -> str:
    """Why a repository candidate (``\\x00`` = a literal $) is no remote URL, when there is more to say."""
    if _RUNTIME_TEXT.search(raw):
        return " (a command substitution: only the step knows its output)"
    if "${{" in _restore(raw):
        return " (a GitHub Actions expression: only the workflow run knows its value)"
    if tool == "gh repo clone" and re.fullmatch(r"[\w.-]+", raw):
        return " (a REPO without OWNER/ is the authenticated user's: only the workflow run knows the owner)"
    return ""


def _new_clone(tool: str, line: int, command: str) -> dict:
    return {"tool": tool, "line": line, "command": command[:200], "url_expr": None, "urls": [], "ref_expr": None,
            "refs": [], "depth": None, "problem": None, "notes": []}


def _resolve_repository(clone: dict, word: str, scope: ShellScope, at: tuple[int, int], to_urls) -> None:
    """``clone["urls"]`` from the repository word, or ``clone["problem"]`` saying exactly why not."""
    clone["url_expr"] = word
    what = f"{clone['tool']} of {word}"
    missing = scope.unassigned(word, at)
    runner = [n for n in missing if CI_RUNTIME_VARS.match(n)]
    others = [n for n in missing if n not in runner]
    if others:
        clone["problem"] = (f"{what}: {', '.join(others)} is not assigned earlier in this run: block or in an env: "
                            f"of the step, job or workflow (a repository resolves from a literal URL, from VAR=<url> "
                            f"or a for loop before the clone, or from the default of ${{VAR:-<url>}})")
        return
    if runner:
        clone["problem"] = (f"{what}: {', '.join(runner)} is set by the CI runner, so the repository is only known "
                            f"when the workflow runs")
        return
    urls: list[str] = []
    bad: list[str] = []
    for raw in scope.expand_raw(word, at):
        found = None if _RUNTIME_TEXT.search(raw) else to_urls(_restore(raw))
        if found and all(_REMOTE_URL.match(u) for u in found):
            urls += found
        else:
            bad.append(raw)
    if bad:
        clone["problem"] = (f"{what}: resolves to {', '.join(_restore(b) for b in bad[:3])}, which is not a remote "
                            f"repository URL{_not_url_reason(clone['tool'], bad[0])}")
    else:
        clone["urls"] = list(dict.fromkeys(normalise_repo_url(u) for u in urls))


def _resolve_ref(clone: dict, opts: dict, scope: ShellScope, at: tuple[int, int]) -> None:
    """``clone["refs"]``: the ``--branch`` candidates, or ``["default branch"]``; ``notes`` for a part that
    is neither assigned in the script nor set by the CI runner (the ref is then recorded as written)."""
    branch = opts.get("--branch", opts.get("-b"))
    if not isinstance(branch, str):
        clone["refs"] = ["default branch"]
        return
    clone["ref_expr"] = branch
    raw = scope.expand_raw(branch, at)
    clone["refs"] = [_restore(r) for r in raw]
    # what is left unexpanded in a candidate is a run-time value: fine when the CI runner sets it
    # (${GITHUB_HEAD_REF:-${GITHUB_REF_NAME}}: the same-named branch), a note otherwise
    left = sorted({n for value in raw for n in _ref_names(value) if not CI_RUNTIME_VARS.match(n)})
    for name in left:
        clone["notes"].append(f"--branch {branch}: {name} is not assigned in this run: block or in an env: of the "
                              f"step, job or workflow, and is not set by the CI runner; the ref is recorded as written")
    if any(_RUNTIME_TEXT.search(r) for r in raw):
        clone["notes"].append(f"--branch {branch}: a command substitution (only the step knows its output); the ref "
                              f"is recorded as written")


def shell_clones(lines: list[tuple[int, str]], env: dict[str, str] | None = None) -> list[dict]:
    """``git clone`` and ``gh repo clone`` commands of a shell script, repository and ref resolved
    through ``ShellScope``.

    ``lines`` are (line number, text) with comments removed and continuation lines joined. One
    dict per clone: ``tool``, ``line``, ``command``, ``url_expr`` / ``urls`` (normalised remote URLs,
    one per candidate), ``ref_expr`` / ``refs`` (the ``--branch`` candidates, or ``["default
    branch"]``), ``depth``, ``problem`` and ``notes``. A clone is never dropped: when the repository
    does not resolve to a remote URL, ``urls`` is empty and ``problem`` says exactly why (an
    unassigned variable, a runner variable, a command substitution, a GitHub expression, a local
    path; the caller makes it an unresolved name). ``notes`` name ref parts that are neither
    assigned in the script nor set by the CI runner (the ref is then recorded as written).

    ``gh repo clone [HOST/]OWNER/REPO [dir] [-- <git clone flags>]`` clones
    ``https://<host>/OWNER/REPO`` (host: GH_HOST when the script sets it, else github.com); its
    ``--branch`` / ``--depth`` are the git flags after ``--``.
    """
    scope = ShellScope(lines, env)
    found: list[tuple[int, int, str, str]] = []
    for lineno, text in lines:
        found += [(lineno, m.start(1), "git clone", m.group(1)) for m in _GIT_CLONE.finditer(text)]
        found += [(lineno, m.start(1), "gh repo clone", m.group(1)) for m in _GH_CLONE.finditer(text)]
    out: list[dict] = []
    for lineno, col, tool, args in sorted(found):
        at = (lineno, col)
        clone = _new_clone(tool, lineno, f"{tool}{args.rstrip()}")
        out.append(clone)
        try:
            words = _sh_split(args)
        except ValueError as e:
            clone["problem"] = f"{clone['command']}: the command does not tokenise ({e})"
            continue
        if tool == "gh repo clone":
            k = words.index("--") if "--" in words else len(words)
            positional = _clone_words(words[:k], _GH_VALUE_OPTS)[1]
            opts = _clone_words(words[k + 1:])[0]
            hosts = scope.expand("$GH_HOST", at) if scope.assigned("GH_HOST", at) else ["github.com"]
            to_urls = functools.partial(_gh_repo_urls, hosts=hosts)
        else:
            opts, positional = _clone_words(words)
            to_urls = _git_repo_urls
        depth = opts.get("--depth")
        if isinstance(depth, str):
            values = scope.expand(depth, at)
            clone["depth"] = int(values[0]) if len(values) == 1 and values[0].isdigit() else None
        if not positional:
            clone["problem"] = f"{clone['command']}: no repository argument"
            continue
        _resolve_repository(clone, positional[0], scope, at, to_urls)
        _resolve_ref(clone, opts, scope, at)
    return out


def _yaml_value(text: str) -> str:
    """A plain one-line YAML value: a trailing `` # comment`` removed, one pair of quotes removed."""
    text = re.sub(r"\s+#.*$", "", text.strip())
    if len(text) >= 2 and text[0] == text[-1] and text[0] in "'\"":
        return text[1:-1]
    return text


def checkout_clone(block: list[tuple[int, str]]) -> dict | None:
    """The clone of an ``actions/checkout`` step that checks out ANOTHER repository (``with: repository:
    OWNER/REPO``) as a ``shell_clones`` record: ``https://<github-server-url or github.com>/OWNER/REPO``,
    ``ref`` (else the default branch; a ``${{ }}`` ref is a run-time value, kept as written),
    ``fetch-depth`` (default 1; 0 = the whole history, depth None). None for a step that is no
    checkout, or that checks out the workflow's own repository (no ``repository:``, or
    ``${{ github.repository }}``). A repository that is not OWNER/REPO is kept with a ``problem``."""
    if not any(re.match(r"^\s*(?:- )?uses:\s*['\"]?actions/checkout@", t) for _, t in block):
        return None
    inputs: dict[str, tuple[int, str]] = {}
    for lineno, t in block:
        m = re.match(r"^\s+(repository|ref|fetch-depth|github-server-url):\s*(.*)$", t)
        if m:
            inputs[m.group(1)] = (lineno, _yaml_value(m.group(2)))
    if "repository" not in inputs or re.fullmatch(r"\$\{\{\s*github\.repository\s*\}\}", inputs["repository"][1]):
        return None
    lineno, repo = inputs["repository"]
    clone = _new_clone("actions/checkout", lineno, f"actions/checkout repository: {repo}")
    clone["url_expr"] = repo
    host = re.sub(r"^https?://", "", inputs.get("github-server-url", (0, "github.com"))[1]).rstrip("/")
    m = re.fullmatch(r"([\w.-]+)/([\w.-]+)", repo)
    url = f"https://{host}/{m.group(1)}/{m.group(2)}" if m else ""
    if m and _REMOTE_URL.match(url):
        clone["urls"] = [normalise_repo_url(url)]
    else:
        why = (" (a GitHub Actions expression: only the workflow run knows its value)" if "${{" in repo + host
               else "")
        clone["problem"] = f"actions/checkout of repository: {repo}: not OWNER/REPO on a known host{why}"
    if "ref" in inputs and inputs["ref"][1]:
        clone["ref_expr"] = inputs["ref"][1]
        clone["refs"] = [inputs["ref"][1]]
    else:
        clone["refs"] = ["default branch"]
    depth = inputs.get("fetch-depth", (0, "1"))[1]
    clone["depth"] = int(depth) if depth.isdigit() and int(depth) > 0 else None
    return clone


def _ref_names(text: str) -> list[str]:
    """Every variable a word references, including those inside a ``${A:-$B}`` default."""
    names: list[str] = []
    for m in _SH_REF.finditer(text):
        names.append(m.group(1) or m.group(3))
        if m.group(2):
            names += _ref_names(m.group(2))
    return names


def script_lines(block: list[tuple[int, str]]) -> list[tuple[int, str]]:
    """(line number, text) of a step's lines with comments removed and ``\\``-continued lines joined."""
    out: list[tuple[int, str]] = []
    pending: tuple[int, str] | None = None
    for lineno, raw in block:
        text = re.sub(r"(^|\s)#.*$", "", raw)
        if pending is not None:
            lineno, text = pending[0], pending[1] + " " + text.lstrip()
            pending = None
        if text.rstrip().endswith("\\"):
            pending = (lineno, text.rstrip()[:-1])
            continue
        out.append((lineno, text))
    if pending is not None:
        out.append(pending)
    return out


def _env_mapping(lines: list[str], start: int, indent: int) -> dict[str, str]:
    """The ``NAME: value`` lines of an ``env:`` block whose keys sit at ``indent`` spaces."""
    out: dict[str, str] = {}
    for line in lines[start:]:
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if len(line) - len(line.lstrip()) < indent:
            break
        m = re.match(rf"^ {{{indent}}}([A-Za-z_]\w*):[ \t]*(.*)$", line)
        if m:
            out[m.group(1)] = _yaml_value(m.group(2))
    return out


def ci_env_blocks(lines: list[str]) -> tuple[dict[str, str], dict[str, dict[str, str]]]:
    """(the workflow's ``env:``, {job: the job's ``env:``}) of ci.yml: what every step of a job inherits
    (a step's own ``env:`` overrides the job's, which overrides the workflow's)."""
    workflow: dict[str, str] = {}
    jobs: dict[str, dict[str, str]] = {}
    job, in_jobs = None, False
    for k, line in enumerate(lines):
        if re.match(r"^[^\s#]", line):
            in_jobs, job = bool(re.match(r"^jobs:\s*$", line)), None
            if re.match(r"^env:\s*$", line):
                workflow.update(_env_mapping(lines, k + 1, 2))
        elif in_jobs and (mj := re.match(r"^  ([\w-]+):\s*$", line)):
            job = mj.group(1)
        elif job and re.match(r"^    env:\s*$", line):
            jobs.setdefault(job, {}).update(_env_mapping(lines, k + 1, 6))
    return workflow, jobs


def extract_ci(files: SourceFiles) -> list[dict]:
    """Steps of .github/workflows/ci.yml that run a script, a make target, a shell pipeline or
    clone a repository (a light line parser: PyYAML is not a dependency).

    ``clones``: the ``git clone`` / ``gh repo clone`` commands of the step's ``run:`` block
    (``shell_clones``: repository and ref resolve from a literal URL, from ``VAR=...`` assignments
    and ``for`` loops earlier in the same block, from the ``env:`` of the step, the job or the
    workflow, or from a ``${VAR:-default}``), and an ``actions/checkout`` of another repository
    (``checkout_clone``). ``clone_problems`` are the clones whose repository does not resolve, as
    unresolved names (``where`` = the clone's line)."""
    if not files.exists(spec.CI_WORKFLOW):
        return []
    lines = files.text(spec.CI_WORKFLOW).splitlines()
    workflow_env, job_env = ci_env_blocks(lines)
    steps: list[dict] = []
    job = None
    in_jobs = False
    i = 0
    while i < len(lines):
        line = lines[i]
        if re.match(r"^jobs:\s*$", line):
            in_jobs = True
        elif in_jobs and (mj := re.match(r"^  ([\w-]+):\s*$", line)):
            job = mj.group(1)
        ms = re.match(r"^(\s+)- (name|uses|run):\s*(.*)$", line) if job else None
        if ms and len(ms.group(1)) >= 4:
            indent = len(ms.group(1))
            j = i + 1
            while j < len(lines) and not re.match(rf"^\s{{0,{indent}}}- ", lines[j]) \
                    and not re.match(r"^ {0,2}[\w-]+:\s*$", lines[j]):
                j += 1
            block = lines[i:j]
            name = ms.group(3).strip() if ms.group(2) == "name" else next(
                (re.sub(r"^\s*name:\s*", "", b).strip() for b in block if re.match(r"^\s*name:", b)), None)
            body = "\n".join(re.sub(r"(^|\s)#.*$", "", b) for b in block if not re.match(r"^\s*(- )?name:", b))
            runs = command_runs(body)
            env = dict(re.findall(r"^\s*([A-Z][A-Z0-9_]+): (.+)$", "\n".join(block), re.M))
            script = script_lines([(i + 1 + k, b) for k, b in enumerate(block)
                                    if not re.match(r"^\s*(- )?name:", b)])
            scope_env = {**workflow_env, **job_env.get(job, {}), **{k: v.strip() for k, v in env.items()}}
            clones = shell_clones(script, scope_env)
            checkout = checkout_clone([(i + 1 + k, b) for k, b in enumerate(block)])
            if checkout:
                clones.insert(0, checkout)
            step = {"job": job, "name": name or f"step at line {i + 1}", "line": i + 1, "py": runs["py"], "body": body,
                    "sh": runs["sh"], "make": MAKE_COMMAND.findall(body), "env": env, "clones": clones,
                    "clone_problems": [{"where": f"{spec.CI_WORKFLOW}:{c['line']}", "what": c["problem"]}
                                       for c in clones if c["problem"]]}
            if step["py"] or step["sh"] or step["make"] or step["clones"]:
                steps.append(step)
            i = j
            continue
        i += 1
    return steps


# --------------------------------------------------------------------------- contracts
CHECK_LISTS = {"errors": "error", "warnings": "warn", "problems": "error"}


def extract_contract_checks(tree: ast.Module) -> list[dict]:
    """Every ``errors.append(...)`` / ``warnings.append(...)`` with the chain of if / for
    guards around it (outermost first), its severity, line and message."""
    out: list[dict] = []

    def visit(node: ast.AST, guards: tuple[str, ...], fn: str | None) -> None:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            fn, guards = node.name, ()
        if isinstance(node, ast.If):
            test = ast.unparse(node.test)
            for child in node.body:
                visit(child, guards + (test,), fn)
            for child in node.orelse:
                visit(child, guards + (f"not ({test})",), fn)
            return
        if isinstance(node, ast.For):
            loop = f"for {ast.unparse(node.target)} in {ast.unparse(node.iter)}"
            for child in node.body:
                visit(child, guards + (loop,), fn)
            return
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "append" \
                and isinstance(node.func.value, ast.Name) and node.func.value.id in CHECK_LISTS and node.args \
                and guards:
            out.append({"fn": fn, "lineno": node.lineno, "severity": CHECK_LISTS[node.func.value.id],
                        "guards": list(guards), "test": " / ".join(guards),
                        "message": ast.unparse(node.args[0])[:200]})
        for child in ast.iter_child_nodes(node):
            visit(child, guards, fn)

    visit(tree, (), None)
    return sorted(out, key=lambda o: o["lineno"])


def extract_export_contract(files: SourceFiles) -> dict:
    tree = files.tree(spec.EXPORT_CONTRACT)
    consts = {}
    for name in ("TRAIN_COLUMNS", "PLAN_TIERS", "BINARY", "RANGES", "LEAKY"):
        consts[name] = module_const(tree, name)
        if not consts[name]:
            raise LineageExtractError(f"{spec.EXPORT_CONTRACT}: the constant {name} is missing or not a literal")
    try:
        ranges = [(str(c), float(lo), float(hi)) for c, lo, hi in consts["RANGES"]]
    except (ValueError, TypeError) as e:
        raise LineageExtractError(f"{spec.EXPORT_CONTRACT}: RANGES must be (column, min, max) tuples ({e})") from e
    env = Env(tree)
    paths = {**{k: v for k, v in env.locals.items() if isinstance(v, str)}, **env.local_patterns}
    return {"train_columns": list(consts["TRAIN_COLUMNS"]), "plan_tiers": sorted(consts["PLAN_TIERS"]),
            "binary": list(consts["BINARY"]), "ranges": ranges, "leaky": sorted(consts["LEAKY"]),
            "checks": extract_contract_checks(tree), "doc": ast.get_docstring(tree) or "",
            "path_vars": {k: v for k, v in paths.items() if v.endswith((".csv", ".json"))}}


def extract_parity_contract(files: SourceFiles, twin: dict) -> dict:
    """Columns compared by check_gold_parity.py (``cols = local.TRAIN_COLUMNS + [...]``) and its tolerance."""
    tree = files.tree(spec.PARITY_CONTRACT)
    env = Env(tree)
    twin_cols = twin["consts"].get("TRAIN_COLUMNS")
    cols = None
    for n in ast.walk(tree):
        if isinstance(n, ast.Assign) and isinstance(n.targets[0], ast.Name) and n.targets[0].id == "cols":
            local = {ast.unparse(a): twin_cols for a in ast.walk(n.value)
                     if isinstance(a, ast.Attribute) and a.attr == "TRAIN_COLUMNS"}
            cols = env.ev(n.value, local)
    if not isinstance(cols, list) or not all(isinstance(c, str) for c in cols):
        raise LineageExtractError(f"{spec.PARITY_CONTRACT}: the compared column list "
                                  f"(cols = <twin>.TRAIN_COLUMNS + [...]) was not found")
    atol = None
    for n in ast.walk(tree):
        if isinstance(n, ast.Call) and getattr(n.func, "attr", "") == "isclose":
            atol = next((k.value.value for k in n.keywords if k.arg == "atol" and isinstance(k.value, ast.Constant)),
                        None)
    return {"columns": cols, "atol": atol, "checks": extract_contract_checks(tree)}


# --------------------------------------------------------------------------- parameters
def extract_parameters(twin_tree: ast.Module, generator_tree: ast.Module) -> dict:
    """ALLOWANCE / CAP_CUT / T-N offset in the pandas twin and the generator (the SQL side
    comes from the scope walk); DUNNING_DAYS is defined in the twin and never read."""
    twin_offset = None
    for n in ast.walk(twin_tree):   # snapshot_date == current_period_end - pd.Timedelta(days=7)
        if isinstance(n, ast.BinOp) and isinstance(n.op, ast.Sub) and "current_period_end" in ast.unparse(n.left):
            twin_offset = _timedelta_days(n.right)
            if twin_offset is not None:
                break
    gen_offsets = set()
    for n in ast.walk(generator_tree):   # "snapshot_date": (renewal - pd.Timedelta(days=7))...
        if isinstance(n, ast.Dict):
            for k, v in zip(n.keys, n.values, strict=True):
                if isinstance(k, ast.Constant) and k.value == "snapshot_date" and _timedelta_days(v) is not None:
                    gen_offsets.add(_timedelta_days(v))
    dunning_reads = sum(isinstance(n, ast.Name) and n.id == "DUNNING_DAYS" and isinstance(n.ctx, ast.Load)
                        for n in ast.walk(twin_tree))
    return {"twin": {"allowance": module_const(twin_tree, "ALLOWANCE"), "cap_cut": module_const(twin_tree, "CAP_CUT"),
                     "as_of_offset_days": twin_offset, "dunning_days": module_const(twin_tree, "DUNNING_DAYS"),
                     "dunning_days_reads": int(dunning_reads)},
            "generator": {"allowance": module_const(generator_tree, "ALLOWANCE"),
                          "cap_cut": module_const(generator_tree, "CAP_CUT"),
                          "as_of_offset_days": gen_offsets.pop() if len(gen_offsets) == 1 else None}}


# --------------------------------------------------------------------------- README
def extract_readme(files: SourceFiles) -> dict:
    """Contract numbers the README states (documentation assertions; None when the sentence is gone)."""
    if not files.exists(spec.README):
        return {"churn": None, "retail_daily": [], "retail_orders": None}
    text = files.text(spec.README)

    def num(s: str) -> int:
        return int(s.replace(",", ""))

    m = re.search(r"\(([\d,]+) subscriptions\): ([\d,]+) renewals routed to the model \(([\d.]+)% voluntary lapse\), "
                  r"([\d,]+) to dunning, ([\d,]+) to the cancel flow, and (\w+) scored today", text)
    churn = None
    if m:
        today = m.group(6)
        churn = {"n_subscriptions": num(m.group(1)), "route_model": num(m.group(2)), "lapse_pct": float(m.group(3)),
                 "route_dunning": num(m.group(4)), "route_cancel_flow": num(m.group(5)),
                 "route_score_today": {"one": 1, "two": 2}.get(today, int(today) if today.isdigit() else None)}
    retail = re.findall(r"\| (\d{4}-\d\d-\d\d) \| (\d+) \| ([\d.]+) \| ([\d.]+) \|", text)
    orders = re.search(r"Bronze orders ≈ \*\*(\d+)\*\* rows → silver \*\*(\d+)\*\*", text)
    return {"churn": churn, "retail_daily": retail, "retail_orders": orders.groups() if orders else None}


# --------------------------------------------------------------------------- everything
def extract_all(repo: str | Path) -> dict:
    """All Tier-0 facts of the repo at ``repo``; ``facts["files"]`` is the SourceFiles used."""
    files = SourceFiles(repo)
    missing = [rel for rel in spec.REQUIRED_FILES if not files.exists(rel)]
    if missing:
        raise LineageExtractError(f"required source file(s) missing: {', '.join(missing)}")
    job_paths = sorted({rel for pattern in spec.JOB_GLOBS for rel in files.glob(pattern)})
    jobs = {rel: extract_job(files, rel) for rel in job_paths}
    dags, dag_unresolved = extract_dags(files)
    twin_tree, gen_tree = files.tree(spec.PANDAS_TWIN), files.tree(spec.GENERATOR)
    ci = extract_ci(files)
    unresolved = [u for j in jobs.values() for u in j["unresolved"]] + dag_unresolved + \
        [p for s in ci for p in s["clone_problems"]]
    return {
        "files": files, "jobs": jobs, "dags": dags, "make": extract_make(files), "shell": extract_shell(files),
        "ci": ci, "export_contract": extract_export_contract(files),
        "parity_contract": extract_parity_contract(files, jobs[spec.PANDAS_TWIN]),
        "parameters": extract_parameters(twin_tree, gen_tree), "readme": extract_readme(files),
        "unresolved": unresolved,
    }


def match_names(pattern: str, names) -> list[str]:
    """Known names a ``*`` pattern stands for (exact name when there is no wildcard)."""
    if "*" not in pattern:
        return [pattern] if pattern in names else []
    return sorted(n for n in names if fnmatch.fnmatchcase(n, pattern))


def dumps(obj) -> str:
    """Canonical JSON for list / dict properties stored as strings."""
    return json.dumps(obj, sort_keys=True, separators=(", ", ": "), default=str)
