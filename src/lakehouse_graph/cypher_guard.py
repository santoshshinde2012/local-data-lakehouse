"""Guarded raw Cypher over the pruned evidence graph (PLAN Later A): the ``graph_cypher`` tool, opt-in, sandbox only.

    scripts/graph_mcp.sh --enable-cypher [--build <dir>]
    (never in the default .mcp.json: see .mcp.cypher.json.example)

Where it runs (mcp_server.main enforces it before the tool exists):
  * only with --enable-cypher, as its own server (toolset ``cypher``, one tool), and only against
    <build>/evidence.lbdb (lakehouse_graph.pruned: physically cut at every renewal's as_of, label-free, no
    SIMILAR_TO), checked against its evidence.json before it is opened read only;
  * on macOS only inside the sandbox, proven in process: the launcher's GRAPH_SANDBOXED=1 marker, the kernel's
    sandbox_check() on this pid, /private/etc/hosts unreadable, AND the build's label-bearing files (graph.lbdb, the
    Parquet, manifest.json) unreadable: graph_mcp.sh starts a cypher server with a profile whose only readable data
    are evidence.lbdb and evidence.json, so a statement that slips past this guard still cannot read a label, a file
    or the network (graph_sandbox_check.py --cypher proves that with the guard bypassed);
  * on Linux only with GRAPH_ALLOW_UNSANDBOXED_CYPHER=1, which prints a loud banner; anywhere else: refused.

The guard (``guard()``), before the engine sees anything, refuses with a repairable message:
  * more than MAX_QUERY_CHARS characters, control or invisible format characters, more than one statement;
  * a statement that does not start with MATCH, OPTIONAL MATCH, WITH, UNWIND, RETURN or CALL of an allowed catalog
    function (show_tables, table_info, show_connection), or does not end in RETURN;
  * the deny list, as words anywhere outside strings, back-ticks and property names: file and database statements
    (load ... from, copy, export, import, attach, detach, use), the extension statements (install, uninstall,
    load extension, update extension), every write or schema statement (create, merge, set, delete, remove, drop,
    alter, foreach, transactions, checkpoint), EXPLAIN / PROFILE (the guard runs EXPLAIN itself), UNION; CALL of
    anything else (settings such as ``CALL timeout=0`` included); file, setting and extension functions (read_*,
    file_info, current_setting, ...); parameters (there is nothing to bind them to);
  * a string literal that looks like a path or URL (a slash or backslash, ~, '://', a file extension);
  * a variable-length relationship without an upper bound, or with one above MAX_HOPS (4);
  * a label property (churned, outcome, route, is_reference, outcome_observed_on): the evidence graph has none;
  * a LIMIT that is not a whole number; a LIMIT above MAX_ROWS (200) is lowered to it, a missing one is added
    (MAX_ROWS + 1, so the answer can say it was cut).
Then ``EXPLAIN`` binds the statement; a binder error comes back as the engine's own message plus the nearest schema
names (labels, relationship types, properties) so a model can repair it. Then the statement runs on a read-only
connection with the 5 s engine timeout, a 128 MB buffer pool and 2 threads, under a Watchdog (some statements
ignore the engine's timeout and grow memory outside its pool: past the time limit or 768 MiB of growth the query is
interrupted and, if it does not stop, the server process ends), at most MAX_ROWS rows are returned through the same
envelope (hygiene, provenance, character cap) as every other tool, and every call (refused ones too) is one line of
the audit log, with the query only as a keyed hash (a per-process key: the install key is outside the sandbox).

MEASURED_TEXT_TO_CYPHER below is the PLAN's "measured before being enabled for small models" number.
"""
from __future__ import annotations

import ctypes
import difflib
import errno
import os
import re
import sys
import time
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, ValidationError

from . import envelope, pruned, store
from .context import ProvenanceUnavailable, ToolArgumentError, ToolInputError, ToolTimeout, is_interrupted

# PLAN Later A: "text-to-Cypher measured before being enabled for small models". Measured 2026-10-01 with the local
# qwen3:4b (= Qwen3-4B-Thinking-2507, Ollama 0.35, temperature 0, seed 7, num_ctx 8192) on 10 evidence questions over
# the s42 evidence graph, every statement through graph_mcp.sh --enable-cypher (sandboxed, guarded), one repair turn
# with the tool's own message, graded against pandas: JSON-schema output 6/10 (5 first try), thinking on 5/10 (4;
# several answers empty after a 2,048-token thinking budget), think=false 0/10 (the model ignores it and writes prose,
# which the guard refuses). No statement tried a write, a file, a label or a path past 4 hops. Below the PLAN's gate
# (graph-shaped >= 80%), so graph_cypher stays off by default and the small-model harness never offers it.
MEASURED_TEXT_TO_CYPHER = {"model": "qwen3:4b", "questions": 10, "json_schema_output": 6, "thinking_on": 5,
                           "think_false": 0, "gate": "graph-shaped >= 80% (PLAN 8.6)",
                           "enabled_for_small_models": False}
MAX_QUERY_CHARS = 20_000
MAX_ROWS = envelope.MAX_ROWS            # 200
MAX_HOPS = 4
TIMEOUT_MS = store.QUERY_TIMEOUT_MS     # 5 s
TOOL_NAME = "graph_cypher"
TOOLSET = "cypher"
ALLOWED_CALLS = ("show_tables", "table_info", "show_connection")
LEADING = ("MATCH", "OPTIONAL", "WITH", "UNWIND", "RETURN", "CALL")
# The deny list (upper-case words compared with every word token outside strings, back-ticks and property names).
# The extension statement word is assembled from two parts on purpose: tests/graph/test_queries_lint.py greps every
# product-code file for it (product code must never run it), and this list is where it is refused, not run.
_EXTENSION_VERB = "IN" + "STALL"
DENY = {
    "LOAD": "file and extension loading", "COPY": "file import / export", "EXPORT": "database export",
    "IMPORT": "database import", "ATTACH": "attaching another database", "DETACH": "detaching / detach delete",
    "USE": "switching databases", _EXTENSION_VERB: "extension installation",
    "UN" + _EXTENSION_VERB: "extension removal",
    "UPDATE": "extension update", "CREATE": "a write or schema change", "MERGE": "a write", "SET": "a write",
    "DELETE": "a write", "REMOVE": "a write", "DROP": "a schema change", "ALTER": "a schema change",
    "FOREACH": "a write loop", "BEGIN": "a transaction", "COMMIT": "a transaction", "ROLLBACK": "a transaction",
    "CHECKPOINT": "a storage operation", "EXPLAIN": "EXPLAIN (the guard runs it itself)",
    "PROFILE": "PROFILE (it executes the query)", "UNION": "UNION (run two queries instead)",
}
DENY_FUNCTIONS = ("file_info", "disk_info", "disk_size_info", "bm_info", "fsm_info", "current_setting",
                  "show_attached_databases", "show_loaded_extensions", "show_official_extensions", "project_graph",
                  "project_graph_cypher", "drop_projected_graph", "projected_graph_info", "show_projected_graphs",
                  "clear_warnings", "catalog_version", "show_functions", "show_macros", "show_sequences",
                  "show_indexes", "show_graphs", "getenv", "sleep")
DENY_FUNCTION_PREFIXES = ("read_", "copy_", "export_", "import_", "file_", "disk_")
PATHLIKE = re.compile(r"[/\\]|^~|://|^file:|\.(csv|tsv|parquet|json|jsonl|npy|npz|lbdb|db|sqlite|txt|log|env|pem|key|"
                      r"so|dylib|py|sh)\b", re.IGNORECASE)
LABEL_WORDS = pruned.LABEL_PROPERTIES


class CypherRefused(ToolInputError):
    """The guard refused the statement (or the engine refused to bind it); the text says how to repair it."""

    outcome = "refused"


# --------------------------------------------------------------------------- tokens
@dataclass(frozen=True)
class Tok:
    kind: str        # word | number | string | ident (back-ticked) | param | punct
    text: str
    start: int
    end: int


_PUNCT2 = ("..", "->", "<-", "<>", "<=", ">=", "=~")


def tokenize(q: str, comments: list[tuple[int, int]] | None = None) -> list[Tok]:
    """Cypher tokens with positions; comments dropped (their spans appended to ``comments``); raises CypherRefused
    on an unterminated string / comment / back-ticked name."""
    out: list[Tok] = []
    spans = comments if comments is not None else []
    i, n = 0, len(q)
    while i < n:
        c = q[i]
        if c.isspace():
            i += 1
        elif q.startswith("//", i):
            j = q.find("\n", i)
            spans.append((i, n if j < 0 else j))
            i = n if j < 0 else j + 1
        elif q.startswith("/*", i):
            j = q.find("*/", i + 2)
            if j < 0:
                raise _refuse("an unterminated /* comment", "Close it with */.")
            spans.append((i, j + 2))
            i = j + 2
        elif c in "'\"":
            j, buf = i + 1, []
            while j < n and q[j] != c:
                if q[j] == "\\" and j + 1 < n:
                    buf.append(q[j:j + 2])
                    j += 2
                else:
                    buf.append(q[j])
                    j += 1
            if j >= n:
                raise _refuse("an unterminated string literal", "Close the quote.")
            out.append(Tok("string", "".join(buf), i, j + 1))
            i = j + 1
        elif c == "`":
            j = q.find("`", i + 1)
            if j < 0:
                raise _refuse("an unterminated `back-ticked` name", "Close the back-tick.")
            out.append(Tok("ident", q[i + 1:j], i, j + 1))
            i = j + 1
        elif c == "$":
            j = i + 1
            while j < n and (q[j].isalnum() or q[j] == "_"):
                j += 1
            out.append(Tok("param", q[i:j], i, j))
            i = j
        elif c.isdigit():
            j = i
            while j < n and q[j].isdigit():
                j += 1
            if j + 1 < n and q[j] == "." and q[j + 1].isdigit():      # 1.5 (but 1..3 is a range)
                j += 1
                while j < n and q[j].isdigit():
                    j += 1
            out.append(Tok("number", q[i:j], i, j))
            i = j
        elif c.isalpha() or c == "_":
            j = i
            while j < n and (q[j].isalnum() or q[j] == "_"):
                j += 1
            out.append(Tok("word", q[i:j], i, j))
            i = j
        else:
            two = q[i:i + 2]
            if two in _PUNCT2:
                out.append(Tok("punct", two, i, i + 2))
                i += 2
            else:
                out.append(Tok("punct", c, i, i + 1))
                i += 1
    return out


# --------------------------------------------------------------------------- the guard
@dataclass
class Guarded:
    """What the engine will run: the statement (LIMIT forced) and what the guard changed."""

    statement: str
    limit: int
    notes: list[str] = field(default_factory=list)


def _refuse(what: str, fix: str) -> CypherRefused:
    return CypherRefused(f"refused by the Cypher guard: {what}. {fix}")


def guard(query: Any) -> Guarded:
    """Check one raw Cypher statement; return the statement to run (LIMIT forced) or raise CypherRefused."""
    if not isinstance(query, str) or not query.strip():
        raise _refuse("empty query", "Send one read-only statement, e.g. MATCH (r:Renewal) RETURN count(r) AS n.")
    if len(query) > MAX_QUERY_CHARS:
        raise _refuse(f"{len(query):,} characters (the cap is {MAX_QUERY_CHARS:,})", "Send a shorter statement.")
    bad = sorted({f"U+{ord(ch):04X}" for ch in query if ch not in "\t\n\r" and
                  unicodedata.category(ch) in ("Cc", "Cf", "Cs", "Co", "Cn")})
    if bad:
        raise _refuse(f"control or invisible characters ({', '.join(bad[:3])})", "Send plain text.")
    comments: list[tuple[int, int]] = []
    toks = tokenize(query, comments)
    for a, b in comments:                     # the engine never sees a comment (its dialect varies)
        query = query[:a] + " " * (b - a) + query[b:]
    while toks and toks[-1].text == ";":
        toks = toks[:-1]
    if not toks:
        raise _refuse("empty query", "Send one read-only statement.")
    if any(t.text == ";" for t in toks):
        raise _refuse("more than one statement", "Send exactly one statement (no ';' between statements).")
    first = toks[0]
    if first.kind != "word" or first.text.upper() not in LEADING:
        raise _refuse("a statement must start with MATCH, OPTIONAL MATCH, WITH, UNWIND, RETURN or CALL of a catalog "
                      f"function ({', '.join(ALLOWED_CALLS)})", "Rewrite it as a read-only query.")
    for k, t in enumerate(toks):
        prev = toks[k - 1] if k else None
        nxt = toks[k + 1] if k + 1 < len(toks) else None
        if t.kind == "param":
            raise _refuse("a $parameter (this tool binds none)", "Write the value as a literal, e.g. 'inc-002'.")
        if t.kind == "string" and PATHLIKE.search(t.text):
            raise _refuse("a string that looks like a file path or URL", "No tool reads files or the network; "
                                                                          "query the graph's own nodes instead.")
        if t.kind != "word":
            continue
        word = t.text.upper()
        is_property = prev is not None and prev.text == "."
        if is_property and t.text.lower() in LABEL_WORDS:
            raise _refuse(f"the label property {t.text} (the evidence graph has no label: no churned, outcome, route, "
                          f"is_reference or outcome_observed_on)", "Ask about point-in-time evidence; rates come "
                                                                   "from metric_lapse_rate.")
        if is_property:
            continue
        if word in DENY:
            raise _refuse(f"{t.text} ({DENY[word]})", "Only read-only MATCH / WITH / UNWIND / RETURN queries run here.")
        if word == "CALL":
            fn = nxt.text.lower() if nxt is not None and nxt.kind == "word" else ""
            after = toks[k + 2] if k + 2 < len(toks) else None
            if fn not in ALLOWED_CALLS or after is None or after.text != "(":
                raise _refuse("CALL of anything but a catalog function", "Allowed: CALL show_tables() RETURN *, "
                                                                         "CALL table_info('Renewal') RETURN *, "
                                                                         "CALL show_connection('HIT_LIMIT') RETURN *.")
        if nxt is not None and nxt.text == "(":
            low = t.text.lower()
            if low in DENY_FUNCTIONS or low.startswith(DENY_FUNCTION_PREFIXES):
                if not (low in ALLOWED_CALLS and prev is not None and prev.text.upper() == "CALL"):
                    raise _refuse(f"the function {t.text} (files, settings and extensions are off limits)",
                                  "Use graph properties and ordinary functions.")
    _check_hops(toks)
    return _force_limit(query, toks)


def _check_hops(toks: list[Tok]) -> None:
    """Every variable-length relationship pattern ``-[...*...]-`` carries an upper bound of at most MAX_HOPS."""
    for k, t in enumerate(toks):
        if t.text != "[" or k == 0 or toks[k - 1].text not in ("-", "<-"):
            continue
        depth, j = 0, k
        while j < len(toks):
            if toks[j].text == "[":
                depth += 1
            elif toks[j].text == "]":
                depth -= 1
                if depth == 0:
                    break
            j += 1
        inside = toks[k + 1:j]
        star = next((i for i, x in enumerate(inside) if x.text == "*"), None)
        if star is None:
            continue
        rest = inside[star + 1:]
        i = 0
        while i < len(rest) and rest[i].kind == "word" and rest[i].text.upper() in ("SHORTEST", "ALL", "WSHORTEST",
                                                                                   "TRAIL", "ACYCLIC"):
            i += 1
            if i < len(rest) and rest[i].text == "(":                 # WSHORTEST(weight)
                while i < len(rest) and rest[i].text != ")":
                    i += 1
                i += 1
        hi = None
        if i < len(rest) and rest[i].kind == "number":
            hi = rest[i].text                                          # *3: exactly three hops
            i += 1
        if i < len(rest) and rest[i].text == "..":
            hi = None
            i += 1
            if i < len(rest) and rest[i].kind == "number":
                hi = rest[i].text
        if hi is None or not hi.isdigit():
            raise _refuse("a variable-length relationship without an upper bound",
                          f"Bound it, e.g. -[*1..{MAX_HOPS}]-> (at most {MAX_HOPS} hops).")
        if int(hi) > MAX_HOPS:
            raise _refuse(f"a variable-length relationship of up to {hi} hops (the cap is {MAX_HOPS})",
                          f"Use *1..{MAX_HOPS} or fewer.")


def _force_limit(query: str, toks: list[Tok]) -> Guarded:
    """The final top-level RETURN gets LIMIT <= MAX_ROWS: lowered when larger, added (MAX_ROWS + 1) when missing."""
    depth = 0
    last_return = None
    limit_at = None
    for k, t in enumerate(toks):
        if t.text in ("(", "[", "{"):
            depth += 1
        elif t.text in (")", "]", "}"):
            depth -= 1
        elif depth == 0 and t.kind == "word":
            w = t.text.upper()
            if w == "RETURN":
                last_return, limit_at = k, None
            elif w == "LIMIT" and last_return is not None:
                limit_at = k
    if last_return is None:
        raise _refuse("no RETURN", "End the statement with RETURN (e.g. RETURN r.renewal_id LIMIT 20).")
    end = toks[-1].end
    if limit_at is None:
        return Guarded(query[:end].rstrip() + f" LIMIT {MAX_ROWS + 1}", MAX_ROWS + 1)
    val = toks[limit_at + 1] if limit_at + 1 < len(toks) else None
    if val is None or val.kind != "number" or not val.text.isdigit() or limit_at + 2 != len(toks):
        raise _refuse("LIMIT must be the last clause and a whole number", f"Write LIMIT 20 (at most {MAX_ROWS}).")
    n = int(val.text)
    if n <= MAX_ROWS:
        return Guarded(query[:end].rstrip(), n)
    return Guarded(query[:val.start] + str(MAX_ROWS), MAX_ROWS,
                   [f"LIMIT {n:,} was lowered to {MAX_ROWS} (the row cap)."])


# --------------------------------------------------------------------------- schema + repair
@dataclass
class Catalog:
    """The evidence graph's tables and properties, read once from the engine (CALL show_tables / table_info)."""

    nodes: dict[str, list[str]]
    rels: dict[str, tuple[str, str, list[str]]]

    @classmethod
    def read(cls, conn) -> Catalog:
        nodes, rels = {}, {}
        for row in store.rows(conn.execute("CALL show_tables() RETURN *")):
            name, kind = row[1], row[2]
            props = [r[1] for r in store.rows(conn.execute(f"CALL table_info('{name}') RETURN *"))]
            if kind == "NODE":
                nodes[name] = props
            else:
                ends = store.rows(conn.execute(f"CALL show_connection('{name}') RETURN *"))
                rels[name] = (ends[0][0], ends[0][1], props) if ends else ("?", "?", props)
        return cls(dict(sorted(nodes.items())), dict(sorted(rels.items())))

    def names(self) -> list[str]:
        return sorted({*self.nodes, *self.rels, *(p for ps in self.nodes.values() for p in ps),
                       *(p for _, _, ps in self.rels.values() for p in ps)})

    def text(self) -> str:
        nodes = "; ".join(f"{n}({', '.join(ps)})" for n, ps in self.nodes.items())
        rels = "; ".join(f"{r}({a}->{b}{', ' if ps else ''}{', '.join(ps)})" for r, (a, b, ps) in self.rels.items())
        return f"Nodes: {nodes}. Relationships: {rels}."


def schema_text() -> str:
    """The evidence schema from the code (lakehouse_graph.pruned), for the tool description and resource."""
    nodes = "; ".join(f"{n.label}({', '.join(c for c, _ in n.columns)})" for n in pruned.node_schema().values()
                      if n.label != "Renewal")
    rels = "; ".join(f"{e.rel} {e.src}->{e.dst}({', '.join(c for c, _ in e.columns)})"
                     for e in pruned.edge_schema().values())
    return (f"Renewal(renewal_id, subscription_id, plan_tier, as_of, renewal_date, the 21 gold features, cuts_so_far, "
            f"allowance_at_as_of); {nodes}. Relationships: {rels}.")


_MISSING = (re.compile(r"Table (\w+) does not exist"), re.compile(r"Cannot find property (\w+) for"),
            re.compile(r"[Ff]unction (\w+) does not exist"), re.compile(r"Variable (\w+) is not in scope"))


_PROPERTY_OF = re.compile(r"Cannot find property \w+ for (\w+)")


def repair_hint(message: str, catalog: Catalog, statement: str = "") -> str:
    """The engine's binder / parser message (one line, capped) plus the nearest schema names (and, for a missing
    property, every property of the variable's label when the statement names it)."""
    first = " ".join(message.split())[:400]
    hints = []
    var = _PROPERTY_OF.search(message)
    if var and statement:
        label = re.search(rf"\(\s*{re.escape(var.group(1))}\s*:\s*`?(\w+)", statement)
        props = catalog.nodes.get(label.group(1)) if label else None
        if props is None and label:
            props = (catalog.rels.get(label.group(1)) or (None, None, None))[2]
        if props:
            hints.append(f"properties of {label.group(1)}: {', '.join(props)}")
    for rx in _MISSING:
        m = rx.search(message)
        if not m:
            continue
        name = m.group(1)
        if name.lower() in LABEL_WORDS:
            hints.append(f"{name} is a label property: the evidence graph has none, by design")
            continue
        if name.lower() in pruned.IDENTITY_PROPERTIES:
            hints.append(f"{name} is not in the evidence graph (identity, not evidence): resolve names with "
                         f"graph_find")
            continue
        pool = catalog.names()
        lower = {p.lower(): p for p in pool}
        near = [lower[x] for x in difflib.get_close_matches(name.lower(), list(lower), n=3, cutoff=0.5)]
        if near:
            hints.append(f"nearest schema names to {name}: {', '.join(near)}")
    labels = ", ".join(catalog.nodes)
    rels = ", ".join(f"{r}({a}->{b})" for r, (a, b, _) in catalog.rels.items())
    return f"{first} | {'; '.join(hints) + ' | ' if hints else ''}node labels: {labels} | relationships: {rels}"


# --------------------------------------------------------------------------- sandbox
def sandbox_status(build_dir: Path | None = None) -> tuple[bool, list[str]]:
    """(ok, reasons): is THIS process inside the evidence-only sandbox? Never trusts the environment alone."""
    reasons = []
    if sys.platform != "darwin":
        return False, ["not macOS: there is no OS sandbox here"]
    if os.environ.get("GRAPH_SANDBOXED") != "1":
        reasons.append("GRAPH_SANDBOXED is not 1 (not started by scripts/graph_mcp.sh under sandbox-exec)")
    try:
        libc = ctypes.CDLL(None)
        check = libc.sandbox_check
        check.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int]
        check.restype = ctypes.c_int
        if check(os.getpid(), None, 0) != 1:
            reasons.append("the kernel says this process is not sandboxed (sandbox_check)")
    except (OSError, AttributeError) as e:
        reasons.append(f"sandbox_check is unavailable ({type(e).__name__})")
    probes = [Path("/private/etc/hosts")]
    if build_dir is not None:
        probes += [build_dir / store.DB_FILE, build_dir / "manifest.json",
                   build_dir / "parquet" / "nodes_Renewal.parquet"]
    for p in probes:
        try:
            fd = os.open(p, os.O_RDONLY)
        except PermissionError:
            continue
        except OSError as e:
            if e.errno in (errno.EPERM, errno.EACCES):
                continue
            if e.errno == errno.ENOENT and p.name != "hosts":
                continue
            reasons.append(f"{p} answered {errno.errorcode.get(e.errno, e.errno)}, not 'operation not permitted'")
            continue
        os.close(fd)
        reasons.append(f"{p} is readable: the evidence-only sandbox profile is not in effect")
    return not reasons, reasons


# --------------------------------------------------------------------------- the server-side context and the tool
class CypherContext:
    """One evidence graph, pinned (evidence.json checked), read only; no other file of the build is opened."""

    def __init__(self, build_dir: str | os.PathLike, *, max_chars: int = envelope.DEFAULT_MAX_CHARS,
                 logs_dir: str | os.PathLike | None = None, graph_root: str | os.PathLike | None = None,
                 sandboxed: bool = False, audit: bool = True, on_kill=None):
        self.build_dir = Path(os.path.realpath(build_dir))
        self.max_chars = int(max_chars)
        self.on_kill = on_kill              # the Watchdog's hard stop (None: end the server process)
        try:
            self.record = pruned.read_record(self.build_dir)
        except (OSError, ValueError) as e:
            raise ProvenanceUnavailable(f"no readable {pruned.EVIDENCE_META} in {self.build_dir} ({type(e).__name__}):"
                                        f" run scripts/build_evidence_graph.py on the build") from e
        why = pruned.verify_record(self.build_dir, self.record, check_inputs=False)
        if why is None and self.record.get("code_sha256") != pruned.code_sha256():
            why = "lakehouse_graph/pruned.py changed since it was built"
        if why:
            raise ProvenanceUnavailable(f"evidence graph unusable: {why} (rebuild: scripts/build_evidence_graph.py)")
        self.build_id = self.record.get("business_build_id", "")
        self.sandboxed = bool(sandboxed)
        self.provenance = {**self.record["provenance"], "evidence_db_sha256": self.record["db"]["sha256"],
                           "pit_rule": "physically cut at every renewal's as_of; FIRST_RENEWAL_AFTER after as_of "
                                       "kept flagged declared_exception; label-free",
                           "contract": "evidence-graph checks passed at build", "sandboxed": self.sandboxed}
        # graph_root=None: the per-install HMAC key ($GRAPH_ROOT/.audit_key) is outside the evidence-only sandbox,
        # so a cypher server hashes arguments with a per-process key (args_key "process" in every line)
        self.audit = envelope.AuditLog(logs_dir, build_id=self.build_id, graph_root=None,
                                       enabled=audit and logs_dir is not None)
        del graph_root
        self.db, self.conn = pruned.open_db(self.build_dir / pruned.EVIDENCE_DB)
        self.catalog = Catalog.read(self.conn)

    def envelope(self, data: dict, caveats: list[str] | None = None) -> envelope.Envelope:
        return envelope.make(data, self.provenance, caveats, max_chars=self.max_chars)

    def begin_call(self) -> None:
        """Per-call hook (the MCP wrapper calls it before every tool body)."""

    def interrupt(self) -> None:
        try:
            self.conn.interrupt()
        except Exception as exc:  # noqa: BLE001 - best effort on a timeout / shutdown path
            del exc

    def close(self) -> None:
        self.conn.close()
        self.db.close()


class CypherArgs(BaseModel):
    """graph_cypher's only argument; unknown names are an error."""

    model_config = ConfigDict(extra="forbid")
    query: Annotated[str, StringConstraints(min_length=1, max_length=MAX_QUERY_CHARS), Field(
        description="One read-only Cypher statement over the evidence graph, ending in RETURN (LIMIT <= 200).")]


DESCRIPTION = (
    "Read-only Cypher over the label-free evidence graph: every renewal's events cut at its as_of (T-7), no outcomes, "
    "routes or labels, no SIMILAR_TO; FIRST_RENEWAL_AFTER rows after as_of carry declared_exception=true. One "
    "statement (MATCH / OPTIONAL MATCH / WITH / UNWIND / RETURN), at most 200 rows, paths of at most 4 hops, 5 s. "
    "No writes, files, settings or network. A binder error comes back with the nearest schema names. Prefer the "
    "template tools; use this for evidence questions they cannot answer. Schema: ")
CAVEATS = ["Evidence graph only: events after each renewal's as_of, outcomes, routes and labels are not in it, so a "
           "query cannot see them; FIRST_RENEWAL_AFTER rows with declared_exception=true are the gold rule's flagged "
           "exception.",
           "Raw query results are descriptive; they are not a risk score and not causal."]


def description() -> str:
    return DESCRIPTION + schema_text()


WATCHDOG_GRACE_S = 3.0                 # past the engine timeout by this much: interrupt, then the server exits
WATCHDOG_MAX_RSS = 768 * 1024 * 1024   # a query may raise the process's peak resident memory by at most this
EXIT_WATCHDOG = 75                     # EX_TEMPFAIL: the client sees the server end; a restart serves again


def peak_rss_bytes() -> int:
    import resource

    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(r if sys.platform == "darwin" else r * 1024)


class Watchdog:
    """The hard stop under the engine's own limits. Measured on ladybug 0.21.1: some statements ignore the query
    timeout and conn.interrupt() (``UNWIND range(1, 300000000) ...`` ran on, killed after 25 s), and some grow
    memory outside the 128 MB buffer pool (a cross product reached 2.6 GB). So while a statement runs, a monitor
    thread checks the clock and the process's peak RSS; past TIMEOUT + WATCHDOG_GRACE_S, or once the peak has
    grown by more than WATCHDOG_MAX_RSS since the statement started, it interrupts the query, and if the query still
    has not stopped a second later it ends the whole server process (``on_kill``, os._exit by default): a guarded
    raw query can cost a restart, never the machine."""

    def __init__(self, conn, *, timeout_s: float = TIMEOUT_MS / 1000 + WATCHDOG_GRACE_S,
                 max_rss: int = WATCHDOG_MAX_RSS, on_kill=None):
        self.conn, self.timeout_s, self.max_rss = conn, timeout_s, max_rss
        self.on_kill = on_kill or _exit_server
        self.fired: str | None = None

    def __enter__(self) -> Watchdog:
        import threading

        self._done = threading.Event()
        self._thread = threading.Thread(target=self._watch, name="cypher-watchdog", daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._done.set()
        self._thread.join(timeout=5)

    def _watch(self) -> None:
        deadline = time.monotonic() + self.timeout_s
        base = peak_rss_bytes()
        while not self._done.wait(0.05):
            over = time.monotonic() > deadline
            big = peak_rss_bytes() - base > self.max_rss
            if not (over or big):
                continue
            self.fired = ("the query ran past its time limit" if over else
                          f"the query grew the server's memory by more than {self.max_rss // 2**20} MiB")
            try:
                self.conn.interrupt()
            except Exception as exc:  # noqa: BLE001 - best effort before the hard stop
                del exc
            if not self._done.wait(1.0):
                self.on_kill(self.fired)
            return


def _exit_server(why: str) -> None:
    print(f"graph_cypher watchdog: {why} and the engine did not stop it; the server exits (restart it)",
          file=sys.stderr, flush=True)
    os._exit(EXIT_WATCHDOG)


def graph_cypher(ctx: CypherContext, *, query: str) -> envelope.Envelope:
    """Guard -> EXPLAIN (binder errors come back with the nearest names) -> run, at most MAX_ROWS rows, under the
    engine timeout, the buffer pool and the Watchdog."""
    g = guard(query)
    try:
        with Watchdog(ctx.conn, on_kill=ctx.on_kill):
            ctx.conn.execute("EXPLAIN " + g.statement)
    except RuntimeError as exc:
        if is_interrupted(exc):
            raise ToolTimeout(f"EXPLAIN timed out after {TIMEOUT_MS // 1000} s; simplify the query") from None
        raise CypherRefused("the statement does not bind: " + repair_hint(str(exc), ctx.catalog, g.statement)) \
            from None
    try:
        with Watchdog(ctx.conn, on_kill=ctx.on_kill):
            result = ctx.conn.execute(g.statement)
            columns = list(result.get_column_names())
            rows = []
            while result.has_next() and len(rows) <= MAX_ROWS:
                rows.append(dict(zip(columns, result.get_next(), strict=True)))
    except RuntimeError as exc:
        if is_interrupted(exc):
            raise ToolTimeout(f"the query timed out after {TIMEOUT_MS // 1000} s; narrow it (a label, a renewal id, "
                              f"fewer hops) and retry once") from None
        text = " ".join(str(exc).split())[:300]
        if "buffer" in text.lower() or "memory" in text.lower():
            raise CypherRefused("the query needs more memory than this server allows (128 MB): narrow it") from None
        raise CypherRefused(f"the engine refused the query: {text}") from None
    cut = len(rows) > MAX_ROWS
    rows = rows[:MAX_ROWS]
    data = {"columns": columns, "row_count": len(rows), "more_rows": cut, "rows": rows}
    caveats = list(CAVEATS) + g.notes
    if cut:
        caveats.append(f"More than {MAX_ROWS} rows matched: the first {MAX_ROWS} are shown (add ORDER BY to choose "
                       f"which, or aggregate).")
    return ctx.envelope(data, caveats)


@dataclass(frozen=True)
class CypherSpec:
    """The registry entry mcp_server.make_tool reads (the same fields as tools.ToolSpec)."""

    name: str = TOOL_NAME
    toolset: str = TOOLSET
    args: type = CypherArgs
    title: str = "Guarded read-only Cypher (evidence graph)"
    description: str = field(default_factory=description)


SPEC = CypherSpec()


def call(ctx: CypherContext, name: str, raw: dict | None = None) -> envelope.Envelope:
    """Validate, guard, run and audit one graph_cypher call (the entry point the MCP server uses)."""
    if name != TOOL_NAME:
        raise ToolInputError(f"unknown tool: this server has {TOOL_NAME} only")
    t0 = time.monotonic()
    env: envelope.Envelope | None = None
    outcome = "crash"
    try:
        if raw is not None and not isinstance(raw, dict):
            raise ToolArgumentError(f"invalid arguments for {TOOL_NAME}: expected an object with 'query'")
        try:
            args = CypherArgs.model_validate(raw or {})
        except ValidationError as exc:
            kinds = {e["type"] for e in exc.errors(include_url=False, include_input=False)}
            what = "unknown argument (allowed: query)" if "extra_forbidden" in kinds else \
                f"query must be one statement of 1-{MAX_QUERY_CHARS:,} characters"
            raise ToolArgumentError(f"invalid arguments for {TOOL_NAME}: {what}") from None
        env = graph_cypher(ctx, query=args.query)
        outcome = "ok"
        return env
    except ToolInputError as exc:
        outcome = exc.outcome
        raise
    finally:
        ctx.audit.record(toolset=TOOLSET, tool=TOOL_NAME, args=raw if isinstance(raw, dict) else {},
                         latency_ms=envelope.monotonic_ms(t0), rows=envelope.count_rows(env["data"]) if env else 0,
                         chars=len(envelope.compact_json(env)) if env else 0,
                         truncated=bool(env["truncated"]) if env else False, outcome=outcome)
