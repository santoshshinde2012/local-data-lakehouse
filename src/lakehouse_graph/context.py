"""ToolContext: everything one serving process pins for its lifetime, opened lazily and read only.

A ToolContext is bound to ONE build directory, resolved to its real path once (a later swap of
``$GRAPH_ROOT/current`` cannot change what a running server serves; a new build is picked up by a
restart). At construction it refuses a build it cannot vouch for (``ProvenanceUnavailable``):

  * manifest.json must be present and every Parquet file it lists must have the recorded SHA-256
    (``verify="sha256"``, about 30 MB at seed 42) or at least the recorded row count (``"rows"``);
  * the installed ladybug version must equal the one that loaded graph.lbdb (rebuild, never migrate);
  * unless ``allow_unchecked`` is given, the build must carry a passing graph contract
    (contract.json for this very build id and these Parquet bytes; strict or not).

Then it serves, lazily and behind one lock:
  * ``provenance``      the dict every answer embeds (PLAN 6.5, kept compact for small context windows);
  * pandas frames       Renewal, Subscription (never the city column), hubs, a few edge tables;
  * ``graph_conn``      a read-only Ladybug connection to graph.lbdb: 128 MB buffer pool, 2 threads,
                        5 s query timeout, pybind backend only (a missing native backend fails loudly
                        instead of falling back to a loader that searches the working directory);
                        no engine extension is ever installed or loaded;
  * ``lineage_conn``    the same for lineage.lbdb, or None when the build has no lineage
                        (the interface lakehouse_graph.lineage.tools expects);
  * ``search``          the entity index of lakehouse_graph.search;
  * ``audit``           the JSONL audit log (lakehouse_graph.envelope.AuditLog).

``interrupt()`` stops the query running on any open connection (the MCP wrapper calls it on a
timeout or a client cancel); ``begin_call()`` is the per-call hook.

Errors raised by tools (``ToolInputError`` and its kinds) are defined here so that this module,
tools.py and metrics.py share them without importing each other in a cycle. Only their text reaches
the model; every other exception is a crash whose text stays on the server.

Nothing here writes outside the logs directory, uses a temp dir, starts a subprocess or reads the
home directory, so the same code runs under the macOS sandbox profile (config/graph/sandbox.sb).
"""
from __future__ import annotations

import decimal
import importlib.metadata
import logging
import os
import threading
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from . import envelope, queries, spec, store
from . import manifest as mf

log = logging.getLogger("lakehouse_graph.context")

GRAPH_POOL_MB = store.SERVE_BUFFER_POOL_MB
LINEAGE_POOL_MB = store.SERVE_BUFFER_POOL_MB
THREADS = store.THREADS
QUERY_TIMEOUT_MS = store.QUERY_TIMEOUT_MS
LINEAGE_DB_FILE = "lineage.lbdb"
COHORTS_FILE = "cohorts.parquet"
CURRENT_ROUTES = ("score_today", "pending")   # their as_of is the current T-7; every other route is history
PIT_RULE_SHORT = ("e.event_date <= r.as_of (+ feature window); FIRST_RENEWAL_AFTER by the gold rule, flagged "
                  "known_by_as_of / declared_exception; BILLED outcome evidence never served")
VERIFY_MODES = ("sha256", "rows")
_MB = 1024 * 1024
_SUBSCRIPTION_COLUMNS = ["subscription_id", "user_name", "plan_tier", "started_at"]  # city is never loaded


# --------------------------------------------------------------------------- errors
class ToolInputError(ValueError):
    """An anticipated, model-repairable failure: its text reaches the model (as an MCP ToolError)."""

    outcome = "input_error"


class ToolArgumentError(ToolInputError):
    """The arguments failed validation (unknown name, wrong type, out of range, bad id format)."""

    outcome = "invalid_arguments"


class ToolUnavailable(ToolInputError):
    """The build lacks what the tool needs (lineage.lbdb, cohorts.parquet)."""

    outcome = "unavailable"


class ToolTimeout(ToolInputError):
    """The engine query hit the query timeout or was interrupted."""

    outcome = "timeout"


class ToolBusy(ToolInputError):
    """Another call holds the connection."""

    outcome = "busy"


class ProvenanceUnavailable(RuntimeError):
    """The build cannot be vouched for (files differ from the manifest, wrong engine, no contract pass)."""


# --------------------------------------------------------------------------- build checks
def resolve_build(build: str | os.PathLike | None, graph_root: str | os.PathLike | None = None) -> tuple[Path, Path]:
    """(real build dir, real graph root): ``build`` or $GRAPH_ROOT/current, which must lie inside the root."""
    root = Path(os.path.realpath(spec.graph_root(graph_root)))
    raw = Path(build) if build else spec.current_link(root)
    real = Path(os.path.realpath(raw))
    if not real.is_dir():
        raise ProvenanceUnavailable(f"build directory not found: {raw} (build a graph first: make graph-local)")
    if root not in real.parents:
        raise ProvenanceUnavailable(f"build {real} is not inside GRAPH_ROOT {root}: refusing to serve it")
    return real, root


def verify_build(build_dir: Path, man: dict, mode: str = "sha256") -> None:
    """Raise ProvenanceUnavailable unless the build's files and engine are what its manifest says."""
    if mode not in VERIFY_MODES:
        raise ValueError(f"verify must be one of {VERIFY_MODES}")
    files = man.get("files") or {}
    if not files:
        raise ProvenanceUnavailable(f"provenance unavailable: {build_dir / mf.MANIFEST_FILE} lists no files")
    for rel, rec in sorted(files.items()):
        path = build_dir / rel
        if not path.is_file():
            raise ProvenanceUnavailable(f"provenance unavailable: {rel} is missing from {build_dir}")
        if mode == "sha256":
            if mf.sha256_file(path) != rec.get("sha256"):
                raise ProvenanceUnavailable(f"provenance unavailable: {rel} differs from the sha256 in the manifest "
                                            f"(rebuild: make graph-build)")
        elif pq.ParquetFile(path).metadata.num_rows != rec.get("rows"):
            raise ProvenanceUnavailable(f"provenance unavailable: {rel} row count differs from the manifest")
    pinned = (man.get("versions") or {}).get("ladybug")
    installed = importlib.metadata.version("ladybug")
    if pinned != installed:
        raise ProvenanceUnavailable(f"provenance unavailable: graph.lbdb was loaded by ladybug {pinned}, this "
                                    f"interpreter has {installed} (rebuild, never migrate: make graph-build)")
    if not (build_dir / store.DB_FILE).is_file():
        raise ProvenanceUnavailable(f"provenance unavailable: {store.DB_FILE} is missing from {build_dir}")


def contract_state(build_dir: Path, man: dict) -> dict:
    """The build's own graph contract record: state strict_pass | pass | fail | stale | absent."""
    c = store.read_contract(build_dir)
    if not c:
        return {"state": "absent"}
    same = (c.get("business_build_id") == man.get("business_build_id")
            and c.get("files_sha256") == {k: v.get("sha256") for k, v in (man.get("files") or {}).items()})
    if not same:
        state = "stale"
    elif c.get("status") != "pass":
        state = "fail"
    else:
        state = "strict_pass" if store.is_strict_pass(c, man) else "pass"
    return {"state": state, "strict": bool(c.get("strict")), "checked_at": c.get("checked_at"),
            "golden": c.get("golden")}


def provenance(build_dir: Path, man: dict, contract: dict, sandboxed: bool) -> dict:
    """The compact provenance block of every answer (combined hashes; per-file hashes are in manifest.json)."""
    exports = (man.get("exports") or {}).get("sha256") or {}
    out: dict[str, Any] = {
        "build_id": man.get("business_build_id"),
        "profile": man.get("profile"),
        "spec": dict(man.get("spec") or spec.SPEC_VERSIONS),
        "inputs_sha256": (man.get("inputs") or {}).get("combined_sha256"),
        "code_sha256": mf.sha256_json(man.get("code_sha256") or {}),
        "exports_sha256": mf.sha256_json(exports) if exports else None,
        "manifest_sha256": mf.sha256_file(build_dir / mf.MANIFEST_FILE),
        "seed": man.get("seed"), "n_users": man.get("n_users"), "seed_n_status": man.get("seed_n_status"),
        "commit": man.get("commit"), "dirty": man.get("dirty"), "data_end": man.get("data_end"),
        "synthetic": bool(man.get("synthetic", True)),
        "pit_rule": PIT_RULE_SHORT,
        "contract": contract.get("state"),
        "sandboxed": sandboxed,
    }
    lineage = man.get("lineage") or {}
    if lineage:
        out["spec"]["lineage"] = lineage.get("spec")
        out["lineage_build_id"] = lineage.get("lineage_build_id")
    cohorts = man.get("cohorts") or {}
    if cohorts:
        out["spec"]["cohorts"] = cohorts.get("spec") or cohorts.get("spec_version")
    ice = man.get("iceberg") or {}
    if ice:
        tables = sorted((ice.get("tables") or {}).items())
        gold = next((t for name, t in tables if name.endswith("churn_renewal_features")), {})
        out["iceberg"] = {"tag": ice.get("tag"), "lakehouse_build_id": ice.get("lakehouse_build_id"),
                          "gold_table_uuid": gold.get("table_uuid"), "gold_snapshot_id": gold.get("snapshot_id")}
    return out


# --------------------------------------------------------------------------- context
class ToolContext:
    """One build, pinned: manifest, provenance, lazy frames and lazy read-only connections."""

    def __init__(self, build_dir: str | os.PathLike, *, max_chars: int = envelope.DEFAULT_MAX_CHARS,
                 max_rows: int = envelope.MAX_ROWS, logs_dir: str | os.PathLike | None = None,
                 graph_root: str | os.PathLike | None = None, audit: bool = True, verify: str = "sha256",
                 allow_unchecked: bool = False, sandboxed: bool | None = None):
        if not envelope.MIN_MAX_CHARS <= int(max_chars) <= envelope.MAX_MAX_CHARS:
            raise ValueError(f"max_chars must be between {envelope.MIN_MAX_CHARS} and {envelope.MAX_MAX_CHARS}")
        if not 1 <= int(max_rows) <= envelope.MAX_ROWS:
            raise ValueError(f"max_rows must be between 1 and {envelope.MAX_ROWS}")
        self.build_dir = Path(os.path.realpath(build_dir))
        self.max_chars, self.max_rows = int(max_chars), int(max_rows)
        if not (self.build_dir / mf.MANIFEST_FILE).is_file():
            raise ProvenanceUnavailable(f"provenance unavailable: no {mf.MANIFEST_FILE} in {self.build_dir}")
        try:
            self.manifest = mf.read_manifest(self.build_dir)
        except (OSError, ValueError) as exc:
            raise ProvenanceUnavailable(f"provenance unavailable: unreadable manifest ({type(exc).__name__})") from exc
        verify_build(self.build_dir, self.manifest, verify)
        self.contract = contract_state(self.build_dir, self.manifest)
        if not allow_unchecked and self.contract["state"] not in ("strict_pass", "pass"):
            raise ProvenanceUnavailable(
                f"no passing graph contract for build {self.manifest.get('business_build_id')} "
                f"(contract.json: {self.contract['state']}): run make graph-check (scripts/check_graph_contract.py) "
                f"on it, or pass --allow-unchecked for a scratch build")
        self.sandboxed = (os.environ.get("GRAPH_SANDBOXED") == "1") if sandboxed is None else bool(sandboxed)
        self.provenance = provenance(self.build_dir, self.manifest, self.contract, self.sandboxed)
        self.build_id = self.manifest.get("business_build_id", "")
        self.graph_root = spec.graph_root(graph_root)   # argument > $GRAPH_ROOT > <repo>/data/graph
        logs = logs_dir or os.environ.get("GRAPH_LOGS_DIR") or (self.graph_root / "logs")
        if audit and not Path(logs).is_dir():
            try:
                Path(logs).mkdir(parents=True, exist_ok=True)   # the launcher creates it; denied under the sandbox
            except OSError:
                pass
        self.audit = envelope.AuditLog(logs, build_id=self.build_id, graph_root=self.graph_root, enabled=audit)
        self._lock = threading.RLock()
        self._frames: dict[str, Any] = {}
        self._dbs: dict[str, tuple[Any, Any]] = {}
        self._search = None

    # ------------------------------------------------------------------ answers
    def envelope(self, data: dict, caveats: list[str] | None = None) -> envelope.Envelope:
        return envelope.make(data, self.provenance, caveats, max_chars=self.max_chars, max_rows=self.max_rows)

    def begin_call(self) -> None:
        """Per-call hook (the MCP wrapper calls it before every tool body)."""

    # ------------------------------------------------------------------ frames
    def _frame(self, key: str, loader) -> Any:
        with self._lock:
            if key not in self._frames:
                self._frames[key] = loader()
            return self._frames[key]

    def cached(self, key: str, loader) -> Any:
        """A value computed once per context (a build is immutable while it is served)."""
        return self._frame(f"cached:{key}", loader)

    def _parquet(self, rel: str, columns: list[str] | None = None) -> pd.DataFrame:
        return pq.read_table(self.build_dir / rel, columns=columns).to_pandas()

    def renewals(self) -> pd.DataFrame:
        """Renewal nodes indexed by renewal_id (dates as datetime.date)."""
        return self._frame("Renewal", lambda: self._parquet(f"parquet/{spec.NODE_SCHEMA['Renewal'].file}")
                           .set_index("renewal_id", drop=False))

    def subscriptions(self) -> pd.DataFrame:
        """Subscription nodes indexed by subscription_id: user_name, plan_tier, started_at (no city)."""
        return self._frame("Subscription", lambda: self._parquet(
            f"parquet/{spec.NODE_SCHEMA['Subscription'].file}", _SUBSCRIPTION_COLUMNS)
            .set_index("subscription_id", drop=False))

    def nodes(self, label: str) -> pd.DataFrame:
        if label == "Subscription":
            return self.subscriptions()
        if label not in spec.NODE_SCHEMA:
            raise KeyError(label)
        return self._frame(label, lambda: self._parquet(f"parquet/{spec.NODE_SCHEMA[label].file}"))

    def edges(self, rel: str) -> pd.DataFrame:
        if rel not in spec.EDGE_SCHEMA:
            raise KeyError(rel)
        return self._frame(f"edges:{rel}", lambda: self._parquet(f"parquet/{spec.EDGE_SCHEMA[rel].file}"))

    def zscores(self) -> tuple[dict[str, int], np.ndarray, list[str]]:
        """(row of renewal_id, z-score matrix over the 20 SIMILAR_TO features, feature names), persisted scaler."""
        def load():
            scaler = self._parquet(spec.SCALER_FILE)
            ren = self.renewals()
            feats = list(scaler["feature"])
            x = ren[feats].to_numpy(dtype=np.float64)
            mean = scaler["mean"].to_numpy(dtype=np.float64)
            std = scaler["std"].to_numpy(dtype=np.float64)
            z = np.where(std > 0, (x - mean) / np.where(std > 0, std, 1.0), 0.0)
            return {rid: i for i, rid in enumerate(ren.index)}, z, feats
        return self._frame("zscores", load)

    def renewal(self, renewal_id: str) -> pd.Series | None:
        ren = self.renewals()
        return ren.loc[renewal_id] if renewal_id in ren.index else None

    def has_lineage(self) -> bool:
        return (self.build_dir / LINEAGE_DB_FILE).is_file()

    def has_cohorts(self) -> bool:
        return (self.build_dir / COHORTS_FILE).is_file()

    # ------------------------------------------------------------------ engine
    def _open(self, name: str, path: Path, pool_mb: int):
        with self._lock:
            if name not in self._dbs:
                import ladybug as lb

                db = lb.Database(str(path), read_only=True, buffer_pool_size=pool_mb * _MB, max_num_threads=THREADS,
                                 backend="pybind")
                try:
                    conn = store.connect(db, THREADS, QUERY_TIMEOUT_MS)
                except BaseException:
                    db.close()
                    raise
                self._dbs[name] = (db, conn)
            return self._dbs[name][1]

    @property
    def graph_conn(self):
        return self._open("graph", self.build_dir / store.DB_FILE, GRAPH_POOL_MB)

    @property
    def lineage_conn(self):
        if not self.has_lineage():
            return None
        return self._open("lineage", self.build_dir / LINEAGE_DB_FILE, LINEAGE_POOL_MB)

    def fetch(self, name: str, params: dict | None = None) -> list[dict]:
        """A named TOOL template on graph.lbdb (contract-only templates are refused by queries.fetch)."""
        try:
            rows = queries.fetch(self.graph_conn, name, params)
        except RuntimeError as exc:
            if is_interrupted(exc):
                raise ToolTimeout(f"the graph query timed out after {QUERY_TIMEOUT_MS // 1000} s; ask for less "
                                  f"(a smaller k or limit) and retry once") from None
            raise
        return [{k: _native(v) for k, v in r.items()} for r in rows]

    def evidence(self, renewal_id: str, limit: int) -> list[dict]:
        """queries.evidence() (the PIT-safe evidence merge) with the timeout conversion."""
        try:
            return queries.evidence(self.graph_conn, renewal_id, limit)
        except RuntimeError as exc:
            if is_interrupted(exc):
                raise ToolTimeout(f"the evidence query timed out after {QUERY_TIMEOUT_MS // 1000} s; retry once") \
                    from None
            raise

    def interrupt(self) -> None:
        """Stop the query running on every open connection (harmless when idle)."""
        for _db, conn in list(self._dbs.values()):
            try:
                conn.interrupt()
            except Exception as exc:  # noqa: BLE001 - best effort on a timeout / shutdown path
                log.debug("interrupt failed: %s", type(exc).__name__)

    def close(self) -> None:
        with self._lock:
            for db, conn in self._dbs.values():
                conn.close()
                db.close()
            self._dbs.clear()

    # ------------------------------------------------------------------ search
    @property
    def search(self):
        with self._lock:
            if self._search is None:
                from .search import EntityIndex

                self._search = EntityIndex.from_context(self)
            return self._search


def is_interrupted(exc: BaseException) -> bool:
    """Ladybug's timeout / interrupt error ('Interrupted.')."""
    return isinstance(exc, RuntimeError) and str(exc).strip().lower().startswith("interrupted")


def _native(value: Any) -> Any:
    """Ladybug returns sum() as Decimal: integral ones become int, others float; everything else as is."""
    if isinstance(value, decimal.Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    return value
