"""Write the lineage graph as deterministic Parquet, project it into lineage.lbdb, record identity.

  <build_dir>/lineage/nodes_<Label>.parquet   one per node label (24; Snapshot, Ref, Run empty unless an
                                              overlay fills them)
  <build_dir>/lineage/edges_<TYPE>.parquet    one per edge type (48; the 7 Tier-1 / Tier-2 types empty
                                              unless an overlay fills them); columns src, dst, src_label,
                                              dst_label + properties
  <build_dir>/lineage/manifest.json           identity, inputs, counts, unresolved names, warnings (about
                                              the repo; fatal with --strict), environment_warnings
                                              (about this machine: no RADAR_DIR, no exports, the state of
                                              the lakehouse or an OpenLineage file; never fatal) and
                                              overlays (what --iceberg / --openlineage read)
  <build_dir>/lineage.lbdb                    Ladybug projection (rebuildable; Parquet is canonical)
  <build_dir>/manifest.json["lineage"]        a summary, when the business manifest exists

``lineage_build_id`` = first 12 hex chars of sha256 over (canonical JSON): the sha256 of every
file the extractor read (SQL, jobs, scripts, DAGs, Makefile, shell, CI YAML, README, the graph
spec; plus data and radar files in the full profile; plus the Iceberg metadata facts and the
pins of the business build with --iceberg, and the OpenLineage file with --openlineage), the
sha256 of the lineage code that shapes content, the spec version, the profile and the sqlglot /
pyarrow / ladybug versions.
It shares no input with the bronze data: a README edit re-keys the lineage graph and leaves
``business_build_id`` alone, and new bronze re-keys the business graph only.

The build is read-only towards the repo and offline (the one network path is the explicit
``radar_public`` flag). The new tables are written next to the old ones and swapped in
(atomic directory exchange + file replace), under the graph root's build lock when the
target is a profile build. An unchanged rebuild (same id, byte-identical Parquet) is kept.

Loader CLI (run in a child process, like the business graph loader):
  python -m lakehouse_graph.lineage.build load --lineage-dir <dir> --db <file>
"""
from __future__ import annotations

import argparse
import contextlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .. import build as gbuild
from .. import manifest as mf
from .. import spec as gspec
from .. import store
from . import extract as ex
from . import spec
from .graph import LineageGraph, assemble
from .spec import LineageExtractError

TMP_PREFIX = ".lineage-tmp-"


class LineageBuildError(RuntimeError):
    """The environment violates a lineage build precondition (or a rebuild is not deterministic)."""


# --------------------------------------------------------------------------- identity
def code_hashes() -> dict[str, str]:
    """sha256 of the lineage modules that shape content (of the code that is running)."""
    root = Path(__file__).resolve().parents[3]
    return {rel: (mf.sha256_file(root / rel) if (root / rel).is_file() else "missing") for rel in spec.CONTENT_CODE}


def lineage_identity(graph: LineageGraph) -> dict:
    payload = {"spec": spec.SPEC_VERSION, "profile": graph.profile, "inputs": dict(sorted(graph.inputs.items())),
               "code": code_hashes(), "versions": mf.package_versions(["sqlglot", "pyarrow", "ladybug"])}
    return {"lineage_build_id": mf.sha256_json(payload)[:12], "payload": payload}


# --------------------------------------------------------------------------- Parquet
def _cell(value, typ: pa.DataType):
    if value is None:
        return None
    if pa.types.is_string(typ):
        if isinstance(value, (list, tuple, dict, set, frozenset)):
            return ex.dumps(sorted(value) if isinstance(value, (set, frozenset)) else value)
        return value if isinstance(value, str) else str(value)
    if pa.types.is_int64(typ):
        return int(value)
    if pa.types.is_float64(typ):
        return float(value)
    return bool(value)


def node_rows(graph: LineageGraph) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {label: [] for label in spec.NODE_SCHEMA}
    for nid in sorted(graph.nodes):
        n = graph.nodes[nid]
        ns = spec.NODE_SCHEMA[n["label"]]
        out[n["label"]].append({"id": nid, **{c: _cell(n["props"].get(c), t) for c, t in ns.columns}})
    return out


def edge_rows(graph: LineageGraph) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {rel: [] for rel in spec.EDGE_SCHEMA}
    for e in graph.edges:
        es = spec.EDGE_SCHEMA[e["rel"]]
        out[e["rel"]].append({"src": e["src"], "dst": e["dst"], "src_label": graph.label(e["src"]),
                              "dst_label": graph.label(e["dst"]),
                              **{c: _cell(e["props"].get(c), t) for c, t in es.columns}})
    for rel, rows in out.items():
        names = [c for c, _ in spec.EDGE_SCHEMA[rel].all_columns]
        rows.sort(key=lambda r, names=names: tuple("" if r[c] is None else str(r[c]) for c in names))
    return out


def table_files() -> list[tuple[str, str, pa.Schema]]:
    """(table name, file name, schema) for every lineage Parquet file, in write order."""
    return [(n.label, n.file, n.schema) for n in spec.NODE_SCHEMA.values()] + \
           [(e.rel, e.file, e.schema) for e in spec.EDGE_SCHEMA.values()]


def write_tables(graph: LineageGraph, out_dir: Path) -> dict[str, dict]:
    """Explicit schema, fixed row and column order, the business builder's writer options."""
    out_dir.mkdir(parents=True, exist_ok=True)
    rows = {**node_rows(graph), **edge_rows(graph)}
    files = {}
    for name, file, schema in table_files():
        path = out_dir / file
        pq.write_table(pa.Table.from_pylist(rows[name], schema=schema), path, **gbuild.PARQUET_OPTIONS)
        files[file] = {"table": name, "rows": len(rows[name]), "sha256": mf.sha256_file(path)}
    return files


def read_tables(lineage_dir: str | os.PathLike) -> tuple[dict[str, list[dict]], dict[str, list[dict]]]:
    """(nodes by label, edges by type) as lists of dicts, from the Parquet tables."""
    d = Path(lineage_dir)
    nodes = {n.label: pq.read_table(d / n.file).to_pylist() for n in spec.NODE_SCHEMA.values()}
    edges = {e.rel: pq.read_table(d / e.file).to_pylist() for e in spec.EDGE_SCHEMA.values()}
    return nodes, edges


# --------------------------------------------------------------------------- Ladybug
def _lb_type(t: pa.DataType) -> str:
    if pa.types.is_string(t):
        return "STRING"
    if pa.types.is_int64(t):
        return "INT64"
    if pa.types.is_float64(t):
        return "DOUBLE"
    if pa.types.is_boolean(t):
        return "BOOLEAN"
    raise ValueError(f"no Ladybug type for arrow type {t}")


def ddl_statements() -> list[str]:
    """CREATE NODE / REL TABLE statements generated from the lineage spec (identifiers quoted)."""
    out = []
    for n in spec.NODE_SCHEMA.values():
        cols = ", ".join(f"`{c}` {_lb_type(t)}" for c, t in n.all_columns)
        out.append(f"CREATE NODE TABLE `{n.label}`({cols}, PRIMARY KEY(`id`))")
    for e in spec.EDGE_SCHEMA.values():
        pairs = ", ".join(f"FROM `{a}` TO `{b}`" for a, b in e.pairs)
        props = "".join(f", `{c}` {_lb_type(t)}" for c, t in e.columns)
        out.append(f"CREATE REL TABLE `{e.rel}`({pairs}{props})")
    return out


def count_all(conn) -> dict[str, int]:
    counts = {}
    for label in spec.NODE_SCHEMA:
        counts[label] = int(store.rows(conn.execute(f"MATCH (n:`{label}`) RETURN count(n)"))[0][0])
    for rel in spec.EDGE_SCHEMA:
        counts[rel] = int(store.rows(conn.execute(f"MATCH ()-[e:`{rel}`]->() RETURN count(e)"))[0][0])
    return counts


def _copy_path(path: Path) -> str:
    s = str(path)
    if "'" in s or "\\" in s:
        raise ValueError(f"unsupported character in path for COPY: {s}")
    return f"'{s}'"


def load_ladybug(lineage_dir: str | os.PathLike, db_path: str | os.PathLike,
                 buffer_pool_mb: int = store.LOAD_BUFFER_POOL_MB, threads: int = store.THREADS) -> dict:
    """COPY every node table, then every edge table (one COPY per FROM/TO pair) into a fresh
    database, then CHECKPOINT. Edge files are staged per pair in a temp directory that is
    removed afterwards (a rel table with several pairs is loaded one pair at a time)."""
    import ladybug as lb

    lineage_dir, db_path = Path(lineage_dir), Path(db_path)
    if db_path.exists():
        raise FileExistsError(f"{db_path} exists: the loader only writes a fresh database (rebuild, never migrate)")
    t0 = time.perf_counter()
    stage = Path(tempfile.mkdtemp(prefix=".lineage-stage-", dir=db_path.parent))
    db = lb.Database(str(db_path), buffer_pool_size=buffer_pool_mb * 1024 * 1024, max_num_threads=threads)
    conn = lb.Connection(db, num_threads=threads)
    try:
        # One transaction for the ~70 DDL and ~75 COPY statements: committing each on its own
        # costs a log flush per statement, which dominates the load of these tiny tables.
        conn.execute("BEGIN TRANSACTION")
        for stmt in ddl_statements():
            conn.execute(stmt)
        for n in spec.NODE_SCHEMA.values():
            if pq.read_metadata(lineage_dir / n.file).num_rows:
                conn.execute(f"COPY `{n.label}` FROM {_copy_path(lineage_dir / n.file)}")
        for e in spec.EDGE_SCHEMA.values():
            table = pq.read_table(lineage_dir / e.file)
            if not table.num_rows:
                continue
            keep = ["src", "dst"] + [c for c, _ in e.columns]
            src_label, dst_label = table["src_label"].to_pylist(), table["dst_label"].to_pylist()
            for a, b in e.pairs:
                mask = pa.array([x == a and y == b for x, y in zip(src_label, dst_label, strict=True)])
                part = table.filter(mask).select(keep)
                if not part.num_rows:
                    continue
                staged = stage / f"{e.rel}__{a}__{b}.parquet"
                pq.write_table(part, staged)
                option = f" (from='{a}', to='{b}')" if len(e.pairs) > 1 else ""
                conn.execute(f"COPY `{e.rel}` FROM {_copy_path(staged)}{option}")
        conn.execute("COMMIT")
        conn.execute("CHECKPOINT")
        load_s = time.perf_counter() - t0
        counts = count_all(conn)
    finally:
        conn.close()
        db.close()
        shutil.rmtree(stage, ignore_errors=True)
    size = sum(p.stat().st_size for p in db_path.parent.glob(db_path.name + "*") if p.is_file())
    return {"db": spec.DB_FILE, "load_s": round(load_s, 2), "counts": counts, "db_bytes": int(size),
            "buffer_pool_mb": buffer_pool_mb, "threads": threads}


def load_ladybug_subprocess(lineage_dir: Path, db_path: Path) -> dict:
    """Run the loader in a child process (isolates an engine crash from the builder)."""
    src = str(Path(__file__).resolve().parents[2])
    env = dict(os.environ, PYTHONPATH=os.pathsep.join(filter(None, [src, os.environ.get("PYTHONPATH")])))
    cmd = [sys.executable, "-m", "lakehouse_graph.lineage.build", "load", "--lineage-dir", str(lineage_dir),
           "--db", str(db_path)]
    p = subprocess.run(cmd, env=env, capture_output=True, text=True, check=False)
    if p.returncode:
        raise LineageBuildError(f"Ladybug load failed (exit {p.returncode}): {p.stderr.strip()[-2000:]}")
    return json.loads(p.stdout)


# --------------------------------------------------------------------------- manifest
def read_manifest(build_dir: str | os.PathLike) -> dict:
    return json.loads((Path(build_dir) / spec.LINEAGE_DIR / spec.MANIFEST_FILE).read_text())


def graph_root_of(build_dir: Path) -> Path | None:
    """The graph root when ``build_dir`` is <root>/<profile>/builds/<id> (then the build lock applies)."""
    build_dir = build_dir.resolve()
    if build_dir.parent.name == "builds" and gspec.is_profile(build_dir.parent.parent.name):
        return build_dir.parents[2]
    return None


def is_fresh(man: dict, repo: str | os.PathLike | None = None) -> bool:
    """True while every file the lineage build read, and the lineage code, is unchanged."""
    root = Path(repo) if repo else gspec.repo_root()
    for rel, digest in man.get("inputs_sha256", {}).items():
        if ":" in rel:   # data: / radar: entries of the full profile are pinned, not re-read here
            continue
        if not (root / rel).is_file() or mf.sha256_file(root / rel) != digest:
            return False
    return man.get("code_sha256") == code_hashes()


def _summary(man: dict) -> dict:
    return {"lineage_build_id": man["lineage_build_id"], "profile": man["profile"], "spec": man["spec"],
            "contract": man["contract"], "total_nodes": man["counts"]["total_nodes"],
            "total_edges": man["counts"]["total_edges"], "unresolved": len(man["unresolved"]),
            "overlays": sorted(man.get("overlays") or {}),
            "files_sha256": mf.sha256_json({k: v["sha256"] for k, v in man["files"].items()}),
            "manifest": f"{spec.LINEAGE_DIR}/{spec.MANIFEST_FILE}", "db": spec.DB_FILE, "built_at": man["built_at"]}


# --------------------------------------------------------------------------- orchestration
def build_lineage(build_dir: str | os.PathLike, profile: str = "core", *, repo: str | os.PathLike | None = None,
                  export_dir: str | os.PathLike | None = None, sample_dir: str | os.PathLike | None = None,
                  radar_dir: str | os.PathLike | None = None, radar_public: bool = False, rebuild: bool = False,
                  lock_timeout: float = 600.0, overlays=(), log=print) -> tuple[Path, dict]:
    """Extract, write and load the lineage graph into ``build_dir``; returns (lineage dir, manifest).

    ``build_dir`` is an existing business build directory or any scratch directory (it is
    created). ``core`` ignores ``export_dir`` / ``sample_dir`` / ``radar_dir`` / ``radar_public``.
    ``overlays`` are ``f(graph)`` callables applied after the Tier-0 assembly.
    """
    t0 = time.perf_counter()
    if profile not in spec.PROFILES:
        raise ValueError(f"invalid lineage profile {profile!r}: expected one of {', '.join(spec.PROFILES)}")
    bdir = Path(build_dir).absolute()
    bdir.mkdir(parents=True, exist_ok=True)
    root = graph_root_of(bdir)
    lock = store.BuildLock(root, timeout=lock_timeout, log=log) if root else contextlib.nullcontext()
    final_dir, final_db = bdir / spec.LINEAGE_DIR, bdir / spec.DB_FILE
    tmp_dir = bdir / f"{TMP_PREFIX}{os.getpid()}"
    tmp_db = bdir / f"{TMP_PREFIX}{os.getpid()}.lbdb"
    with lock:
        _clean_leftovers(bdir)
        try:
            graph = assemble(repo, profile, export_dir=export_dir, sample_dir=sample_dir, radar_dir=radar_dir,
                             radar_public=radar_public, overlays=overlays)
            ident = lineage_identity(graph)
            lid = ident["lineage_build_id"]
            log(f"==> lineage build: profile {profile}, spec {spec.SPEC_VERSION}, lineage_build_id {lid}, "
                f"{len(graph.inputs)} files read")
            files = write_tables(graph, tmp_dir)
            counts = graph.counts()
            old = None
            if final_dir.is_dir() and not rebuild:
                with contextlib.suppress(OSError, ValueError):
                    old = read_manifest(bdir)
            if old and old.get("lineage_build_id") == lid and final_db.is_file():
                same = {k: v["sha256"] for k, v in old.get("files", {}).items()} == \
                       {k: v["sha256"] for k, v in files.items()}
                if not same:
                    raise LineageBuildError(
                        f"determinism violation: {final_dir} has the same lineage_build_id {lid} but different "
                        f"Parquet bytes (rerun with --rebuild to replace it)")
                shutil.rmtree(tmp_dir)
                log(f"    unchanged: rebuilt lineage Parquet is byte-identical to the existing one ({len(files)} "
                    f"files, sha256 equal); kept it")
                _record_in_business_manifest(bdir, old)
                return final_dir, old
            ld = load_ladybug_subprocess(tmp_dir, tmp_db)
            want = {**counts["nodes"], **counts["edges"]}
            bad = {k: (want[k], ld["counts"].get(k)) for k in want if ld["counts"].get(k) != want[k]}
            if bad:
                raise LineageBuildError(f"Ladybug counts differ from Parquet counts: {bad}")
            payload = ident["payload"]
            man = {
                "manifest_version": 1, "lineage_build_id": lid, "profile": profile, "spec": spec.SPEC_VERSION,
                "contract": spec.CONTRACT_VERSION, "inputs_sha256": payload["inputs"], "code_sha256": payload["code"],
                "versions": {**payload["versions"], "python": sys.version.split()[0]},
                "sources": {"repo": mf.display_path(Path(repo) if repo else gspec.repo_root()) or ".",
                            "export_dir": mf.display_path(export_dir) if export_dir and profile == "full" else None,
                            "sample_dir": mf.display_path(sample_dir) if sample_dir and profile == "full" else None,
                            "radar_dir": str(radar_dir) if radar_dir and profile == "full" else None,
                            "radar_public": bool(radar_public and profile == "full")},
                "files": files, "counts": counts, "unresolved": graph.unresolved, "warnings": graph.warnings,
                "environment_warnings": graph.environment_warnings, "overlays": graph.overlays,
                "ladybug": {**{k: ld[k] for k in ("db", "load_s", "db_bytes", "buffer_pool_mb", "threads")},
                            "version": payload["versions"].get("ladybug"), "counts_equal_parquet": True},
                "builder": {"seconds": round(time.perf_counter() - t0, 2)},
                **mf.git_state(repo), "built_at": mf.utc_now(),
            }
            mf.write_json_atomic(tmp_dir / spec.MANIFEST_FILE, man)
            if final_dir.exists() or final_db.exists():
                held = store.live_pids(bdir)
                if held:
                    raise LineageBuildError(f"replacing the lineage tables of {bdir.name} refused: the build is "
                                            f"held by live pid(s) {held} (stop the server first)")
            if final_dir.is_dir():
                store.replace_dir(tmp_dir, final_dir)
            else:
                os.replace(tmp_dir, final_dir)
            os.replace(tmp_db, final_db)
            for stale in bdir.glob(spec.DB_FILE + ".*"):   # a write-ahead log of the replaced database
                stale.unlink()
            _record_in_business_manifest(bdir, man)
        except BaseException:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            for p in bdir.glob(tmp_db.name + "*"):
                p.unlink(missing_ok=True)
            raise
    log(f"    {counts['total_nodes']:,} nodes / {counts['total_edges']:,} edges; Parquet + {spec.DB_FILE} "
        f"({ld['db_bytes'] / 1e6:.1f} MB, load {ld['load_s']} s) -> {final_dir}")
    return final_dir, man


def _clean_leftovers(bdir: Path) -> None:
    """Remove what a crashed lineage build left in ``bdir``: temp tables / databases of dead
    processes (and of this one) and loader staging directories."""
    for p in bdir.glob(f"{TMP_PREFIX}*"):
        try:
            pid = int(p.name[len(TMP_PREFIX):].split(".", 1)[0])
        except ValueError:
            pid = -1
        if pid == os.getpid() or not store.pid_alive(pid):
            if p.is_dir():
                shutil.rmtree(p, ignore_errors=True)
            else:
                p.unlink(missing_ok=True)
    for p in bdir.glob(".lineage-stage-*"):
        shutil.rmtree(p, ignore_errors=True)


def _record_in_business_manifest(bdir: Path, man: dict) -> None:
    """manifest.json["lineage"] = summary (only when the business manifest exists)."""
    try:
        business = mf.read_manifest(bdir)
    except (OSError, ValueError):
        return
    summary = _summary(man)
    if business.get("lineage") != summary:
        business["lineage"] = summary
        mf.write_manifest(bdir, business)


# --------------------------------------------------------------------------- CLI (loader)
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Load the lineage Parquet tables into a fresh Ladybug database.")
    sub = ap.add_subparsers(dest="cmd", required=True)
    ld = sub.add_parser("load")
    ld.add_argument("--lineage-dir", required=True)
    ld.add_argument("--db", required=True)
    ld.add_argument("--buffer-pool-mb", type=int, default=store.LOAD_BUFFER_POOL_MB)
    ld.add_argument("--threads", type=int, default=store.THREADS)
    a = ap.parse_args(argv)
    print(json.dumps(load_ladybug(a.lineage_dir, a.db, a.buffer_pool_mb, a.threads)))
    return 0


__all__ = ["LineageBuildError", "LineageExtractError", "build_lineage", "lineage_identity", "load_ladybug",
           "read_manifest", "read_tables", "write_tables"]

if __name__ == "__main__":
    sys.exit(main())
