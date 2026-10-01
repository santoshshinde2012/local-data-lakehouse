"""Build identity and provenance manifest (manifest.json) for a renewal graph build.

business_build_id = first 12 hex chars of sha256 over (canonical JSON):
  * sha256 of every bronze CSV the gold twin reads (spec.BRONZE_FILES)
  * sha256 of the code that shapes content (CONTENT_CODE below)
  * spec versions, K, the quantisation factor, the block and the 20 features
  * ladybug / pyarrow / pandas / numpy versions and the platform tag
So the same bronze with changed gold or builder logic gets a new id, while a README or
tool edit does not re-key the business graph. (Only content-shaping modules are hashed:
query templates, the oracle and the agent tools read the graph, they do not shape it.)

The manifest adds seed / N_USERS (declared | verified), repo commit + dirty flag,
data_end, synthetic=true, sha256 of the profile's exports and of the guarded user files,
per-file Parquet sha256 + row counts, SIMILAR_TO tie diagnostics, Ladybug load stats and
the builder's max RSS.

The pins that describe the world *around* an unchanged build (``exports``,
``guarded_sha256``, the seed / N_USERS status) are refreshed in place by the builder when
the same build id is built again (build.repin); ``repinned_at`` records when.

Iceberg-sourced builds (``manifest["iceberg"]``, lakehouse_graph.iceberg_source) keep the bronze
identity when their Parquet is byte-identical to the local path's; otherwise their id is computed
over the Iceberg input pins as well (``iceberg.identity_payload``). ``current_identity()`` /
``is_fresh()`` recompute whichever applies, so freshness (and promote) follows the build's source.
"""
from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from . import spec

MANIFEST_FILE = "manifest.json"
SAMPLE_META_FILE = "sample_meta.json"
CONTENT_CODE = [
    "scripts/build_churn_gold_local.py",
    "scripts/build_graph_local.py",
    "src/lakehouse_graph/__init__.py",
    "src/lakehouse_graph/spec.py",
    "src/lakehouse_graph/build.py",
    "src/lakehouse_graph/store.py",
    "src/lakehouse_graph/manifest.py",
    "sql/churn/gold_renewal_features.sql",
]
ID_PACKAGES = ["ladybug", "pyarrow", "pandas", "numpy"]
RSS_SOFT_LIMIT_MIB = 512
GENERATOR = "scripts/generate_churn_sample.py"    # the user's scripts: run unchanged, never edited
GOLD_SCRIPT = "scripts/build_churn_gold_local.py"


# --------------------------------------------------------------------------- hashing
def sha256_file(path: str | os.PathLike) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def sha256_json(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def input_hashes(sample_dir: str | os.PathLike) -> dict[str, str]:
    d = Path(sample_dir)
    return {name: sha256_file(d / name) for name in spec.BRONZE_FILES if (d / name).is_file()}


def combined_sha256(hashes: dict[str, str]) -> str:
    return hashlib.sha256("".join(f"{k}:{v}\n" for k, v in sorted(hashes.items())).encode()).hexdigest()


def code_hashes(repo: str | os.PathLike | None = None) -> dict[str, str]:
    root = Path(repo) if repo else spec.repo_root()
    return {rel: (sha256_file(root / rel) if (root / rel).is_file() else "missing") for rel in CONTENT_CODE}


def package_versions(names: list[str] = ID_PACKAGES) -> dict[str, str]:
    out = {}
    for n in names:
        try:
            out[n] = importlib.metadata.version(n)
        except importlib.metadata.PackageNotFoundError:
            out[n] = "not-installed"
    return out


def platform_tag() -> str:
    """e.g. macosx_arm64, manylinux_x86_64 (Parquet bytes are asserted per platform only)."""
    system = platform.system().lower()
    machine = platform.machine().lower()
    machine = {"amd64": "x86_64"}.get(machine, machine)
    os_tag = {"darwin": "macosx", "linux": "manylinux", "windows": "win"}.get(system, system)
    return f"{os_tag}_{machine}"


def build_identity(sample_dir: str | os.PathLike, repo: str | os.PathLike | None = None) -> dict:
    payload = {
        "inputs": input_hashes(sample_dir),
        "code": code_hashes(repo),
        "spec": dict(spec.SPEC_VERSIONS),
        "params": {"k": spec.K, "quant": spec.QUANT, "block": spec.BLOCK, "features": list(spec.FEATURES),
                   "reference_route": spec.REFERENCE_ROUTE},
        "versions": package_versions(),
        "platform": platform_tag(),
    }
    return {"business_build_id": sha256_json(payload)[:12], "payload": payload}


def iceberg_pins(man: dict) -> dict[str, int] | None:
    """The Iceberg input pins (table -> snapshot id) an Iceberg-sourced build's identity is computed
    over, or None: a CSV build, or an Iceberg build byte-identical to the local path, which keeps
    the bronze identity (lakehouse_graph.iceberg_source)."""
    payload = (man.get("iceberg") or {}).get("identity_payload")
    return None if payload is None else dict(payload["iceberg_inputs"])


def current_identity(man: dict, repo: str | os.PathLike | None = None) -> dict:
    """The identity a build's sources and code give NOW, the way it was computed at build time.

    A CSV build (and an Iceberg build that kept the bronze identity): build_identity() of its
    bronze. An Iceberg-sourced build whose content is not the local path's: the identity over its
    recorded Iceberg input pins, the bronze, the content code + iceberg_source.py, spec, versions and
    platform (iceberg_source.iceberg_identity). ``repo`` applies to the bronze identity only.
    """
    sdir = resolve_path(man["inputs"]["sample_dir"])
    pins = iceberg_pins(man)
    if pins is None:
        return build_identity(sdir, repo)
    from .iceberg_source import iceberg_identity  # lazy: iceberg_source imports this module
    return iceberg_identity(sdir, pins)


def is_fresh(man: dict, repo: str | os.PathLike | None = None) -> bool:
    """True while the build's sources (its bronze, or its Iceberg input pins), content code, spec,
    versions and platform are what they were when it was built (current_identity() = the recorded id)."""
    try:
        return current_identity(man, repo)["business_build_id"] == man["business_build_id"]
    except (KeyError, TypeError, OSError):
        return False


# --------------------------------------------------------------------------- environment facts
def git_state(repo: str | os.PathLike | None = None) -> dict:
    root = str(repo or spec.repo_root())
    try:
        head = subprocess.run(["git", "-C", root, "rev-parse", "--short=7", "HEAD"], capture_output=True, text=True,
                              timeout=20, check=False)
        status = subprocess.run(["git", "-C", root, "status", "--porcelain"], capture_output=True, text=True,
                                timeout=60, check=False)
    except (OSError, subprocess.SubprocessError):
        return {"commit": None, "dirty": None}
    if head.returncode:
        return {"commit": None, "dirty": None}
    return {"commit": head.stdout.strip(), "dirty": bool(status.stdout.strip()) if status.returncode == 0 else None}


def export_hashes(export_dir: str | os.PathLike) -> dict[str, str]:
    d = Path(export_dir)
    return {n: sha256_file(d / n) for n in spec.EXPORT_FILES if (d / n).is_file()}


def guarded_hashes() -> dict[str, str]:
    """sha256 of the user's bronze (data/sample/churn, incl. the tiny fixture) and exports."""
    root = spec.repo_root()
    return {str(p.relative_to(root)): sha256_file(p) for p in spec.guarded_paths()}


def display_path(p: str | os.PathLike) -> str:
    """Repo-relative when inside the repo (portable manifests), else absolute."""
    p = Path(p).absolute()
    try:
        return str(p.relative_to(spec.repo_root()))
    except ValueError:
        return str(p)


def resolve_path(s: str) -> Path:
    p = Path(s)
    return p if p.is_absolute() else spec.repo_root() / p


# --------------------------------------------------------------------------- seed / N
def sample_meta_path(profile: str, root: str | os.PathLike | None = None) -> Path:
    return spec.profile_dir(profile, root) / SAMPLE_META_FILE


def read_sample_meta(profile: str, root: str | os.PathLike | None = None) -> dict | None:
    p = sample_meta_path(profile, root)
    try:
        return json.loads(p.read_text())
    except (OSError, ValueError):
        return None


def verify_seed(sample_dir: str | os.PathLike, seed: int, n_users: int, scratch_parent: Path) -> bool:
    """Regenerate bronze with the user's generator into scratch; True if every CSV sha256 matches."""
    scratch_parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(tempfile.mkdtemp(prefix=".verify-seed-", dir=scratch_parent))
    try:
        env = dict(os.environ, CHURN_SAMPLE_DIR=str(tmp), CHURN_SEED=str(seed), N_USERS=str(n_users))
        p = subprocess.run([sys.executable, str(spec.repo_root() / GENERATOR)], env=env, capture_output=True,
                           text=True, check=False)
        if p.returncode:
            return False
        return input_hashes(tmp) == input_hashes(sample_dir)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def declared_seed(profile: str) -> tuple[int, int, str]:
    if profile == "tiny":
        return spec.TINY_SEED, spec.TINY_N_USERS, "spec: committed fixture = generator seed 42, N_USERS 120"
    if profile == "inject":
        return spec.TINY_SEED, spec.TINY_N_USERS, ("spec: the tiny fixture (generator seed 42, N_USERS 120) with "
                                                   "one poisoned user_name")
    m = spec.SEED_PROFILE_RE.match(profile)
    if m:
        return int(m.group(1)), int(os.environ.get("N_USERS") or spec.DEFAULT_N_USERS), "profile name + N_USERS"
    seed, n = os.environ.get("CHURN_SEED"), os.environ.get("N_USERS")
    src = "environment (CHURN_SEED, N_USERS)" if seed or n else "make churn-sample defaults"
    return int(seed or spec.DEFAULT_SEED), int(n or spec.DEFAULT_N_USERS), src


def seed_info(profile: str, sample_dir: str | os.PathLike, root: str | os.PathLike | None = None,
              verify: bool = False, scratch: Path | None = None) -> dict:
    """seed / n_users with status verified (graph-sample by construction or regeneration) or declared."""
    inputs = input_hashes(sample_dir)
    meta = read_sample_meta(profile, root)
    if meta and meta.get("sample_sha256") == inputs:
        return {"seed": meta["seed"], "n_users": meta["n_users"], "status": "verified", "source": meta["method"]}
    seed, n, src = declared_seed(profile)
    info = {"seed": seed, "n_users": n, "status": "declared", "source": src}
    if verify:
        ok = verify_seed(sample_dir, seed, n, scratch or spec.profile_dir(profile, root))
        info["status"] = "verified" if ok else "declared"
        info["verification"] = ("regenerated with scripts/generate_churn_sample.py: sha256 match" if ok else
                                "regenerated with scripts/generate_churn_sample.py: MISMATCH (bronze is not "
                                "generator output for this seed / N_USERS)")
    return info


def write_sample_meta(profile: str, root: str | os.PathLike | None, *, seed: int, n_users: int, method: str,
                      extra: dict | None = None) -> dict:
    """Record what a profile's bronze + exports are (sample_meta.json, written by graph-sample).

    ``seed_info`` reads it back: while the bronze sha256 still match, seed / N_USERS count as
    verified, with ``method`` as the reason.
    """
    sdir, edir = spec.sample_dir(profile, root), spec.export_dir(profile, root)
    repo = spec.repo_root()
    meta = {
        "profile": profile, "seed": seed, "n_users": n_users, "method": method,
        "sample_dir": display_path(sdir), "export_dir": display_path(edir),
        "sample_sha256": input_hashes(sdir), "export_sha256": export_hashes(edir),
        "generator_sha256": sha256_file(repo / GENERATOR), "gold_script_sha256": sha256_file(repo / GOLD_SCRIPT),
        "created_at": utc_now(), **(extra or {}),
    }
    path = sample_meta_path(profile, root)
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json_atomic(path, meta)
    return meta


def seed_fields(seed: dict) -> dict:
    """The manifest keys that carry seed / N_USERS and how far they are trusted."""
    return {"seed": seed["seed"], "n_users": seed["n_users"], "seed_n_status": seed["status"],
            "seed_n_source": seed.get("source"), "seed_n_verification": seed.get("verification")}


def export_pin(export_dir: str | os.PathLike) -> dict:
    """The manifest's ``exports`` entry: where the profile's exports are and their sha256 now."""
    return {"dir": display_path(export_dir), "sha256": export_hashes(export_dir)}


def utc_now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# --------------------------------------------------------------------------- manifest
def assemble_manifest(*, ident: dict, profile: str, sample_dir: Path, export_dir: Path, files: dict, counts: dict,
                      today, diagnostics: dict, seed: dict, guard: dict, ladybug: dict, builder: dict,
                      built_at: str) -> dict:
    payload = ident["payload"]
    rss_mib = builder["max_rss_bytes"] / 2**20
    return {
        "manifest_version": 1,
        "business_build_id": ident["business_build_id"],
        "profile": profile,
        "spec": {**spec.SPEC_VERSIONS, "contract": spec.CONTRACT_VERSION},
        "pit_rule": spec.PIT_RULE,
        "params": payload["params"],
        "inputs": {"sample_dir": display_path(sample_dir), "sha256": payload["inputs"],
                   "combined_sha256": combined_sha256(payload["inputs"])},
        "code_sha256": payload["code"],
        "versions": {**payload["versions"], "python": platform.python_version()},
        "platform": payload["platform"],
        **seed_fields(seed),
        **git_state(),
        "data_end": str(today.date()) if hasattr(today, "date") else str(today),
        "synthetic": True,
        "exports": export_pin(export_dir),
        "guarded_sha256": guard,
        "files": files,
        "counts": counts,
        "similar_to": diagnostics,
        "ladybug": {"db": ladybug.get("db", "graph.lbdb"), "version": payload["versions"].get("ladybug"),
                    "load_s": ladybug["load_s"], "db_bytes": ladybug["db_bytes"],
                    "buffer_pool_mb": ladybug["buffer_pool_mb"], "threads": ladybug["threads"],
                    "max_rss_bytes": ladybug["max_rss_bytes"], "counts_equal_parquet": True},
        "builder": {**builder, "max_rss_mib": round(rss_mib, 1), "rss_soft_limit_mib": RSS_SOFT_LIMIT_MIB,
                    "rss_over_soft_limit": rss_mib > RSS_SOFT_LIMIT_MIB},
        "built_at": built_at,
    }


def write_json_atomic(path: Path, obj) -> None:
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    tmp.write_text(json.dumps(obj, indent=2, sort_keys=True, default=str) + "\n")
    os.replace(tmp, path)


def write_manifest(build_dir: str | os.PathLike, man: dict) -> Path:
    path = Path(build_dir) / MANIFEST_FILE
    write_json_atomic(path, man)
    return path


def read_manifest(build_dir: str | os.PathLike) -> dict:
    return json.loads((Path(build_dir) / MANIFEST_FILE).read_text())
