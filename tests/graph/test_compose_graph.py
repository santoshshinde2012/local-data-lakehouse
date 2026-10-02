"""docker-compose.graph.yml, docker/graph/Dockerfile and the graph lock files: static checks.

The merged Compose model comes from `docker compose ... config` (the CLI works without a running
daemon; skipped when there is no docker CLI with the compose plugin). The Dockerfile, ignore file
and lock checks are plain text checks and always run.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))

from lakehouse_graph import iceberg_source  # noqa: E402 (after the sys.path line; pyiceberg is imported lazily)

BASE, AIRFLOW, GRAPH = "docker-compose.yml", "docker-compose.airflow.yml", "docker-compose.graph.yml"
RW = "/opt/data/graph"
PIN = re.compile(r"^([A-Za-z0-9_.\-]+)==([^\s;\\]+)", re.M)
URI = "PYICEBERG_CATALOG__LAKEHOUSE__URI"
# Shell variables that would override .env.example in the rendered model.
CALLER_VARS = ("LAKEKEEPER_WAREHOUSE", "S3_REGION", "S3_ACCESS_KEY", "S3_SECRET_KEY", "POSTGRES_USER",
               "POSTGRES_PASSWORD", "POSTGRES_DB")
# The Airflow overlay refuses to render without its generated secrets (pipelines/airflow_env.sh);
# placeholders are enough for `config`.
AIRFLOW_PLACEHOLDERS = {k: "unused" for k in ("AIRFLOW_DB_PASSWORD", "AIRFLOW_FERNET_KEY", "AIRFLOW_JWT_SECRET",
                                             "AIRFLOW_API_SECRET_KEY")}


def _compose(*files: str, fmt: str | None = "json", env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    args = ["docker", "compose", "--env-file", ".env.example"]
    for f in files:
        args += ["-f", f]
    args += ["--profile", "*", "config"] + (["--format", fmt] if fmt else ["-q"])
    run_env = {k: v for k, v in os.environ.items() if k not in CALLER_VARS}
    run_env.update(AIRFLOW_PLACEHOLDERS)
    run_env.update(env or {})
    return subprocess.run(args, cwd=REPO, capture_output=True, text=True, check=False, env=run_env)


@pytest.fixture(scope="module")
def models() -> dict:
    if shutil.which("docker") is None:
        pytest.skip("no docker CLI on this machine")
    p = _compose(BASE)
    if p.returncode and ("is not a docker command" in p.stderr or "unknown" in p.stderr.lower()):
        pytest.skip("docker CLI without the compose plugin")
    assert p.returncode == 0, p.stderr
    out = {"base": json.loads(p.stdout)}
    p = _compose(BASE, GRAPH)
    assert p.returncode == 0, p.stderr
    out["merged"] = json.loads(p.stdout)
    return out


def _target(v) -> str:
    return v["target"] if isinstance(v, dict) else v.split(":")[1]


def test_config_is_valid_with_and_without_the_airflow_overlay(models):
    for files in ((BASE, GRAPH), (BASE, AIRFLOW, GRAPH), (BASE, GRAPH, AIRFLOW)):
        p = _compose(*files, fmt=None)
        assert p.returncode == 0, f"{files}: {p.stderr}"
    p = _compose(GRAPH, fmt=None)
    assert p.returncode != 0, "the overlay alone must not be a valid project (spark has no image there)"


def test_overlay_only_adds_to_the_spark_service(models):
    b, m = models["base"]["services"], models["merged"]["services"]
    assert set(m) - set(b) == {"graph"}
    assert m["graph"]["profiles"] == ["full"], "the graph container is part of the full profile"
    assert m["graph"]["depends_on"]["lakehouse-init"]["condition"] == "service_healthy"
    for name in b:
        if name != "spark":
            assert b[name] == m[name], f"overlay changed base service {name}"
    bs, ms = b["spark"], m["spark"]
    bt, mt = [_target(v) for v in bs["volumes"]], [_target(v) for v in ms["volumes"]]
    assert mt[:len(bt)] == bt and mt[len(bt):] == [RW]
    assert all(v in ms["volumes"] for v in bs["volumes"])
    assert {k: ms["environment"][k] for k in bs["environment"]} == bs["environment"]
    assert set(ms["environment"]) - set(bs["environment"]) == {"GRAPH_ROOT"}
    for k in set(bs) | set(ms):
        if k not in ("volumes", "environment"):
            assert bs.get(k) == ms.get(k), f"spark: overlay changed {k}"


def test_graph_service_is_locked_down(models):
    g = models["merged"]["services"]["graph"]
    assert g["container_name"] == "ldl-graph"
    assert not g.get("ports") and not g.get("privileged")
    assert g.get("read_only") is True and "ALL" in g.get("cap_drop", [])
    assert any("no-new-privileges" in s for s in g.get("security_opt", []))
    assert str(g["user"]).split(":")[0] not in ("0", "root")
    assert g.get("mem_limit") and g.get("command") == ["sleep", "infinity"]
    assert "lakehouse" in g["networks"] and models["merged"]["networks"]["lakehouse"]["name"] == "ldl-net"
    rw = [_target(v) for v in g["volumes"] if not v.get("read_only")]
    assert rw == [RW], f"the only writable mount must be {RW}: {rw}"
    assert not any("docker.sock" in str(v.get("source", "")) for v in g["volumes"])
    src = {v["target"]: v["source"] for v in g["volumes"]}
    spark_src = {v["target"]: v["source"] for v in models["merged"]["services"]["spark"]["volumes"]}
    assert src[RW] == spark_src[RW], "ldl-graph and ldl-spark share one host GRAPH_ROOT"
    assert g["environment"]["GRAPH_ROOT"] == RW
    for mount in ("src", "scripts", "sql", "data/sample", "data/export", "airflow/dags", "pipelines", "Makefile"):
        assert f"/opt/lakehouse/{mount}" in src, f"the repo-shaped tree needs {mount}"


def test_pyiceberg_settings_match_spark(models):
    """ldl-graph opens the SAME Lakekeeper REST catalog and warehouse Spark uses, with vended credentials."""
    env = models["merged"]["services"]["graph"]["environment"]
    conf = dict(line.split(None, 1) for line in (REPO / "config/spark-defaults.conf").read_text().splitlines()
                if line.strip() and not line.startswith("#"))
    p = "PYICEBERG_CATALOG__LAKEHOUSE__"
    assert conf["spark.sql.defaultCatalog"].strip() == "lakehouse"
    assert env[p + "TYPE"] == "rest" == conf["spark.sql.catalog.lakehouse.type"].strip()
    assert env[p + "URI"] == conf["spark.sql.catalog.lakehouse.uri"].strip() == "http://lakekeeper:8181/catalog"
    assert env[p + "WAREHOUSE"] == conf["spark.sql.catalog.lakehouse.warehouse"].strip()
    assert env[p + "HEADER__X_ICEBERG_ACCESS_DELEGATION"] == "vended-credentials" == \
        conf["spark.sql.catalog.lakehouse.header.X-Iceberg-Access-Delegation"].strip()
    assert env[p + "PY_IO_IMPL"] == "pyiceberg.io.fsspec.FsspecFileIO", \
        "pyarrow's S3 client resolves objectstore.localhost (the vended endpoint) to the container itself"
    assert env[p + "S3__REGION"] == "us-east-1", "without a region PyIceberg asks real AWS"


def test_graph_holds_no_catalog_or_store_secret(models):
    """No database URI, no S3 keys, no password: S3 access comes only from Lakekeeper's vended credentials."""
    env = models["merged"]["services"]["graph"]["environment"]
    secrets = {models["base"]["services"]["objectstore"]["environment"].get(k) for k in
               ("RUSTFS_ACCESS_KEY", "RUSTFS_SECRET_KEY")} - {None}
    assert secrets, "the base objectstore must name its keys (the check below needs them)"
    for k, v in env.items():
        assert not re.search(r"KEY|SECRET|PASSWORD|TOKEN|CREDENTIAL", k.replace("ACCESS_DELEGATION", "")), k
        assert "postgresql" not in str(v) and str(v) not in secrets, k


def test_iceberg_source_redacts_rest_uris():
    """A REST catalog URI prints as scheme://host[:port]/path, never with a userinfo or query."""
    assert iceberg_source._redact("http://lakekeeper:8181/catalog") == "http://lakekeeper:8181/catalog"
    assert iceberg_source._redact("http://u:p%40ss@lakekeeper:8181/catalog?token=x") == \
        "http://lakekeeper:8181/catalog"


def test_dockerfile_is_pinned_nonroot_and_wheels_only():
    text = (REPO / "docker/graph/Dockerfile").read_text()
    image = re.search(r"^ARG PYTHON_IMAGE=(\S+)$", text, re.M)
    assert image and re.fullmatch(r"python:3\.12\.\d+-slim-[a-z]+@sha256:[0-9a-f]{64}", image.group(1))
    for needle in ("--require-hashes", "--only-binary=:all:", "requirements-graph.txt",
                   "requirements-graph-spark-client.txt", "pip check"):
        assert needle in text
    users = re.findall(r"^USER\s+(\S+)", text, re.M)
    assert users and users[-1].split(":")[0] not in ("root", "0")
    assert not re.search(r"apt-get|curl |wget |gcc|build-essential", text), "no build tools / downloads"
    assert not re.search(r"^(EXPOSE|VOLUME)\b", text, re.M)


def test_dockerignore_sends_only_the_two_locks():
    lines = [ln.strip() for ln in (REPO / "docker/graph/Dockerfile.dockerignore").read_text().splitlines()
             if ln.strip() and not ln.startswith("#")]
    assert lines == ["*", "!requirements-graph.txt", "!requirements-graph-spark-client.txt"]


@pytest.mark.parametrize(("lock", "must", "must_not"), [
    ("requirements-graph-spark-client.txt", {"pyiceberg": "0.12.0", "s3fs": None},
     {"pyspark", "setuptools", "sqlalchemy", "psycopg2-binary", "psycopg"}),
    ("requirements-graph-spark.txt", {"pyiceberg": "0.12.0", "pyspark": "4.1.3", "setuptools": None},
     {"psycopg2-binary", "psycopg"}),
])
def test_locks_agree_with_the_core_lock(lock, must, must_not):
    core = dict(PIN.findall((REPO / "requirements-graph.txt").read_text()))
    text = (REPO / lock).read_text()
    pins = dict(PIN.findall(text))
    for name, version in must.items():
        assert name in pins and (version is None or pins[name] == version), f"{lock}: {name}"
    assert not (must_not & set(pins)), f"{lock} must not pin {must_not & set(pins)}"
    clash = {k: (core[k], pins[k]) for k in core.keys() & pins.keys() if core[k] != pins[k]}
    assert not clash, f"{lock} disagrees with requirements-graph.txt (recompile with -c): {clash}"
    blocks = re.split(r"\n(?=[A-Za-z0-9_.\-]+==)", text)
    assert all("--hash=sha256:" in b for b in blocks if PIN.match(b)), f"{lock}: every pin is hashed"
