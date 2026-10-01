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
CALLER_VARS = ("GRAPH_PG_USER", "GRAPH_PG_PASSWORD", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB")


def _compose(*files: str, fmt: str | None = "json", env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    args = ["docker", "compose", "--env-file", ".env.example"]
    for f in files:
        args += ["-f", f]
    args += ["config"] + (["--format", fmt] if fmt else ["-q"])
    run_env = None
    if env is not None:
        run_env = {k: v for k, v in os.environ.items() if k not in CALLER_VARS}
        run_env.update(env)
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
    env = models["merged"]["services"]["graph"]["environment"]
    conf = dict(line.split(None, 1) for line in (REPO / "config/spark-defaults.conf").read_text().splitlines()
                if line.strip() and not line.startswith("#"))
    p = "PYICEBERG_CATALOG__LAKEHOUSE__"
    assert conf["spark.sql.defaultCatalog"].strip() == "lakehouse"
    assert env[p + "TYPE"] == "sql"
    assert env[p + "URI"].startswith("postgresql+psycopg2://")
    assert env[p + "URI"].rsplit("@", 1)[-1] == conf["spark.sql.catalog.lakehouse.uri"].strip().replace(
        "jdbc:postgresql://", "")
    assert env[p + "WAREHOUSE"] == conf["spark.sql.catalog.lakehouse.warehouse"].strip()
    assert env[p + "S3__ENDPOINT"] == conf["spark.hadoop.fs.s3a.endpoint"].strip()
    assert env[p + "S3__REGION"] == "us-east-1", "without a region PyIceberg asks real AWS"
    assert not any("INIT_CATALOG_TABLES" in k or "SCHEMA_VERSION" in k for k in env), \
        "init_catalog_tables is passed in code (the env var is ignored); schema_version is never set"


def _graph_env(env: dict[str, str]) -> dict:
    p = _compose(BASE, GRAPH, env=env)
    assert p.returncode == 0, p.stderr
    return json.loads(p.stdout)["services"]


def test_a_select_only_role_renders_into_the_pyiceberg_uri(models):
    """GRAPH_PG_USER / GRAPH_PG_PASSWORD (config/graph/postgres_graph_ro.sql) replace only the catalog
    login of ldl-graph; unset, the catalog owner from .env is used. Spark keeps the owner."""
    default = _graph_env({})
    assert default["graph"]["environment"][URI] == "postgresql+psycopg2://iceberg:iceberg@postgres:5432/iceberg"
    password = "Ro-pass_1.x~"     # URL-safe: the role script accepts it and the URI needs no encoding
    ro = _graph_env({"GRAPH_PG_USER": "graph_ro", "GRAPH_PG_PASSWORD": password})
    uri = ro["graph"]["environment"][URI]
    assert uri == f"postgresql+psycopg2://graph_ro:{password}@postgres:5432/iceberg"
    assert ro["spark"] == default["spark"], "the read-only role is for ldl-graph only; Spark owns the catalog"
    assert {k: v for k, v in ro["graph"]["environment"].items() if k != URI} == \
        {k: v for k, v in default["graph"]["environment"].items() if k != URI}
    # The provenance a build records (manifest iceberg.catalog_uri) and every error message carry no
    # credentials, whatever the password holds (the owner's is not restricted like the role's).
    for pw in (password, "pw/secret-rest", "a:b?c#d%41e", "p@ss@word", "x y"):
        assert iceberg_source._redact(uri.replace(password, pw) + "?sslmode=disable") == \
            "postgresql+psycopg2://postgres:5432/iceberg", pw
    # Why the role script refuses other passwords: Compose interpolates them verbatim into the URI.
    try:
        from sqlalchemy.engine import make_url
    except ImportError:
        return   # the core venv has no SQLAlchemy; the spark venv and ldl-graph do
    url = make_url(uri)
    assert (url.username, url.password, url.host, url.port, url.database) == \
        ("graph_ro", password, "postgres", 5432, "iceberg")
    # SQLAlchemy itself silently means something else: another host, or another (decoded) password.
    for bad_password, why in (("p@ss", "an unencoded @ moves the host"), ("ab%41cd", "%41 is decoded to A")):
        bad = _graph_env({"GRAPH_PG_USER": "graph_ro", "GRAPH_PG_PASSWORD": bad_password})["graph"]["environment"][URI]
        u = make_url(bad)
        assert (u.username, u.password, u.host) != ("graph_ro", bad_password, "postgres"), why


def test_the_overlay_names_the_role_script():
    text = (REPO / GRAPH).read_text()
    assert "config/graph/postgres_graph_ro.sql" in text and "GRAPH_PG_USER=graph_ro" in text
    assert "URL-safe" in text and "ROOT key" in text
    assert (REPO / "config/graph/postgres_graph_ro.sql").is_file()


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
    ("requirements-graph-spark-client.txt", {"pyiceberg": "0.12.0", "sqlalchemy": None}, {"pyspark", "setuptools"}),
    ("requirements-graph-spark.txt", {"pyiceberg": "0.12.0", "pyspark": "3.5.3", "setuptools": None}, set()),
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
