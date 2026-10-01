"""config/graph/postgres_graph_ro.sql: the optional SELECT-only Postgres role for ldl-graph.

Always (text checks, no Docker): the script is idempotent psql, grants SELECT on BOTH Iceberg
catalog tables (PyIceberg mis-detects the catalog schema with one) plus default privileges for
tables created later, grants nothing that writes, sets default_transaction_read_only, and accepts
exactly the URL-safe passwords (docker-compose.graph.yml puts the password verbatim into the
SQLAlchemy URI).

Opt-in (GRAPH_PG_DOCKER_TEST=1 and a reachable Docker daemon with postgres:16-alpine, the base
stack's image, already pulled; nothing is pulled): the script runs with the image's psql against a
disposable container (TCP + scram, like ldl-postgres) BEFORE the catalog tables exist, twice; then
the owner creates the Iceberg 1.6.1 V0 catalog tables, and

  * the role can read both tables (default privileges) and nothing else;
  * every write fails, also after the session switches default_transaction_read_only off;
  * a URL-unsafe password is refused before anything changes;
  * with pyiceberg + psycopg2 (.venv-graph-spark, ldl-graph): lakehouse_graph.iceberg_source opens
    the catalog AS THE ROLE (probe of both tables, v0 detected, schema unchanged), a PyIceberg write
    fails, and a missing grant on one table is explained.
The container is removed afterwards.
"""
from __future__ import annotations

import os
import re
import shutil
import string
import subprocess
import sys
import time
import uuid
from pathlib import Path
from urllib.parse import quote

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))

from lakehouse_graph import iceberg_source as ice  # noqa: E402 (after the sys.path line; pyiceberg is lazy)

ROLE_SQL = REPO / "config/graph/postgres_graph_ro.sql"
ROLE = "graph_ro"
IMAGE = "postgres:16-alpine"
# Iceberg 1.6.1 JdbcUtil V0 DDL: the catalog Spark creates in ldl-postgres.
V0 = (
    "CREATE TABLE iceberg_tables(catalog_name VARCHAR(255) NOT NULL,table_namespace VARCHAR(255) NOT NULL,"
    "table_name VARCHAR(255) NOT NULL,metadata_location VARCHAR(1000),previous_metadata_location VARCHAR(1000),"
    "PRIMARY KEY (catalog_name, table_namespace, table_name))",
    "CREATE TABLE iceberg_namespace_properties(catalog_name VARCHAR(255) NOT NULL,namespace VARCHAR(255) NOT NULL,"
    "property_key VARCHAR(255),property_value VARCHAR(1000),PRIMARY KEY (catalog_name, namespace, property_key))",
)


def _code() -> list[str]:
    """The script's lines without SQL comments."""
    return [ln for ln in ROLE_SQL.read_text().splitlines() if ln.strip() and not ln.lstrip().startswith("--")]


# --------------------------------------------------------------------------- text checks
def test_role_script_is_idempotent_psql_outside_sql():
    assert ROLE_SQL.is_file() and "sql" not in ROLE_SQL.relative_to(REPO).parts[:1], \
        "psql meta-commands: keep it out of sql/ (Spark SQL files only)"
    code = "\n".join(_code())
    assert code.splitlines()[0] == r"\set ON_ERROR_STOP on"
    assert re.search(r"SELECT 'CREATE ROLE graph_ro'\s+WHERE NOT EXISTS \(SELECT 1 FROM pg_catalog\.pg_roles "
                     r"WHERE rolname = 'graph_ro'\) \\gexec", code), "CREATE ROLE only when the role is absent"
    assert code.count("CREATE ROLE") == 1
    assert re.search(r"ALTER ROLE graph_ro WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS\s+"
                     r"NOINHERIT CONNECTION LIMIT \d+ PASSWORD :'ro_password';", code)


def test_role_script_reads_both_catalog_tables_now_and_later():
    code = "\n".join(_code())
    grant_now = re.search(r"GRANT SELECT ON TABLE public\.%I TO graph_ro'.*?c\.relname IN \(([^)]*)\) \\gexec", code,
                          re.S)
    assert grant_now, "SELECT on the catalog tables that exist when the script runs"
    assert set(re.findall(r"'(\w+)'", grant_now.group(1))) == set(ice.CATALOG_TABLES), \
        "BOTH catalog tables: with one, PyIceberg mis-detects the catalog schema as v1"
    assert "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO graph_ro;" in code, \
        "tables the catalog owner creates after the script ran (the first Spark job)"
    assert "ALTER ROLE graph_ro SET default_transaction_read_only = on;" in code


def test_role_script_grants_nothing_that_writes():
    code = "\n".join(_code())
    granted = set(re.findall(r"\bGRANT\s+([A-Z]+)", code))
    assert granted == {"SELECT", "CONNECT", "USAGE"}, granted
    for word in ("INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "ALL PRIVILEGES", " CREATE ON",
                 "SUPERUSER ", "CREATEDB ", "CREATEROLE ", "BYPASSRLS ", "OWNER TO", "REVOKE"):
        hits = [ln for ln in code.splitlines() if word in ln and f"NO{word.strip()}" not in ln]
        assert not hits, f"{word!r}: {hits}"


def test_role_script_accepts_exactly_the_url_safe_passwords():
    """The check is a POSIX bracket expression; the same pattern in Python accepts a character
    exactly when it needs no percent-encoding in a URI userinfo (RFC 3986 unreserved)."""
    m = re.search(r"SELECT :'ro_password' ~ '(\^\[[^']+\]\+\$)' AS ro_password_url_safe \\gset", ROLE_SQL.read_text())
    assert m, "the password check runs first, before the role is created or changed"
    text = ROLE_SQL.read_text()
    assert text.index("ro_password_url_safe") < text.index("CREATE ROLE")
    assert re.search(r"\\if :ro_password_url_safe\s+\\else\s+DO \$\$ BEGIN RAISE EXCEPTION", text)
    pattern = re.compile(m.group(1))
    for ch in string.printable:
        assert bool(pattern.match(ch)) == (quote(ch, safe="") == ch), repr(ch)
    assert pattern.match("graph_ro") and not pattern.match("") and not pattern.match("p@ss")


def test_role_script_states_what_it_does_not_narrow():
    head = ROLE_SQL.read_text().split(r"\set ON_ERROR_STOP on")[0]
    assert "Silo ROOT key" in head and "URL-safe" in head and "GRAPH_PG_USER=graph_ro" in head


# --------------------------------------------------------------------------- disposable Postgres (opt-in)
def _docker(*args: str, inp: str | None = None, timeout: int = 120) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", *args], input=inp, capture_output=True, text=True, timeout=timeout, check=False)


class PG:
    def __init__(self, cid: str, port: int, ip: str):
        # ip: the container's own network address. The image trusts 127.0.0.1 (pg_hba), so a role login
        # over loopback would not check the password; over this address it is scram, as from ldl-graph.
        self.cid, self.port, self.ip, self.password = cid, port, ip, "Ro-pass_1"

    def owner(self, sql: str | None = None, *, script: str | None = None, ro_password: str | None = None):
        """psql as the catalog owner inside the container (local socket), like `docker compose exec postgres`."""
        args = ["exec", "-i", self.cid, "psql", "-X", "-At", "-v", "ON_ERROR_STOP=1", "-U", "iceberg", "-d", "iceberg"]
        if ro_password is not None:
            args += ["-v", f"ro_password={ro_password}"]
        args += ["-f", "-"] if script is not None else ["-c", sql or ""]
        return _docker(*args, inp=script)

    def role(self, *statements: str, password: str | None = None) -> subprocess.CompletedProcess:
        """psql as graph_ro over TCP with its password (scram), each -c in its own transaction."""
        args = ["exec", "-e", f"PGPASSWORD={password or self.password}", self.cid, "psql", "-X", "-At", "-v",
                "ON_ERROR_STOP=1", "-h", self.ip, "-U", ROLE, "-d", "iceberg"]
        for s in statements:
            args += ["-c", s]
        return _docker(*args)


def _pg_skip() -> str | None:
    if os.environ.get("GRAPH_PG_DOCKER_TEST") != "1":
        return "opt-in: set GRAPH_PG_DOCKER_TEST=1 (starts a disposable postgres:16-alpine container)"
    if shutil.which("docker") is None:
        return "no docker CLI"
    if _docker("info", "--format", "{{.ServerVersion}}", timeout=30).returncode:
        return "the Docker daemon is not reachable"
    if _docker("image", "inspect", IMAGE, timeout=30).returncode:
        return f"{IMAGE} is not pulled (this test never pulls; `docker pull {IMAGE}`)"
    return None


@pytest.fixture(scope="module")
def pg():
    reason = _pg_skip()
    if reason:
        pytest.skip(reason)
    name = f"ldl-graph-pgtest-{uuid.uuid4().hex[:8]}"
    p = _docker("run", "-d", "--rm", "--pull", "never", "--name", name, "-p", "127.0.0.1::5432",
                "-e", "POSTGRES_USER=iceberg", "-e", "POSTGRES_PASSWORD=iceberg", "-e", "POSTGRES_DB=iceberg", IMAGE)
    assert p.returncode == 0, p.stderr
    cid = p.stdout.strip()
    try:
        deadline = time.time() + 90
        # TCP only answers once the entrypoint's init phase (socket-only server) is over.
        while _docker("exec", cid, "pg_isready", "-h", "127.0.0.1", "-U", "iceberg", "-d", "iceberg").returncode:
            assert time.time() < deadline, "postgres did not become ready in 90 s"
            time.sleep(0.5)
        port = int(_docker("port", cid, "5432/tcp").stdout.strip().splitlines()[0].rsplit(":", 1)[1])
        ip = _docker("inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}", cid).stdout.split()
        assert ip, "the container has no network address"
        db = PG(cid, port, ip[0])
        script = ROLE_SQL.read_text()
        # Before the catalog exists (a fresh stack, no Spark job yet), twice: idempotent.
        db.runs = [db.owner(script=script, ro_password=db.password) for _ in range(2)]
        db.tables_before = db.owner("SELECT count(*) FROM pg_tables WHERE schemaname = 'public'").stdout.strip()
        # The first Spark job then creates the V0 catalog as the owner, and registers a namespace.
        for ddl in V0:
            assert db.owner(ddl).returncode == 0
        assert db.owner("INSERT INTO iceberg_namespace_properties VALUES "
                        "('lakehouse', 'gold', 'exists', 'true')").returncode == 0
        yield db
    finally:
        _docker("rm", "-f", cid)


def test_the_role_script_applies_before_the_catalog_and_twice(pg):
    for run in pg.runs:
        assert run.returncode == 0, run.stderr
        assert "graph_ro|t|f|f|f|8" in run.stdout, run.stdout     # login, not super, no createdb / createrole
    assert pg.tables_before == "0"
    out = pg.owner("SELECT count(*) FROM pg_roles WHERE rolname = 'graph_ro'").stdout.strip()
    assert out == "1"
    settings = pg.owner("SELECT array_to_string(setconfig, ',') FROM pg_db_role_setting s "
                        "JOIN pg_roles r ON r.oid = s.setrole WHERE r.rolname = 'graph_ro'").stdout
    assert "default_transaction_read_only=on" in settings


def test_the_role_reads_both_catalog_tables_through_default_privileges(pg):
    privs = pg.owner("SELECT string_agg(t || ':' || p || '=' || "
                     "has_table_privilege('graph_ro', 'public.' || t, p)::text, ',' ORDER BY t, p) "
                     "FROM unnest(array['iceberg_tables', 'iceberg_namespace_properties']) t, "
                     "unnest(array['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES', 'TRIGGER']) p")
    got = dict(kv.split("=") for kv in privs.stdout.strip().split(","))
    assert {k for k, v in got.items() if v == "true"} == {"iceberg_namespace_properties:SELECT",
                                                          "iceberg_tables:SELECT"}, got
    r = pg.role("SELECT count(*) FROM iceberg_tables", "SELECT namespace FROM iceberg_namespace_properties",
                "SHOW default_transaction_read_only")
    assert r.returncode == 0, r.stderr
    assert r.stdout.split() == ["0", "gold", "on"]
    wrong = pg.role("SELECT 1", password="not-the-password")
    assert wrong.returncode != 0 and "password authentication failed" in wrong.stderr, "the login checks the password"


@pytest.mark.parametrize("statement", [
    "INSERT INTO iceberg_tables VALUES ('lakehouse', 'gold', 'x', NULL, NULL)",
    "UPDATE iceberg_tables SET metadata_location = NULL",
    "DELETE FROM iceberg_namespace_properties",
    "TRUNCATE iceberg_tables",
    "ALTER TABLE iceberg_tables ADD COLUMN iceberg_type VARCHAR(5)",
    "DROP TABLE iceberg_namespace_properties",
    "CREATE TABLE public.t(i int)",
])
def test_the_role_cannot_write_even_with_read_only_switched_off(pg, statement):
    guarded = pg.role(statement)
    assert guarded.returncode != 0 and "read-only transaction" in guarded.stderr, guarded.stderr
    r = pg.role("SET default_transaction_read_only = off", statement)
    assert r.returncode != 0
    assert "permission denied" in r.stderr or "must be owner" in r.stderr, r.stderr
    assert pg.owner("SELECT count(*) FROM iceberg_namespace_properties").stdout.strip() == "1"


@pytest.mark.parametrize("password", ["p@ss", "a:b", "with space", "x/y", "100%", "q'uote"])
def test_a_url_unsafe_password_is_refused_before_anything_changes(pg, password):
    r = pg.owner(script=ROLE_SQL.read_text(), ro_password=password)
    assert r.returncode != 0 and "must be URL-safe" in r.stderr, r.stdout + r.stderr
    assert pg.role("SELECT 1").stdout.strip() == "1", "the role keeps its old password"
    assert pg.role("SELECT 1", password=password).returncode != 0


def test_pyiceberg_reads_the_catalog_as_the_role_and_cannot_write(pg, monkeypatch, tmp_path):
    pytest.importorskip("pyiceberg", reason="pyiceberg is in requirements-graph-spark*.txt (.venv-graph-spark)")
    pytest.importorskip("psycopg2", reason="the sql-postgres extra (.venv-graph-spark, ldl-graph)")
    from sqlalchemy.exc import SQLAlchemyError

    for k in [k for k in os.environ if k.startswith("PYICEBERG_")]:
        monkeypatch.delenv(k)
    monkeypatch.setenv("PYICEBERG_HOME", str(tmp_path / "pyiceberg_home"))
    uri = f"postgresql+psycopg2://{ROLE}:{pg.password}@127.0.0.1:{pg.port}/iceberg"
    columns = "SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns " \
              "WHERE table_name = 'iceberg_tables'"
    before = pg.owner(columns).stdout
    cat = ice.open_catalog(uri, f"file://{tmp_path}/warehouse")
    try:
        assert cat._schema_version == "v0", "SELECT on both tables: the V0 catalog Spark created is detected as such"
        assert cat.list_namespaces() == [("gold",)]
        with pytest.raises(SQLAlchemyError, match="read-only transaction"):
            cat.create_namespace("evil")
        assert [t for t, _cols in ice.catalog_schema(cat)] == sorted(ice.CATALOG_TABLES)
    finally:
        cat.engine.dispose()
    assert pg.owner(columns).stdout == before, "the catalog schema changed (no iceberg_type column may appear)"
    assert pg.owner("REVOKE SELECT ON iceberg_namespace_properties FROM graph_ro").returncode == 0
    try:
        with pytest.raises(ice.ProvenanceUnavailable, match="lacks SELECT on iceberg_tables and "
                                                            "iceberg_namespace_properties"):
            ice.open_catalog(uri, f"file://{tmp_path}/warehouse")
    finally:
        assert pg.owner("GRANT SELECT ON iceberg_namespace_properties TO graph_ro").returncode == 0
    with pytest.raises(ice.ProvenanceUnavailable, match="catalog login rejected"):
        ice.open_catalog(uri.replace(pg.password, "wrong-password"), f"file://{tmp_path}/warehouse")
