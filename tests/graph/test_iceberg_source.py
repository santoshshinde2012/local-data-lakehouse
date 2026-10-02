"""PyIceberg safety rules of lakehouse_graph.iceberg_source (no Spark, no Java).

The catalog is opened only as `lakehouse`, with init_catalog_tables=false passed in code,
schema_version refused, an s3 warehouse only with a local endpoint AND s3.region, SQLite
read-only after checking the file exists, and both catalog tables probed; on a V0 catalog (the
shape Iceberg 1.6.1 / the Docker stack creates) opening and probing issue no DDL at all. Those
tests need pyiceberg (requirements-graph-spark*.txt, .venv-graph-spark) and skip without it.

In any venv: the catalog URI a manifest or an error message prints never carries a fragment of a
secret, wherever the password is ('@' in the user-info part or in a query parameter), and an
Iceberg-sourced build's identity is the pins + code + spec + versions + platform, never the local
bronze CSVs (the same tag and pins give the same build id with any sample dir, or none).
"""
from __future__ import annotations

import hashlib
import random
import shutil
import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))

from lakehouse_graph import build, spec  # noqa: E402
from lakehouse_graph import iceberg_source as ice  # noqa: E402
from lakehouse_graph import manifest as mf  # noqa: E402

TINY = REPO / spec.TINY_FIXTURE


@pytest.fixture
def pyiceberg():
    """The tests that open a real catalog: pyiceberg is in requirements-graph-spark*.txt."""
    return pytest.importorskip("pyiceberg", reason="pyiceberg is in requirements-graph-spark*.txt (.venv-graph-spark)")

# Iceberg 1.6.1 JdbcUtil V0 DDL (the same text scripts/check_graph_parity.py pre-creates).
V0 = (
    "CREATE TABLE iceberg_tables(catalog_name VARCHAR(255) NOT NULL,table_namespace VARCHAR(255) NOT NULL,"
    "table_name VARCHAR(255) NOT NULL,metadata_location VARCHAR(1000),previous_metadata_location VARCHAR(1000),"
    "PRIMARY KEY (catalog_name, table_namespace, table_name))",
    "CREATE TABLE iceberg_namespace_properties(catalog_name VARCHAR(255) NOT NULL,namespace VARCHAR(255) NOT NULL,"
    "property_key VARCHAR(255),property_value VARCHAR(1000),PRIMARY KEY (catalog_name, namespace, property_key))",
)


@pytest.fixture(autouse=True)
def isolated_pyiceberg_config(monkeypatch, tmp_path):
    """No ~/.pyiceberg.yaml and no PYICEBERG_* from the caller's shell."""
    import os

    for k in [k for k in os.environ if k.startswith("PYICEBERG_")]:
        monkeypatch.delenv(k)
    monkeypatch.setenv("PYICEBERG_HOME", str(tmp_path / "pyiceberg_home"))


def _v0_catalog(path: Path) -> Path:
    con = sqlite3.connect(path)
    for ddl in V0:
        con.execute(ddl)
    con.commit()
    con.close()
    return path


def _schema(path: Path) -> tuple:
    con = sqlite3.connect(path)
    try:
        master = con.execute("SELECT type, name, sql FROM sqlite_master ORDER BY name").fetchall()
        version = con.execute("PRAGMA schema_version").fetchone()
        cols = {t: con.execute(f"PRAGMA table_info({t})").fetchall() for _k, t, _s in master if _k == "table"}
    finally:
        con.close()
    return master, version, cols


def test_provenance_unavailable_says_so():
    assert str(ice.ProvenanceUnavailable("tag graph_x moved")) == "provenance unavailable: tag graph_x moved"


def test_schema_version_is_refused(tmp_path, pyiceberg):
    db = _v0_catalog(tmp_path / "c.db")
    for key in ("schema_version", "schema-version"):
        with pytest.raises(ice.ProvenanceUnavailable, match="ALTER"):
            ice.catalog_config(f"sqlite:///{db}", f"file://{tmp_path}", **{key: "v1"})


def test_init_catalog_tables_is_false_in_code_whatever_the_environment(tmp_path, monkeypatch, pyiceberg):
    db = _v0_catalog(tmp_path / "c.db")
    monkeypatch.setenv("PYICEBERG_CATALOG__LAKEHOUSE__URI", f"sqlite:///{db}")
    monkeypatch.setenv("PYICEBERG_CATALOG__LAKEHOUSE__INIT_CATALOG_TABLES", "true")
    cfg = ice.catalog_config()
    assert cfg["init_catalog_tables"] == "false"
    assert cfg["uri"] == f"sqlite:///file:{db}?mode=ro&uri=true", "the environment URI is used, read-only"


def test_s3_warehouse_needs_a_local_endpoint_and_a_region(tmp_path, pyiceberg):
    db = _v0_catalog(tmp_path / "c.db")
    uri = f"sqlite:///{db}"
    with pytest.raises(ice.ProvenanceUnavailable, match="local s3.endpoint"):
        ice.catalog_config(uri, "s3a://lake/warehouse")
    with pytest.raises(ice.ProvenanceUnavailable, match="s3.region"):
        ice.catalog_config(uri, "s3a://lake/warehouse", **{"s3.endpoint": "http://silo:9000"})
    with pytest.raises(ice.ProvenanceUnavailable, match="must be local"):
        ice.catalog_config(uri, "s3a://lake/warehouse", **{"s3.endpoint": "https://s3.us-east-1.amazonaws.com",
                                                           "s3.region": "us-east-1"})
    local = {"s3.endpoint": "http://silo:9000", "s3.region": "us-east-1"}
    cfg = ice.catalog_config(uri, "s3a://lake/warehouse", **local)
    assert cfg["s3.region"] == "us-east-1" and cfg["s3.endpoint"] == "http://silo:9000"


def test_only_a_rest_or_a_sql_catalog_is_accepted(tmp_path, pyiceberg):
    db = _v0_catalog(tmp_path / "c.db")
    with pytest.raises(ice.ProvenanceUnavailable, match="type rest.*type sql"):
        ice.catalog_config(f"sqlite:///{db}", f"file://{tmp_path}", type="hive")
    with pytest.raises(ice.ProvenanceUnavailable, match="REST catalog URI must be http"):
        ice.catalog_config(f"sqlite:///{db}", f"file://{tmp_path}", type="rest")
    with pytest.raises(ice.ProvenanceUnavailable, match="not configured"):
        ice.catalog_config(None, f"file://{tmp_path}")


def test_a_missing_sqlite_catalog_is_refused_and_never_created(tmp_path, pyiceberg):
    db = tmp_path / "nope" / "catalog.db"
    with pytest.raises(ice.ProvenanceUnavailable, match="does not exist"):
        ice.open_catalog(f"sqlite:///{db}", f"file://{tmp_path}")
    assert not db.exists()
    with pytest.raises(ice.ProvenanceUnavailable, match="absolute path"):
        ice.catalog_config("sqlite:///relative/catalog.db", f"file://{tmp_path}")


def test_an_empty_database_is_explained_not_initialised(tmp_path, pyiceberg):
    db = tmp_path / "empty.db"
    sqlite3.connect(db).close()
    before = db.read_bytes()
    with pytest.raises(ice.ProvenanceUnavailable, match="no catalog tables yet"):
        ice.open_catalog(f"sqlite:///{db}", f"file://{tmp_path}")
    assert db.read_bytes() == before, "no catalog table was created"


def test_opening_a_v0_catalog_issues_no_ddl(tmp_path, pyiceberg):
    db = _v0_catalog(tmp_path / "c.db")
    before, digest = _schema(db), hashlib.sha256(db.read_bytes()).hexdigest()
    cat = ice.open_catalog(f"sqlite:///{db}", f"file://{tmp_path / 'warehouse'}")
    assert cat.name == "lakehouse"
    assert [t for t, _ in ice.catalog_schema(cat)] == ["iceberg_namespace_properties", "iceberg_tables"]
    assert cat.list_namespaces() == []
    with pytest.raises(ice.ProvenanceUnavailable, match="no publish yet|does not exist"):
        ice.resolve_publish(cat)
    cat.engine.dispose()
    assert _schema(db) == before, "catalog schema changed (an ALTER / CREATE ran)"
    assert hashlib.sha256(db.read_bytes()).hexdigest() == digest


def test_catalog_uri_is_redacted_in_provenance():
    assert ice._redact("postgresql+psycopg2://iceberg:s3cret@postgres:5432/iceberg") == \
        "postgresql+psycopg2://postgres:5432/iceberg"
    # the verifier's case (p2c-lakehouse-twin round 2): '@' inside a query parameter's password. The last
    # '@' used to end the "user-info", and the tail of the password was printed as the host.
    got = ice._redact("postgresql+psycopg2://host:5432/db?user=u&password=sec@ret-tail")
    assert got == "postgresql+psycopg2://<redacted>" and "ret-tail" not in got
    for uri, want in (
            ("postgresql+psycopg2://graph_ro:p@ss@word@postgres:5432/iceberg?sslmode=disable", "postgres:5432/iceberg"),
            ("postgresql+psycopg2://graph_ro:a:b?c#d%41e@postgres:5432/iceberg", "postgres:5432/iceberg"),
            ("postgresql://host:5432/db?password=secret", "host:5432/db"),        # no '@': the query is dropped
            ("postgresql://u:pw@host:5432/db?password=a@b", ice.REDACTED_URI),    # both: ambiguous
            ("postgresql://u:x@evil.example?q@postgres:5432/db", ice.REDACTED_URI),  # a host-like password part
            ("postgresql://u:1234@[::1]:5432/db#frag", "[::1]:5432/db"),
            ("postgresql://u:pw@host", "host")):
        assert ice._redact(uri) == f"{uri.partition('://')[0]}://{want}", uri
    assert ice._redact("sqlite:////abs/catalog.db") == "sqlite:////abs/catalog.db"     # a path, no credentials
    assert ice._redact("no scheme at all") == "<catalog URI without a scheme>"


def test_redaction_never_prints_a_fragment_of_a_secret():
    """A property over random catalog URIs (seeded): passwords of 6 to 14 characters drawn with
    / : ? # % @ = & and spaces, in the user-info part, in a query parameter, in both or in neither.
    The redacted URI is either the true scheme://host[:port][/db] or scheme://<redacted>, never holds a
    password, a user name or an '@', and is the true authority whenever no '@' is ambiguous."""
    rng = random.Random(20261001)
    alphabet = "abcdefgh0123456789/:?#%@=& ._-"
    printed = redacted = 0
    for _ in range(3000):
        secret = "".join(rng.choice(alphabet) for _ in range(rng.randint(6, 14)))
        query_secret = "".join(rng.choice(alphabet) for _ in range(rng.randint(6, 14)))
        host = rng.choice(["postgres", "db.internal", "10.0.0.7", "[::1]"])
        authority = host + rng.choice(["", ":5432"]) + rng.choice(["", "/iceberg", "/lake_db"])
        user = rng.choice(["", "graph_ro:", "iceberg:"])
        where = rng.choice(["userinfo", "query", "both", "neither"])
        uri = "postgresql+psycopg2://"
        uri += f"{user or 'u:'}{secret}@" if where in ("userinfo", "both") else ""
        uri += authority
        uri += f"?sslmode=disable&password={query_secret}" if where in ("query", "both") else ""
        got = ice._redact(uri)
        assert got in (f"postgresql+psycopg2://{authority}", f"postgresql+psycopg2://{ice.REDACTED_URI}"), uri
        assert "@" not in got and "graph_ro" not in got and "sslmode" not in got, uri
        for s in (secret, query_secret):
            assert s not in got or s in authority, (uri, got)
        # what can make two readings valid: an '@' in the query, or an '@', '?' or '#' in the user-info password
        # ('u:83709#x@postgres' also reads as host u, port 83709 and a fragment)
        ambiguous = (where in ("userinfo", "both") and any(c in secret for c in "@?#")) or \
            (where in ("query", "both") and "@" in query_secret)
        if not ambiguous:
            assert got == f"postgresql+psycopg2://{authority}", (uri, got)          # useful when it can be
        printed += got.endswith(authority)
        redacted += got.endswith(ice.REDACTED_URI)
    assert printed > 2000 and redacted > 200, (printed, redacted)          # measured: 2,731 and 269 of 3,000


# --------------------------------------------------------------------------- identity: pins, never the local CSVs
PINS = {"lakehouse.gold.churn_renewal_features": 7_000_011, "lakehouse.silver.churn_usage_daily": 7_000_009}


def test_the_iceberg_identity_never_reads_or_hashes_the_local_bronze(tmp_path, monkeypatch):
    """iceberg_identity is sha256 over the pins, the content code + iceberg_source.py, spec, the SIMILAR_TO
    parameters (the bronze identity's), versions and platform: the same pins give the same id with the
    tiny bronze, an empty sample dir, no sample dir, or other CSV bytes; another pin is another id; and
    nothing under the sample dir is read (the verifier's p2c round 2 minor: the id hashed unread CSVs)."""
    other = tmp_path / "other"
    shutil.copytree(TINY, other)
    first = sorted(other.glob("*.csv"))[0]
    first.write_bytes(first.read_bytes() + b"\n")                         # other bronze bytes
    (tmp_path / "empty").mkdir()
    ids = {str(d): ice.iceberg_identity(d, PINS) for d in (TINY, other, tmp_path / "empty", None)}
    assert len({i["business_build_id"] for i in ids.values()}) == 1, ids
    payload = ids[str(TINY)]["payload"]
    assert set(payload) == {"code", "spec", "params", "versions", "platform", "iceberg_inputs"}
    assert payload["params"] == mf.build_identity(TINY)["payload"]["params"] == ice.identity_params()
    assert payload["code"]["src/lakehouse_graph/iceberg_source.py"] == mf.sha256_file(REPO / "src/lakehouse_graph/"
                                                                                       "iceberg_source.py")
    assert set(mf.CONTENT_CODE) < set(payload["code"]) and payload["iceberg_inputs"] == dict(sorted(PINS.items()))
    assert mf.sha256_json(payload)[:12] == ids[str(TINY)]["business_build_id"]
    moved = {**PINS, "lakehouse.gold.churn_renewal_features": 7_000_012}
    assert ice.iceberg_identity(TINY, moved)["business_build_id"] != ids[str(TINY)]["business_build_id"]

    def refuse(*_a, **_k):
        raise AssertionError("iceberg_identity read the local bronze")

    monkeypatch.setattr(mf, "input_hashes", refuse)
    monkeypatch.setattr(mf, "build_identity", refuse)
    assert ice.iceberg_identity(TINY, PINS) == ids[str(TINY)]


def test_an_iceberg_build_id_is_the_same_with_or_without_local_bronze(tmp_path, monkeypatch):
    """End to end with test_contract_iceberg's fake lakehouse (a drifted gold: an Iceberg identity): the
    same tag and pins give the same business_build_id and byte-identical Parquet with the profile's bronze
    and with an empty sample dir. Without bronze the build says so (a NOTE: no local comparison, no drift
    record), the manifest keeps the local bronze sha256 only as information, and both builds are fresh
    by their pins."""
    from test_contract_iceberg import DRIFT, FakeLake, Holder, drifted, install

    silver, gold, _ = build.run_gold(build.load_gold_twin(TINY))
    holder = Holder()
    install(monkeypatch, holder)
    holder.lake = FakeLake(silver, drifted(gold, DRIFT))
    (tmp_path / "empty").mkdir()
    built, notes = {}, {}
    for name, sample in (("bronze", TINY), ("empty", tmp_path / "empty")):
        lines: list[str] = []
        built[name] = ice.build_from_iceberg("default", tmp_path / name / "graph", sample_dir=sample,
                                             export_dir=tmp_path / name / "export", log=lines.append)
        notes[name] = [line for line in lines if "NOTE: no bronze CSVs" in line]
    (bdir_a, man_a), (bdir_b, man_b) = built["bronze"], built["empty"]
    assert man_a["iceberg"]["identity"] == man_b["iceberg"]["identity"] == "iceberg inputs"
    assert man_a["business_build_id"] == man_b["business_build_id"] == bdir_a.name == bdir_b.name
    assert {k: v["sha256"] for k, v in man_a["files"].items()} == {k: v["sha256"] for k, v in man_b["files"].items()}
    assert man_a["iceberg"]["identity_payload"] == man_b["iceberg"]["identity_payload"]
    assert "inputs" not in man_a["iceberg"]["identity_payload"]
    assert man_a["inputs"]["sha256"] == mf.input_hashes(TINY) and man_b["inputs"]["sha256"] == {}   # information
    assert man_a["iceberg"]["local_path"]["compared"] and not man_b["iceberg"]["local_path"]["compared"]
    assert notes == {"bronze": [], "empty": notes["empty"]} and len(notes["empty"]) == 1, notes
    assert mf.is_fresh(man_a) and mf.is_fresh(man_b)
