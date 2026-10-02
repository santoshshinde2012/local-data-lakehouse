"""T1: engine contract against throwaway containers (testcontainers): Postgres 18 + Lakekeeper + RustFS.

Independent of the Compose stack (own network, random ports, nothing published on 8181/9000), so it
runs next to a live `make up-*`. The images are the digests docker-compose.yml pins.

Checks: Lakekeeper migrate -> serve -> bootstrap -> warehouse with STS; PyIceberg create + append
with vended credentials (no S3 keys on the client); DuckDB ATTACH + INSERT/UPDATE/DELETE/MERGE;
PyIceberg sees DuckDB's snapshots; time travel to an earlier snapshot; Polars reads through PyIceberg.

The object store listens on the SAME free port P inside the network and on the host, and the
warehouse records http://objectstore.localhost:P (network alias inside, loopback outside), the
trick docker-compose.yml uses with port 9000.
"""
from __future__ import annotations

import socket
import time
import uuid

import pytest
from support.ldl import http_json, image, wait_http

pytest.importorskip("testcontainers")
docker = pytest.importorskip("docker")

from testcontainers.core.container import DockerContainer  # noqa: E402
from testcontainers.core.network import Network  # noqa: E402

AK, SK = "contract-admin", "contract-secret-0123456789"
PG_USER, PG_PASS, PG_DB = "lakekeeper", "contract-pg", "lakekeeper"
WAREHOUSE, BUCKET, REGION = "contract", "contract", "us-east-1"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_exec(c: DockerContainer, cmd: list[str], timeout: float = 60) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if c.get_wrapped_container().exec_run(cmd).exit_code == 0:
            return
        time.sleep(0.5)
    raise TimeoutError(f"{cmd} never succeeded")


@pytest.fixture(scope="module")
def stack():
    try:
        docker.from_env().ping()
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"no Docker daemon: {e}")
    t0 = time.perf_counter()
    started: list[DockerContainer] = []
    net = Network()
    net.create()
    lk_env = {"LAKEKEEPER__PG_DATABASE_URL_READ": f"postgresql://{PG_USER}:{PG_PASS}@pg:5432/{PG_DB}",
              "LAKEKEEPER__PG_DATABASE_URL_WRITE": f"postgresql://{PG_USER}:{PG_PASS}@pg:5432/{PG_DB}",
              "LAKEKEEPER__PG_ENCRYPTION_KEY": "contract-encryption-key-0123456789"}
    try:
        pg = (DockerContainer(image("postgres")).with_env("POSTGRES_USER", PG_USER)
              .with_env("POSTGRES_PASSWORD", PG_PASS).with_env("POSTGRES_DB", PG_DB)
              .with_network(net).with_network_aliases("pg"))
        pg.start()
        started.append(pg)
        port = _free_port()
        store = (DockerContainer(image("rustfs/rustfs")).with_env("RUSTFS_ACCESS_KEY", AK)
                 .with_env("RUSTFS_SECRET_KEY", SK).with_env("RUSTFS_ADDRESS", f":{port}")
                 .with_env("RUSTFS_OBS_LOG_DIRECTORY", "").with_bind_ports(port, port)
                 .with_network(net).with_network_aliases("objectstore", "objectstore.localhost"))
        store.start()
        started.append(store)
        _wait_exec(pg, ["pg_isready", "-U", PG_USER, "-d", PG_DB])
        _wait_exec(pg, ["psql", "-U", PG_USER, "-d", PG_DB, "-c", "select 1"])

        migrate = DockerContainer(image("lakekeeper/catalog")).with_command("migrate").with_network(net)
        for k, v in lk_env.items():
            migrate.with_env(k, v)
        migrate.start()
        started.append(migrate)
        rc = migrate.get_wrapped_container().wait(timeout=120)["StatusCode"]
        assert rc == 0, migrate.get_logs()

        lk = DockerContainer(image("lakekeeper/catalog")).with_command("serve").with_exposed_ports(8181).with_network(net)
        for k, v in lk_env.items():
            lk.with_env(k, v)
        lk.start()
        started.append(lk)
        base = f"http://{lk.get_container_host_ip()}:{lk.get_exposed_port(8181)}"
        wait_http(f"{base}/health", 60)
        wait_http(f"http://127.0.0.1:{port}/health", 60)

        import s3fs

        s3fs.S3FileSystem(key=AK, secret=SK, client_kwargs={"endpoint_url": f"http://127.0.0.1:{port}",
                                                            "region_name": REGION}).mkdir(BUCKET)
        http_json(f"{base}/management/v1/bootstrap", {"accept-terms-of-use": True}, "POST")
        http_json(f"{base}/management/v1/warehouse", {
            "warehouse-name": WAREHOUSE,
            "storage-profile": {"type": "s3", "bucket": BUCKET, "key-prefix": "wh", "region": REGION,
                                "endpoint": f"http://objectstore.localhost:{port}",
                                "sts-endpoint": f"http://objectstore:{port}", "path-style-access": True,
                                "flavor": "s3-compat", "sts-enabled": True},
            "storage-credential": {"type": "s3", "credential-type": "access-key", "aws-access-key-id": AK,
                                   "aws-secret-access-key": SK}}, "POST")
        yield {"base": base, "port": port, "startup_s": round(time.perf_counter() - t0, 1)}
    finally:
        for c in reversed(started):
            try:
                c.stop()
            except Exception:  # noqa: BLE001
                pass
        net.remove()


@pytest.fixture(scope="module")
def ns(stack):
    import lakehouse_client as lc

    cat = lc.catalog(stack["base"], WAREHOUSE)
    name = f"t1_{uuid.uuid4().hex[:8]}"
    cat.create_namespace(name)
    return cat, name


def test_startup_is_quick(stack):
    print(f"T1 stack (Postgres + RustFS + Lakekeeper migrate/serve + bootstrap) ready in {stack['startup_s']} s")
    assert stack["startup_s"] < 180


def test_pyiceberg_create_append_with_vended_credentials(ns):
    import pyarrow as pa

    cat, name = ns
    data = pa.table({"id": pa.array(range(1000), pa.int64()), "amount": pa.array([i * 0.5 for i in range(1000)]),
                     "customer": pa.array([f"c{i % 10}" for i in range(1000)])})
    t = cat.create_table(f"{name}.orders", schema=data.schema)
    t.append(data)
    props = t.io.properties
    assert props.get("s3.session-token"), "Lakekeeper must vend a session token"
    assert props.get("s3.access-key-id") != AK, "the client must never get the store's root key"
    assert props.get("s3.endpoint", "").startswith("http://objectstore.localhost:")
    assert t.scan().to_arrow().num_rows == 1000


def test_duckdb_attach_insert_update_delete_merge(stack, ns):
    import lakehouse_client as lc

    cat, name = ns
    first = cat.load_table(f"{name}.orders").current_snapshot().snapshot_id
    con = lc.duckdb_connect(stack["base"], WAREHOUSE)
    tbl = f"lakehouse.{name}.orders"
    assert con.sql(f"SELECT count(*), sum(amount) FROM {tbl}").fetchone() == (1000, 249750.0)
    con.sql(f"INSERT INTO {tbl} VALUES (1000, 1.0, 'c_new')")
    con.sql(f"UPDATE {tbl} SET amount = amount + 1 WHERE id < 10")
    con.sql(f"DELETE FROM {tbl} WHERE id = 999")
    con.sql(f"""MERGE INTO {tbl} t USING (SELECT 5::BIGINT AS id, 42.0::DOUBLE AS amount, 'm' AS customer) s
                ON t.id = s.id WHEN MATCHED THEN UPDATE SET amount = s.amount
                WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.amount, s.customer)""")
    con.sql(f"CREATE TABLE lakehouse.{name}.by_customer AS SELECT customer, sum(amount) AS total FROM {tbl} GROUP BY 1")
    assert con.sql(f"SELECT amount FROM {tbl} WHERE id = 5").fetchone() == (42.0,)
    assert con.sql(f"SELECT count(*) FROM {tbl}").fetchone() == (1000,)
    # Time travel: the PyIceberg append is still readable after four DuckDB commits.
    assert con.sql(f"SELECT count(*) FROM {tbl} AT (VERSION => {first})").fetchone() == (1000,)
    assert con.sql(f"SELECT max(id) FROM {tbl} AT (VERSION => {first})").fetchone() == (999,)


def test_pyiceberg_sees_duckdb_commits(ns):
    cat, name = ns
    t = cat.load_table(f"{name}.orders")
    assert len(t.metadata.snapshots) >= 5
    rows = t.scan(row_filter="id = 5").to_arrow().to_pylist()
    assert rows == [{"id": 5, "amount": 42.0, "customer": "c5"}]
    assert cat.load_table(f"{name}.by_customer").scan().to_arrow().num_rows == 11


def test_polars_reads_through_pyiceberg(stack, ns):
    import polars as pl

    import lakehouse_client as lc

    cat, name = ns
    t = cat.load_table(f"{name}.orders")
    df = lc.polars_scan(t).filter(pl.col("id") < 10).collect()
    assert df.height == 10 and df["amount"].sum() == pytest.approx(sum(i * 0.5 + 1 for i in range(10)) - (2.5 + 1) + 42.0)
    snaps = sorted(t.metadata.snapshots, key=lambda s: s.sequence_number)
    print("T1 snapshots:", [(s.sequence_number, s.summary.operation.value, s.summary.get("total-records")) for s in snaps])
    earliest = snaps[0].snapshot_id   # the PyIceberg append
    assert lc.polars_scan(t, snapshot_id=earliest).collect().height == 1000
