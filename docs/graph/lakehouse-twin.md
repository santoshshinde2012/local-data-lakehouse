# Graph on gold: the lakehouse twin (Spark, Iceberg, Docker, Airflow)

The local build reads the pandas gold twin. The lakehouse path builds the same graph inside the stack:
Spark publishes it as Iceberg tables next to gold, tags every table it read or wrote, and a Python
container reads it back pinned by tag and snapshot id, runs the unchanged builder and the same contract.
Both paths share one spec (`src/lakehouse_graph/spec.py`); a Docker-free parity check proves the Spark
SQL gives the same tables.

> **TEACHING-ONLY.** The graph container holds **no credentials**: no database URI and no S3 keys. It
> talks only to the Lakekeeper REST catalog, which runs without authentication in this stack, and gets
> short-lived per-table S3 credentials from it (vended credentials). No published ports, no Docker
> socket, non-root, read-only root filesystem, all capabilities dropped.

## The pieces

| Piece | What it does |
|---|---|
| `sql/graph/nodes.sql`, `edges.sql`, `similar_to.sql` | Spark SQL for all 21 node and edge tables with `$silver` / `$gold` placeholders. `similar_to.sql` is generated from the spec (`scripts/check_graph_parity.py sql --write`); a test checks it is current. Same quantised key, explicit left-to-right d2 sum, `ROW_NUMBER` over `(d2_q, dst)`. |
| `src/jobs/graph/01_publish_gold_graph.py` | pins all 11 inputs (gold + 10 silver) to one snapshot each, writes `lakehouse.gold.graph_nodes` (PARTITIONED BY label), `graph_edges` (PARTITIONED BY rel_type), `graph_similar_to_scaler` (fitted in SQL, persisted, read back for the kNN) and appends `graph_build_manifest`; then `CREATE TAG graph_<build_id>` on every input and output table. The build id is a hash of the input snapshot ids, the code and the spec, so a re-run with the same inputs writes nothing and never moves a tag. |
| `src/lakehouse_graph/iceberg_source.py` | PyIceberg 0.12 reader of the Lakekeeper **REST** catalog (`type rest`; `SqlCatalog` only for the local SQLite harness) under every safety rule below; `scripts/build_graph_local.py build --source iceberg` |
| `scripts/check_graph_parity.py` | the Docker-free parity gate (`parity`), the SQL generator (`sql`), and a local Iceberg lakehouse on a SQLite JDBC catalog (`lakehouse`) |
| `docker-compose.graph.yml`, `docker/graph/Dockerfile` | the `ldl-graph` container (Python 3.12, hash-locked graph deps + the PyIceberg client `pyiceberg[pyarrow,s3fs]==0.12.0`, no JVM, part of the `full` profile, waits for `lakehouse-init`) and one extra mount on `ldl-spark` |
| `pipelines/run_graph_e2e.sh` | publish (Spark) → build from Iceberg → strict contract → lineage → lineage contract → cohorts → promote; every step required |
| `airflow/dags/lakehouse_graph.py` + `lakehouse_graph_operators.py` | the same chain as 7 Airflow tasks, manual trigger only, `max_active_runs=1` |

## Reading Iceberg safely

PyIceberg reads the same Lakekeeper REST catalog Spark writes (`http://lakekeeper:8181/catalog`, warehouse
`lakehouse`; `tests/graph/test_compose_graph.py` checks the overlay against `config/spark-defaults.conf`).
These rules are enforced in code (`iceberg_source.catalog_config` / `open_catalog`):

- the catalog name must be `lakehouse`; the type must be `rest` (the stack) or `sql` (the local SQLite
  harness of `scripts/check_graph_parity.py`), anything else is refused;
- a REST catalog needs an `http(s)://` URI and the warehouse name; `X-Iceberg-Access-Delegation:
  vended-credentials` is set by default, so S3 access comes only from the credentials Lakekeeper vends;
- an `s3.endpoint` on `*.amazonaws.com` is refused, and `s3.region` is set (`us-east-1`), or PyIceberg
  asks real AWS for the bucket's region;
- inside the container PyIceberg uses the fsspec/s3fs FileIO (`py-io-impl`), because pyarrow's bundled
  AWS SDK resolves `objectstore.localhost` (the vended endpoint) to the container itself;
- the catalog is probed before use (`list_namespaces` for REST), so a dead host is an error, not an empty
  catalog. For the SQLite harness, `init_catalog_tables=false` is passed in code, `schema_version` is
  never set (v1 would `ALTER` the catalog), and the file opens read-only after an existence check;
- reads are by tag only, never by timestamp: `createOrReplace` cuts the snapshot ancestry and
  `expire_snapshots` trims history, while tags survive both. Each read checks that the tag exists and is a
  tag, points at the recorded snapshot, that the snapshot still exists, and that its row count equals the
  snapshot's total-records and the manifest's count. Any mismatch raises "provenance unavailable", never a
  fallback.

The build then runs the unchanged `build_tables()` on those frames, compares the published Spark twin
with it (every table equal; SIMILAR_TO differences only at quantised ties, because the twin's scaler is
fitted in SQL), and compares with the local CSV path. Byte-identical Parquet keeps the bronze identity;
anything else gets its own identity over the Iceberg pins, with the drift recorded in
`manifest.json["iceberg"]`.

## The source-aware contract

An Iceberg-sourced build is checked like any other, plus:

- **provenance**: the recorded tag, the 11 inputs and 3 twin tables (uuid, snapshot, rows), the identity
  pins and the twin result must be complete and consistent; a payload that does not hash to the build id
  is an error;
- **freshness** follows the build's source (its Iceberg pins), so promote does too;
- **gold content**: the build's gold is compared with the pandas twin's for the same bronze. Differences
  are classified: a rounded feature off by at most one unit in its last decimal is `rounding` (info);
  anything else, or a renewal on one side only, is `beyond rounding` (a warning, so strict fails). With
  rounding drift, goldens that the drifted columns can move are derived from "the bronze's events + this
  build's gold", and every invariant is still computed from the build's own data;
- **determinism**: `iceberg_source.verify_build` re-reads the pins and rebuilds byte for byte; pins that
  no longer hold are an error.

## Parity: Spark SQL vs numpy

`scripts/check_graph_parity.py parity --strict` runs the twin's SQL in local PySpark 4.1.3 (JDK 17 or
21, no Docker, no jars needed) on the same pandas input as the numpy builder. The hermetic harness
(`requirements-graph-spark.txt`: pyspark 4.1.3, iceberg-spark-runtime-4.1_2.13 1.12.0, a SQLite JDBC
catalog) is on the same Spark and Iceberg versions as the stack, and the Docker run below checks the
same SQL there. On 2026-10-02 both parities passed on 4.1.3 (tiny 24.4 s, s42 74.5 s) with the same numbers as the
2026-10-01 pyspark 3.5.3 record in the table below. Latest run
([results/graph-parity-s42.md](results/graph-parity-s42.md), [tiny](results/graph-parity-tiny.md)):

| | tiny | seed 42 |
|---|---|---|
| hard gate: all 21 tables equal cell for cell; SIMILAR_TO identical (src, dst, rank), d2 bit-identical with the persisted scaler | pass (1,182 edges) | pass (80,010 edges) |
| reported (a): scaler fitted in SQL | tie-only | 74,616 d2 values differ (max 5.68e-13), 0 edge or rank differences |
| reported (b): Spark gold vs pandas gold | 0 cells | 2 cells of `accept_rate_change`, max 1.0e-4; labels equal; the twin on Spark gold is tie-only |

So the quantised key is a robustness measure, not a correctness requirement: with the persisted scaler
even the raw order matches. The new-model Spark-vs-pandas drift was measured here for the first time
(the plan's 2.8e-14 was an old-model figure).

### Why gold drifts by 1e-4 in two cells

The two cells (`sub_02258`, `sub_03954`, seed 42) are a rounding difference, not a data difference. The
value being rounded is the same double, bit for bit, on both sides. Spark's `bround(x, 4)` rounds the
decimal string of the double half to even (`BigDecimal.valueOf`, checked in the Spark 3.5.3 bytecode);
numpy and pandas compute `rint(x * 1e4) / 1e4`:

| Renewal | x | Spark | numpy |
|---|---|---:|---:|
| sub_02258 | 0.9483499999999999 (`x * 1e4` rounds to exactly 9483.5) | 0.9483 | 0.9484 |
| sub_03954 | 0.61335 (`Double.toString` gives the tie "0.61335") | 0.6134 | 0.6133 |

A proposal (not applied: the gold SQL is the data owner's file) rewrites every rounded feature in
`sql/churn/gold_renewal_features.sql` as a DOUBLE rounded with `rint(x * 1e4) / 1e4` and casts the
`engagement_trend` counts to DOUBLE. Measured on scratch copies: `check_gold_parity` then passes exactly
(atol = 0) on tiny (121 x 27) and seed 42 (8,001 x 27), Spark gold changes in exactly those 2 cells, the
graph parity report (b) drops to 0 cells, and the lineage answers are unchanged. It makes Spark and numpy
agree; it does not make either rounding "correct". If applied, the lineage golden and the business build
id are re-keyed once; the graph goldens and README numbers do not change.

## The export fix (`04_export_features.py`)

The first Docker run found that the user's Spark export job crashed with
`TypeError: Object of type Decimal is not JSON serializable` after overwriting 2 of its 3 exports, and
wrote `engagement_trend` as `0.8000` (the column is `decimal(31,4)` in Spark gold, because of the
`1.0` / `4.0` literals in its SQL). This branch carries the minimal fix in
`src/jobs/churn/04_export_features.py` (a user file; review it before merging):

- `_plain()` turns a `Decimal` into the float with the same value;
- the three exports are written to hidden staged siblings and moved into place with `os.replace` only
  after all three are written; a failed run leaves the previous exports untouched and no staged file.

`tests/graph/test_export_decimal.py` (6 tests, no Spark needed) fails on the original and passes on the
fix. The three renames are separate steps (a microsecond window), and the staged names are fixed, so two
overlapping export runs could collide; the churn DAG does not set `max_active_runs=1`.

## Docker overlay

```bash
make up-full && make churn-sample && make churn-e2e   # the lakehouse with churn gold
make graph-e2e        # mkdir data/graph; base + docker-compose.graph.yml up --build --wait; run_graph_e2e.sh
make down             # loads every overlay, so it stops ldl-graph too
```

By hand: `docker compose -f docker-compose.yml -f docker-compose.graph.yml --profile full up -d --build --wait`,
then `./pipelines/run_graph_e2e.sh`.

- Always pass both compose files, base first; the overlay is not a project on its own. Switching between
  plain `make up-full` and the overlay recreates `ldl-spark` (its config hash changes).
- `ldl-graph`: `python:3.12.15-slim-trixie` pinned by digest, `pip install --require-hashes
  --only-binary=:all:` of the core lock plus the PyIceberg client lock, uid 10001, `read_only: true`, a
  256 MB tmpfs `/tmp`, `cap_drop: ALL`, `no-new-privileges`, `mem_limit: 1536m`, `pids_limit: 256`, no
  ports. The image measured 629 MB.
- `GRAPH_HOST_ROOT` points `/opt/data/graph` at another host directory; `GRAPH_E2E_SAMPLE_DIR` /
  `GRAPH_E2E_EXPORT_DIR` point the default profile at other bronze and exports inside the container.
- The SELECT-only Postgres role of the JDBC-catalog era (`config/graph/postgres_graph_ro.sql`,
  `GRAPH_PG_USER`) is gone: the graph container no longer connects to Postgres at all.
- `OPENLINEAGE=1 ./pipelines/run_graph_e2e.sh` adds the OpenLineage Spark listener
  (`io.openlineage:openlineage-spark_2.13:1.53.0`, downloaded from Maven Central on first use) with a file
  transport to `data/graph/lineage/openlineage.jsonl`. Not re-run on the REST catalog yet; on the earlier
  JDBC catalog it recorded runs, parents and timing but no Iceberg datasets.

## Airflow

DAG `lakehouse_graph` (`airflow/dags/lakehouse_graph.py`): `publish_gold_graph >> build_graph >>
check_graph_contract >> build_lineage >> check_lineage_contract >> build_cohorts >> promote`, manual
trigger, one run at a time. Run `lakehouse_churn_features` first. Trigger with
`./pipelines/airflow_trigger.sh lakehouse_graph` after `make airflow-up`, with `ldl-graph` running (the
socket proxy allows `docker exec` into `ldl-spark` and `ldl-graph` only). With the API server on another
port, set `AIRFLOW_API_PORT` in `.env`. Optional OpenLineage: `--conf '{"openlineage": true}'`.

The DAG now runs on **Airflow 3.3.2** (`airflow.sdk.DAG`, the standard provider's `BashOperator`; the task
chain is unchanged). `tests/graph/test_dag_graph.py` checks the chain with stubbed Airflow modules; the
`lakehouse_graph` DAG itself has not been triggered on Airflow 3 yet (the retail and churn DAGs have, see
[the Airflow excerpt](../demo/airflow-e2e.excerpt.md)). The DAG module shares its name with the
`lakehouse_graph` package; Airflow loads DAG files by path, so this is harmless there.

## The Docker run

Docker is not started by the docs tooling; these are recorded runs on this Mac (Docker Desktop, seed 42).

**2026-10-03, re-run of every step at `59b08b6`** on existing volumes: `make graph-e2e` passed in 126.6 s
(`run_graph_e2e.sh` itself 109 s) with 40,204 nodes / 130,366 edges, both contracts passing, 15 Leiden and 15 Louvain
cohorts, and promote. The Airflow `lakehouse_graph` DAG then ran all 7 tasks `success` in 141.8 s. The record, with
every step's timing and the summary lines, is [results/docker-e2e.md](results/docker-e2e.md). Earlier the same day,
from empty volumes, `make graph-e2e` took 118.1 s and 103.1 s ([graph-e2e.excerpt.md](../demo/graph-e2e.excerpt.md)).

**2026-10-02, after the Iceberg 1.12.0 bump** (`ldl-graph` on python 3.12.15, ladybug 0.21.2): `make
graph-e2e` passed again, 161 s including the overlay build (`run_graph_e2e.sh` itself 97 s), with the same
40,204 nodes / 130,366 edges and both contracts passing.

**2026-10-02, REST catalog stack** (Spark 4.1.3 / Iceberg 1.11.0 / Lakekeeper v0.13.6 / RustFS 1.0.0,
`pipelines/run_graph_e2e.sh` on branch `feat/local-first-stack-2026`): every step passed in **178 s**
end to end: Spark publish, PyIceberg build from the REST catalog (40,204 nodes / 130,366 edges), strict
source-aware contract (PIT parity 0 mismatches; golden s42 derived; gold drift 2 cells, info), lineage
build and lineage contract (golden `core.json`), cohorts (15 Leiden / 15 Louvain) and promote. Excerpt:
[graph-e2e.excerpt.md](../demo/graph-e2e.excerpt.md). The Tier-1 overlay
(`build_lineage_local.py --iceberg`) also reads snapshots and refs through the REST catalog.

**2026-10-01, JDBC catalog stack** (Spark 3.5.3 / Iceberg 1.6.1 / SILO / Airflow 2.10.4). Kept for history;
`results/docker-e2e.md` now holds the 2026-10-03 run:

| Step | Seconds | Result |
|---|---:|---|
| `make up` / `make wait` | 8.7 / 0.3 | ok |
| `make churn-e2e` | 38 to 40 | ok, export fixed (all three files written) |
| overlay up | 6.7 | ok |
| Spark publish (40,204 nodes / 130,366 edges / 80,010 SIMILAR_TO, 15 tables tagged) | 28 | ok; a second publish writes nothing (7.5 to 8.1 s) |
| PyIceberg build | 16 to 18 | ok: twin equal, SIMILAR_TO tie-only, own "iceberg inputs" identity |
| strict source-aware contract | 19 to 20 | **OK**: golden s42 (derived), gold drift 2 cells (rounding, info), pins re-read byte-identically |
| negative test: build pinned to a tag that does not exist (`graph_000000000000`) | 1.5 | refused as expected (exit 1, "provenance unavailable") |
| lineage build / lineage contract | 3.5 / 1.8 | **FAIL** on HEAD `d317368` (the CI clone line, [lineage.md](lineage.md#known-issue-the-ci-clone-line)) |
| the same chain with the pre-`d317368` `ci.yml` shown to the container | 55.7 to 78 | complete: lineage contract OK, cohorts, promote |
| read-only role (removed since): build, strict contract, cohorts, promote as `graph_ro`; write probe | 15.8 / 20.2 / 3.8 / 0.8; 1.3 | ok; 3 writes refused |
| Airflow `lakehouse_graph` (real Airflow 2.10.4) | 73.5 | all 7 tasks `success` with the pre-`d317368` `ci.yml` (task states in the record); on HEAD its `check_lineage_contract` task runs the same failing contract as step A |

The builder's max RSS inside the container was 576 to 582 MiB: above the 512 MiB soft limit (reported,
not gated), within the 1,536 MiB limit. Each run backed up `data/export` and restored it byte-identical
afterwards, because `make churn-e2e` rewrites it.

## Not done yet

- OpenLineage (Tier 2) has not been re-run on the REST catalog, and there is no OpenLineage loader yet.
- No packet capture shows that PyIceberg makes no AWS DNS lookup; it is enforced by configuration.
- The Make targets `graph-up`, `graph-down`, `airflow-trigger-graph` and `graph-parity` are not in the
  Makefile; `make graph-e2e` and the commands above work today ([operations.md](operations.md#make-targets)).
