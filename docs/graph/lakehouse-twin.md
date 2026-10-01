# Graph on gold: the lakehouse twin (Spark, Iceberg, Docker, Airflow)

The local build reads the pandas gold twin. The lakehouse path builds the same graph inside the stack:
Spark publishes it as Iceberg tables next to gold, tags every table it read or wrote, and a Python
container reads it back pinned by tag and snapshot id, runs the unchanged builder and the same contract.
Both paths share one spec (`src/lakehouse_graph/spec.py`); a Docker-free parity check proves the Spark
SQL gives the same tables.

> **TEACHING-ONLY.** The overlay reuses the base stack's sample credentials (Postgres `iceberg`, the Silo
> root key). The Postgres side can be narrowed to a SELECT-only role; the S3 side cannot without a second
> Silo user. No published ports, no Docker socket, non-root, read-only root filesystem, all capabilities
> dropped.

## The pieces

| Piece | What it does |
|---|---|
| `sql/graph/nodes.sql`, `edges.sql`, `similar_to.sql` | Spark SQL for all 21 node and edge tables with `$silver` / `$gold` placeholders. `similar_to.sql` is generated from the spec (`scripts/check_graph_parity.py sql --write`); a test checks it is current. Same quantised key, explicit left-to-right d2 sum, `ROW_NUMBER` over `(d2_q, dst)`. |
| `src/jobs/graph/01_publish_gold_graph.py` | pins all 11 inputs (gold + 10 silver) to one snapshot each, writes `lakehouse.gold.graph_nodes` (PARTITIONED BY label), `graph_edges` (PARTITIONED BY rel_type), `graph_similar_to_scaler` (fitted in SQL, persisted, read back for the kNN) and appends `graph_build_manifest`; then `CREATE TAG graph_<build_id>` on every input and output table. The build id is a hash of the input snapshot ids, the code and the spec, so a re-run with the same inputs writes nothing and never moves a tag. |
| `src/lakehouse_graph/iceberg_source.py` | PyIceberg 0.12 `SqlCatalog` reader under every safety rule below; `scripts/build_graph_local.py build --source iceberg` |
| `scripts/check_graph_parity.py` | the Docker-free parity gate (`parity`), the SQL generator (`sql`), and a local Iceberg lakehouse on a SQLite JDBC catalog (`lakehouse`) |
| `docker-compose.graph.yml`, `docker/graph/Dockerfile` | the `ldl-graph` container (Python 3.12, hash-locked graph deps + the PyIceberg client, no JVM) and one extra mount on `ldl-spark` |
| `pipelines/run_graph_e2e.sh` | publish (Spark) → build from Iceberg → strict contract → lineage → lineage contract → cohorts → promote; every step required |
| `airflow/dags/lakehouse_graph.py` + `lakehouse_graph_operators.py` | the same chain as 7 Airflow tasks, manual trigger only, `max_active_runs=1` |
| `config/graph/postgres_graph_ro.sql` | an idempotent SELECT-only Postgres role for the catalog |

## Reading Iceberg safely

PyIceberg reads the same Postgres JDBC catalog Spark writes. These rules come from probes on this repo's
Iceberg 1.6.1 catalog (V0 schema) and are enforced in code:

- the catalog name must be `lakehouse` (it is the `catalog_name` column of `iceberg_tables`);
- `init_catalog_tables=false` is passed in code (PyIceberg ignores the environment variable);
- `schema_version` is never set: v1 would `ALTER` the catalog Spark owns; a configuration that sets it is
  refused;
- an S3 warehouse needs a local `s3.endpoint` (never `*.amazonaws.com`) and `s3.region`, or PyIceberg
  asks real AWS for the bucket's region;
- both catalog tables are probed before use, so a dead host or a missing grant is an error, not an empty
  catalog; a SQLite catalog opens read-only after an existence check;
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

`scripts/check_graph_parity.py parity --strict` runs the twin's SQL in local PySpark 3.5.3 (JDK 17, no
Docker, no jars needed) on the same pandas input as the numpy builder. Latest run
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
make up && make wait && make churn-e2e          # the lakehouse with churn gold (POSTGRES_PORT=5433 if 5432 is taken)
mkdir -p data/graph
docker compose -f docker-compose.yml -f docker-compose.graph.yml up -d --build spark graph
./pipelines/run_graph_e2e.sh                     # publish -> build from Iceberg -> contract -> lineage -> cohorts -> promote
docker compose -f docker-compose.yml -f docker-compose.graph.yml stop graph && make down
```

- Always pass both compose files, base first; the overlay is not a project on its own. Stop `ldl-graph`
  before `make down` (the base file does not know it).
- `ldl-graph`: `python:3.12.14-slim-trixie` pinned by digest, `pip install --require-hashes
  --only-binary=:all:` of the core lock plus the PyIceberg client lock, uid 10001, `read_only: true`, a
  256 MB tmpfs `/tmp`, `cap_drop: ALL`, `no-new-privileges`, `mem_limit: 1536m`, `pids_limit: 256`, no
  ports. The image measured 629 MB.
- `GRAPH_HOST_ROOT` points `/opt/data/graph` at another host directory; `GRAPH_E2E_SAMPLE_DIR` /
  `GRAPH_E2E_EXPORT_DIR` point the default profile at other bronze and exports inside the container.
- SELECT-only catalog role: run `config/graph/postgres_graph_ro.sql` once against the stack's Postgres
  (its header shows how), then start the overlay with `GRAPH_PG_USER=graph_ro GRAPH_PG_PASSWORD=...`. The
  password goes verbatim into a SQLAlchemy URI, so it must be URL-safe (the script refuses anything else).
  Default privileges cover any future table the catalog owner creates in schema `public` of that
  database. Under this role PyIceberg's writes were refused (three probes).
- `OPENLINEAGE=1 ./pipelines/run_graph_e2e.sh` adds the OpenLineage Spark listener (downloaded from Maven
  Central on first use) with a file transport to `data/graph/lineage/openlineage.jsonl`: runs, parents
  and timing only; on a JDBC catalog it sees no Iceberg datasets.

## Airflow

DAG `lakehouse_graph` (`airflow/dags/lakehouse_graph.py`): `publish_gold_graph >> build_graph >>
check_graph_contract >> build_lineage >> check_lineage_contract >> build_cohorts >> promote`, manual
trigger, one run at a time. Run `lakehouse_churn_features` first. Trigger with
`./pipelines/airflow_trigger.sh lakehouse_graph` after `make airflow-up`; with the webserver on another
port, export `AIRFLOW_WEBSERVER_PORT` in the shell too. Optional OpenLineage:
`--conf '{"openlineage": true}'`.

**This repo pins Airflow 2.10.4, and Airflow 2.x reached end of life on 2026-04-22.** The DAG uses only
`airflow.DAG` and the Bash operator, so an Airflow 3 port changes two imports and the overlay's services,
not the task chain. The DAG module shares its name with the `lakehouse_graph` package; Airflow loads DAG
files by path, so this is harmless there.

## The Docker run

Docker is not started by the docs tooling; these are recorded runs on this Mac (Docker Desktop, seed 42,
2026-10-01). The latest record, with step timings and summary lines, is
[results/docker-e2e.md](results/docker-e2e.md).

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
| read-only role: build, strict contract, cohorts, promote as `graph_ro`; write probe | 15.8 / 20.2 / 3.8 / 0.8; 1.3 | ok; 3 writes refused |
| Airflow `lakehouse_graph` (real Airflow 2.10.4) | 73.5 | all 7 tasks `success` with the pre-`d317368` `ci.yml` (task states in the record); on HEAD its `check_lineage_contract` task runs the same failing contract as step A |

The builder's max RSS inside the container was 576 to 582 MiB: above the 512 MiB soft limit (reported,
not gated), within the 1,536 MiB limit. Each run backed up `data/export` and restored it byte-identical
afterwards, because `make churn-e2e` rewrites it.

## Not done yet

- Tier-1 lineage facts from Iceberg (Snapshot / Ref nodes) and an OpenLineage loader: placeholders only.
- No packet capture shows that PyIceberg makes no AWS DNS lookup; it is enforced by configuration.
- The Iceberg identity also hashes the local bronze CSVs, so the same pins give a different build id with
  or without them, and without local bronze the drift comparison is skipped silently (a minor review
  finding, not fixed).
- `iceberg_source._redact` hides a password in the URI's user-info part, but a password passed as a
  query parameter that contains `@` can leak its tail into the manifest (the compose overlay uses
  user-info only; a minor review finding, not fixed).
- The Make targets `graph-up`, `graph-down`, `graph-e2e`, `airflow-trigger-graph`, `graph-parity` and
  `graph-pg-readonly` are not in the Makefile yet; the commands above work today
  ([operations.md](operations.md#make-targets)).
