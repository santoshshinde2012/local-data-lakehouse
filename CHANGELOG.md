# Changelog

Notable changes, newest first. Dates are when the change was verified. Each entry links its PR.

## Unreleased: repo structure cleanup

- `config/catalog.md` moved to `docs/catalog.md` and `MIGRATION.md` to `docs/migration.md`.
  This changelog was split out of the migration guide. Added `SECURITY.md`, `CODE_OF_CONDUCT.md`, `.editorconfig`,
  and issue and pull-request templates; `.gitignore` gaps filled.

## 2026-10-03: sample customer, naming and docs (PR #16)

- **Sample customer:** the worked-example customer is now `sub_santosh` across the generator, fixtures, graph, tools, evals, tests and docs.
  The golden values are unchanged.
- **Naming:** one lowercase-kebab convention for docs, checked by `make docs-check` (T0 and CI).
- **Docs:** README rewrite; new `docs/reference.md`, `docs/renewal-features.md`, `docs/airflow.md` and `docs/troubleshooting.md`.
  `RESULTS.md`, the demo excerpts and the graph evidence were re-captured from an empty-volume run on 2026-10-03.
- **Fixes:** `pipelines/airflow_wait.sh` reads `AIRFLOW_API_PORT` from `.env`; `make graph-e2e` recreates `ldl-graph` so its single-file mounts are current.

## 2026-10-02: local-first stack

PR #13, branch `feat/local-first-stack-2026`. Upgrade steps: [docs/migration.md](docs/migration.md).

- **Stack:** Lakekeeper v0.13.6 REST catalog on Postgres 18.6; RustFS 1.0.0 (default) or SILO
  RELEASE.2026-09-16T00-00-00Z (`STORE=silo`); `lakehouse-init` (curl 8.22.0); profiles `light`
  (no JVM), `full` (+ Spark 4.1.3), `trino` (+ Trino 483). Every image pinned by tag + digest.
- **Engines:** Spark 4.1.3 (Scala 2.13, Java 21) + Iceberg 1.12.0 with `S3FileIO`; host DuckDB 1.5.6,
  PyIceberg 0.12.0, Polars 1.44.2, pyarrow 25.0.1 (`requirements.txt`, hash-locked, uv).
- **Security:** no secrets in tracked files; vended credentials; non-root containers; Airflow behind a
  socket proxy with generated secrets.
- **Airflow 3.3.2** overlay (was 2.10.4).
- **Graph:** PyIceberg REST catalog (was `SqlCatalog` on the JDBC tables); Postgres read-only role removed.
- **Graph dependencies:** `ldl-graph` on python 3.12.15; ladybug 0.21.2, sqlglot 30.21.0, pydantic-ai-slim
  2.53.0, openai 3.23.0; the local Spark harness on pyspark 4.1.3 + Iceberg 1.12.0 (was 3.5.3 / 1.6.1),
  psycopg2 removed from every lock.
- **Tests and CI:** T0–T3 tiers; CI on Node 24 actions (checkout@v7, setup-python@v7, setup-java@v6), uv 0.12.22, pyspark 4.1.3 parity on Java 21;
  the Retention Radar step uses radar `main` (v2 reader since radar #21; `RADAR_REF` for a paired change).
- **Fixes:** nondeterministic silver dedupe; Iceberg 1.11+ time-travel option; lineage extractor
  resolution of the bronze ingest and the radar step; CI Airflow compose validation; shellcheck SC2015
  in `scripts/graph_mcp.sh`.
- **Docs:** README, `docs/catalog.md` (then `config/catalog.md`), `docs/object-store.md`, `docs/graph/*`, demo excerpts; the
  49 MB demo video removed from the tree (attach it to a GitHub Release).
