# Object store decision record

## Decision (October 2026)

**Default: RustFS 1.0.0** (`rustfs/rustfs:1.0.0`, Apache-2.0, Rust). **Drop-in alternative: SILO**
(`pgsty/silo:RELEASE.2026-09-16T00-00-00Z`, the September security release), selected with
`STORE=silo` on any Make target or `-f docker-compose.silo.yml` on any Compose command.

Both images are pinned by tag **and** multi-arch index digest in `docker-compose.yml` /
`docker-compose.silo.yml`; `tests/unit/test_stack_static.py` fails if a tag floats or a digest is missing.

| | RustFS (default) | SILO (alternative) |
|---|---|---|
| Image | `rustfs/rustfs:1.0.0@sha256:8cc98017…` | `pgsty/silo:RELEASE.2026-09-16T00-00-00Z@sha256:635197cb…` |
| License | Apache-2.0 | AGPL-3.0 (MinIO fork) |
| Compose service / container | `objectstore` / `ldl-objectstore` | same (override) |
| Volume | `objectstore-data` | `silo-data` |
| Ports | 9000 S3 API, 9001 console | same |
| STS `AssumeRole` (what Lakekeeper vends) | yes | yes |
| Healthcheck | `curl -fsS http://127.0.0.1:${S3_API_PORT}/health` | `silo healthcheck ready` |
| RAM measured on the Mac (light profile) | 113 MiB idle, 140 MiB after `make demo-light` | 182–196 MiB after `make demo-light` |
| Cold `make up-light` (empty volumes, images cached) | 8.1 s (warm restart 7.2 s) | 8.7 s (one run 17.2 s) |
| T2 smoke + light demo | 6 passed in 3.0 s; demo OK | 6 passed in 4.25 s; demo OK (1.8 s + 1.2 s) |

The service is called `objectstore` for both, so nothing else (Lakekeeper warehouse, Spark, Trino,
host clients) changes when you switch.

## One endpoint for every client: `http://objectstore.localhost:9000`

Lakekeeper vends short-lived STS credentials **and the endpoint** to every engine. The endpoint stored
in the warehouse must therefore work from inside the Compose network (Spark, Trino, the graph
container) *and* from the host (DuckDB, PyIceberg, Polars). This repo uses one name for both:

- inside `ldl-net`, `objectstore.localhost` is a network alias of the `objectstore` service;
- on the host, `*.localhost` resolves to loopback (macOS, systemd-resolved, glibc 2.36+), and port
  9000 is published as `9000:9000`, so the same URL reaches the same server.

Keep `S3_API_PORT` equal on both sides of the mapping (the store listens on it inside the container
too). If your resolver does not map `*.localhost`, add `127.0.0.1 objectstore.localhost` to `/etc/hosts`
(CI does this as a belt-and-braces step).

Gotcha found while testing: pyarrow's bundled AWS SDK (curl) short-circuits `*.localhost` to 127.0.0.1
*inside containers* too, where that is the container itself. Python **inside a container** therefore
uses PyIceberg's fsspec/s3fs FileIO (`py-io-impl=pyiceberg.io.fsspec.FsspecFileIO`, set in
`docker-compose.graph.yml`). Host Python and the JVM engines are not affected.

## Why RustFS is the default now

- **License and project health:** Apache-2.0, an active project, 1.0.0 GA. MinIO community is
  archived; SILO keeps the MinIO code base alive, but under AGPL.
- **What the stack needs, verified:** S3 + path-style + STS `AssumeRole` for Lakekeeper's vended
  credentials, with PyIceberg, DuckDB, Spark 4.1 (Iceberg `S3FileIO`) and Trino 483 (`fs.native-s3`).
- **Small:** one static binary; 113 MiB idle on the light profile (measured 2026-10-02 on macOS arm64).

## Alternatives considered

| Store | Why not the default |
|---|---|
| **SILO** | Works (kept as the alternative). AGPL; heavier; MinIO-era console. |
| **Garage** | No STS, so no vended credentials; you would hand root keys to every engine. |
| **SeaweedFS** | Its built-in Iceberg catalog refused DuckDB writes in testing; with an external catalog it is a valid store but heavier to operate. |
| **MinIO community** | Archived upstream; no maintained community binaries. |

## Bucket and warehouse setup

`lakehouse-init` (`docker/init/bootstrap.sh`, `curlimages/curl`, non-root) runs on every `up`, idempotently, then
stays up idle (about 1 MiB) and turns healthy (`/tmp/ready`): that is the signal `up --wait`, Spark, Trino
and the graph container wait for. `./pipelines/create_bucket.sh` re-runs it with `--once`. Steps:

1. creates bucket `${S3_BUCKET}` with a SigV4-signed `PUT` (200 or 409 are fine);
2. bootstraps Lakekeeper (400/409 = already bootstrapped);
3. creates warehouse `${LAKEKEEPER_WAREHOUSE}` (bucket, key prefix `warehouse`, region, path-style,
   flavor `s3-compat`, `sts-enabled`, endpoint `S3_ENDPOINT`, STS endpoint `http://objectstore:${S3_API_PORT}`)
   unless it exists. The store's access key is handed to Lakekeeper here once (stored encrypted with
   `LAKEKEEPER_ENCRYPTION_KEY`); engines never see it.

No `mc` client is needed any more (the old `pgsty/mc` image is gone).

## Switching stores, rollback, volumes

- `make up-light STORE=silo` (or `docker compose -f docker-compose.yml -f docker-compose.silo.yml --profile light up -d --wait`).
- The two stores use **different volumes** (`objectstore-data` vs `silo-data`), but the catalog in
  Postgres remembers tables. After switching, run `make reset` (`make purge` + `make up-light`; this
  project's volumes only) so catalog and objects match. `make purge` makes a second `down -v` pass
  without the SILO overlay, because `down -v` only removes volumes that a loaded service mounts.
- Old volumes from the JDBC-catalog era (`silo-data` holding `s3a://lake/warehouse`, Postgres 16
  `postgres-data`) are not readable by the new stack: Postgres 18 refuses a 16 data directory. See
  [MIGRATION.md](../MIGRATION.md).

## Pin policy

- Immutable release tags plus index digests; never `:latest`.
- Bump deliberately: change tag + digest together (`docker buildx imagetools inspect <image>:<tag>`),
  run `make test`, record it in the changelog in [MIGRATION.md](../MIGRATION.md#changelog).
