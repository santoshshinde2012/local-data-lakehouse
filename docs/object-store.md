# Object store decision record (beginner)

## Decision

**Use SILO** (`docker.io/pgsty/silo`) as the S3-compatible object store for this lakehouse.

Pinned images (immutable tags — never `:latest` for server/client):

| Role | Image |
|------|--------|
| Server | `docker.io/pgsty/silo:RELEASE.2026-09-03T13-18-01Z` |
| Client (`mc` alias) | `docker.io/pgsty/mc:RELEASE.2026-09-03T07-13-05Z` |

Compose service: `silo` · container: `ldl-silo` · volume: `silo-data` · API `:9000` · console `:9001`.

## Why we left MinIO

The **MinIO community** GitHub project is **archived** (maintenance mode; no maintained community binaries). For a teaching repo that needs ongoing S3A + Iceberg compatibility, staying on `quay.io/minio/minio:latest` is a dead end.

Optional background: [MinIO is dead — maholick.com](https://maholick.com/blog/minio-is-dead-the-end-of-an-era-in-open-source-object-storage).

## Why SILO for *this* repo

Spark **3.5** + Hadoop **S3A** + **Iceberg** need broad MinIO-compatible S3. SILO is a drop-in fork:

- Same `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` env contract
- Same on-disk format and ports (`9000` / `9001`)
- Same `mc` client alias (`pgsty/mc` preserves `mc`)
- Endpoint becomes `http://silo:9000` in `config/spark-defaults.conf`

Docs: [silo.pgsty.com](https://silo.pgsty.com) · source: [github.com/pgsty/silo](https://github.com/pgsty/silo)

## Alternatives considered (not chosen)

| Store | License / notes | Why not here |
|-------|-----------------|--------------|
| **Garage** | FOSS, lightweight | Partial S3 API — risky for Spark S3A + Iceberg teaching path |
| **RustFS** | Apache-2.0, promising | Still maturing for this Spark 3.5 / S3A / Iceberg drop-in path |
| **MinIO community** | Archived | No maintained community binaries |

## Pin policy

- Always pin **immutable release tags** for `silo` and `mc`.
- Do **not** use `:latest` for the object-store server or client.
- Bump tags deliberately and record the change in this file or the PR body.

## Volume migration (one-time)

Compose renamed `minio-data` → `silo-data`. Existing MinIO volume users can:

1. **Re-init** (simplest for demos): `make reset` (wipes lakehouse volumes), or
2. **Rename / copy** the Docker volume contents into `silo-data` if you must keep objects (disk format is compatible; still verify with `make wait && make e2e`).

## Runtime pins (related)

| Component | Current pin | Note |
|-----------|-------------|------|
| Spark image | `apache/spark:3.5.3-java17` | Kept; newer 3.5.x tags (up to 3.5.8) exist but were not tested for this bump |
| Iceberg runtime | `1.6.1` (`iceberg-spark-runtime-3.5_2.12`) | Latest **1.6.x**; 1.7+/1.11 exist on Maven but are minor/major jumps — left alone for this object-store PR |

## Rollback

1. Revert Compose to a MinIO image **only** if you accept archived upstream risk.
2. Prefer rolling SILO tags forward within the same major line.
3. Restore `spark.hadoop.fs.s3a.endpoint` to match the service hostname (`http://silo:9000` or legacy `http://minio:9000`).
4. Volume name must match Compose (`silo-data` vs `minio-data`).

## Remaining risks

- **AGPL lineage**: SILO inherits MinIO’s AGPL-family licensing posture — fine for local learning; review before commercial redistribution.
- **20260903 console caveat**: embedded Console is v2.3.0 in this release; treat console UX as secondary to S3 API + `mc` for automation. Prefer API/`mc` for bucket setup (`silo-init`).
- Healthcheck uses `silo healthcheck ready` (classic image). If a future tag drops that subcommand, fall back to a TCP probe on `:9000`.
