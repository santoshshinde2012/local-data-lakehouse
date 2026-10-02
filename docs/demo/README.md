# Demo transcripts

Excerpts from real green runs on macOS arm64 (Apple Silicon, Docker Desktop), 2026-10-02, on branch
`feat/local-first-stack-2026`. Volatile values (snapshot ids, timestamps, absolute paths) are shortened.

| File | Covers |
|---|---|
| `light-demo.excerpt.md` | `make demo-light`: retail medallion + churn twin with DuckDB / PyIceberg / Polars (light profile, no JVM) |
| `retail-e2e.excerpt.md` | `make e2e`: Spark 4.1.3 retail bronze → gold + Iceberg time travel (full profile) |
| `churn-e2e.excerpt.md` | `make churn-e2e`: Spark renewal gold (T-7 features) + export, and the no-Docker pandas path |
| `airflow-e2e.excerpt.md` | `make airflow-demo`: both DAGs on the Airflow 3.3.2 overlay |
| `graph-e2e.excerpt.md` | `pipelines/run_graph_e2e.sh`: Spark graph tables → Iceberg → graph container build + contracts |

## Demo video

The original screen recording (`lakehouse-demo-original.mov`, 49 MB, recorded on the earlier
SILO + JDBC-catalog stack) is no longer kept in the repository tree, to keep clones small. It can be
attached to a GitHub Release as a release asset; until then it is only in the git history
(`git log --all -- docs/demo/lakehouse-demo-original.mov`).

`architecture-e2e.png` is the drawing of the earlier stack (SILO, JDBC catalog, Spark 3.5); the
current architecture is the Mermaid diagram in the root `README.md`.

Canonical instructions live in the repository root `README.md`.
