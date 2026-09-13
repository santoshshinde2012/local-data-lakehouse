# local-data-lakehouse

Open-source **data lakehouse on your laptop**.

| Component | Role |
|---|---|
| **MinIO** | S3-compatible object store |
| **PostgreSQL** | Iceberg JDBC catalog |
| **Apache Spark 3.5** | Ingest, transform, query |
| **Apache Iceberg** | ACID tables, snapshots, time travel |

Two runnable paths:

1. **Retail** — bronze → silver → gold metrics + Iceberg time travel  
2. **Churn features** — AI-platform usage/tickets/payments → `gold.churn_user_features` → CSV/JSON export  

---

## Prerequisites

- Docker Desktop (Compose v2)
- Make
- ~8–16 GB RAM
- Free ports: `9000`, `9001`, `5432`, `4040`

---

## Quick start

```bash
git clone https://github.com/santoshshinde2012/local-data-lakehouse.git
cd local-data-lakehouse
cp .env.example .env
make up
make wait
make demo
```

| Command | What it does |
|---|---|
| `make up` | Build Spark image; start MinIO, Postgres, Spark |
| `make wait` | Block until the stack is healthy |
| `make e2e` | Retail jobs under `src/jobs/retail/` |
| `make churn-e2e` | Churn jobs under `src/jobs/churn/` |
| `make demo` | Retail then churn (full video path) |
| `make reset` | Wipe volumes, recreate, retail E2E |
| `make help` | List targets |

**UIs**

- MinIO: http://localhost:9001 (`minioadmin` / `minioadmin`) — bucket `lake`  
- Spark: http://localhost:4040 (while a job runs)

---

## Expected results

### Retail (`make e2e`)

| order_date | orders | revenue | AOV |
|---|---:|---:|---:|
| 2024-03-01 | 10 | 424.94 | 60.71 |
| 2024-03-02 | 9 | 537.94 | 76.85 |

- ~22 bronze order rows → **19** silver  
- Customer **Santosh Shinde** (`c-01`) on `o-1001`  
- Time travel count **19**

### Churn (`make churn-e2e`)

- Table: `lakehouse.gold.churn_user_features`  
- Files: `data/export/churn_user_features.csv`, `data/export/santosh_inference_record.json`

---

## Layout (SOLID-oriented)

```text
local-data-lakehouse/
  config/                 # Spark + catalog configuration (single place)
  docker/spark/           # Spark runtime image
  src/jobs/
    retail/               # SRP: retail medallion jobs only
    churn/                # SRP: churn feature jobs only
  pipelines/              # orchestration scripts (no business logic)
  data/sample/            # immutable sample inputs
  data/export/            # generated outputs (gitignored)
  sql/retail|churn/       # companion DDL / contracts
  docs/demo/              # verified excerpts
```

- **Single responsibility** — each job owns one stage; pipelines only sequence submits  
- **Open for extension** — add a new domain folder under `src/jobs/` without touching retail  
- **Dependency direction** — jobs depend on catalog/warehouse config, not on each other  
- **Interface stability** — table names and export contracts stay documented in `sql/` and README  

---

## Without Make

```bash
cp .env.example .env
docker compose up -d --build
./pipelines/wait_for_stack.sh
./pipelines/run_retail_e2e.sh
./pipelines/run_churn_e2e.sh
```

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| Docker errors | Start Docker Desktop; `make up` |
| Port conflict | Free ports or edit `.env` |
| `NoSuchBucket` | `./pipelines/create_bucket.sh` or `make reset` |
| JDBC errors | `make wait`; `make ps` |
| Missing jars | `docker compose build --no-cache spark && make up` |
| Dirty state | `make reset` then `make churn-e2e` |

---

## License

MIT (see repository; add `LICENSE` if required by your org policy).
