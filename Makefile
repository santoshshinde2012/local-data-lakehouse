# local-data-lakehouse: one Iceberg REST catalog (Lakekeeper) on one S3 store (RustFS), two profiles.
#   light = Postgres 18 + Lakekeeper + RustFS + init; engines on the host (DuckDB, PyIceberg, Polars). No JVM.
#   full  = light + Spark 4.1.3 (+ Trino 483 with TRINO=1). Airflow 3 is an optional overlay on full.
# STORE=silo swaps RustFS for SILO (docker-compose.silo.yml) in every target.
STORE ?= rustfs
TRINO ?= 0
COMPOSE_FILES := -f docker-compose.yml $(if $(filter silo,$(STORE)),-f docker-compose.silo.yml,)
COMPOSE := docker compose $(COMPOSE_FILES)
FULL_PROFILES := --profile full $(if $(filter 1,$(TRINO)),--profile trino,)
COMPOSE_AIRFLOW := $(COMPOSE) -f docker-compose.airflow.yml --profile full
PY ?= .venv/bin/python
PY_CHECK = @command -v "$(PY)" >/dev/null 2>&1 || { echo "PY=$(PY) not found: run 'make venv' (or pass PY=python)"; exit 2; }
PYTEST = $(PY) -m pytest

.PHONY: help env venv up up-light up-full wait down purge ps logs reset demo-light e2e churn-e2e churn-sample churn-gold-local \
	churn-check churn-parity demo test test-t0 test-t1 test-t2 test-t3 stats \
	airflow-up airflow-down airflow-wait airflow-trigger-retail airflow-trigger-churn airflow-demo
.PHONY: graph-test graph-venv graph-sample graph-build graph-check graph-local graph-promote graph-clean graph-golden graph-e2e

help:
	@echo "local-data-lakehouse (Iceberg 1.11 REST catalog: Lakekeeper + RustFS; see README)"
	@echo "  make venv                .venv (Python 3.12) from the hash-locked requirements.txt (uv)"
	@echo "  make up-light            light profile: Postgres 18 + Lakekeeper + RustFS + init, waits for health"
	@echo "  make up-full [TRINO=1]   full profile: light + Spark 4.1.3 (+ Trino 483), waits for health"
	@echo "  (STORE=silo on any up/down/test target swaps RustFS for SILO)"
	@echo "  make demo-light          Retail + churn twin with DuckDB / PyIceberg / Polars on the host (light)"
	@echo "  make e2e                 Retail medallion in Spark (full)"
	@echo "  make churn-e2e           Renewal gold (T-7 features) + export via Spark (full)"
	@echo "  make demo                e2e + churn-e2e (full)"
	@echo "  make churn-sample        Generate bronze billing + usage events (N_USERS=8000, CHURN_SEED=42)"
	@echo "  make churn-gold-local    Same gold export in pandas, no Docker"
	@echo "  make churn-check         Validate data/export/ against the retention-radar contract"
	@echo "  make churn-parity        Spark SQL (local pyspark 4.1.3, Java 17+) vs pandas gold, row by row"
	@echo "  make test-t0             T0 unit + static checks, no containers"
	@echo "  make graph-test          graph-layer tests in .venv-graph (no containers)"
	@echo "  make test-t1             T1 contract: throwaway Postgres + Lakekeeper + RustFS (testcontainers)"
	@echo "  make test-t2             T2 smoke: brings up light, runs the host engines against it"
	@echo "  make test-t3             T3 parity: brings up full + Trino, Spark vs DuckDB / PyIceberg / Polars / Trino"
	@echo "  make test                T0 + T1 + T2 + T3"
	@echo "  make stats               docker stats snapshot of this project's containers"
	@echo "  make airflow-up          Airflow 3.3 overlay on full (generates .env secrets on first run)"
	@echo "  make airflow-wait | airflow-trigger-retail | airflow-trigger-churn | airflow-demo | airflow-down"
	@echo "  make ps | logs           Status / follow logs"
	@echo "  make down                Stop every profile (volumes kept)"
	@echo "  make purge               Stop everything and delete THIS project's volumes (catalog, objects, Airflow DB)"
	@echo "  make reset               purge, then up-light"
	@echo ""
	@echo "Ports: 8181 Lakekeeper, 9000 S3 API, 9001 store console, 4040 Spark UI, 8088 Trino, 8080 Airflow."
	@echo ""
	@echo "Graph on gold (Python 3.12 venv; never writes data/sample or data/export):"
	@echo "  make graph-venv          Create (or re-sync) .venv-graph from requirements-graph.txt (uv, hash-checked)"
	@echo "  make graph-e2e           Spark graph tables -> Iceberg -> graph container build + contract (full)"
	@echo "  make graph-sample PROFILE=<s<seed>|tiny|inject> [N_USERS=8000]"
	@echo "                           Fill a profile's inputs under \$$GRAPH_ROOT/<profile>/ (the seed comes from the name):"
	@echo "                             PROFILE=s42     generator (seed 42, N_USERS) + gold script -> sample/ and export/"
	@echo "                             PROFILE=tiny    EXPORTS ONLY -> tiny/export; its bronze is the committed fixture"
	@echo "                                             (seed 42, N_USERS 120), never regenerated: SEED/N_USERS are rejected"
	@echo "                             PROFILE=inject  tiny bronze copied to inject/sample with ONE poisoned user_name"
	@echo "                                             (prompt-injection fixture for evals), + exports"
	@echo "                             PROFILE=default refused (data/sample/churn and data/export belong to make churn-*)"
	@echo "  make graph-build         Build the renewal graph for PROFILE (default: data/sample/churn, read only);"
	@echo "                           an unchanged build is kept and re-pinned (GRAPH_BUILD_FLAGS=--rebuild replaces it"
	@echo "                           atomically); PROFILE=inject prepares its own bronze + exports first"
	@echo "  make graph-check         Graph contract --strict + repo contract checks for PROFILE"
	@echo "  make graph-local         graph-build + graph-check for PROFILE (no sample regeneration)"
	@echo "  make graph-promote       Point \$$GRAPH_ROOT/current at the newest fresh default build with a strict"
	@echo "                           contract pass (atomic, under the build lock); BUILD=<id> names one (roll back)"
	@echo "  make graph-clean         Keep the last 3 builds per profile (never a build a live server holds)"
	@echo "  make graph-golden        Diff the committed goldens against fresh tiny + s42 builds in a scratch GRAPH_ROOT;"
	@echo "                           writes nothing unless CONFIRM=1 (ONLY=tiny or ONLY=s42 limits it to one)"

env:
	@test -f .env || { cp .env.example .env; echo "==> created .env from .env.example (local-only secrets; edit before sharing a machine)"; }

venv:
	@command -v uv >/dev/null 2>&1 || { echo "uv not found: https://docs.astral.sh/uv/ (curl -LsSf https://astral.sh/uv/install.sh | sh)"; exit 2; }
	@if [ ! -x .venv/bin/python ]; then uv venv --python 3.12 .venv; fi
	uv pip sync --python .venv/bin/python --require-hashes requirements.txt

up: up-light

up-light: env
	$(COMPOSE) --profile light up -d --wait
	@echo "==> light up: catalog http://localhost:8181/catalog  S3 http://objectstore.localhost:9000  console http://localhost:9001"

up-full: env
	$(COMPOSE) $(FULL_PROFILES) up -d --build --wait
	@echo "==> full up: Spark UI http://localhost:4040 (while a job runs)$(if $(filter 1,$(TRINO)),  Trino http://localhost:8088,)"

wait:
	./pipelines/wait_for_stack.sh

# down / reset load every overlay so nothing of this project is left behind. The Airflow overlay
# requires its secrets even to be parsed: placeholders are fine for tearing down.
OVERLAY_ENV := AIRFLOW_FERNET_KEY=$${AIRFLOW_FERNET_KEY:-unused} AIRFLOW_JWT_SECRET=$${AIRFLOW_JWT_SECRET:-unused} \
	AIRFLOW_API_SECRET_KEY=$${AIRFLOW_API_SECRET_KEY:-unused} AIRFLOW_DB_PASSWORD=$${AIRFLOW_DB_PASSWORD:-unused}
COMPOSE_EVERYTHING := $(OVERLAY_ENV) docker compose -f docker-compose.yml -f docker-compose.silo.yml \
	-f docker-compose.airflow.yml -f docker-compose.graph.yml --profile '*'

down: env
	$(COMPOSE_EVERYTHING) down --remove-orphans

ps:
	$(COMPOSE) --profile '*' ps -a

logs:
	$(COMPOSE) --profile '*' logs -f --tail 100

stats:
	@docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}' \
	  $$($(COMPOSE) --profile '*' ps -q) 2>/dev/null || echo "nothing running"

# Deletes THIS Compose project's volumes only (postgres-data, objectstore-data, silo-data, Airflow's).
# Volumes: `down -v` only removes the volumes that a service of the loaded model mounts, and the SILO
# overlay replaces RustFS's volume, so a second pass without it removes objectstore-data too.
purge: env
	$(COMPOSE_EVERYTHING) down -v --remove-orphans
	$(OVERLAY_ENV) docker compose -f docker-compose.yml --profile '*' down -v

reset: purge
	$(MAKE) up-light

demo-light: up-light
	$(PY_CHECK)
	$(PY) scripts/light_demo.py all

e2e: wait
	./pipelines/run_retail_e2e.sh

churn-e2e: wait
	@if [ ! -f data/sample/churn/subscription_snapshots.csv ]; then \
	  if [ -f data/sample/churn/fixtures/tiny/subscription_snapshots.csv ]; then \
	    cp data/sample/churn/fixtures/tiny/*.csv data/sample/churn/; \
	  else $(MAKE) churn-sample; fi; \
	fi
	./pipelines/run_churn_e2e.sh

churn-sample:
	$(PY_CHECK)
	N_USERS=$${N_USERS:-8000} CHURN_SEED=$${CHURN_SEED:-42} $(PY) scripts/generate_churn_sample.py

churn-gold-local: churn-sample
	$(PY) scripts/build_churn_gold_local.py
	$(PY) scripts/check_churn_export.py

churn-check:
	$(PY_CHECK)
	$(PY) scripts/check_churn_export.py

churn-parity:
	$(PY_CHECK)
	$(PY) scripts/check_gold_parity.py

demo: e2e churn-e2e
	@echo ""
	@echo "==> Full demo complete."
	@echo "    Store console: http://localhost:9001  (S3_ACCESS_KEY / S3_SECRET_KEY from .env)"
	@echo "    Exports:       data/export/"

test: test-t0 test-t1 test-t2 test-t3

test-t0:
	$(PY_CHECK)
	$(PYTEST) -q tests/unit

# The graph layer's own tests run in its own venv (make graph-venv).
graph-test: graph-venv
	.venv-graph/bin/python -m pytest -q tests/graph

test-t1:
	$(PY_CHECK)
	$(PYTEST) -q -s tests/contract

test-t2: up-light
	$(PY_CHECK)
	LDL_REQUIRE_STACK=1 $(PYTEST) -q -s tests/smoke

test-t3:
	$(MAKE) up-full TRINO=1
	$(PY_CHECK)
	LDL_REQUIRE_STACK=1 $(PYTEST) -q -s tests/parity

airflow-up: env
	./pipelines/airflow_env.sh
	$(COMPOSE_AIRFLOW) up -d --build --wait

# Stops and removes only the Airflow overlay's containers; the full stack keeps running (make down stops all).
AIRFLOW_SERVICES := docker-proxy airflow-postgres airflow-init airflow-apiserver airflow-scheduler airflow-dag-processor
airflow-down: env
	$(OVERLAY_ENV) $(COMPOSE_AIRFLOW) rm -sf $(AIRFLOW_SERVICES)

airflow-wait:
	./pipelines/airflow_wait.sh

airflow-trigger-retail: airflow-wait
	./pipelines/airflow_trigger.sh lakehouse_retail_medallion

airflow-trigger-churn: airflow-wait
	./pipelines/airflow_trigger.sh lakehouse_churn_features

airflow-demo: airflow-trigger-retail airflow-trigger-churn
	@echo ""
	@echo "==> Airflow demo complete."
	@echo "    Airflow UI: http://localhost:$${AIRFLOW_API_PORT:-8080}  (user and generated password in .env)"
	@echo "    Exports:    data/export/"

graph-e2e: up-full
	mkdir -p data/graph
	$(COMPOSE) -f docker-compose.graph.yml $(FULL_PROFILES) up -d --build --wait
	./pipelines/run_graph_e2e.sh

# ---------------------------------------------------------------------------
# Graph on gold: renewal graph data product + contract (how it works: the module
# docstrings of scripts/build_graph_local.py and scripts/check_graph_contract.py).
# Every graph target runs $(GRAPH_PY) (Python 3.12 venv; CI sets GRAPH_PY=python),
# never chains the churn-* targets (they run the system python3 and rewrite
# data/sample/churn + data/export), and writes only under $(GRAPH_ROOT).
# Profiles (a profile's seed is derived from its name; no other names are valid):
# default (data/sample/churn, read only), s<seed> (own bronze + exports under
# $(GRAPH_ROOT)/s<seed>/), tiny (committed fixture; exports under $(GRAPH_ROOT)/tiny/),
# inject (tiny bronze copy with one poisoned user_name under $(GRAPH_ROOT)/inject/).
GRAPH_PY ?= .venv-graph/bin/python
GRAPH_ROOT ?= $(CURDIR)/data/graph
PROFILE ?= default
# The contract runs --strict (warnings fail, exports and a matching golden required). Profiles
# without a committed golden (e.g. an eval seed): make graph-check PROFILE=s7 GRAPH_CHECK_FLAGS=
# An Iceberg-sourced build (build_graph_local.py build --source iceberg) is checked source-aware: its
# pins are re-read from PYICEBERG_CATALOG__LAKEHOUSE__* (or the SQLite catalog its manifest
# records), or pass them: GRAPH_CHECK_FLAGS="--strict --catalog-uri <uri> --warehouse <uri>"
GRAPH_CHECK_FLAGS ?= --strict
# An unchanged build is kept and its manifest pins (exports, seed status) are refreshed, so
# graph-local stays green after exports are regenerated. Force a replacement with:
#   make graph-build GRAPH_BUILD_FLAGS=--rebuild
GRAPH_BUILD_FLAGS ?=
GRAPH_PY_CHECK = @command -v "$(GRAPH_PY)" >/dev/null 2>&1 || { echo "GRAPH_PY=$(GRAPH_PY) not found: run 'make graph-venv' (or pass GRAPH_PY=python)"; exit 2; }

graph-venv:
	@if [ -x .venv-graph/bin/python ]; then \
	  .venv-graph/bin/python -c 'import sys; sys.exit(sys.version_info[:2] != (3, 12))' || \
	    { echo ".venv-graph is not Python 3.12: remove it (rm -rf .venv-graph) and rerun make graph-venv"; exit 2; }; \
	  echo "==> graph-venv: .venv-graph exists (Python 3.12); re-syncing it to requirements-graph.txt"; \
	else \
	  uv venv --python 3.12 .venv-graph; \
	fi
	uv pip sync --python .venv-graph/bin/python --require-hashes requirements-graph.txt

# One Python entry point does the work (build.prepare_sample): it validates PROFILE / SEED /
# N_USERS with explicit messages, runs the user's generator and gold script unchanged with
# $(GRAPH_PY) and their directories pointed into $(GRAPH_ROOT)/<profile>/, records
# sample_meta.json and asserts data/sample/churn + data/export are untouched.
# SEED / N_USERS are passed as given (empty = derive from the profile name).
graph-sample:
	$(GRAPH_PY_CHECK)
	@$(GRAPH_PY) scripts/build_graph_local.py sample --profile "$(PROFILE)" \
	  --seed "$(SEED)" --n-users "$(N_USERS)" --graph-root "$(GRAPH_ROOT)"

graph-build:
	$(GRAPH_PY_CHECK)
	$(GRAPH_PY) scripts/build_graph_local.py build --profile "$(PROFILE)" --graph-root "$(GRAPH_ROOT)" \
	  $(if $(filter default,$(PROFILE)),--verify-seed,) $(GRAPH_BUILD_FLAGS)

graph-check:
	$(GRAPH_PY_CHECK)
	$(GRAPH_PY) scripts/check_graph_contract.py --profile "$(PROFILE)" --graph-root "$(GRAPH_ROOT)" $(GRAPH_CHECK_FLAGS)
	$(GRAPH_PY) scripts/check_repo_contracts.py

graph-local:
	@$(MAKE) --no-print-directory graph-build
	@$(MAKE) --no-print-directory graph-check

graph-promote:
	$(GRAPH_PY_CHECK)
	$(GRAPH_PY) scripts/build_graph_local.py promote --profile "$(PROFILE)" --graph-root "$(GRAPH_ROOT)" \
	  $(if $(BUILD),--build "$(BUILD)",)

graph-clean:
	$(GRAPH_PY_CHECK)
	$(GRAPH_PY) scripts/build_graph_local.py gc --keep 3 --graph-root "$(GRAPH_ROOT)"

# Goldens are oracle output, never typed by hand. This builds the tiny and s42 profiles from
# scratch in a temporary GRAPH_ROOT (never $(GRAPH_ROOT), never data/sample or data/export),
# prints the diff against src/lakehouse_graph/goldens/*.json and exits non-zero if they differ.
# It only overwrites a golden with CONFIRM=1. ONLY=tiny (or s42) limits it to one file;
# GOLDEN_SCRATCH=<dir> keeps the scratch builds there.
graph-golden:
	$(GRAPH_PY_CHECK)
	$(GRAPH_PY) scripts/build_graph_local.py golden $(if $(filter 1,$(CONFIRM)),--write,) \
	  $(if $(ONLY),--only "$(ONLY)",) $(if $(GOLDEN_SCRATCH),--scratch "$(GOLDEN_SCRATCH)",)
