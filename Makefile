COMPOSE_AIRFLOW := docker compose -f docker-compose.yml -f docker-compose.airflow.yml

.PHONY: help up down wait e2e churn-e2e churn-sample churn-gold-local churn-check churn-parity demo reset ps \
	airflow-up airflow-down airflow-wait airflow-trigger-retail airflow-trigger-churn airflow-demo
.PHONY: graph-venv graph-sample graph-build graph-check graph-local graph-promote graph-clean graph-golden

help:
	@echo "local-data-lakehouse"
	@echo "  make up                  Start Silo + Postgres + Spark"
	@echo "  make wait                Wait until lakehouse healthy"
	@echo "  make e2e                 Retail medallion (shell)"
	@echo "  make churn-e2e           Renewal gold (T-7 features) + export via Spark"
	@echo "  make churn-sample        Generate bronze billing + usage events (N_USERS=8000 default)"
	@echo "  make churn-gold-local    Same gold export in pandas, without Docker"
	@echo "  make churn-check         Validate data/export/ against the retention-radar contract"
	@echo "  make churn-parity        Spark SQL (local mode) vs pandas gold, row by row (needs pyspark + Java 17)"
	@echo "  make demo                Full shell demo (retail then churn)"
	@echo "  make airflow-up          Start Airflow (needs make up first)"
	@echo "  make airflow-wait        Wait for Airflow UI"
	@echo "  make airflow-trigger-retail   Run retail DAG via Airflow"
	@echo "  make airflow-trigger-churn    Run churn DAG via Airflow"
	@echo "  make airflow-demo        Retail + churn DAGs via Airflow"
	@echo "  make airflow-down        Stop Airflow only"
	@echo "  make reset               Wipe lakehouse volumes, up, retail e2e"
	@echo "  make down | ps           Stop all / status"
	@echo ""
	@echo "Graph on gold (Python 3.12 venv; never writes data/sample or data/export):"
	@echo "  make graph-venv          Create (or re-sync) .venv-graph from requirements-graph.txt (uv, hash-checked)"
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

up:
	cp -n .env.example .env 2>/dev/null || true
	docker compose up -d --build

down:
	-$(COMPOSE_AIRFLOW) down
	docker compose down

wait:
	./pipelines/wait_for_stack.sh

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
	N_USERS=$${N_USERS:-8000} CHURN_SEED=$${CHURN_SEED:-42} python3 scripts/generate_churn_sample.py

churn-gold-local: churn-sample
	python3 scripts/build_churn_gold_local.py
	python3 scripts/check_churn_export.py

churn-check:
	python3 scripts/check_churn_export.py

churn-parity:
	python3 scripts/check_gold_parity.py

demo: e2e churn-e2e
	@echo ""
	@echo "==> Full demo complete."
	@echo "    Silo console: http://localhost:9001  (minioadmin / minioadmin)"
	@echo "    Exports:       data/export/"

airflow-up: wait
	@echo "==> Starting Airflow (separate metadata DB + webserver + scheduler)"
	$(COMPOSE_AIRFLOW) up -d --build airflow-postgres
	$(COMPOSE_AIRFLOW) up -d --build airflow-init
	$(COMPOSE_AIRFLOW) up -d --build airflow-webserver airflow-scheduler

airflow-down:
	$(COMPOSE_AIRFLOW) stop airflow-webserver airflow-scheduler airflow-postgres || true
	$(COMPOSE_AIRFLOW) rm -f airflow-webserver airflow-scheduler airflow-init airflow-postgres || true

airflow-wait:
	./pipelines/airflow_wait.sh

airflow-trigger-retail: airflow-wait
	./pipelines/airflow_trigger.sh lakehouse_retail_medallion

airflow-trigger-churn: airflow-wait
	./pipelines/airflow_trigger.sh lakehouse_churn_features

airflow-demo: airflow-trigger-retail airflow-trigger-churn
	@echo ""
	@echo "==> Airflow demo complete."
	@echo "    Airflow UI: http://localhost:$${AIRFLOW_WEBSERVER_PORT:-8080}  (admin / admin)"
	@echo "    Silo:      http://localhost:9001  (minioadmin / minioadmin)"
	@echo "    Exports:    data/export/"

reset:
	docker compose down -v
	$(MAKE) up
	$(MAKE) e2e

ps:
	docker compose ps
	-$(COMPOSE_AIRFLOW) ps

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
