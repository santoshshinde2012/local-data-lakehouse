COMPOSE_AIRFLOW := docker compose -f docker-compose.yml -f docker-compose.airflow.yml

.PHONY: help up down wait e2e churn-e2e demo reset ps \
	airflow-up airflow-down airflow-wait airflow-trigger-retail airflow-trigger-churn airflow-demo

help:
	@echo "local-data-lakehouse"
	@echo "  make up                  Start MinIO + Postgres + Spark"
	@echo "  make wait                Wait until lakehouse healthy"
	@echo "  make e2e                 Retail medallion (shell)"
	@echo "  make churn-e2e           Churn gold + export (shell)"
	@echo "  make demo                Full shell demo (retail then churn)"
	@echo "  make airflow-up          Start Airflow (needs make up first)"
	@echo "  make airflow-wait        Wait for Airflow UI"
	@echo "  make airflow-trigger-retail   Run retail DAG via Airflow"
	@echo "  make airflow-trigger-churn    Run churn DAG via Airflow"
	@echo "  make airflow-demo        Retail + churn DAGs via Airflow"
	@echo "  make airflow-down        Stop Airflow only"
	@echo "  make reset               Wipe lakehouse volumes, up, retail e2e"
	@echo "  make down | ps           Stop all / status"

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
	./pipelines/run_churn_e2e.sh

demo: e2e churn-e2e
	@echo ""
	@echo "==> Full demo complete."
	@echo "    MinIO console: http://localhost:9001  (minioadmin / minioadmin)"
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
	@echo "    MinIO:      http://localhost:9001  (minioadmin / minioadmin)"
	@echo "    Exports:    data/export/"

reset:
	docker compose down -v
	$(MAKE) up
	$(MAKE) e2e

ps:
	docker compose ps
	-$(COMPOSE_AIRFLOW) ps
