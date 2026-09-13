.PHONY: help up down wait e2e churn-e2e demo reset ps

help:
	@echo "local-data-lakehouse"
	@echo "  make up           Start MinIO + Postgres + Spark"
	@echo "  make wait         Wait until healthy"
	@echo "  make e2e          Retail medallion path"
	@echo "  make churn-e2e    AI-platform churn gold features + export"
	@echo "  make demo         Full demo (retail then churn)"
	@echo "  make reset        Wipe volumes, up, retail e2e"
	@echo "  make down | ps    Stop / status"

up:
	cp -n .env.example .env 2>/dev/null || true
	docker compose up -d --build

down:
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

reset:
	docker compose down -v
	$(MAKE) up
	$(MAKE) e2e

ps:
	docker compose ps
