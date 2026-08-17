# =============================================================================
#  Common tasks. Run `make` for the list.
# =============================================================================

SHELL := /bin/bash
PYTHON ?= python
COMPOSE ?= docker compose

.DEFAULT_GOAL := help
.PHONY: help up down logs restore schema setup run full test metrics experiment reset validate-adf clean

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# --- environment -------------------------------------------------------------
up:  ## Start SQL Server 2022 and wait for it to be healthy
	$(COMPOSE) up -d
	@echo "Waiting for SQL Server to become healthy..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' etl-sqlserver 2>/dev/null)" = "healthy" ]; do \
		if [ "$$(docker inspect -f '{{.State.Running}}' etl-sqlserver 2>/dev/null)" != "true" ]; then \
			echo "Container died. Check: docker logs etl-sqlserver"; exit 1; \
		fi; sleep 5; \
	done
	@echo "SQL Server is ready."

down:  ## Stop the container (keeps the data volume)
	$(COMPOSE) down

logs:  ## Tail the SQL Server log
	docker logs -f etl-sqlserver

# --- setup -------------------------------------------------------------------
restore:  ## Download and restore AdventureWorks2022 into the source database
	./scripts/restore_adventureworks.sh

schema:  ## Deploy the target schema and all procedures (idempotent)
	./scripts/deploy_schema.sh

setup: up restore schema  ## Full first-time setup
	@echo ""
	@echo "Setup complete. Run the first load with: make full"

# --- running -----------------------------------------------------------------
run:  ## Incremental pipeline run
	$(PYTHON) -m etl.run_pipeline --trigger Scheduled

full:  ## Full load, ignoring the watermark
	$(PYTHON) -m etl.run_pipeline --trigger Manual --load-type Full

# --- verification ------------------------------------------------------------
test:  ## Run the integration test suite against the live database
	$(PYTHON) -m pytest tests/ -q

validate-adf:  ## Check every ADF artifact is valid JSON and correctly named
	@$(PYTHON) -c "$$VALIDATE_ADF"

metrics:  ## Print the measured metrics report
	$(PYTHON) -m etl.metrics

experiment:  ## Run the full measured experiment and regenerate reports/
	$(PYTHON) scripts/run_experiment.py --nights 6 --fault-count 4
	$(PYTHON) -m etl.metrics --write

# --- housekeeping ------------------------------------------------------------
reset:  ## Empty the warehouse and the entire run history
	./scripts/reset_warehouse.sh

clean:  ## Remove the container AND its data volume, plus local caches
	$(COMPOSE) down -v
	find . -type d -name __pycache__ -prune -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache
	@echo "Removed the container, its volume, and local caches."
	@echo "data/backup/*.bak is kept -- delete it manually to force a re-download."

# ADF git integration requires the file name to match the resource name, so a
# rename that misses one of the two silently breaks publishing.
define VALIDATE_ADF
import json, pathlib, sys
bad = 0
for p in sorted(pathlib.Path('adf').rglob('*.json')):
    try:
        d = json.loads(p.read_text())
    except Exception as e:
        print(f'INVALID JSON  {p}: {e}'); bad += 1; continue
    if 'name' in d and d['name'] != p.stem:
        print(f"NAME MISMATCH {p}: name={d['name']!r} filename={p.stem!r}"); bad += 1
    else:
        print(f'ok  {p}')
sys.exit(1 if bad else 0)
endef
export VALIDATE_ADF
