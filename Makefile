# NYC 311 Urban Intelligence Pipeline — Makefile
#
# All commands assume you are running from the project root directory.
# Install dependencies first: make install
#
# Quick start:
#   make install && make pipeline && make dev-dashboard

.PHONY: help install install-dev clean-env \
        ingest clean-data transform quality \
        pipeline full-refresh \
        dev-dashboard build-dashboard \
        dagster dagster-stop \
        dbt-docs dbt-compile \
        test lint format typecheck \
        clean-all

# ── Colors ────────────────────────────────────────────────────────────────────
BOLD   := \033[1m
RESET  := \033[0m
GREEN  := \033[32m
YELLOW := \033[33m
BLUE   := \033[34m

# ── Config ────────────────────────────────────────────────────────────────────
PYTHON      := python
PIP         := pip
UV          := uv
NODE        := node
NPM         := npm
DBT         := dbt
DAGSTER     := dagster
DAGSTER_WS  := dagster-webserver

PROJECT_ROOT   := $(shell pwd)
BRONZE_DB      := data/bronze/bronze.duckdb
SILVER_DB      := data/silver/silver.duckdb
GOLD_DB        := data/gold/gold.duckdb
DBT_DIR        := transform
DASHBOARD_DIR  := dashboard

# Load .env if it exists
-include .env
export

# ─────────────────────────────────────────────────────────────────────────────
# HELP
# ─────────────────────────────────────────────────────────────────────────────

help: ## Show this help message
	@echo ""
	@echo "$(BOLD)NYC 311 Urban Intelligence Pipeline$(RESET)"
	@echo "====================================="
	@echo ""
	@echo "$(BOLD)Setup:$(RESET)"
	@grep -E '^(install|install-dev|clean-env).*:.*##' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-25s$(RESET) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(BOLD)Pipeline:$(RESET)"
	@grep -E '^(ingest|clean-data|transform|quality|pipeline|full-refresh).*:.*##' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(BLUE)%-25s$(RESET) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(BOLD)Dashboard:$(RESET)"
	@grep -E '^(dev-dashboard|build-dashboard|dagster|dbt-docs).*:.*##' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-25s$(RESET) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(BOLD)Development:$(RESET)"
	@grep -E '^(test|lint|format|typecheck).*:.*##' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-25s$(RESET) %s\n", $$1, $$2}'
	@echo ""

# ─────────────────────────────────────────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────────────────────────────────────────

install: ## Install Python dependencies (uses uv if available, else pip)
	@echo "$(BOLD)Installing Python dependencies...$(RESET)"
	@if command -v uv >/dev/null 2>&1; then \
		echo "Using uv (fast)"; \
		uv sync; \
	else \
		echo "Using pip (install uv for faster installs: pip install uv)"; \
		$(PIP) install -e ".[dev]"; \
	fi
	@echo "$(GREEN)✅ Python dependencies installed$(RESET)"

install-dev: install ## Install Python + Node.js dependencies (for dashboard)
	@echo "$(BOLD)Installing Node.js dependencies for Evidence.dev...$(RESET)"
	cd $(DASHBOARD_DIR) && $(NPM) install
	@echo "$(GREEN)✅ All dependencies installed$(RESET)"

install-dbt: ## Install dbt packages (dbt_utils, dbt_expectations)
	@echo "$(BOLD)Installing dbt packages...$(RESET)"
	cd $(DBT_DIR) && $(DBT) deps
	@echo "$(GREEN)✅ dbt packages installed$(RESET)"

setup: install-dev install-dbt ## Full developer setup (Python + Node + dbt packages)
	@cp -n .env.example .env 2>/dev/null || true
	@mkdir -p data/bronze data/silver data/gold
	@mkdir -p .dagster
	@echo "$(GREEN)✅ Project setup complete. Edit .env to add your API token.$(RESET)"

# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE STEPS
# ─────────────────────────────────────────────────────────────────────────────

ingest: ## Bronze: Pull NYC 311 data from Socrata API via dlt
	@echo "$(BOLD)$(BLUE)🔄 Bronze Ingestion (dlt → DuckDB)$(RESET)"
	$(PYTHON) -m ingestion.pipeline
	@echo "$(GREEN)✅ Bronze ingestion complete → $(BRONZE_DB)$(RESET)"

ingest-full: ## Bronze: Full refresh (re-fetch all data from 2020)
	@echo "$(BOLD)$(YELLOW)⚠️  Full refresh: re-fetching all data from 2020$(RESET)"
	$(PYTHON) -m ingestion.pipeline --full-refresh
	@echo "$(GREEN)✅ Bronze full refresh complete$(RESET)"

ingest-sample: ## Bronze: Ingest a 50k row sample (for testing)
	@echo "$(BOLD)$(BLUE)🔄 Bronze Sample Ingestion (50k rows)$(RESET)"
	$(PYTHON) -m ingestion.pipeline --max-rows 50000
	@echo "$(GREEN)✅ Sample ingestion complete$(RESET)"

clean-data: ## Silver: Run Polars Bronze→Silver cleaning pipeline
	@echo "$(BOLD)$(BLUE)🧹 Silver Cleaning (Polars)$(RESET)"
	$(PYTHON) -m clean.silver_cleaner
	@echo "$(GREEN)✅ Silver cleaning complete → $(SILVER_DB)$(RESET)"

transform: ## Gold: Run dbt-core models (Silver→Gold)
	@echo "$(BOLD)$(BLUE)🔨 Gold Transformation (dbt-core)$(RESET)"
	cd $(DBT_DIR) && $(DBT) build --select +tag:gold
	@echo "$(GREEN)✅ Gold transformation complete → $(GOLD_DB)$(RESET)"

quality: ## Run Great Expectations validation for all layers
	@echo "$(BOLD)$(BLUE)🔍 Data Quality Validation (Great Expectations)$(RESET)"
	$(PYTHON) -m quality.runner all
	@echo "$(GREEN)✅ Quality validation complete$(RESET)"

quality-bronze: ## Run GE validation for Bronze layer only
	$(PYTHON) -m quality.runner bronze

quality-silver: ## Run GE validation for Silver layer only
	$(PYTHON) -m quality.runner silver

quality-gold: ## Run GE validation for Gold layer only
	$(PYTHON) -m quality.runner gold

# ─────────────────────────────────────────────────────────────────────────────
# FULL PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

pipeline: ingest clean-data transform quality ## Run the full pipeline end-to-end (incremental)
	@echo ""
	@echo "$(BOLD)$(GREEN)🏙️  Pipeline complete!$(RESET)"
	@echo "  Bronze: $(BRONZE_DB)"
	@echo "  Silver: $(SILVER_DB)"
	@echo "  Gold:   $(GOLD_DB)"
	@echo ""
	@echo "  Run 'make dev-dashboard' to view results"

full-refresh: ## Full pipeline refresh from scratch (re-fetches all data)
	@echo "$(BOLD)$(YELLOW)⚠️  Full refresh: this will re-process all data from 2020$(RESET)"
	$(PYTHON) -m ingestion.pipeline --full-refresh
	$(PYTHON) -m clean.silver_cleaner --full-refresh
	cd $(DBT_DIR) && $(DBT) build --select +tag:gold --full-refresh
	$(PYTHON) -m quality.runner all

# ─────────────────────────────────────────────────────────────────────────────
# DASHBOARD
# ─────────────────────────────────────────────────────────────────────────────

dev-dashboard: ## Launch Evidence.dev development server (→ http://localhost:3000)
	@echo "$(BOLD)$(YELLOW)📊 Launching Evidence.dev dashboard...$(RESET)"
	cd $(DASHBOARD_DIR) && $(NPM) run dev
	@echo "→ http://localhost:$(EVIDENCE_DEV_PORT)"

build-dashboard: ## Build Evidence.dev dashboard as static site
	@echo "$(BOLD)$(YELLOW)📊 Building Evidence.dev dashboard...$(RESET)"
	cd $(DASHBOARD_DIR) && $(NPM) run build
	@echo "$(GREEN)✅ Dashboard built at $(DASHBOARD_DIR)/build/$(RESET)"

# ─────────────────────────────────────────────────────────────────────────────
# DAGSTER
# ─────────────────────────────────────────────────────────────────────────────

dagster: ## Launch Dagster UI (→ http://localhost:3001)
	@echo "$(BOLD)$(YELLOW)⚙️  Launching Dagster UI...$(RESET)"
	@mkdir -p .dagster
	DAGSTER_HOME=$(PROJECT_ROOT)/.dagster \
		$(DAGSTER_WS) -f orchestration/definitions.py -p $(DAGSTER_PORT)

dagster-run: ## Run the full pipeline job via Dagster CLI (no UI)
	@mkdir -p .dagster
	DAGSTER_HOME=$(PROJECT_ROOT)/.dagster \
		$(DAGSTER) job execute \
		-f orchestration/definitions.py \
		-j full_pipeline_job

# ─────────────────────────────────────────────────────────────────────────────
# DBT
# ─────────────────────────────────────────────────────────────────────────────

dbt-docs: ## Serve dbt documentation (→ http://localhost:8080)
	@echo "$(BOLD)Generating dbt docs...$(RESET)"
	cd $(DBT_DIR) && $(DBT) docs generate
	cd $(DBT_DIR) && $(DBT) docs serve --port 8080

dbt-compile: ## Compile dbt project (generates manifest.json for Dagster)
	cd $(DBT_DIR) && $(DBT) compile

dbt-test: ## Run dbt tests only
	cd $(DBT_DIR) && $(DBT) test --select tag:gold

# ─────────────────────────────────────────────────────────────────────────────
# DEVELOPMENT
# ─────────────────────────────────────────────────────────────────────────────

test: ## Run pytest unit + integration tests
	$(PYTHON) -m pytest tests/ -v

test-coverage: ## Run tests with coverage report
	$(PYTHON) -m pytest tests/ -v --cov=ingestion --cov=clean --cov-report=html

lint: ## Lint Python code with ruff
	$(PYTHON) -m ruff check ingestion/ clean/ quality/ orchestration/

format: ## Format Python code with ruff
	$(PYTHON) -m ruff format ingestion/ clean/ quality/ orchestration/

format-check: ## Check formatting without modifying files
	$(PYTHON) -m ruff format --check ingestion/ clean/ quality/ orchestration/

typecheck: ## Run mypy type checking
	$(PYTHON) -m mypy ingestion/ clean/ quality/ orchestration/

# ─────────────────────────────────────────────────────────────────────────────
# CLEANUP
# ─────────────────────────────────────────────────────────────────────────────

clean-all: ## Remove all generated data and build artifacts
	@echo "$(BOLD)$(YELLOW)⚠️  This removes all DuckDB files and build artifacts$(RESET)"
	@read -p "Are you sure? [y/N] " confirm && [ "$$confirm" = "y" ] || exit 1
	rm -f $(BRONZE_DB) $(SILVER_DB) $(GOLD_DB)
	rm -rf $(DBT_DIR)/target
	rm -rf $(DASHBOARD_DIR)/build $(DASHBOARD_DIR)/.evidence-queries
	rm -rf quality/uncommitted/
	@echo "$(GREEN)✅ Clean complete$(RESET)"

clean-dbt: ## Remove dbt compilation artifacts only
	rm -rf $(DBT_DIR)/target

clean-dashboard: ## Remove Evidence.dev build cache
	rm -rf $(DASHBOARD_DIR)/build $(DASHBOARD_DIR)/.evidence-queries
