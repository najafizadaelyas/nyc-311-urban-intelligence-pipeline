"""
NYC 311 Urban Intelligence Pipeline — Dagster Definitions

This is the entry point for the Dagster orchestration layer.

To launch the Dagster UI:
    dagster dev -f orchestration/definitions.py
    → http://localhost:3001

The Definitions object combines:
  - Assets:    What data we produce (Bronze, Silver, Gold tables)
  - Checks:    Data quality gates (Great Expectations suites per layer)
  - Jobs:      Collections of assets to run together
  - Schedules: When to run jobs
  - Resources: Shared tools (dbt CLI, DuckDB connection)

Why Dagster's asset model?
  The traditional Airflow approach treats a pipeline as a graph of TASKS.
  Dagster treats it as a graph of ASSETS — the data products themselves.
  This means:
    - The UI shows you what data you HAVE, not just what ran
    - You can materialize any subset of assets (e.g., just re-run Gold)
    - Data freshness is first-class — Dagster knows when each table was last updated
    - Asset checks (GE validations) live alongside the assets they validate
"""

from __future__ import annotations

from pathlib import Path

from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource

from orchestration.assets import (
    bronze_service_requests,
    silver_service_requests,
    bronze_quality_check,
    silver_quality_check,
    gold_quality_check,
)
from orchestration.assets.gold import nyc_311_dbt_assets, dbt_resource
from orchestration.jobs.full_pipeline import full_pipeline_job
from orchestration.schedules.daily_schedule import daily_refresh_schedule

PROJECT_ROOT = Path(__file__).parent.parent
DBT_PROJECT_DIR = PROJECT_ROOT / "transform"

# ── Definitions ───────────────────────────────────────────────────────────────
# Dagster's Definitions is the top-level registry of everything in the pipeline.
# All assets, checks, jobs, schedules, and resources are registered here.

defs = Definitions(
    assets=[
        bronze_service_requests,
        silver_service_requests,
        nyc_311_dbt_assets,
    ],
    asset_checks=[
        bronze_quality_check,
        silver_quality_check,
        gold_quality_check,
    ],
    jobs=[
        full_pipeline_job,
    ],
    schedules=[
        daily_refresh_schedule,
    ],
    resources={
        "dbt": dbt_resource,
    },
)
