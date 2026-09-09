"""
NYC 311 Urban Intelligence Pipeline — Great Expectations Quality Runner

Programmatic runner for all three GE validation checkpoints.
Called by Dagster as the quality gate asset, or directly via CLI.

Usage:
    python -m quality.runner [bronze|silver|gold|all]

Or via Make:
    make quality

Design:
  - One GE suite per layer (bronze, silver, gold)
  - Suites define explicit contracts about what valid data looks like
  - Validation results written to uncommitted/validations/ (git-ignored)
  - Data Docs (HTML reports) written to uncommitted/data_docs/ (git-ignored)
  - Any FAILED validation raises an exception to halt the pipeline

Why Great Expectations and not just dbt tests?
  - GE tests run as a standalone quality gate BEFORE and AFTER each layer
  - GE gives you Data Docs: a browsable HTML report of every expectation result
  - GE can validate any data source (not just dbt-managed tables)
  - GE expectations are reusable across pipeline runs and tools
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import great_expectations as gx
import structlog
from dotenv import load_dotenv

log = structlog.get_logger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent
GE_ROOT = PROJECT_ROOT / "quality"

BRONZE_DB = PROJECT_ROOT / "data" / "bronze" / "nyc_311.duckdb"
SILVER_DB = PROJECT_ROOT / "data" / "silver" / "silver.duckdb"
GOLD_DB   = PROJECT_ROOT / "data" / "gold"   / "gold.duckdb"


def run_bronze_validation() -> bool:
    """
    Validate the Bronze layer (raw data from dlt ingestion).

    Connects to bronze.duckdb and checks:
      - Table exists and is non-empty
      - unique_key is non-null and mostly unique
      - created_date is present and parseable
      - Borough values are within known set
    """
    log.info("running_bronze_validation")
    return _run_checkpoint(
        db_path=BRONZE_DB,
        schema="bronze",
        table="service_requests",
        suite_name="bronze_service_requests",
    )


def run_silver_validation() -> bool:
    """
    Validate the Silver layer (after Polars cleaning).

    Stricter than Bronze — all cleaning contracts must now hold:
      - Full deduplication on unique_key
      - Valid geo coordinates only
      - No negative response_hours
      - Status in controlled vocabulary
      - Temporal feature columns present and valid
    """
    log.info("running_silver_validation")
    return _run_checkpoint(
        db_path=SILVER_DB,
        schema="main",
        table="service_requests",
        suite_name="silver_service_requests",
    )


def run_gold_validation() -> bool:
    """
    Validate the Gold layer (after dbt transformations).

    Business-level contracts:
      - Fact table has substantial data (> 1000 rows)
      - Surrogate keys are unique and non-null
      - response_hours is in valid range
      - response_time_bucket values are from controlled set
    """
    log.info("running_gold_validation")
    return _run_checkpoint(
        db_path=GOLD_DB,
        schema="gold",
        table="fact_311_requests",
        suite_name="gold_fact_311_requests",
    )


def _run_checkpoint(
    db_path: Path,
    schema: str,
    table: str,
    suite_name: str,
) -> bool:
    """
    Run a single Great Expectations validation checkpoint.

    Returns True if all expectations pass, False otherwise.
    """
    if not db_path.exists():
        log.error("database_not_found", path=str(db_path))
        return False

    context = gx.get_context(context_root_dir=str(GE_ROOT))

    # Build connection string for DuckDB
    connection_string = f"duckdb:///{db_path}"

    try:
        # Use GE's fluent datasource API (GE 0.18.x+)
        datasource = context.sources.add_or_update_sql(
            name=f"{schema}_duckdb",
            connection_string=connection_string,
        )

        asset = datasource.add_table_asset(
            name=table,
            schema_name=schema,
            table_name=table,
        )

        batch_request = asset.build_batch_request()

        # Load the expectation suite
        suite = context.get_expectation_suite(suite_name)

        # Run validation
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite=suite,
        )

        results = validator.validate()

        # Build Data Docs
        context.build_data_docs()

        success = results.success
        stats = results.statistics

        log.info(
            "validation_complete",
            suite=suite_name,
            success=success,
            evaluated=stats["evaluated_expectations"],
            successful=stats["successful_expectations"],
            unsuccessful=stats["unsuccessful_expectations"],
        )

        if not success:
            log.error(
                "validation_failed",
                suite=suite_name,
                failed_count=stats["unsuccessful_expectations"],
            )

        return success

    except Exception as exc:
        log.exception("validation_error", suite=suite_name, error=str(exc))
        return False


def run_all_validations(fail_fast: bool = True) -> dict[str, bool]:
    """
    Run validations for all three layers in sequence.

    Args:
        fail_fast: If True, raise an exception on first failure (blocks pipeline).
                   If False, run all and return a results dict.

    Returns:
        Dict of {layer_name: passed_bool}
    """
    load_dotenv()

    results = {}

    for layer_name, fn in [
        ("bronze", run_bronze_validation),
        ("silver", run_silver_validation),
        ("gold",   run_gold_validation),
    ]:
        passed = fn()
        results[layer_name] = passed

        if fail_fast and not passed:
            raise RuntimeError(
                f"Data quality validation FAILED for {layer_name} layer. "
                f"Check Data Docs at quality/uncommitted/data_docs/local_site/index.html"
            )

    all_passed = all(results.values())
    log.info("all_validations_complete", results=results, all_passed=all_passed)
    return results


if __name__ == "__main__":
    layer = sys.argv[1] if len(sys.argv) > 1 else "all"

    if layer == "bronze":
        ok = run_bronze_validation()
    elif layer == "silver":
        ok = run_silver_validation()
    elif layer == "gold":
        ok = run_gold_validation()
    else:
        results = run_all_validations(fail_fast=False)
        ok = all(results.values())
        print("\nValidation Results:")
        for lyr, passed in results.items():
            icon = "✅" if passed else "❌"
            print(f"  {icon} {lyr}")

    sys.exit(0 if ok else 1)
