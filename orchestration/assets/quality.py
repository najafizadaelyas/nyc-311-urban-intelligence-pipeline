"""
Dagster asset: Data quality validation (Great Expectations)

Runs GE validation suites at Bronze and Silver layer boundaries.
A failure here blocks the Gold transformation from running.
"""

from __future__ import annotations

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    asset_check,
)

from quality.runner import run_bronze_validation, run_silver_validation, run_gold_validation


@asset_check(
    asset="bronze_service_requests",
    name="bronze_data_quality",
    description="Great Expectations validation for the Bronze layer. Checks uniqueness, nulls, date formats.",
    blocking=True,   # Blocks Silver if this fails
)
def bronze_quality_check(context: AssetCheckExecutionContext) -> AssetCheckResult:
    """Run Bronze GE validation suite."""
    context.log.info("Running Bronze Great Expectations validation")
    passed = run_bronze_validation()
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "suite": "bronze_service_requests",
            "docs": "quality/uncommitted/data_docs/local_site/index.html",
        },
    )


@asset_check(
    asset="silver_service_requests",
    name="silver_data_quality",
    description="Great Expectations validation for the Silver layer. Checks dedup, geo, status enum, temporal features.",
    blocking=True,   # Blocks Gold if this fails
)
def silver_quality_check(context: AssetCheckExecutionContext) -> AssetCheckResult:
    """Run Silver GE validation suite."""
    context.log.info("Running Silver Great Expectations validation")
    passed = run_silver_validation()
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "suite": "silver_service_requests",
            "docs": "quality/uncommitted/data_docs/local_site/index.html",
        },
    )


@asset_check(
    asset="gold_fact_311_requests",
    name="gold_data_quality",
    description="Great Expectations validation for the Gold fact table. Checks row count, surrogate keys, response time range.",
    blocking=False,  # Gold validation warns but doesn't block dashboard
)
def gold_quality_check(context: AssetCheckExecutionContext) -> AssetCheckResult:
    """Run Gold GE validation suite."""
    context.log.info("Running Gold Great Expectations validation")
    passed = run_gold_validation()
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "suite": "gold_fact_311_requests",
            "docs": "quality/uncommitted/data_docs/local_site/index.html",
        },
    )
