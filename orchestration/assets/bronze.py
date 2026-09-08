"""
Dagster asset: Bronze ingestion (dlt → DuckDB)

This asset wraps the dlt ingestion pipeline. When materialized, it:
  1. Calls the ingestion pipeline (incremental by default)
  2. Returns metadata about how many rows were loaded
  3. Marks the Bronze dataset as materialized in Dagster's asset catalog

Dagster will track the last materialization time and only re-run if the
schedule triggers it or a user explicitly re-materializes.
"""

from __future__ import annotations

from pathlib import Path

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)

from ingestion.pipeline import run_ingestion

PROJECT_ROOT = Path(__file__).parent.parent.parent
BRONZE_DB = PROJECT_ROOT / "data" / "bronze" / "bronze.duckdb"


@asset(
    name="bronze_service_requests",
    group_name="bronze",
    description=(
        "Raw NYC 311 service requests loaded from the Socrata Open Data API "
        "into Bronze DuckDB via dlt. Append-only, schema-inferred, incremental."
    ),
    kinds={"dlt", "duckdb"},
    owners=["data-engineering-team"],
    tags={"layer": "bronze", "source": "nyc_open_data"},
)
def bronze_service_requests(context: AssetExecutionContext) -> MaterializeResult:
    """
    Materialize the Bronze service_requests asset.

    Calls dlt ingestion pipeline. In normal runs, this is incremental —
    only records newer than the last run's cursor are fetched.
    """
    context.log.info("Starting Bronze ingestion via dlt")

    load_info = run_ingestion(
        full_refresh=False,
        max_rows=None,
    )

    # Extract row counts from dlt LoadInfo
    rows_loaded = 0
    for pkg in load_info.load_packages:
        rows_loaded += sum(
            j.row_counts.get("inserted", 0)
            for j in pkg.jobs.get("completed_jobs", [])
        )

    # Check Bronze DB size
    db_size_mb = 0.0
    if BRONZE_DB.exists():
        db_size_mb = round(BRONZE_DB.stat().st_size / 1024 / 1024, 2)

    context.log.info(f"Bronze ingestion complete. Rows loaded: {rows_loaded}")

    return MaterializeResult(
        metadata={
            "rows_loaded":   MetadataValue.int(rows_loaded),
            "bronze_db_path": MetadataValue.path(str(BRONZE_DB)),
            "bronze_db_size_mb": MetadataValue.float(db_size_mb),
            "dlt_pipeline_name": MetadataValue.text("nyc_311_bronze"),
        }
    )
