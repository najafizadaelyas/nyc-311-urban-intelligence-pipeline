"""
Dagster asset: Silver cleaning (Polars Bronze→Silver)

Depends on: bronze_service_requests
Triggers the Polars-based cleaning pipeline.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)

from clean.silver_cleaner import run_cleaning

PROJECT_ROOT = Path(__file__).parent.parent.parent
SILVER_DB = PROJECT_ROOT / "data" / "silver" / "silver.duckdb"


@asset(
    name="silver_service_requests",
    deps=["bronze_service_requests"],
    group_name="silver",
    description=(
        "Cleaned and deduplicated 311 service requests in Silver DuckDB. "
        "Produced by the Polars cleaning pipeline from Bronze. Adds temporal "
        "features, normalizes categoricals, validates geo coordinates."
    ),
    kinds={"polars", "duckdb"},
    tags={"layer": "silver"},
)
def silver_service_requests(context: AssetExecutionContext) -> MaterializeResult:
    """
    Materialize the Silver service_requests asset.

    Runs the Polars cleaning pipeline in incremental mode:
    only new Bronze records are processed.
    """
    context.log.info("Starting Silver cleaning via Polars")

    stats = run_cleaning(full_refresh=False)

    # Count total rows in Silver after cleaning
    total_silver_rows = 0
    if SILVER_DB.exists():
        with duckdb.connect(str(SILVER_DB), read_only=True) as con:
            try:
                total_silver_rows = con.execute(
                    "SELECT COUNT(*) FROM silver.service_requests"
                ).fetchone()[0]
            except Exception:  # noqa: BLE001
                pass

    db_size_mb = round(SILVER_DB.stat().st_size / 1024 / 1024, 2) if SILVER_DB.exists() else 0.0

    context.log.info(f"Silver cleaning complete. Total rows: {total_silver_rows}")

    return MaterializeResult(
        metadata={
            "rows_input":          MetadataValue.int(stats.get("input", 0)),
            "rows_after_dedup":    MetadataValue.int(stats.get("deduplicated", 0)),
            "rows_written":        MetadataValue.int(stats.get("written", 0)),
            "total_silver_rows":   MetadataValue.int(total_silver_rows),
            "silver_db_size_mb":   MetadataValue.float(db_size_mb),
        }
    )
