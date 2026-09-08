"""
NYC 311 Urban Intelligence Pipeline — dlt Bronze Ingestion

Entry point for the Bronze layer ingestion job. This script:

  1. Connects to the NYC Open Data Socrata API
  2. Fetches 311 service request records (incremental by default)
  3. Loads them into the Bronze DuckDB database via dlt
  4. Prints a load summary to stdout

Usage:
    python -m ingestion.pipeline [--full-refresh] [--max-rows N] [--start-date YYYY-MM-DD]

Or via Make:
    make ingest

Design decisions:
  - dlt write_disposition="append" means we never overwrite Bronze data.
    Bronze is immutable raw history. Silver cleaning runs separately.
  - dlt's incremental cursor (created_date) automatically makes every
    run after the first a cheap delta load.
  - The DuckDB database is a single file at data/bronze/bronze.duckdb.
    This is portable, zero-maintenance, and queryable with any DuckDB client.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import dlt
import structlog
from dotenv import load_dotenv

from ingestion.sources.nyc_311 import nyc_311_source

# ─── Logging setup ────────────────────────────────────────────────────────────

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ]
)
log = structlog.get_logger(__name__)

# ─── Paths ────────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).parent.parent
BRONZE_DB_PATH = PROJECT_ROOT / "data" / "bronze" / "bronze.duckdb"

# ─── Pipeline definition ──────────────────────────────────────────────────────

def build_pipeline() -> dlt.Pipeline:
    """
    Build and configure the dlt pipeline.

    The pipeline:
    - Name: "nyc_311_bronze"  (used as the schema prefix in DuckDB)
    - Destination: DuckDB at data/bronze/bronze.duckdb
    - Dataset name: "bronze" (all tables land in this schema)
    """
    return dlt.pipeline(
        pipeline_name="nyc_311_bronze",
        destination=dlt.destinations.duckdb(str(BRONZE_DB_PATH)),
        dataset_name="bronze",
        dev_mode=False,
    )


def run_ingestion(
    full_refresh: bool = False,
    max_rows: int | None = None,
    start_date: str | None = None,
) -> dlt.LoadInfo:
    """
    Run the Bronze ingestion job.

    Args:
        full_refresh:  If True, resets the incremental cursor and reloads all history.
                       WARNING: This re-fetches potentially millions of rows.
        max_rows:      Cap the number of rows fetched. Useful for testing.
        start_date:    Override the start date for the initial load (YYYY-MM-DD).

    Returns:
        dlt LoadInfo object with row counts, timing, and schema info.
    """
    load_dotenv()
    app_token = os.getenv("SOCRATA_APP_TOKEN", "")

    log.info(
        "starting_bronze_ingestion",
        full_refresh=full_refresh,
        max_rows=max_rows,
        start_date=start_date,
        has_app_token=bool(app_token),
        bronze_db=str(BRONZE_DB_PATH),
    )

    # Ensure output directory exists
    BRONZE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    pipeline = build_pipeline()

    source = nyc_311_source(
        app_token=app_token,
        max_rows=max_rows,
        start_date=start_date,
    )

    if full_refresh:
        # Reset state → forces a full reload from scratch
        log.warning("full_refresh_requested", warning="This will reload all history")
        pipeline.reset_run()

    load_info = pipeline.run(source)

    log.info(
        "bronze_ingestion_complete",
        rows_loaded=sum(p.jobs["completed_jobs"] for p in load_info.load_packages),
        schema_name=pipeline.dataset_name,
        destination=str(BRONZE_DB_PATH),
    )

    # Print the dlt-generated load summary (includes row counts, schema changes)
    print(load_info)

    return load_info


# ─── CLI entry point ──────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="NYC 311 Bronze Ingestion — loads service request data via dlt → DuckDB"
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        default=False,
        help="Reset incremental cursor and reload all history from scratch",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Maximum rows to fetch (useful for testing, e.g. --max-rows 10000)",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Start date for initial load (YYYY-MM-DD, default: 2020-01-01)",
    )
    args = parser.parse_args()

    try:
        run_ingestion(
            full_refresh=args.full_refresh,
            max_rows=args.max_rows,
            start_date=args.start_date,
        )
    except Exception as exc:
        log.exception("bronze_ingestion_failed", error=str(exc))
        sys.exit(1)


if __name__ == "__main__":
    main()
