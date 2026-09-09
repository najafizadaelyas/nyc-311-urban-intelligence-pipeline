"""
NYC 311 Urban Intelligence Pipeline — Polars Silver Cleaner

Reads raw 311 service request data from Bronze DuckDB, applies a comprehensive
cleaning and enrichment pipeline using Polars, and writes the results to
Silver DuckDB.

Cleaning operations performed:
  1.  DEDUPLICATION    — Remove exact duplicates and re-ingested rows by unique_key
  2.  TIMESTAMP PARSE  — Parse all date strings into proper UTC timestamps
  3.  TEMPORAL LOGIC   — Validate closed_date > created_date; compute response_hours
  4.  STRING NORMALIZE — Strip whitespace, normalize casing on categorical fields
  5.  BOROUGH CLEAN    — Standardize borough values, null out "Unspecified"
  6.  COMPLAINT CLEAN  — Deduplicate complaint_type aliases (e.g. HEAT/HOT WATER)
  7.  GEO VALIDATION   — Null out lat/lon outside NYC bounding box
  8.  NULL ENRICHMENT  — Flag rows with critical nulls for downstream filtering
  9.  DERIVED COLUMNS  — created_year, created_month, created_dow, is_weekend
  10. STATUS ENUM      — Map status to a controlled vocabulary

Usage:
    python -m clean.silver_cleaner [--full-refresh]

Or via Make:
    make clean
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import polars as pl
import structlog
from dotenv import load_dotenv

# ─── Logging ──────────────────────────────────────────────────────────────────

log = structlog.get_logger(__name__)

# ─── Paths ────────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).parent.parent
BRONZE_DB = PROJECT_ROOT / "data" / "bronze" / "nyc_311.duckdb"
SILVER_DB = PROJECT_ROOT / "data" / "silver" / "silver.duckdb"

# ─── NYC Bounding Box ─────────────────────────────────────────────────────────

NYC_LAT_MIN, NYC_LAT_MAX = 40.4, 40.95
NYC_LON_MIN, NYC_LON_MAX = -74.3, -73.7

# ─── Controlled vocabularies ──────────────────────────────────────────────────

VALID_BOROUGHS = frozenset({
    "MANHATTAN", "BRONX", "BROOKLYN", "QUEENS", "STATEN ISLAND"
})

# Map known messy agency codes to canonical names
AGENCY_CORRECTIONS: dict[str, str] = {
    "DOHMH": "DOHMH",
    "DOT":   "DOT",
    "NYPD":  "NYPD",
    "DEP":   "DEP",
    "HPD":   "HPD",
    "DSNY":  "DSNY",
    "DOB":   "DOB",
    "DCA":   "DCA",
    "DCAS":  "DCAS",
    "DPR":   "DPR",
    "EDC":   "EDC",
    "FDNY":  "FDNY",
    "MTA":   "MTA",
    "HRA":   "HRA",
    "DFTA":  "DFTA",
    "DHS":   "DHS",
    "DOE":   "DOE",
    "DCP":   "DCP",
}

# Complaint type aliases to normalize (Socrata data has inconsistencies)
COMPLAINT_TYPE_ALIASES: dict[str, str] = {
    "HEAT/HOT WATER":        "HEAT/HOT WATER",
    "Heating":               "HEAT/HOT WATER",
    "HEATING":               "HEAT/HOT WATER",
    "Noise - Residential":   "NOISE - RESIDENTIAL",
    "Noise Residential":     "NOISE - RESIDENTIAL",
    "Noise-Residential":     "NOISE - RESIDENTIAL",
    "Noise - Street/Sidewalk":"NOISE - STREET/SIDEWALK",
    "NOISE-STREET/SIDEWALK": "NOISE - STREET/SIDEWALK",
    "Illegal Parking":       "ILLEGAL PARKING",
    "ILLEGAL PARKING":       "ILLEGAL PARKING",
    "Blocked Driveway":      "BLOCKED DRIVEWAY",
    "BLOCKED DRIVEWAY":      "BLOCKED DRIVEWAY",
    "Street Light Condition":"STREET LIGHT CONDITION",
    "Dirty Conditions":      "DIRTY CONDITIONS",
    "DIRTY CONDITIONS":      "DIRTY CONDITIONS",
    "Rodent":                "RODENT",
    "RODENT":                "RODENT",
}

# Status normalization
# Keys must include BOTH the original mixed-case form AND the post-_clean_strings
# uppercase form, because _clean_strings runs before _normalize_status.
STATUS_NORMALIZATION: dict[str, str] = {
    "Open":               "OPEN",
    "Closed":             "CLOSED",
    "Assigned":           "ASSIGNED",
    "Pending":            "PENDING",
    "In Progress":        "IN_PROGRESS",
    "IN PROGRESS":        "IN_PROGRESS",
    "In-Progress":        "IN_PROGRESS",
    "IN-PROGRESS":        "IN_PROGRESS",
    "Started":            "IN_PROGRESS",
    "STARTED":            "IN_PROGRESS",
    "Draft":              "DRAFT",
    "Email Sent":         "EMAIL_SENT",
    "EMAIL SENT":         "EMAIL_SENT",
    "More Information Requested": "MORE_INFO_REQUESTED",
    "MORE INFORMATION REQUESTED": "MORE_INFO_REQUESTED",
}


# ─── Main cleaning pipeline ───────────────────────────────────────────────────

def run_cleaning(full_refresh: bool = False) -> dict[str, int]:
    """
    Execute the full Bronze → Silver cleaning pipeline.

    Strategy:
      - Read from Bronze DuckDB using DuckDB Python API (efficient columnar scan)
      - Transfer to Polars DataFrame via Arrow (zero-copy where possible)
      - Apply all cleaning transformations using Polars lazy API
      - Write the cleaned result back to Silver DuckDB

    Args:
        full_refresh: If True, truncate and fully rebuild the Silver table.
                      If False (default), only process records newer than the
                      latest created_date already in Silver (incremental mode).

    Returns:
        dict with row counts: input, deduplicated, cleaned, written
    """
    load_dotenv()
    log.info("silver_cleaning_start", bronze_db=str(BRONZE_DB), silver_db=str(SILVER_DB))

    SILVER_DB.parent.mkdir(parents=True, exist_ok=True)

    # ── Step 1: Read from Bronze ──────────────────────────────────────────────
    df = _read_bronze(full_refresh)
    input_rows = len(df)
    log.info("bronze_data_loaded", rows=input_rows)

    if input_rows == 0:
        log.info("no_new_data", message="Silver is already up-to-date")
        return {"input": 0, "deduplicated": 0, "cleaned": 0, "written": 0}

    # ── Step 2: Apply cleaning pipeline ──────────────────────────────────────
    df_clean = (
        df.lazy()
        .pipe(_deduplicate)
        .pipe(_parse_timestamps)
        .pipe(_compute_temporal_features)
        .pipe(_clean_strings)
        .pipe(_normalize_borough)
        .pipe(_normalize_complaint_type)
        .pipe(_normalize_status)
        .pipe(_validate_geo)
        .pipe(_flag_nulls)
        .collect()
    )

    dedup_rows = len(df_clean)
    log.info("cleaning_complete", input_rows=input_rows, clean_rows=dedup_rows,
             dropped=input_rows - dedup_rows)

    # ── Step 3: Write to Silver DuckDB ────────────────────────────────────────
    written_rows = _write_silver(df_clean, full_refresh)
    log.info("silver_write_complete", rows_written=written_rows)

    return {
        "input": input_rows,
        "deduplicated": dedup_rows,
        "cleaned": dedup_rows,
        "written": written_rows,
    }


# ─── Bronze reader ────────────────────────────────────────────────────────────

def _read_bronze(full_refresh: bool) -> pl.DataFrame:
    """
    Read service request records from Bronze DuckDB.

    In incremental mode, only reads records newer than the max created_date
    already present in Silver (avoids re-processing old data).
    """
    with duckdb.connect(str(BRONZE_DB), read_only=True) as bronze_con:
        # Check if the bronze table exists
        tables = bronze_con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'bronze'"
        ).fetchall()

        if not tables:
            log.warning("bronze_table_missing", message="Bronze has no tables yet. Run ingestion first.")
            return pl.DataFrame()

        query = "SELECT * FROM bronze.service_requests"

        if not full_refresh:
            # Incremental: get the latest created_date already in Silver
            cutoff = _get_silver_max_created_date()
            if cutoff:
                query += f" WHERE created_date > '{cutoff}'"
                log.info("incremental_load", cutoff=cutoff)

        arrow_table = bronze_con.execute(query).arrow()

    return pl.from_arrow(arrow_table)


def _get_silver_max_created_date() -> str | None:
    """Return the maximum created_date in Silver, or None if Silver is empty."""
    if not SILVER_DB.exists():
        return None
    try:
        with duckdb.connect(str(SILVER_DB), read_only=True) as silver_con:
            result = silver_con.execute(
                "SELECT MAX(created_date) FROM service_requests"
            ).fetchone()
            if result and result[0]:
                return str(result[0])
    except Exception:  # noqa: BLE001
        pass
    return None


# ─── Cleaning transformations ─────────────────────────────────────────────────

def _deduplicate(df: pl.LazyFrame) -> pl.LazyFrame:
    """
    Remove duplicate rows.

    Strategy:
      - Keep the latest record per unique_key (by _pipeline_loaded_at)
      - This handles re-ingested records from dlt's append-only Bronze
    """
    return (
        df
        .sort("_pipeline_loaded_at", descending=True, nulls_last=True)
        .unique(subset=["unique_key"], keep="first", maintain_order=True)
    )


def _parse_timestamps(df: pl.LazyFrame) -> pl.LazyFrame:
    """
    Ensure all date/time columns are proper Polars Datetime types.

    dlt already coerces timestamps before writing to DuckDB, so these columns
    arrive as datetime[μs, tz] types. We only apply str.to_datetime() if a
    column is still a String (e.g. if the schema changes upstream).
    """
    timestamp_cols = ["created_date", "closed_date", "due_date", "resolution_action_updated_date"]
    schema = df.collect_schema()

    exprs = []
    for col in timestamp_cols:
        if col not in schema:
            continue
        if schema[col] == pl.String:
            exprs.append(
                pl.col(col)
                .str.to_datetime(
                    format="%Y-%m-%dT%H:%M:%S%.f",
                    strict=False,
                    time_unit="us",
                )
                .alias(col)
            )
        # Already a datetime type (dlt-typed) — no conversion needed

    return df.with_columns(exprs) if exprs else df


def _compute_temporal_features(df: pl.LazyFrame) -> pl.LazyFrame:
    """
    Compute derived temporal features for analytics:
      - response_hours:   hours from created_date to closed_date
      - created_year/month/day/hour/dow: date part extractions
      - is_weekend:       True if created on Saturday or Sunday
      - is_after_hours:   True if created outside 8am–6pm

    NOTE: closed_date correction and response_hours are in separate with_columns
    calls so that response_hours is computed from the already-corrected closed_date,
    never producing a negative value.
    """
    # Step 1: Null out closed_date when it precedes created_date
    df = df.with_columns([
        pl.when(
            pl.col("closed_date").is_not_null()
            & pl.col("created_date").is_not_null()
            & (pl.col("closed_date") < pl.col("created_date"))
        )
        .then(None)
        .otherwise(pl.col("closed_date"))
        .alias("closed_date"),
    ])

    # Step 2: Compute response_hours from the corrected closed_date, plus date parts
    return df.with_columns([
        # Response time in hours (null when ticket still open or closed_date was bad)
        pl.when(pl.col("closed_date").is_not_null() & pl.col("created_date").is_not_null())
          .then(
              (pl.col("closed_date") - pl.col("created_date"))
              .dt.total_seconds()
              .truediv(3600.0)
          )
          .otherwise(None)
          .alias("response_hours"),

        # Date part extractions
        pl.col("created_date").dt.year().alias("created_year"),
        pl.col("created_date").dt.month().alias("created_month"),
        pl.col("created_date").dt.day().alias("created_day"),
        pl.col("created_date").dt.hour().alias("created_hour"),
        pl.col("created_date").dt.weekday().alias("created_dow"),  # 0=Mon, 6=Sun

        # Flags
        (pl.col("created_date").dt.weekday() >= 5).alias("is_weekend"),
        (
            (pl.col("created_date").dt.hour() < 8)
            | (pl.col("created_date").dt.hour() >= 18)
        ).alias("is_after_hours"),
    ])


def _clean_strings(df: pl.LazyFrame) -> pl.LazyFrame:
    """
    Normalize string columns:
      - Strip leading/trailing whitespace
      - Convert categorical fields to UPPER_CASE for consistency
      - Replace empty strings with null
    """
    # Columns to strip + uppercase
    upper_cols = ["agency", "complaint_type", "descriptor", "borough",
                  "status", "facility_type", "location_type",
                  "open_data_channel_type", "park_borough"]

    # Columns to just strip (preserve mixed case)
    strip_cols = ["incident_address", "street_name", "cross_street_1",
                  "cross_street_2", "city", "landmark", "resolution_description",
                  "agency_name"]

    exprs = []
    for col in upper_cols:
        if col in df.schema:
            exprs.append(
                pl.col(col)
                .str.strip_chars()
                .str.to_uppercase()
                .str.replace_all(r"\s+", " ")       # collapse multiple spaces
                .pipe(lambda s: pl.when(s == "").then(None).otherwise(s))
                .alias(col)
            )

    for col in strip_cols:
        if col in df.schema:
            exprs.append(
                pl.col(col)
                .str.strip_chars()
                .pipe(lambda s: pl.when(s == "").then(None).otherwise(s))
                .alias(col)
            )

    return df.with_columns(exprs) if exprs else df


def _normalize_borough(df: pl.LazyFrame) -> pl.LazyFrame:
    """
    Standardize borough values.
    - "Unspecified" → null (it's not a borough, it's a missing value)
    - "STATEN ISLAND" kept (has a space — consistent with NYC naming)
    - Anything not in VALID_BOROUGHS → null
    """
    return df.with_columns(
        pl.when(pl.col("borough").is_in(list(VALID_BOROUGHS)))
          .then(pl.col("borough"))
          .otherwise(None)
          .alias("borough")
    )


def _normalize_complaint_type(df: pl.LazyFrame) -> pl.LazyFrame:
    """
    Normalize complaint_type values to a controlled vocabulary.
    Socrata data has capitalization inconsistencies and alias variants.
    """
    # Build a Polars-native mapping using a when/then chain
    # For unmapped values, uppercase the existing value (already done in _clean_strings)
    complaint_map = pl.Series(
        name="complaint_type",
        values=list(COMPLAINT_TYPE_ALIASES.values()),
    )
    keys = list(COMPLAINT_TYPE_ALIASES.keys())

    # replace() keeps the original value for unmatched entries (Polars 1.x default)
    return df.with_columns(
        pl.col("complaint_type")
          .replace(old=keys, new=list(COMPLAINT_TYPE_ALIASES.values()))
          .alias("complaint_type")
    )


def _normalize_status(df: pl.LazyFrame) -> pl.LazyFrame:
    """Map status values to a controlled vocabulary."""
    keys = list(STATUS_NORMALIZATION.keys())
    values = list(STATUS_NORMALIZATION.values())
    return df.with_columns(
        pl.col("status")
          .replace(old=keys, new=values)
          .alias("status")
    )


def _validate_geo(df: pl.LazyFrame) -> pl.LazyFrame:
    """
    Null out latitude/longitude values outside the NYC bounding box.

    Invalid coordinates include:
      - (0, 0) — common placeholder for missing geo
      - Coordinates that are outside NYC's geographic extent
      - Records where only one of lat/lon is present (partial geo = invalid)
    """
    valid_lat = (
        pl.col("latitude").is_not_null()
        & pl.col("latitude").is_between(NYC_LAT_MIN, NYC_LAT_MAX)
    )
    valid_lon = (
        pl.col("longitude").is_not_null()
        & pl.col("longitude").is_between(NYC_LON_MIN, NYC_LON_MAX)
    )
    valid_geo = valid_lat & valid_lon

    return df.with_columns([
        pl.when(valid_geo).then(pl.col("latitude")).otherwise(None).alias("latitude"),
        pl.when(valid_geo).then(pl.col("longitude")).otherwise(None).alias("longitude"),
        valid_geo.alias("has_valid_geo"),
    ])


def _flag_nulls(df: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add boolean quality flag columns for downstream filtering.
    These don't drop rows — they make quality issues explicit and queryable.
    """
    return df.with_columns([
        pl.col("closed_date").is_null().alias("is_open_ticket"),
        pl.col("borough").is_null().alias("has_unknown_borough"),
        pl.col("complaint_type").is_null().alias("has_null_complaint_type"),
        (pl.col("response_hours").is_not_null() & (pl.col("response_hours") < 0))
          .alias("has_negative_response_time"),

        # Silver metadata
        pl.lit(datetime.now(timezone.utc).isoformat()).alias("_silver_cleaned_at"),
    ])


# ─── Silver writer ────────────────────────────────────────────────────────────

def _write_silver(df: pl.DataFrame, full_refresh: bool) -> int:
    """
    Write the cleaned DataFrame to Silver DuckDB.

    In full_refresh mode: DROP and recreate the table.
    In incremental mode: INSERT the new rows (Silver is append+update).
    """
    arrow_table = df.to_arrow()

    with duckdb.connect(str(SILVER_DB)) as silver_con:
        if full_refresh:
            silver_con.execute("DROP TABLE IF EXISTS service_requests")

        # Register the Arrow table as a DuckDB view, then INSERT into Silver
        silver_con.register("cleaned_batch", arrow_table)
        silver_con.execute("""
            CREATE TABLE IF NOT EXISTS service_requests AS
                SELECT * FROM cleaned_batch WHERE 1=0
        """)

        if full_refresh:
            silver_con.execute(
                "INSERT INTO service_requests SELECT * FROM cleaned_batch"
            )
        else:
            # Upsert: update if unique_key already exists, insert if new
            silver_con.execute("""
                DELETE FROM service_requests
                WHERE unique_key IN (SELECT unique_key FROM cleaned_batch)
            """)
            silver_con.execute(
                "INSERT INTO service_requests SELECT * FROM cleaned_batch"
            )

        silver_con.unregister("cleaned_batch")

        count = silver_con.execute(
            "SELECT COUNT(*) FROM service_requests"
        ).fetchone()[0]

    return count


# ─── CLI entry point ──────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="NYC 311 Silver Cleaner — Polars-based Bronze→Silver transformation"
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        default=False,
        help="Truncate Silver and rebuild from all Bronze data",
    )
    args = parser.parse_args()

    try:
        stats = run_cleaning(full_refresh=args.full_refresh)
        print(f"\nSilver cleaning complete:")
        print(f"   Input rows:        {stats['input']:,}")
        print(f"   After dedup:       {stats['deduplicated']:,}")
        print(f"   Written to Silver: {stats['written']:,}")
    except Exception as exc:
        log.exception("silver_cleaning_failed", error=str(exc))
        sys.exit(1)


if __name__ == "__main__":
    main()
