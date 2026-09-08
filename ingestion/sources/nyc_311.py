"""
NYC 311 Socrata SODA API — dlt Source Connector

Fetches NYC 311 service requests from the NYC Open Data portal using the
Socrata SODA 2.0 API. Supports:

  - Full load (initial historical load)
  - Incremental load (append new records by created_date)
  - Configurable page size and row limits
  - Automatic rate-limit retry with exponential backoff

API endpoint: https://data.cityofnewyork.us/resource/erm2-nwe9.json
Dataset docs: https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Iterator

# Default start date for initial full load
_INITIAL_START_DATE = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

import dlt
import requests
from dlt.sources import DltResource
from tenacity import retry, stop_after_attempt, wait_exponential

# ─── Constants ────────────────────────────────────────────────────────────────

SOCRATA_BASE_URL = "https://data.cityofnewyork.us/resource"
DATASET_ID = "erm2-nwe9"
API_ENDPOINT = f"{SOCRATA_BASE_URL}/{DATASET_ID}.json"

# Fields we want from the 41-column source (subset to avoid noise)
SELECTED_FIELDS = [
    "unique_key",
    "created_date",
    "closed_date",
    "agency",
    "agency_name",
    "complaint_type",
    "descriptor",
    "location_type",
    "incident_zip",
    "incident_address",
    "street_name",
    "cross_street_1",
    "cross_street_2",
    "city",
    "landmark",
    "facility_type",
    "status",
    "due_date",
    "resolution_description",
    "resolution_action_updated_date",
    "community_board",
    "bbl",
    "borough",
    "x_coordinate_state_plane",
    "y_coordinate_state_plane",
    "open_data_channel_type",
    "park_facility_name",
    "park_borough",
    "vehicle_type",
    "taxi_company_borough",
    "taxi_pick_up_location",
    "bridge_highway_name",
    "bridge_highway_direction",
    "road_ramp",
    "bridge_highway_segment",
    "latitude",
    "longitude",
    "council_district",
    "police_precinct",
]

DEFAULT_PAGE_SIZE = 1000   # Socrata default limit per request
MAX_PAGE_SIZE = 50_000     # Socrata hard maximum per request


# ─── dlt Source ───────────────────────────────────────────────────────────────

@dlt.source(name="nyc_311")
def nyc_311_source(
    app_token: str = dlt.secrets.value,
    page_size: int = MAX_PAGE_SIZE,
    max_rows: int | None = None,
    start_date: str | None = None,
) -> DltResource:
    """
    dlt source for NYC 311 service requests.

    Args:
        app_token:  NYC Open Data app token (optional; increases rate limit from
                    1,000 req/hr to 10,000 req/hr). Set via SOCRATA_APP_TOKEN env var.
        page_size:  Rows per API page. Max 50,000. Default is max for efficiency.
        max_rows:   Cap total rows fetched. Useful for development/testing.
        start_date: ISO date string (YYYY-MM-DD). Fetch records created after this date.
                    If None, fetches all records from 2020-01-01.

    Returns:
        A dlt resource that yields dictionaries (one per 311 request).
    """
    return nyc_311_requests(
        app_token=app_token,
        page_size=page_size,
        max_rows=max_rows,
        start_date=start_date,
    )


@dlt.resource(
    name="service_requests",
    write_disposition="append",          # append-only Bronze layer
    primary_key="unique_key",            # dlt deduplication key
    columns={
        "unique_key":      {"data_type": "text",      "nullable": False},
        "created_date":    {"data_type": "timestamp",  "nullable": False},
        "closed_date":     {"data_type": "timestamp",  "nullable": True},
        "due_date":        {"data_type": "timestamp",  "nullable": True},
        "latitude":        {"data_type": "double",     "nullable": True},
        "longitude":       {"data_type": "double",     "nullable": True},
        "council_district":{"data_type": "bigint",     "nullable": True},
        "police_precinct": {"data_type": "bigint",     "nullable": True},
    },
)
def nyc_311_requests(
    app_token: str,
    page_size: int,
    max_rows: int | None,
    start_date: str | None,
    # dlt incremental: only fetch records newer than the last pipeline run
    created_at: dlt.sources.incremental[datetime] = dlt.sources.incremental(
        "created_date",
        initial_value=_INITIAL_START_DATE,
        lag=3600,   # 1-hour lag to handle late-arriving records
    ),
) -> Iterator[dict]:
    """
    Paginated, incremental resource that fetches 311 service requests.

    dlt's @incremental decorator automatically tracks the last `created_date`
    seen and passes it as a filter on the next run. This makes every run after
    the initial load a cheap incremental load.
    """
    headers: dict[str, str] = {"Accept": "application/json"}
    if app_token:
        headers["X-App-Token"] = app_token

    # Build the $where clause for incremental load
    where_clauses = []
    _last: datetime = created_at.last_value or (
        datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
        if start_date
        else _INITIAL_START_DATE
    )
    # Socrata expects ISO format without timezone suffix
    effective_start = _last.strftime("%Y-%m-%dT%H:%M:%S.000")
    where_clauses.append(f"created_date >= '{effective_start}'")

    # Select only the fields we need (reduces payload size significantly)
    select_clause = ", ".join(SELECTED_FIELDS)

    offset = 0
    total_fetched = 0

    while True:
        if max_rows and total_fetched >= max_rows:
            break

        params: dict[str, str | int] = {
            "$select": select_clause,
            "$limit": min(page_size, max_rows - total_fetched) if max_rows else page_size,
            "$offset": offset,
            "$order": "created_date ASC",
        }
        if where_clauses:
            params["$where"] = " AND ".join(where_clauses)

        rows = _fetch_page(API_ENDPOINT, headers=headers, params=params)

        if not rows:
            # No more data — pagination complete
            break

        for row in rows:
            yield _coerce_types(row)

        fetched_this_page = len(rows)
        total_fetched += fetched_this_page
        offset += fetched_this_page

        if fetched_this_page < page_size:
            # Last page was partial — we've reached the end
            break


# ─── HTTP helpers ─────────────────────────────────────────────────────────────

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    reraise=True,
)
def _fetch_page(url: str, headers: dict, params: dict) -> list[dict]:
    """
    Fetch a single page from the Socrata API.
    Retries up to 5 times with exponential backoff on network errors or 429s.
    """
    response = requests.get(url, headers=headers, params=params, timeout=60)

    if response.status_code == 429:
        raise requests.HTTPError("Rate limited by Socrata API", response=response)

    response.raise_for_status()
    return response.json()


def _coerce_types(row: dict) -> dict:
    """
    Light type coercion before dlt schema inference runs.

    dlt will infer types from the first batch and enforce them for subsequent
    batches. We standardize a few fields here to prevent type conflicts.
    """
    # Socrata returns timestamps as strings — parse to datetime so dlt's incremental
    # cursor can compare datetime > datetime (required when lag= is set)
    for dt_field in ("created_date", "closed_date", "due_date", "resolution_action_updated_date"):
        if row.get(dt_field) is not None:
            try:
                row[dt_field] = datetime.fromisoformat(row[dt_field]).replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                row[dt_field] = None

    # Socrata returns numbers as strings — coerce the ones we know
    for int_field in ("council_district", "police_precinct"):
        if row.get(int_field) is not None:
            try:
                row[int_field] = int(row[int_field])
            except (ValueError, TypeError):
                row[int_field] = None

    for float_field in ("latitude", "longitude"):
        if row.get(float_field) is not None:
            try:
                row[float_field] = float(row[float_field])
            except (ValueError, TypeError):
                row[float_field] = None

    # Add pipeline metadata
    row["_pipeline_loaded_at"] = datetime.now(timezone.utc).isoformat()

    return row
