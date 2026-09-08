# Data Dictionary — NYC 311 Urban Intelligence Pipeline

Complete field-level documentation with source, lineage, and data quality notes.

---

## Bronze Layer — `bronze.service_requests`

Raw data from the NYC Open Data Socrata SODA API. Loaded by dlt. Append-only.

| Field | Type | Source | Notes |
|---|---|---|---|
| `unique_key` | string | Socrata API | NYC Open Data unique row identifier. Not always globally unique across time — deduplication in Silver. |
| `created_date` | string/timestamp | Socrata API | When the 311 request was submitted. Arrives as ISO 8601 string. |
| `closed_date` | string/timestamp | Socrata API | When resolved. NULL for ~8% of rows (open tickets). |
| `due_date` | string/timestamp | Socrata API | SLA deadline set by the responding agency. Sometimes null. |
| `agency` | string | Socrata API | Short agency code (e.g., "NYPD", "DOT", "DEP"). |
| `agency_name` | string | Socrata API | Full agency name. |
| `complaint_type` | string | Socrata API | Top-level complaint category. 200+ unique values, inconsistent casing. |
| `descriptor` | string | Socrata API | Specific detail within complaint_type. |
| `location_type` | string | Socrata API | Type of location (Street, Residential Building, etc.). |
| `incident_zip` | string | Socrata API | 5-digit ZIP code. |
| `incident_address` | string | Socrata API | Street address of the complaint. |
| `street_name` | string | Socrata API | Street name without number. |
| `city` | string | Socrata API | City name (usually a borough name or neighborhood). |
| `borough` | string | Socrata API | "Manhattan", "Bronx", "Brooklyn", "Queens", "Staten Island", or "Unspecified". |
| `latitude` | float | Socrata API | WGS84 decimal degrees. Some values are outside NYC (bad geo). |
| `longitude` | float | Socrata API | WGS84 decimal degrees. |
| `community_board` | string | Socrata API | Format: "NN BOROUGH-NAME" (e.g., "07 MANHATTAN"). |
| `council_district` | integer | Socrata API | NYC City Council district number. |
| `police_precinct` | integer | Socrata API | NYPD precinct number. |
| `status` | string | Socrata API | Open, Closed, Assigned, Pending, etc. |
| `resolution_description` | string | Socrata API | Free-text resolution note from the agency. |
| `open_data_channel_type` | string | Socrata API | How submitted: PHONE, ONLINE, MOBILE, OTHER. |
| `_pipeline_loaded_at` | timestamp | dlt/pipeline | UTC timestamp when this row was loaded by dlt. |

---

## Silver Layer — `silver.service_requests`

Cleaned, deduplicated, and enriched by the Polars pipeline. One row per unique request.

All Bronze fields are preserved, plus the following derived columns:

| Field | Type | Derivation | Notes |
|---|---|---|---|
| `response_hours` | float | `(closed_date - created_date) / 3600` | NULL for open tickets. Negative values (data error) set to NULL. |
| `created_year` | integer | `YEAR(created_date)` | Year of request creation. |
| `created_month` | integer | `MONTH(created_date)` | Month 1–12. |
| `created_day` | integer | `DAY(created_date)` | Day of month 1–31. |
| `created_hour` | integer | `HOUR(created_date)` | Hour 0–23. |
| `created_dow` | integer | `WEEKDAY(created_date)` | Day of week: 0=Monday, 6=Sunday. |
| `is_weekend` | boolean | `created_dow >= 5` | True if Saturday or Sunday. |
| `is_after_hours` | boolean | `created_hour < 8 OR created_hour >= 18` | True if submitted outside 8am–6pm. |
| `has_valid_geo` | boolean | Lat/lon within NYC bbox | True if coordinates fall within 40.4–40.95 lat, -74.3– -73.7 lon. |
| `is_open_ticket` | boolean | `closed_date IS NULL` | True if not yet resolved. |
| `has_unknown_borough` | boolean | `borough IS NULL` | True if original value was "Unspecified" or invalid. |
| `has_null_complaint_type` | boolean | `complaint_type IS NULL` | Quality flag. |
| `has_negative_response_time` | boolean | `response_hours < 0` (before nulling) | Quality flag for temporal data issues. |
| `_silver_cleaned_at` | timestamp | pipeline | UTC timestamp of Silver processing. |

### Silver Normalization Applied

- **borough**: "Unspecified" and unknown values → NULL. Valid values uppercased.
- **complaint_type**: Alias variants collapsed (e.g., "Heating" → "HEAT/HOT WATER"). All uppercased.
- **status**: Mapped to controlled vocabulary (OPEN, CLOSED, ASSIGNED, PENDING, IN_PROGRESS, etc.).
- **lat/lon**: Values outside NYC bounding box set to NULL. `has_valid_geo` records the outcome.

---

## Gold Layer — Star Schema

### `gold.fact_311_requests`

Central fact table. One row per service request.

| Field | Type | Source | Notes |
|---|---|---|---|
| `fact_key` | string | dbt_utils.generate_surrogate_key | MD5 hash of unique_key. Stable across reloads. |
| `unique_key` | string | Silver | Natural key (traceability to source). |
| `complaint_type_key` | string | dbt | FK to dim_complaint_type. |
| `agency_key` | string | dbt | FK to dim_agency. |
| `location_key` | string | dbt | FK to dim_location. |
| `response_hours` | float | Silver | Hours to resolution. NULL for open tickets. |
| `response_time_bucket` | string | dbt | SAME_DAY, WITHIN_3_DAYS, WITHIN_1_WEEK, WITHIN_1_MONTH, OVER_1_MONTH. |
| `created_year/month/day/hour/dow` | integer | Silver | Date parts for fast grouping. |
| `is_weekend`, `is_after_hours` | boolean | Silver | Temporal flags. |
| `is_open_ticket` | boolean | Silver | True for unresolved requests. |
| `_gold_refreshed_at` | timestamp | dbt | When this row was last refreshed by dbt. |

### `gold.dim_complaint_type`

| Field | Notes |
|---|---|
| `complaint_type_key` | Surrogate key (MD5 of complaint_type). |
| `complaint_type` | Canonical complaint type name. |
| `complaint_type_display` | Title-case display version. |
| `complaint_category` | Super-category grouping (Noise, Housing, Traffic, etc.). |
| `total_requests` | Pre-computed volume count. |
| `is_high_volume_type` | True if in top 10% by volume. |

### `gold.dim_agency`

| Field | Notes |
|---|---|
| `agency_key` | Surrogate key. |
| `agency` | Short agency code. |
| `agency_name` | Full agency name. |
| `agency_category` | Public Safety / Transportation / Housing / etc. |
| `closure_rate_pct` | % of tickets closed overall. |
| `avg_response_hours` | Mean response time for closed tickets. |
| `p95_response_hours` | 95th percentile response time. |
| `performance_tier` | EXCELLENT / GOOD / FAIR / NEEDS_IMPROVEMENT. |

### `gold.dim_location`

| Field | Notes |
|---|---|
| `location_key` | Surrogate key (hash of borough + community_board). |
| `borough` | NYC borough. |
| `community_board` | Community board identifier. |
| `total_requests` | Complaint volume for this area. |
| `open_ticket_rate_pct` | % of tickets still open. |
| `top_complaint_type` | Most common complaint for this area. |
| `centroid_lat/lon` | Median lat/lon of valid geo records. |

### Mart Tables

| Mart | Grain | Key Metrics |
|---|---|---|
| `mart_response_time_by_borough` | year × month × borough × category | avg/median/p95 response, SLA breach rate |
| `mart_complaint_trends` | year × month × borough × category | volume, MoM/YoY change, open rate, channel mix |
| `mart_agency_performance` | year × month × agency | closure rate, overdue count, response time, performance tier |
| `mart_seasonal_patterns` | category × hour × dow × month | volume, % of category, hourly index |
