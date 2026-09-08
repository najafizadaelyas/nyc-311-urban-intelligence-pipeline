{{
    config(
        materialized = 'view',
        tags         = ['bronze']
    )
}}

/*
  stg_bronze__service_requests
  ════════════════════════════
  Lightweight staging view over the Silver source.

  Purpose:
    - Provides a stable model name for downstream Silver/Gold models to depend on
    - Applies any final light-touch casting before Gold models consume it
    - Does NOT perform any business logic — that lives in Silver and Gold

  Source: silver_source.silver.service_requests
  (Silver DuckDB attached read-only to the Gold DuckDB connection)
*/

SELECT
    -- ── Identifiers ──────────────────────────────────────────────────────
    unique_key,

    -- ── Timestamps ───────────────────────────────────────────────────────
    created_date,
    closed_date,
    due_date,
    resolution_action_updated_date,

    -- ── Temporal features (pre-computed by Silver cleaner) ───────────────
    response_hours,
    created_year,
    created_month,
    created_day,
    created_hour,
    created_dow,
    is_weekend,
    is_after_hours,

    -- ── Categorical dimensions ───────────────────────────────────────────
    agency,
    agency_name,
    complaint_type,
    descriptor,
    borough,
    status,
    location_type,
    facility_type,
    open_data_channel_type,

    -- ── Location ─────────────────────────────────────────────────────────
    incident_zip,
    incident_address,
    street_name,
    city,
    community_board,
    council_district,
    police_precinct,
    park_facility_name,
    park_borough,
    latitude,
    longitude,
    has_valid_geo,

    -- ── Resolution ───────────────────────────────────────────────────────
    resolution_description,

    -- ── Quality flags ────────────────────────────────────────────────────
    is_open_ticket,
    has_unknown_borough,
    has_null_complaint_type,
    has_negative_response_time,

    -- ── Metadata ─────────────────────────────────────────────────────────
    _pipeline_loaded_at,
    _silver_cleaned_at

FROM {{ source('silver_source', 'service_requests') }}
