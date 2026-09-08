{{
    config(
        materialized        = 'incremental',
        unique_key          = 'unique_key',
        incremental_strategy = 'delete+insert',
        tags                = ['gold', 'fact']
    )
}}

/*
  fact_311_requests
  ══════════════════
  Central fact table in the Gold star schema.

  One row per NYC 311 service request. References dimension tables via
  surrogate keys. This is the primary table queried by all mart models
  and the Evidence.dev dashboard.

  Grain: 1 row = 1 service request
  Refresh: incremental — reprocesses last {{ var('incremental_lookback_days') }} days

  Surrogate key strategy:
    We use dbt_utils.generate_surrogate_key() to produce stable, consistent
    surrogate keys that survive upstream key changes.
*/

WITH
base AS (
    SELECT * FROM {{ ref('stg_bronze__service_requests') }}
    {% if is_incremental() %}
    -- Incremental: reprocess recent records + any corrections in the lookback window
    WHERE created_date >= CURRENT_TIMESTAMP - INTERVAL '{{ var("incremental_lookback_days") }} days'
    {% endif %}
),

with_keys AS (
    SELECT
        -- ── Fact surrogate key ────────────────────────────────────────────
        {{ dbt_utils.generate_surrogate_key(['unique_key']) }} AS fact_key,

        -- ── Dimension foreign keys ────────────────────────────────────────
        {{ dbt_utils.generate_surrogate_key(['complaint_type']) }}
            AS complaint_type_key,
        {{ dbt_utils.generate_surrogate_key(['agency']) }}
            AS agency_key,
        {{ dbt_utils.generate_surrogate_key(['borough', 'community_board']) }}
            AS location_key,

        -- ── Natural key (kept for traceability) ──────────────────────────
        unique_key,

        -- ── Timestamps ───────────────────────────────────────────────────
        created_date,
        closed_date,
        due_date,

        -- ── Temporal degenerates ─────────────────────────────────────────
        created_year,
        created_month,
        created_day,
        created_hour,
        created_dow,
        is_weekend,
        is_after_hours,

        -- ── Measures ─────────────────────────────────────────────────────
        response_hours,
        CASE
            WHEN response_hours IS NULL THEN NULL
            WHEN response_hours <= 24   THEN 'SAME_DAY'
            WHEN response_hours <= 72   THEN 'WITHIN_3_DAYS'
            WHEN response_hours <= 168  THEN 'WITHIN_1_WEEK'
            WHEN response_hours <= 720  THEN 'WITHIN_1_MONTH'
            ELSE                             'OVER_1_MONTH'
        END AS response_time_bucket,

        -- ── Status ───────────────────────────────────────────────────────
        status,
        is_open_ticket,

        -- ── Channel ──────────────────────────────────────────────────────
        open_data_channel_type,

        -- ── Location degenerates ─────────────────────────────────────────
        borough,
        community_board,
        council_district,
        police_precinct,
        incident_zip,
        latitude,
        longitude,
        has_valid_geo,

        -- ── Quality flags ─────────────────────────────────────────────────
        has_unknown_borough,
        has_null_complaint_type,
        has_negative_response_time,

        -- ── Metadata ─────────────────────────────────────────────────────
        _pipeline_loaded_at,
        _silver_cleaned_at,
        CURRENT_TIMESTAMP AS _gold_refreshed_at

    FROM base
)

SELECT * FROM with_keys
