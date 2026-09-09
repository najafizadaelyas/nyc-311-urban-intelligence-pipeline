{{
    config(
        materialized = 'table',
        tags         = ['gold', 'dimension']
    )
}}

/*
  dim_complaint_type
  ═══════════════════
  Dimension table for complaint types.

  Grain: 1 row = 1 unique complaint_type value
  Populated from distinct complaint_type values in the Silver layer,
  enriched with category groupings and display names.

  The complaint_category column groups 200+ raw types into ~15 meaningful
  super-categories for high-level dashboard filtering.
*/

WITH
distinct_types AS (
    SELECT DISTINCT
        complaint_type,
        -- Count records per type to include frequency in the dimension
        COUNT(*) OVER (PARTITION BY complaint_type) AS total_requests
    FROM {{ ref('stg_bronze__service_requests') }}
    WHERE complaint_type IS NOT NULL
),

enriched AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['complaint_type']) }} AS complaint_type_key,
        complaint_type,

        -- Human-readable display name (lowercase — DuckDB 1.5.5 lacks INITCAP)
        LOWER(complaint_type) AS complaint_type_display,

        -- ── Complaint super-categories ────────────────────────────────────
        CASE
            WHEN complaint_type ILIKE '%NOISE%'                             THEN 'Noise'
            WHEN complaint_type ILIKE '%HEAT%' OR complaint_type ILIKE '%HOT WATER%'
                                                                             THEN 'Housing - Heat & Water'
            WHEN complaint_type ILIKE '%RODENT%' OR complaint_type ILIKE '%PEST%'
                                                                             THEN 'Sanitation - Pests'
            WHEN complaint_type ILIKE '%ILLEGAL PARKING%'
              OR complaint_type ILIKE '%BLOCKED DRIVEWAY%'                  THEN 'Traffic & Parking'
            WHEN complaint_type ILIKE '%STREET LIGHT%'
              OR complaint_type ILIKE '%TRAFFIC%'
              OR complaint_type ILIKE '%POTHOLE%'
              OR complaint_type ILIKE '%STREET CONDITION%'                  THEN 'Infrastructure - Streets'
            WHEN complaint_type ILIKE '%WATER MAIN%'
              OR complaint_type ILIKE '%SEWER%'
              OR complaint_type ILIKE '%WATER QUALITY%'                     THEN 'Infrastructure - Water/Sewer'
            WHEN complaint_type ILIKE '%TREE%'
              OR complaint_type ILIKE '%PARK%'                              THEN 'Parks & Trees'
            WHEN complaint_type ILIKE '%GRAFFITI%'
              OR complaint_type ILIKE '%DIRTY%'
              OR complaint_type ILIKE '%SANITATION%'                        THEN 'Sanitation - Cleanliness'
            WHEN complaint_type ILIKE '%HOMELESS%'                          THEN 'Social Services'
            WHEN complaint_type ILIKE '%CONSTRUCTION%'
              OR complaint_type ILIKE '%BUILDING%'
              OR complaint_type ILIKE '%PLUMBING%'                          THEN 'Housing - Construction'
            WHEN complaint_type ILIKE '%AIR QUALITY%'
              OR complaint_type ILIKE '%SMOKE%'                             THEN 'Environment - Air Quality'
            WHEN complaint_type ILIKE '%ANIMAL%'
              OR complaint_type ILIKE '%DOG%'                               THEN 'Animals'
            ELSE                                                              'Other'
        END AS complaint_category,

        total_requests,

        -- Flag high-volume types (in top 10%)
        CASE
            WHEN total_requests > QUANTILE_CONT(total_requests, 0.9) OVER ()
            THEN TRUE ELSE FALSE
        END AS is_high_volume_type

    FROM distinct_types
)

SELECT * FROM enriched
ORDER BY total_requests DESC
