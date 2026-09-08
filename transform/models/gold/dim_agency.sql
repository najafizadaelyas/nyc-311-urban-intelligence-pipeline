{{
    config(
        materialized = 'table',
        tags         = ['gold', 'dimension']
    )
}}

/*
  dim_agency
  ══════════
  Dimension table for NYC city agencies.

  Grain: 1 row = 1 unique agency code
  Enriched with agency category (infrastructure, public safety, etc.)
  and performance context (median response time, closure rate).
*/

WITH
distinct_agencies AS (
    SELECT DISTINCT
        agency,
        agency_name,
        -- Aggregate performance metrics directly into the dimension
        COUNT(*)                                                AS total_requests,
        ROUND(
            100.0 * SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*), 0),
            1
        )                                                       AS closure_rate_pct,
        ROUND(AVG(response_hours), 1)                          AS avg_response_hours,
        ROUND(MEDIAN(response_hours), 1)                       AS median_response_hours,
        ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP
            (ORDER BY response_hours), 1)                      AS p95_response_hours

    FROM {{ ref('stg_bronze__service_requests') }}
    WHERE agency IS NOT NULL
    GROUP BY agency, agency_name
),

enriched AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['agency']) }} AS agency_key,
        agency,
        COALESCE(agency_name, agency)                          AS agency_name,

        -- ── Agency category grouping ──────────────────────────────────────
        CASE agency
            WHEN 'NYPD'   THEN 'Public Safety'
            WHEN 'FDNY'   THEN 'Public Safety'
            WHEN 'DOT'    THEN 'Transportation'
            WHEN 'MTA'    THEN 'Transportation'
            WHEN 'DEP'    THEN 'Environmental'
            WHEN 'DSNY'   THEN 'Sanitation'
            WHEN 'HPD'    THEN 'Housing'
            WHEN 'DOB'    THEN 'Housing'
            WHEN 'DPR'    THEN 'Parks'
            WHEN 'DOHMH'  THEN 'Health'
            WHEN 'HRA'    THEN 'Social Services'
            WHEN 'DHS'    THEN 'Social Services'
            WHEN 'DFTA'   THEN 'Social Services'
            WHEN 'DOE'    THEN 'Education'
            WHEN 'DCA'    THEN 'Consumer Affairs'
            ELSE               'Other'
        END AS agency_category,

        total_requests,
        closure_rate_pct,
        avg_response_hours,
        median_response_hours,
        p95_response_hours,

        -- Performance tier based on closure rate
        CASE
            WHEN closure_rate_pct >= 95  THEN 'EXCELLENT'
            WHEN closure_rate_pct >= 85  THEN 'GOOD'
            WHEN closure_rate_pct >= 70  THEN 'FAIR'
            ELSE                              'NEEDS_IMPROVEMENT'
        END AS performance_tier

    FROM distinct_agencies
)

SELECT * FROM enriched
ORDER BY total_requests DESC
