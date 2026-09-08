{{
    config(
        materialized        = 'incremental',
        unique_key          = 'surrogate_key',
        incremental_strategy = 'delete+insert',
        tags                = ['gold', 'mart']
    )
}}

/*
  mart_agency_performance
  ════════════════════════
  Monthly agency performance scorecard.

  Grain: 1 row = 1 (year, month, agency) combination
  Used by: Evidence.dev agency-performance.md dashboard page

  This mart answers:
    - Which agencies close tickets fastest?
    - Which agencies have the highest open/overdue ticket rates?
    - How has agency performance changed over time?
*/

WITH
monthly_agency AS (
    SELECT
        f.created_year,
        f.created_month,
        f.agency_key,
        da.agency,
        da.agency_name,
        da.agency_category,

        COUNT(*)                                                        AS total_requests,
        SUM(CASE WHEN f.is_open_ticket      THEN 1 ELSE 0 END)        AS open_requests,
        SUM(CASE WHEN NOT f.is_open_ticket  THEN 1 ELSE 0 END)        AS closed_requests,

        -- Overdue: open tickets where due_date has passed
        SUM(
            CASE WHEN f.is_open_ticket
                  AND f.due_date IS NOT NULL
                  AND f.due_date < CURRENT_TIMESTAMP
            THEN 1 ELSE 0 END
        )                                                               AS overdue_requests,

        -- Response time stats (closed only)
        ROUND(AVG(CASE WHEN NOT f.is_open_ticket THEN f.response_hours END), 1)
                                                                        AS avg_response_hours,
        ROUND(MEDIAN(CASE WHEN NOT f.is_open_ticket THEN f.response_hours END), 1)
                                                                        AS median_response_hours,
        ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP
            (ORDER BY CASE WHEN NOT f.is_open_ticket THEN f.response_hours END), 1)
                                                                        AS p95_response_hours

    FROM {{ ref('fact_311_requests') }} f
    JOIN {{ ref('dim_agency') }} da ON f.agency_key = da.agency_key
    WHERE da.agency IS NOT NULL
    {% if is_incremental() %}
    AND f.created_date >= CURRENT_TIMESTAMP - INTERVAL '{{ var("incremental_lookback_days") }} days'
    {% endif %}
    GROUP BY
        f.created_year, f.created_month, f.agency_key,
        da.agency, da.agency_name, da.agency_category
),

with_rates AS (
    SELECT
        *,
        ROUND(100.0 * closed_requests   / NULLIF(total_requests,   0), 1) AS closure_rate_pct,
        ROUND(100.0 * overdue_requests  / NULLIF(open_requests,    0), 1) AS overdue_rate_pct,

        -- Month-over-month volume change
        ROUND(
            100.0 * (total_requests - LAG(total_requests) OVER w_mom)
            / NULLIF(LAG(total_requests) OVER w_mom, 0),
            1
        ) AS mom_volume_change_pct,

        -- Performance tier (same logic as dim_agency but point-in-time)
        CASE
            WHEN ROUND(100.0 * closed_requests / NULLIF(total_requests, 0), 1) >= 95
            THEN 'EXCELLENT'
            WHEN ROUND(100.0 * closed_requests / NULLIF(total_requests, 0), 1) >= 85
            THEN 'GOOD'
            WHEN ROUND(100.0 * closed_requests / NULLIF(total_requests, 0), 1) >= 70
            THEN 'FAIR'
            ELSE 'NEEDS_IMPROVEMENT'
        END AS performance_tier,

        CURRENT_TIMESTAMP AS _refreshed_at

    FROM monthly_agency
    WINDOW
        w_mom AS (PARTITION BY agency ORDER BY created_year, created_month)
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['created_year', 'created_month', 'agency']) }}
        AS surrogate_key,
    *
FROM with_rates
