{{
    config(
        materialized        = 'incremental',
        unique_key          = 'surrogate_key',
        incremental_strategy = 'delete+insert',
        tags                = ['gold', 'mart']
    )
}}

/*
  mart_response_time_by_borough
  ══════════════════════════════
  Pre-aggregated response time statistics by borough and complaint category.

  Grain: 1 row = 1 (year, month, borough, complaint_category) combination
  Used by: Evidence.dev response-time.md dashboard page

  Key metrics:
    avg_response_hours    — arithmetic mean
    median_response_hours — p50 (robust to outliers)
    p95_response_hours    — p95 (captures tail latency / worst cases)
    sla_breach_rate_pct   — % requests taking > 72 hours (3-day SLA)
*/

WITH
closed_requests AS (
    SELECT
        f.created_year,
        f.created_month,
        f.borough,
        d.complaint_category,
        f.response_hours,
        f.response_time_bucket
    FROM {{ ref('fact_311_requests') }} f
    JOIN {{ ref('dim_complaint_type') }} d
      ON f.complaint_type_key = d.complaint_type_key
    WHERE
        f.response_hours IS NOT NULL
        AND f.response_hours >= 0
        AND f.borough IS NOT NULL
        {% if is_incremental() %}
        AND f.created_date >= CURRENT_TIMESTAMP - INTERVAL '{{ var("incremental_lookback_days") }} days'
        {% endif %}
),

aggregated AS (
    SELECT
        created_year,
        created_month,
        borough,
        complaint_category,

        COUNT(*)                                                        AS total_closed_requests,
        ROUND(AVG(response_hours), 2)                                  AS avg_response_hours,
        ROUND(MEDIAN(response_hours), 2)                               AS median_response_hours,
        ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP
            (ORDER BY response_hours), 2)                              AS p25_response_hours,
        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP
            (ORDER BY response_hours), 2)                              AS p75_response_hours,
        ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP
            (ORDER BY response_hours), 2)                              AS p95_response_hours,
        MIN(response_hours)                                             AS min_response_hours,
        MAX(response_hours)                                             AS max_response_hours,

        -- SLA breach: > 72 hours (3-day threshold)
        ROUND(
            100.0 * SUM(CASE WHEN response_hours > 72 THEN 1 ELSE 0 END)
            / COUNT(*),
            1
        )                                                               AS sla_breach_rate_pct,

        -- Response time distribution buckets
        SUM(CASE WHEN response_time_bucket = 'SAME_DAY'       THEN 1 ELSE 0 END) AS bucket_same_day,
        SUM(CASE WHEN response_time_bucket = 'WITHIN_3_DAYS'  THEN 1 ELSE 0 END) AS bucket_3_days,
        SUM(CASE WHEN response_time_bucket = 'WITHIN_1_WEEK'  THEN 1 ELSE 0 END) AS bucket_1_week,
        SUM(CASE WHEN response_time_bucket = 'WITHIN_1_MONTH' THEN 1 ELSE 0 END) AS bucket_1_month,
        SUM(CASE WHEN response_time_bucket = 'OVER_1_MONTH'   THEN 1 ELSE 0 END) AS bucket_over_1_month,

        CURRENT_TIMESTAMP AS _refreshed_at

    FROM closed_requests
    GROUP BY created_year, created_month, borough, complaint_category
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'created_year', 'created_month', 'borough', 'complaint_category'
    ]) }} AS surrogate_key,
    *
FROM aggregated
