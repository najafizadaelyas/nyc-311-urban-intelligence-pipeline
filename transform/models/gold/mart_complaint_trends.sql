{{
    config(
        materialized        = 'incremental',
        unique_key          = 'surrogate_key',
        incremental_strategy = 'delete+insert',
        tags                = ['gold', 'mart']
    )
}}

/*
  mart_complaint_trends
  ══════════════════════
  Monthly complaint volume trends by category and borough.

  Grain: 1 row = 1 (year, month, borough, complaint_category) combination
  Used by: Evidence.dev complaint-trends.md dashboard page

  Key metrics:
    total_requests        — volume in this period
    mom_change_pct        — month-over-month % change
    yoy_change_pct        — year-over-year % change (same month, prior year)
    open_rate_pct         — % of requests still open
*/

WITH
monthly_volume AS (
    SELECT
        f.created_year,
        f.created_month,
        f.borough,
        d.complaint_category,
        COUNT(*)                                                        AS total_requests,
        SUM(CASE WHEN f.is_open_ticket     THEN 1 ELSE 0 END)         AS open_requests,
        SUM(CASE WHEN NOT f.is_open_ticket THEN 1 ELSE 0 END)         AS closed_requests,
        SUM(CASE WHEN f.is_weekend         THEN 1 ELSE 0 END)         AS weekend_requests,
        SUM(CASE WHEN f.is_after_hours     THEN 1 ELSE 0 END)         AS after_hours_requests,
        -- Channel breakdown
        SUM(CASE WHEN f.open_data_channel_type = 'PHONE'  THEN 1 ELSE 0 END) AS phone_requests,
        SUM(CASE WHEN f.open_data_channel_type = 'ONLINE' THEN 1 ELSE 0 END) AS online_requests,
        SUM(CASE WHEN f.open_data_channel_type = 'MOBILE' THEN 1 ELSE 0 END) AS mobile_requests
    FROM {{ ref('fact_311_requests') }} f
    JOIN {{ ref('dim_complaint_type') }} d
      ON f.complaint_type_key = d.complaint_type_key
    WHERE f.borough IS NOT NULL
    {% if is_incremental() %}
    AND f.created_date >= CURRENT_TIMESTAMP - INTERVAL '{{ var("incremental_lookback_days") }} days'
    {% endif %}
    GROUP BY f.created_year, f.created_month, f.borough, d.complaint_category
),

with_window_stats AS (
    SELECT
        *,
        -- Month-over-month: compare to prior month (LAG)
        ROUND(
            100.0 * (total_requests - LAG(total_requests) OVER w_mom)
            / NULLIF(LAG(total_requests) OVER w_mom, 0),
            1
        ) AS mom_change_pct,

        -- Year-over-year: compare to same month 12 rows ago
        ROUND(
            100.0 * (total_requests - LAG(total_requests, 12) OVER w_yoy)
            / NULLIF(LAG(total_requests, 12) OVER w_yoy, 0),
            1
        ) AS yoy_change_pct,

        -- Open rate
        ROUND(100.0 * open_requests / NULLIF(total_requests, 0), 1) AS open_rate_pct,

        -- Rolling 3-month average
        ROUND(AVG(total_requests) OVER w_rolling_3m, 1) AS rolling_3m_avg,

        CURRENT_TIMESTAMP AS _refreshed_at

    FROM monthly_volume
    WINDOW
        w_mom        AS (PARTITION BY borough, complaint_category
                         ORDER BY created_year, created_month
                         ROWS BETWEEN 1 PRECEDING AND CURRENT ROW),
        w_yoy        AS (PARTITION BY borough, complaint_category
                         ORDER BY created_year, created_month),
        w_rolling_3m AS (PARTITION BY borough, complaint_category
                         ORDER BY created_year, created_month
                         ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'created_year', 'created_month', 'borough', 'complaint_category'
    ]) }} AS surrogate_key,
    *
FROM with_window_stats
