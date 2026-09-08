{{
    config(
        materialized = 'table',
        tags         = ['gold', 'mart']
    )
}}

/*
  mart_seasonal_patterns
  ═══════════════════════
  Seasonality and time-of-day patterns for 311 requests.

  Grain: 1 row = 1 (complaint_category, hour_of_day, day_of_week, month) combination
  Used by: Evidence.dev complaint-trends.md and index.md

  Reveals:
    - Which complaint types peak at specific hours (noise peaks midnight–3am)
    - Which days of the week drive volume
    - Seasonal patterns (heating complaints spike in winter)
*/

WITH
time_patterns AS (
    SELECT
        d.complaint_category,
        f.created_hour,
        f.created_dow,
        f.created_month,

        -- Day name
        CASE f.created_dow
            WHEN 0 THEN 'Monday'
            WHEN 1 THEN 'Tuesday'
            WHEN 2 THEN 'Wednesday'
            WHEN 3 THEN 'Thursday'
            WHEN 4 THEN 'Friday'
            WHEN 5 THEN 'Saturday'
            WHEN 6 THEN 'Sunday'
        END AS day_name,

        -- Month name
        CASE f.created_month
            WHEN 1  THEN 'January'
            WHEN 2  THEN 'February'
            WHEN 3  THEN 'March'
            WHEN 4  THEN 'April'
            WHEN 5  THEN 'May'
            WHEN 6  THEN 'June'
            WHEN 7  THEN 'July'
            WHEN 8  THEN 'August'
            WHEN 9  THEN 'September'
            WHEN 10 THEN 'October'
            WHEN 11 THEN 'November'
            WHEN 12 THEN 'December'
        END AS month_name,

        -- Season
        CASE
            WHEN f.created_month IN (12, 1, 2)  THEN 'Winter'
            WHEN f.created_month IN (3,  4, 5)  THEN 'Spring'
            WHEN f.created_month IN (6,  7, 8)  THEN 'Summer'
            WHEN f.created_month IN (9, 10, 11) THEN 'Fall'
        END AS season,

        COUNT(*) AS total_requests

    FROM {{ ref('fact_311_requests') }} f
    JOIN {{ ref('dim_complaint_type') }} d
      ON f.complaint_type_key = d.complaint_type_key
    GROUP BY
        d.complaint_category,
        f.created_hour,
        f.created_dow,
        f.created_month
),

with_share AS (
    SELECT
        *,
        -- Share of this time slot within the category (% of all requests in category)
        ROUND(
            100.0 * total_requests / SUM(total_requests) OVER (PARTITION BY complaint_category),
            3
        ) AS pct_of_category,

        -- Relative index vs. hourly average (> 100 = busier than average)
        ROUND(
            100.0 * total_requests / NULLIF(
                AVG(total_requests) OVER (PARTITION BY complaint_category, created_dow),
                0
            ),
            1
        ) AS hourly_index,

        CURRENT_TIMESTAMP AS _refreshed_at

    FROM time_patterns
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'complaint_category', 'created_hour', 'created_dow', 'created_month'
    ]) }} AS surrogate_key,
    *
FROM with_share
ORDER BY complaint_category, created_dow, created_hour
