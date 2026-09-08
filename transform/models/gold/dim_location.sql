{{
    config(
        materialized = 'table',
        tags         = ['gold', 'dimension']
    )
}}

/*
  dim_location
  ═════════════
  Geographic dimension table.

  Grain: 1 row = 1 unique (borough, community_board) combination.
  This is the geographic unit at which we analyze complaint patterns.

  Includes complaint density (requests per sq km) as a pre-computed
  measure for the neighborhood heat map dashboard page.
*/

WITH
location_base AS (
    SELECT
        borough,
        community_board,
        -- Community board number extracted from the "NN BOROUGH" format
        TRY_CAST(REGEXP_EXTRACT(community_board, '^\d+') AS INTEGER) AS cb_number,
        COUNT(*)                                                        AS total_requests,
        COUNT(DISTINCT complaint_type)                                  AS unique_complaint_types,
        ROUND(AVG(response_hours), 1)                                  AS avg_response_hours,
        ROUND(
            100.0 * SUM(CASE WHEN is_open_ticket THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*), 0),
            1
        )                                                               AS open_ticket_rate_pct,
        -- Most common complaint type per location
        MODE() WITHIN GROUP (ORDER BY complaint_type)                  AS top_complaint_type,

        -- Approximate centroid (median of valid geo points)
        ROUND(MEDIAN(CASE WHEN has_valid_geo THEN latitude  END), 6)  AS centroid_lat,
        ROUND(MEDIAN(CASE WHEN has_valid_geo THEN longitude END), 6)  AS centroid_lon

    FROM {{ ref('stg_bronze__service_requests') }}
    WHERE borough IS NOT NULL
    GROUP BY borough, community_board
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['borough', 'community_board']) }} AS location_key,
    borough,
    community_board,
    cb_number,
    total_requests,
    unique_complaint_types,
    avg_response_hours,
    open_ticket_rate_pct,
    top_complaint_type,
    centroid_lat,
    centroid_lon,
    CURRENT_TIMESTAMP AS _refreshed_at
FROM location_base
ORDER BY borough, cb_number
