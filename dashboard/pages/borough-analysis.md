---
title: Borough Analysis
---

# 🗺️ Borough Analysis

Complaint patterns, response times, and open ticket rates broken down
by NYC borough. Use the selector below to drill into a specific borough.

---

## Borough Scorecard

```sql borough_scorecard
SELECT
    borough,
    COUNT(*)                                               AS total_requests,
    ROUND(AVG(response_hours), 1)                         AS avg_response_hours,
    ROUND(MEDIAN(response_hours), 1)                      AS median_response_hours,
    ROUND(
        100.0 * SUM(CASE WHEN is_open_ticket THEN 1 ELSE 0 END) / COUNT(*), 1
    )                                                     AS open_rate_pct,
    ROUND(
        100.0 * SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END) / COUNT(*), 1
    )                                                     AS closure_rate_pct,
    COUNT(DISTINCT complaint_type)                        AS unique_complaint_types
FROM gold.fact_311_requests
WHERE borough IS NOT NULL
GROUP BY borough
ORDER BY total_requests DESC
```

<DataTable
  data={borough_scorecard}
  rows=10
/>

---

## Monthly Volume by Borough

```sql monthly_by_borough
SELECT
    borough,
    created_year || '-' || LPAD(CAST(created_month AS VARCHAR), 2, '0') AS month,
    SUM(total_requests) AS total_requests
FROM gold.mart_complaint_trends
WHERE borough IS NOT NULL
GROUP BY borough, created_year, created_month
ORDER BY borough, created_year, created_month
```

<LineChart
  data={monthly_by_borough}
  x="month"
  y="total_requests"
  series="borough"
  title="Monthly Request Volume by Borough"
  yFmt="num0"
/>

---

## Complaint Category Mix by Borough

```sql category_by_borough
SELECT
    borough,
    complaint_category,
    SUM(total_requests) AS total_requests
FROM gold.mart_complaint_trends
WHERE borough IS NOT NULL
GROUP BY borough, complaint_category
```

<BarChart
  data={category_by_borough}
  x="borough"
  y="total_requests"
  series="complaint_category"
  type="stacked"
  title="Complaint Category Mix by Borough"
  yFmt="num0"
/>

---

## Response Time Heatmap (Borough × Category)

```sql response_heatmap
SELECT
    borough,
    complaint_category,
    ROUND(AVG(avg_response_hours), 1) AS avg_response_hours
FROM gold.mart_response_time_by_borough
WHERE borough IS NOT NULL
GROUP BY borough, complaint_category
ORDER BY borough, avg_response_hours DESC
```

<DataTable
  data={response_heatmap}
  rows=25
  search=true
/>

---

## Top Community Boards by Volume

```sql top_cbs
SELECT
    borough,
    community_board,
    total_requests,
    avg_response_hours,
    open_ticket_rate_pct,
    top_complaint_type
FROM gold.dim_location
WHERE borough IS NOT NULL
ORDER BY total_requests DESC
LIMIT 20
```

<DataTable
  data={top_cbs}
  rows=20
  search=true
/>
