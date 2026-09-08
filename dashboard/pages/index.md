---
title: NYC 311 Urban Intelligence — Executive Overview
---

# 🏙️ NYC 311 Urban Intelligence Dashboard

*{last_refreshed.pipeline_run_date} — data updated daily from NYC Open Data*

NYC receives millions of 311 service requests every year. This dashboard
turns that raw data into clear urban intelligence — response performance,
complaint trends, agency accountability, and neighborhood patterns.

---

## At a Glance

```sql total_kpis
SELECT
    FORMAT('{:,.0f}', COUNT(*))                            AS total_requests,
    FORMAT('{:,.0f}', SUM(CASE WHEN is_open_ticket THEN 1 ELSE 0 END))
                                                           AS open_tickets,
    FORMAT('{:.1f}h', AVG(response_hours))                AS avg_response_hours,
    FORMAT('{:.1f}%',
        100.0 * SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END) / COUNT(*)
    )                                                      AS closure_rate
FROM gold.fact_311_requests
```

<BigValue
  data={total_kpis}
  value="total_requests"
  title="Total Requests"
  fmt="num0"
/>
<BigValue
  data={total_kpis}
  value="open_tickets"
  title="Currently Open"
/>
<BigValue
  data={total_kpis}
  value="avg_response_hours"
  title="Avg Response Time"
/>
<BigValue
  data={total_kpis}
  value="closure_rate"
  title="Closure Rate"
/>

---

## Monthly Request Volume

```sql monthly_volume
SELECT
    created_year || '-' || LPAD(CAST(created_month AS VARCHAR), 2, '0') AS month,
    SUM(total_requests) AS total_requests
FROM gold.mart_complaint_trends
GROUP BY created_year, created_month
ORDER BY created_year, created_month
```

<LineChart
  data={monthly_volume}
  x="month"
  y="total_requests"
  title="Monthly 311 Request Volume"
  yFmt="num0"
/>

---

## Requests by Borough

```sql borough_totals
SELECT
    borough,
    COUNT(*)               AS total_requests,
    ROUND(AVG(response_hours), 1) AS avg_response_hours,
    ROUND(100.0 * SUM(CASE WHEN is_open_ticket THEN 1 ELSE 0 END) / COUNT(*), 1)
                           AS open_rate_pct
FROM gold.fact_311_requests
WHERE borough IS NOT NULL
GROUP BY borough
ORDER BY total_requests DESC
```

<BarChart
  data={borough_totals}
  x="borough"
  y="total_requests"
  title="Total Requests by Borough"
  yFmt="num0"
  swapXY=true
/>

---

## Top 10 Complaint Categories

```sql top_categories
SELECT
    d.complaint_category,
    SUM(t.total_requests) AS total_requests,
    ROUND(AVG(t.total_requests), 0) AS avg_monthly_volume
FROM gold.mart_complaint_trends t
JOIN gold.dim_complaint_type d ON t.complaint_category = d.complaint_category
GROUP BY d.complaint_category
ORDER BY total_requests DESC
LIMIT 10
```

<BarChart
  data={top_categories}
  x="complaint_category"
  y="total_requests"
  title="Top 10 Complaint Categories"
  yFmt="num0"
  swapXY=true
/>

---

## Response Time Distribution

```sql response_buckets
SELECT
    response_time_bucket,
    COUNT(*) AS requests,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM gold.fact_311_requests
WHERE response_time_bucket IS NOT NULL
GROUP BY response_time_bucket
ORDER BY CASE response_time_bucket
    WHEN 'SAME_DAY'       THEN 1
    WHEN 'WITHIN_3_DAYS'  THEN 2
    WHEN 'WITHIN_1_WEEK'  THEN 3
    WHEN 'WITHIN_1_MONTH' THEN 4
    WHEN 'OVER_1_MONTH'   THEN 5
END
```

<BarChart
  data={response_buckets}
  x="response_time_bucket"
  y="pct"
  title="Response Time Distribution (%)"
  yFmt="pct0"
/>

---

## Pipeline Info

```sql pipeline_metadata
SELECT
    MIN(created_date)::DATE             AS data_from,
    MAX(created_date)::DATE             AS data_through,
    MAX(_gold_refreshed_at)::TIMESTAMP  AS last_pipeline_run,
    COUNT(*)                            AS total_fact_rows
FROM gold.fact_311_requests
```

<DataTable data={pipeline_metadata} />
