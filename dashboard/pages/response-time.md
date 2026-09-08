---
title: Response Time Analysis
---

# ⏱️ Response Time Deep Dive

How long does it take NYC to respond to service requests? This page
analyzes response times across boroughs, complaint categories, and time.

**Note:** Response time is measured from `created_date` to `closed_date`.
Open tickets are excluded from all response time calculations.

---

## Overall Response Time Statistics

```sql overall_rt
SELECT
    ROUND(AVG(response_hours), 1)                         AS mean_hours,
    ROUND(MEDIAN(response_hours), 1)                      AS median_hours,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP
        (ORDER BY response_hours), 1)                     AS p25_hours,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP
        (ORDER BY response_hours), 1)                     AS p75_hours,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP
        (ORDER BY response_hours), 1)                     AS p95_hours,
    ROUND(100.0 * SUM(CASE WHEN response_hours <= 24 THEN 1 ELSE 0 END)
        / COUNT(*), 1)                                    AS same_day_pct,
    ROUND(100.0 * SUM(CASE WHEN response_hours > 72 THEN 1 ELSE 0 END)
        / COUNT(*), 1)                                    AS sla_breach_pct
FROM gold.fact_311_requests
WHERE response_hours IS NOT NULL AND response_hours >= 0
```

<BigValue data={overall_rt} value="median_hours" title="Median Response (hours)" />
<BigValue data={overall_rt} value="p95_hours"    title="P95 Response (hours)" />
<BigValue data={overall_rt} value="same_day_pct" title="Closed Same Day" fmt="pct1" />
<BigValue data={overall_rt} value="sla_breach_pct" title="Breached 72h SLA" fmt="pct1" />

---

## Median Response Time by Borough

```sql rt_by_borough
SELECT
    borough,
    ROUND(AVG(median_response_hours), 1)   AS median_response_hours,
    ROUND(AVG(avg_response_hours), 1)      AS avg_response_hours,
    ROUND(AVG(p95_response_hours), 1)      AS p95_response_hours,
    ROUND(AVG(sla_breach_rate_pct), 1)     AS sla_breach_rate_pct
FROM gold.mart_response_time_by_borough
WHERE borough IS NOT NULL
GROUP BY borough
ORDER BY median_response_hours
```

<BarChart
  data={rt_by_borough}
  x="borough"
  y="median_response_hours"
  title="Median Response Time by Borough (hours)"
  yFmt="num1"
/>

---

## SLA Breach Rate by Borough × Category

```sql sla_by_borough_cat
SELECT
    borough,
    complaint_category,
    ROUND(AVG(sla_breach_rate_pct), 1) AS sla_breach_rate_pct,
    SUM(total_closed_requests)         AS total_requests
FROM gold.mart_response_time_by_borough
WHERE borough IS NOT NULL
GROUP BY borough, complaint_category
ORDER BY sla_breach_rate_pct DESC
```

<DataTable
  data={sla_by_borough_cat}
  rows=20
  search=true
>
  <Column id="borough" />
  <Column id="complaint_category" />
  <Column id="sla_breach_rate_pct" fmt="pct1"
    contentType="colorscale"
    scaleColor="red"
    min=0 max=100 />
  <Column id="total_requests" fmt="num0" />
</DataTable>

---

## Response Time Trend (Monthly)

```sql rt_monthly_trend
SELECT
    created_year || '-' || LPAD(CAST(created_month AS VARCHAR), 2, '0') AS month,
    ROUND(AVG(median_response_hours), 1) AS median_response_hours,
    ROUND(AVG(avg_response_hours), 1)    AS avg_response_hours,
    ROUND(AVG(sla_breach_rate_pct), 1)   AS sla_breach_rate_pct
FROM gold.mart_response_time_by_borough
GROUP BY created_year, created_month
ORDER BY created_year, created_month
```

<LineChart
  data={rt_monthly_trend}
  x="month"
  y={["median_response_hours", "avg_response_hours"]}
  title="Citywide Response Time Trend"
  yFmt="num1"
/>

---

## Slowest Complaint Types

```sql slowest_types
SELECT
    complaint_category,
    ROUND(AVG(avg_response_hours), 1)    AS avg_response_hours,
    ROUND(AVG(p95_response_hours), 1)    AS p95_response_hours,
    ROUND(AVG(sla_breach_rate_pct), 1)   AS sla_breach_rate_pct,
    SUM(total_closed_requests)           AS total_requests
FROM gold.mart_response_time_by_borough
GROUP BY complaint_category
ORDER BY avg_response_hours DESC
LIMIT 15
```

<DataTable data={slowest_types} rows=15 />
