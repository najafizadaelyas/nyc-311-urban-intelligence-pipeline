---
title: Agency Performance
---

# 🏛️ Agency Performance Scorecard

How well are NYC's city agencies responding to 311 service requests?
This page tracks closure rates, response times, and overdue tickets by agency.

---

## Agency Performance Summary

```sql agency_summary
SELECT
    agency,
    agency_name,
    agency_category,
    total_requests,
    closure_rate_pct,
    avg_response_hours,
    median_response_hours,
    p95_response_hours,
    performance_tier
FROM gold.dim_agency
ORDER BY total_requests DESC
```

<DataTable
  data={agency_summary}
  rows=20
  search=true
>
  <Column id="agency"            />
  <Column id="agency_name"       />
  <Column id="agency_category"   />
  <Column id="total_requests"    fmt="num0" />
  <Column id="closure_rate_pct"  fmt="pct1" contentType="colorscale" scaleColor="green" />
  <Column id="avg_response_hours" fmt="num1" />
  <Column id="p95_response_hours" fmt="num1" />
  <Column id="performance_tier"  contentType="badge" colorMap={{
    "EXCELLENT": "green",
    "GOOD": "blue",
    "FAIR": "yellow",
    "NEEDS_IMPROVEMENT": "red"
  }} />
</DataTable>

---

## Closure Rate by Agency (Top 20)

```sql closure_rates
SELECT
    agency,
    agency_category,
    closure_rate_pct
FROM gold.dim_agency
ORDER BY closure_rate_pct DESC
LIMIT 20
```

<BarChart
  data={closure_rates}
  x="agency"
  y="closure_rate_pct"
  series="agency_category"
  title="Agency Closure Rate (%)"
  yFmt="pct0"
  swapXY=true
/>

---

## Agency Response Time (Median Hours)

```sql response_by_agency
SELECT
    agency,
    agency_category,
    median_response_hours,
    p95_response_hours
FROM gold.dim_agency
WHERE median_response_hours IS NOT NULL
ORDER BY median_response_hours ASC
LIMIT 20
```

<BarChart
  data={response_by_agency}
  x="agency"
  y="median_response_hours"
  title="Median Response Hours (Top 20 Fastest)"
  yFmt="num1"
  swapXY=true
/>

---

## Monthly Performance Trend (Select Agency)

```sql monthly_performance
SELECT
    created_year || '-' || LPAD(CAST(created_month AS VARCHAR), 2, '0') AS month,
    agency,
    total_requests,
    closure_rate_pct,
    avg_response_hours,
    overdue_requests,
    performance_tier
FROM gold.mart_agency_performance
ORDER BY agency, created_year, created_month
```

<LineChart
  data={monthly_performance}
  x="month"
  y="closure_rate_pct"
  series="agency"
  title="Monthly Closure Rate by Agency"
  yFmt="pct0"
/>

---

## Performance by Agency Category

```sql category_perf
SELECT
    agency_category,
    ROUND(AVG(closure_rate_pct), 1)        AS avg_closure_rate,
    ROUND(AVG(avg_response_hours), 1)      AS avg_response_hours,
    ROUND(AVG(median_response_hours), 1)   AS median_response_hours,
    SUM(total_requests)                    AS total_requests,
    COUNT(*)                               AS agency_count
FROM gold.dim_agency
GROUP BY agency_category
ORDER BY avg_closure_rate DESC
```

<DataTable data={category_perf} />
