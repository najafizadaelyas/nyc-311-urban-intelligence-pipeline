---
title: Complaint Trends
---

# 📈 Complaint Trends

How is NYC changing? Which complaint types are rising, which are falling,
and what does 311 data reveal about seasonal and structural urban dynamics?

---

## Year-Over-Year Change by Category

```sql yoy_by_category
SELECT
    complaint_category,
    SUM(CASE WHEN created_year = (SELECT MAX(created_year) FROM gold.mart_complaint_trends)
             THEN total_requests ELSE 0 END) AS current_year_total,
    SUM(CASE WHEN created_year = (SELECT MAX(created_year) FROM gold.mart_complaint_trends) - 1
             THEN total_requests ELSE 0 END) AS prior_year_total,
    ROUND(
        100.0 * (
            SUM(CASE WHEN created_year = (SELECT MAX(created_year) FROM gold.mart_complaint_trends)
                     THEN total_requests ELSE 0 END)
            - SUM(CASE WHEN created_year = (SELECT MAX(created_year) FROM gold.mart_complaint_trends) - 1
                       THEN total_requests ELSE 0 END)
        ) / NULLIF(
            SUM(CASE WHEN created_year = (SELECT MAX(created_year) FROM gold.mart_complaint_trends) - 1
                     THEN total_requests ELSE 0 END),
            0
        ),
        1
    ) AS yoy_change_pct
FROM gold.mart_complaint_trends
GROUP BY complaint_category
ORDER BY yoy_change_pct DESC
```

<DataTable
  data={yoy_by_category}
  rows=20
>
  <Column id="complaint_category" />
  <Column id="current_year_total" fmt="num0" />
  <Column id="prior_year_total"   fmt="num0" />
  <Column id="yoy_change_pct"     fmt="pct1" contentType="colorscale"
           scaleColor="diverging" neutralPoint=0 />
</DataTable>

---

## Monthly Trend Lines by Category

```sql monthly_trends
SELECT
    created_year || '-' || LPAD(CAST(created_month AS VARCHAR), 2, '0') AS month,
    complaint_category,
    SUM(total_requests) AS total_requests
FROM gold.mart_complaint_trends
GROUP BY created_year, created_month, complaint_category
ORDER BY created_year, created_month
```

<LineChart
  data={monthly_trends}
  x="month"
  y="total_requests"
  series="complaint_category"
  title="Monthly Complaint Volume by Category"
  yFmt="num0"
/>

---

## Hour-of-Day Heatmap (When Does NYC Call 311?)

```sql hourly_patterns
SELECT
    complaint_category,
    created_hour AS hour,
    SUM(total_requests) AS total_requests
FROM gold.mart_seasonal_patterns
GROUP BY complaint_category, created_hour
ORDER BY complaint_category, created_hour
```

<BarChart
  data={hourly_patterns}
  x="hour"
  y="total_requests"
  series="complaint_category"
  title="Complaints by Hour of Day"
  yFmt="num0"
/>

---

## Seasonal Patterns

```sql seasonal
SELECT
    season,
    complaint_category,
    SUM(total_requests) AS total_requests
FROM gold.mart_seasonal_patterns
GROUP BY season, complaint_category
ORDER BY CASE season
    WHEN 'Spring' THEN 1
    WHEN 'Summer' THEN 2
    WHEN 'Fall'   THEN 3
    WHEN 'Winter' THEN 4
END
```

<BarChart
  data={seasonal}
  x="season"
  y="total_requests"
  series="complaint_category"
  type="stacked"
  title="Complaints by Season and Category"
  yFmt="num0"
/>

---

## Submission Channel Over Time (Phone vs Online vs Mobile)

```sql channel_trends
SELECT
    created_year || '-' || LPAD(CAST(created_month AS VARCHAR), 2, '0') AS month,
    SUM(phone_requests)   AS phone,
    SUM(online_requests)  AS online,
    SUM(mobile_requests)  AS mobile
FROM gold.mart_complaint_trends
GROUP BY created_year, created_month
ORDER BY created_year, created_month
```

<LineChart
  data={channel_trends}
  x="month"
  y={["phone", "online", "mobile"]}
  title="Request Submission Channel Over Time"
  yFmt="num0"
/>

---

## Weekend vs Weekday Volume

```sql weekend_vs_weekday
SELECT
    complaint_category,
    SUM(weekend_requests)                                AS weekend_total,
    SUM(total_requests) - SUM(weekend_requests)         AS weekday_total,
    ROUND(100.0 * SUM(weekend_requests) / NULLIF(SUM(total_requests), 0), 1)
                                                         AS weekend_pct
FROM gold.mart_complaint_trends
GROUP BY complaint_category
ORDER BY weekend_pct DESC
```

<BarChart
  data={weekend_vs_weekday}
  x="complaint_category"
  y={["weekday_total", "weekend_total"]}
  type="stacked"
  title="Weekday vs Weekend Volume by Category"
  yFmt="num0"
  swapXY=true
/>
