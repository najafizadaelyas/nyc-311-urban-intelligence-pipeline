# 🏙️ NYC 311 Urban Intelligence Pipeline

> A production-grade, end-to-end data engineering pipeline that transforms raw NYC 311 service request data into actionable urban intelligence — built entirely on modern open-source tooling.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/)
[![dlt](https://img.shields.io/badge/ingestion-dlt_0.5.x-orange.svg)](https://dlthub.com/)
[![DuckDB](https://img.shields.io/badge/engine-DuckDB_1.1-yellow.svg)](https://duckdb.org/)
[![dbt-core](https://img.shields.io/badge/transform-dbt--core_1.8-red.svg)](https://getdbt.com/)
[![Polars](https://img.shields.io/badge/cleaning-Polars_1.x-purple.svg)](https://pola.rs/)
[![Dagster](https://img.shields.io/badge/orchestration-Dagster_1.9-green.svg)](https://dagster.io/)
[![Evidence.dev](https://img.shields.io/badge/BI-Evidence.dev-teal.svg)](https://evidence.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-lightgrey.svg)](LICENSE)

---

## 🎯 What This Pipeline Does

New York City receives **over 3 million 311 service requests every year** — noise complaints, illegal dumping, pothole reports, heat outages, and hundreds more. This data is messy, inconsistent, geographically rich, and deeply revealing about urban life.

This pipeline ingests that raw, live data from the NYC Open Data Socrata API and runs it through a full **Medallion Architecture** — Bronze → Silver → Gold — producing clean, trustworthy analytical datasets that answer questions like:

- **Which neighborhoods have the highest unresolved complaint rates?**
- **How long does it take the city to respond by borough and complaint type?**
- **Which complaint categories are trending up or down over time?**
- **What is the seasonal pattern of infrastructure failures?**
- **Which agencies are the slowest to close tickets?**

All of this is surfaced through a **BI-as-code Evidence.dev dashboard** — live, version-controlled, and reproducible.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        NYC 311 Urban Intelligence Pipeline               │
│                                                                          │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐               │
│  │   SOURCE     │    │    BRONZE    │    │    SILVER    │               │
│  │              │    │              │    │              │               │
│  │ NYC Open Data│───▶│  Raw JSON    │───▶│  Cleaned     │               │
│  │ Socrata API  │    │  in DuckDB   │    │  Typed Data  │               │
│  │  (Live)      │    │  (append)    │    │  (Polars)    │               │
│  └──────────────┘    └──────────────┘    └──────┬───────┘               │
│        │                    │                   │                        │
│       dlt              dlt schema             Polars                     │
│   (extraction)        (auto-types)           (quality)                  │
│                                                 │                        │
│                             ┌───────────────────▼───────────────────┐   │
│                             │               GOLD                    │   │
│                             │  ┌──────────────────────────────────┐ │   │
│                             │  │  fact_311_requests               │ │   │
│                             │  │  dim_complaint_type              │ │   │
│                             │  │  dim_agency                      │ │   │
│                             │  │  dim_location                    │ │   │
│                             │  │  mart_response_time_by_borough   │ │   │
│                             │  │  mart_complaint_trends           │ │   │
│                             │  │  mart_agency_performance         │ │   │
│                             │  └──────────────────────────────────┘ │   │
│                             │  (dbt-core models on DuckDB)          │   │
│                             └──────────────┬──────────────────────┘   │
│                                            │                            │
│                              ┌─────────────▼──────────────┐            │
│                              │   Evidence.dev Dashboard    │            │
│                              │   (BI as code, Markdown+SQL)│            │
│                              └────────────────────────────┘            │
│                                                                          │
│  Data Quality: Great Expectations checkpoints at every layer boundary   │
│  Orchestration: Dagster assets + schedules (daily refresh)              │
└─────────────────────────────────────────────────────────────────────────┘
```

### Medallion Layers

| Layer  | Storage       | Tool         | What happens                                      |
|--------|---------------|--------------|---------------------------------------------------|
| Bronze | DuckDB        | dlt          | Raw API data, schema-inferred, append-only        |
| Silver | DuckDB        | Polars       | Type casting, deduplication, null handling, enrich|
| Gold   | DuckDB        | dbt-core     | Star schema, aggregations, business-ready marts   |

---

## 🛠️ Tool Stack (Why We Chose Each)

### `dlt` — Data Load Tool *(ingestion)*
> The newest Python-native ELT library from dlthub. Unlike Airbyte or Fivetran, `dlt` is code-first, runs in-process, and handles schema inference, type coercion, and incremental loading automatically. In 2025/2026, it has become the de-facto choice for Python data engineers who want full control without infrastructure overhead.

**Why not Airbyte?** Airbyte requires Docker and a server. `dlt` is a Python import.

### `DuckDB` — Analytical Engine *(storage + compute)*
> An embedded OLAP database. Zero infrastructure — just a `.duckdb` file. Yet it can scan hundreds of millions of rows in seconds, supports columnar storage, and integrates natively with Polars and dbt. It replaced the need for a Postgres server, S3 bucket, and a dedicated compute cluster for this scale of data.

### `Polars` — DataFrame Library *(cleaning)*
> A Rust-powered DataFrame library that is 5–20× faster than pandas for typical ETL operations. Its lazy evaluation API (`.lazy()`) enables DAG-based query planning — you write transformations and Polars decides the optimal execution plan. We use it for the Bronze → Silver cleaning pass because it handles messy text, mixed types, and large scans elegantly.

**Why not pandas?** Memory safety, speed, and expressive type system. Polars fails fast on type errors; pandas silently downcasts.

### `dbt-core` — Transformation *(Silver → Gold)*
> The industry standard for SQL-based transformation with a software engineering workflow — version control, testing, documentation, and lineage built in. `dbt-core` (open source) with the `dbt-duckdb` adapter runs locally with zero infrastructure.

### `Great Expectations` — Data Quality *(quality gates)*
> Defines explicit contracts (expectations) about what the data should look like at each layer boundary. Provides data docs, suite validation, and checkpoint runs. Catches schema drift, unexpected nulls, and value range violations before they reach the Gold layer.

### `Dagster` — Orchestration *(asset-based pipeline)*
> Unlike Airflow (task-based), Dagster uses an **asset-based model** — each dataset is an asset, and the pipeline is a graph of assets and their dependencies. This makes lineage, reloading, and partial runs first-class features. Dagster's asset catalog gives you a visual, queryable inventory of your data.

### `Evidence.dev` — Dashboards *(BI as code)*
> Write dashboards in Markdown + SQL. Version-controlled in Git, reviewed like code, deployed as a static site. No Tableau license, no Looker server, no drag-and-drop. The queries run directly against DuckDB at build time.

---

## 📂 Project Structure

```
nyc-311-pipeline/
├── ingestion/                  # dlt pipeline: Socrata API → Bronze DuckDB
│   ├── __init__.py
│   ├── pipeline.py             # main dlt pipeline entry point
│   └── sources/
│       ├── __init__.py
│       └── nyc_311.py          # dlt source: NYC 311 Socrata connector
│
├── clean/                      # Polars Bronze → Silver transformation
│   ├── __init__.py
│   └── silver_cleaner.py       # type casting, dedup, null handling, enrichment
│
├── transform/                  # dbt-core project: Silver → Gold
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   ├── models/
│   │   ├── bronze/             # dbt bronze staging views (light touch)
│   │   ├── silver/             # dbt silver intermediate models
│   │   └── gold/               # dbt gold: facts, dims, marts
│   ├── tests/                  # dbt generic + singular tests
│   └── macros/                 # reusable SQL macros
│
├── quality/                    # Great Expectations quality suites
│   ├── great_expectations.yml
│   ├── expectations/           # expectation suites per layer
│   └── checkpoints/            # checkpoint configs
│
├── dashboard/                  # Evidence.dev BI-as-code dashboard
│   ├── package.json
│   ├── evidence.plugins.yaml
│   ├── sources/
│   │   └── nyc311.duckdb.yaml  # DuckDB source connection
│   └── pages/
│       ├── index.md            # executive overview
│       ├── borough-analysis.md # borough-level breakdown
│       ├── agency-performance.md
│       ├── complaint-trends.md
│       └── response-time.md
│
├── orchestration/              # Dagster asset-based orchestration
│   ├── __init__.py
│   ├── definitions.py          # Dagster Definitions entry point
│   ├── assets/
│   │   ├── __init__.py
│   │   ├── bronze.py           # bronze ingestion asset
│   │   ├── silver.py           # silver cleaning asset
│   │   ├── gold.py             # gold transform asset (dbt)
│   │   └── quality.py          # GE validation asset
│   ├── jobs/
│   │   └── full_pipeline.py    # daily full refresh job
│   └── schedules/
│       └── daily_schedule.py   # 6 AM daily cron schedule
│
├── data/
│   ├── bronze/                 # bronze.duckdb
│   ├── silver/                 # silver.duckdb
│   └── gold/                   # gold.duckdb (queried by dashboard)
│
├── docs/
│   ├── diagrams/               # architecture diagrams
│   ├── DATA_DICTIONARY.md      # field definitions + lineage
│   └── TOOL_DECISIONS.md       # ADR-style tool choice docs
│
├── tests/                      # pytest unit + integration tests
├── pyproject.toml              # all dependencies, pinned
├── .env.example                # environment variable template
├── Makefile                    # developer commands
└── README.md                   # this file
```

---

## 🚀 Quickstart

### Prerequisites
- Python 3.11+
- Node.js 18+ (for Evidence.dev dashboard)
- `uv` (recommended) or `pip`

### 1. Clone and install

```bash
git clone https://github.com/your-org/nyc-311-pipeline.git
cd nyc-311-pipeline

# using uv (recommended — fast, deterministic)
uv sync

# or with pip
pip install -e ".[dev]"
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env: add your NYC Open Data API token (optional, increases rate limits)
```

### 3. Run the full pipeline

```bash
make pipeline
```

Or step by step:

```bash
make ingest       # Bronze: pull NYC 311 data via dlt
make clean        # Silver: Polars cleaning pass
make transform    # Gold: dbt-core models
make quality      # Great Expectations validation
make dashboard    # Build Evidence.dev dashboard
```

### 4. Launch the dashboard

```bash
make dev-dashboard
# → http://localhost:3000
```

### 5. Launch Dagster UI (optional)

```bash
make dagster
# → http://localhost:3001
```

---

## 📊 What the Data Looks Like

### Source: NYC 311 Service Requests (2020–Present)
- **Dataset ID:** `erm2-nwe9`
- **API:** Socrata SODA 2.0 (`https://data.cityofnewyork.us/resource/erm2-nwe9.json`)
- **Volume:** ~3 million rows/year, 41 columns
- **Update cadence:** Daily automatic updates by NYC OpenData

### Key columns (of 41 total)

| Column | Type | Notes |
|---|---|---|
| `unique_key` | string | Row identifier |
| `created_date` | timestamp | When the 311 call was made |
| `closed_date` | timestamp | When the request was closed |
| `complaint_type` | string | Top-level complaint category |
| `descriptor` | string | Specific complaint detail |
| `agency` | string | Responding city agency code |
| `agency_name` | string | Full agency name |
| `borough` | string | NYC borough |
| `latitude` / `longitude` | float | Geo coordinates |
| `community_board` | string | CB number |
| `council_district` | int | City Council district |
| `police_precinct` | int | NYPD precinct |
| `status` | string | Open / Closed / Pending |
| `resolution_description` | string | Free-text resolution |

### Known data quality issues (what we clean)
- `closed_date` is null for ~8% of rows (open tickets)
- `borough` contains `"Unspecified"` for ~3% of rows
- `complaint_type` has 200+ unique values with inconsistent casing
- Timestamps arrive as strings with mixed timezone formatting
- Lat/lon have outliers outside NYC bounding box
- `community_board` is sometimes `"0 Unspecified"`

---

## 🧪 Data Quality Contracts

Great Expectations enforces these contracts at each layer:

**Bronze (raw)**
- `unique_key` is never null
- `created_date` is parseable
- Row count > 0

**Silver (cleaned)**
- No duplicate `unique_key` values
- `created_date` < `closed_date` when both present
- `borough` ∈ `{MANHATTAN, BRONX, BROOKLYN, QUEENS, STATEN ISLAND, Unspecified}`
- `latitude` between 40.4 and 40.95 (NYC bounding box)
- `longitude` between -74.3 and -73.7

**Gold (analytical)**
- `response_hours` ≥ 0
- Complaint type dimension is complete (no orphan fact rows)
- marts are not empty

---

## 📈 Gold Layer: Business Questions Answered

| Mart table | Answers |
|---|---|
| `mart_response_time_by_borough` | Avg/median/p95 response time by borough × complaint type |
| `mart_complaint_trends` | Monthly complaint volume trends by category and borough |
| `mart_agency_performance` | Closure rate, avg response time, overdue tickets by agency |
| `mart_neighborhood_heat_map` | Complaint density by community board and council district |
| `mart_seasonal_patterns` | Day-of-week, hour-of-day, and monthly seasonality |

---

## 📚 Documentation

- [Data Dictionary](docs/DATA_DICTIONARY.md) — every field, its source, and its lineage
- [Tool Decisions](docs/TOOL_DECISIONS.md) — Architecture Decision Records for tool choices
- dbt docs: `make dbt-docs` → http://localhost:8080

---

## 🔬 Why This Project Is Different

Most example pipelines use toy data, skip quality gates, and hardcode credentials. This pipeline:

1. **Uses real, live, messy data** — the NYC 311 feed is updated daily and has genuine dirty data issues
2. **Every tool is the real production-grade version** — not a demo wrapper
3. **Data quality is a first-class citizen** — GE contracts are defined before models
4. **Orchestration is asset-aware** — Dagster knows what data assets exist and their health
5. **BI is code** — the dashboard lives in Git and can be reviewed and versioned
6. **Fully local** — no cloud accounts needed; DuckDB is the entire storage and compute layer
7. **Incremental by default** — dlt and dbt both support incremental loads; this isn't a one-shot batch

---

## 🤝 Contributing

See [CONTRIBUTING.md](docs/CONTRIBUTING.md). All contributions welcome.

---

## 📄 License

MIT — see [LICENSE](LICENSE).
