# Tool Decisions — Architecture Decision Records (ADRs)

This document explains why each tool was chosen, what alternatives were
considered, and what trade-offs were accepted. Written for engineers who
want to understand the decision-making behind this stack.

---

## ADR-001: dlt for Ingestion

**Status:** Accepted

**Decision:** Use `dlt` (data load tool) for extracting NYC 311 data from the Socrata SODA API.

**Alternatives considered:**
- Airbyte — requires Docker + separate server; overkill for a single API source
- Custom requests script — no schema inference, no incremental state, no retry logic
- Fivetran — commercial; not appropriate for a local-first project
- Singer/Meltano — heavier installation footprint, less Pythonic

**Why dlt:**
- Pure Python library (`pip install dlt`) — zero infrastructure overhead
- Schema inference on first run; schema evolution on subsequent runs
- Built-in incremental state machine (`dlt.sources.incremental`) — tracks cursors across runs automatically
- Primary key deduplication built-in (`primary_key="unique_key"`)
- Retry, pagination, and type coercion are first-class features
- As of 2026, dlt processes >81,000 pipelines/month and has become the default choice for Python-native teams

**Trade-offs accepted:**
- Less mature ecosystem than Airbyte for pre-built connectors (not relevant here — we wrote a custom connector)
- Incremental state is stored locally (not a distributed store) — fine for single-machine operation

---

## ADR-002: DuckDB as the Storage and Compute Layer

**Status:** Accepted

**Decision:** Use DuckDB for all three storage layers (Bronze, Silver, Gold). Each layer is a separate `.duckdb` file.

**Alternatives considered:**
- PostgreSQL — requires a running server; not portable
- SQLite — no columnar storage; poor performance on analytical scans
- Parquet files on disk — no SQL interface; harder to query interactively
- MotherDuck (DuckDB Cloud) — adds cloud dependency; intentionally avoided

**Why DuckDB:**
- Embedded OLAP: zero installation, zero server — just a file
- Native columnar storage and vectorized execution: handles 100M+ rows efficiently on a laptop
- Native Python and Arrow integration: zero-copy exchange with Polars
- First-class dbt adapter (`dbt-duckdb`)
- First-class Evidence.dev datasource
- SQL-compatible: analysts can query with any DuckDB client (DBeaver, Jupyter, Python)

**Trade-offs accepted:**
- Not suitable for concurrent writes from multiple processes (single-writer model)
- Not a distributed system — appropriate for this data scale (~10–50M rows)

---

## ADR-003: Polars for Bronze→Silver Cleaning

**Status:** Accepted

**Decision:** Use Polars for the cleaning pipeline instead of pandas or pure DuckDB SQL.

**Alternatives considered:**
- pandas — slower (5–20×), unsafe type coercion, mutable index model, memory-inefficient
- PySpark — massive overhead for this data scale; requires JVM
- Pure DuckDB SQL — possible, but less expressive for complex string/datetime cleaning
- dbt macros — SQL is the wrong tool for regex cleaning, enum mapping, and conditional logic

**Why Polars:**
- Rust-powered: 5–20× faster than pandas on typical ETL workloads
- Lazy API (`.lazy()`) enables query planning — write transformations, Polars decides execution order
- Strict type system: fails loudly on type errors rather than silently coercing
- Native DuckDB/Arrow interoperability via `polars.from_arrow()` / `df.to_arrow()`
- Expressive expression API — transformations are readable and chainable
- Memory-safe: doesn't mutate DataFrames (unlike pandas)

**Trade-offs accepted:**
- Slightly less community documentation than pandas for niche use cases
- API changed significantly from 0.x to 1.x — use version-pinned dependency

---

## ADR-004: dbt-core for Silver→Gold Transformation

**Status:** Accepted

**Decision:** Use dbt-core with the dbt-duckdb adapter for all Gold layer transformations.

**Alternatives considered:**
- SQLMesh — newer, stateful alternative with compile-time SQL validation and virtual environments. Compelling choice for 2026, but less ecosystem support and fewer public examples.
- Raw Python (DuckDB Python API) — no versioning, no lineage, no docs, no tests
- Stored procedures — no version control, no testing

**Why dbt-core:**
- Industry standard: enormous community, extensive documentation, battle-tested
- Software engineering best practices built in: version control, testing, documentation, lineage
- `dbt build` runs models + tests in a single command
- `dbt docs generate` produces a browsable lineage + documentation site
- `dbt-duckdb` adapter has zero configuration overhead
- `dagster-dbt` integration automatically creates Dagster assets from dbt models

**Note on SQLMesh:** SQLMesh is the more modern choice in 2026 for teams starting fresh.
It offers compile-time SQL validation via SQLGlot, virtual environments (no data duplication
across dev/prod), and a native plan-based deployment workflow. For a future iteration of
this project, SQLMesh would be a strong replacement for dbt-core.

**Trade-offs accepted:**
- Jinja templating in dbt is a source of syntax errors that only surface at runtime
- dbt's incremental materialization in DuckDB is less efficient than SQLMesh's

---

## ADR-005: Great Expectations for Data Quality

**Status:** Accepted

**Decision:** Use Great Expectations (GE) for data quality validation at each layer boundary.

**Alternatives considered:**
- dbt tests only — dbt tests are great but run inside dbt; GE validates any data source
- pandera — Python-native, works well with Polars, but less mature Data Docs output
- soda-core — strong competitor; slightly less Python-native integration

**Why Great Expectations:**
- Separates quality contracts from transformation logic — contracts live in JSON/YAML, not SQL
- Data Docs: generates a beautiful HTML report of every validation result
- Can validate any data source (Polars, DuckDB, pandas, files)
- Expectation suites are versioned alongside the code
- Integrates with Dagster as asset_checks

**Trade-offs accepted:**
- GE's API changed significantly between v2, v3, and the new fluent API in v0.18.x
  — the code in this project uses the v0.18.x fluent API
- More setup overhead than dbt tests

---

## ADR-006: Dagster for Orchestration

**Status:** Accepted

**Decision:** Use Dagster with the asset-based model for pipeline orchestration.

**Alternatives considered:**
- Apache Airflow — task-based, not asset-based; no native asset catalog; heavier infrastructure
- Prefect — similar to Dagster but weaker asset model; flow-centric rather than asset-centric
- Cron + shell scripts — no lineage, no UI, no retry logic
- dbt Cloud scheduler — only covers the dbt layer; doesn't orchestrate ingestion or quality

**Why Dagster:**
- Asset-based model: the catalog shows what data you HAVE, not just what tasks ran
- Built-in asset freshness tracking
- First-class dbt integration (`dagster-dbt`) — each dbt model becomes a Dagster asset
- Asset checks: GE validations are peers to the assets they validate
- Software-defined assets make partial runs, selective re-materialization, and backfills easy
- Active development: Dagster 1.9 (2025) has substantial improvements over 1.x

**Trade-offs accepted:**
- More complex than Airflow for simple linear pipelines
- Requires generating dbt `manifest.json` before loading Dagster definitions (solved by `make dbt-compile`)

---

## ADR-007: Evidence.dev for Dashboards

**Status:** Accepted

**Decision:** Use Evidence.dev for the BI layer — dashboards as Markdown + SQL, version-controlled in Git.

**Alternatives considered:**
- Tableau / Power BI / Looker — commercial, not version-controlled, not code-first
- Grafana — designed for metrics/ops, not SQL analytics
- Streamlit — Python-based, good for data apps, but more code than Evidence.dev for static dashboards
- Metabase — self-hosted BI; stores logic in a database, not in code

**Why Evidence.dev:**
- BI as code: dashboards are `.md` files with embedded SQL — readable, reviewable, diffable
- Version-controlled in Git alongside the pipeline code
- Queries run directly against DuckDB at build time — no separate BI server
- Deploys as a static site — can be hosted on S3, Netlify, or GitHub Pages for free
- Markdown syntax means the barrier to writing and reviewing dashboards is very low

**Trade-offs accepted:**
- Less interactive than Tableau/Power BI (no drag-and-drop, limited ad-hoc exploration)
- Requires Node.js and npm in addition to Python
- Rebuild required when data changes (not "live" like a BI server against a live DB)
