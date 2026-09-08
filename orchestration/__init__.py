"""
NYC 311 Urban Intelligence Pipeline — Dagster Orchestration Layer

Asset-based orchestration using Dagster 1.9.

Why Dagster over Airflow?
  - Asset-based model: each dataset is a first-class asset, not just a task
  - Dagster knows what data you have, when it was last materialized, and whether
    it's healthy — Airflow only knows whether a task ran
  - Built-in asset catalog: browse lineage, freshness, and health in the UI
  - Partitioned assets: run subsets by date range, borough, or any dimension
  - Software-defined assets integrate naturally with dbt, GE, and dlt

Entry point: orchestration/definitions.py
Launch:      dagster dev -f orchestration/definitions.py
"""
