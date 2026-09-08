"""
NYC 311 Urban Intelligence Pipeline — Cleaning Layer

Polars-based Bronze → Silver transformation. Reads raw data from the Bronze
DuckDB, applies systematic cleaning, and writes the result to the Silver DuckDB.

Why Polars?
  - 5–20× faster than pandas for typical ETL scan + transform operations
  - Lazy evaluation (`.lazy()`) enables query planning and optimization
  - Strict type system — fails loudly on type mismatches instead of silently coercing
  - Native Arrow/DuckDB interoperability — zero-copy data exchange
  - Expressive, chain-able expression API (readable transformation pipelines)
"""
