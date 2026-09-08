"""
NYC 311 Urban Intelligence Pipeline — Ingestion Layer

This package contains the dlt-based ingestion pipeline that pulls
NYC 311 service request data from the Socrata SODA API and loads it
into the Bronze DuckDB layer.

Tool: dlt (data load tool) — https://dlthub.com
Why: Python-native, schema-inferring, incremental-by-default ELT library.
     No Docker, no server. Just a pip install and a function decorator.
"""
