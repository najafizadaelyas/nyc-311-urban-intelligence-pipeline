"""
Dagster job: Full pipeline run

Materializes all assets in the correct order:
  1. bronze_service_requests    (dlt ingestion)
  2. bronze_data_quality        (GE Bronze check)
  3. silver_service_requests    (Polars cleaning)
  4. silver_data_quality        (GE Silver check)
  5. Gold dbt models            (dbt build --select tag:gold)
  6. gold_data_quality          (GE Gold check)

This job is intended to run daily via the schedule in schedules/daily_schedule.py.
It can also be triggered manually from the Dagster UI or via:

    dagster job execute -f orchestration/definitions.py -j full_pipeline_job
"""

from dagster import AssetSelection, define_asset_job

full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    description=(
        "Full NYC 311 pipeline: Bronze ingestion → Silver cleaning → "
        "Gold dbt transformation. Runs daily at 6 AM ET."
    ),
    # Materialize ALL assets in dependency order
    selection=AssetSelection.all(),
    tags={
        "pipeline": "nyc_311",
        "layer": "all",
    },
)
