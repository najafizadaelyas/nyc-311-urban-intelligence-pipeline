"""
Dagster schedule: Daily 6 AM ET

Triggers the full pipeline every day at 6:00 AM Eastern Time.

NYC Open Data updates the 311 dataset daily, so a morning run ensures
the dashboard shows data from the previous day.

The schedule uses cron notation: "0 6 * * *" = "at 06:00 every day".
"""

from dagster import ScheduleDefinition

from orchestration.jobs.full_pipeline import full_pipeline_job

daily_refresh_schedule = ScheduleDefinition(
    name="daily_pipeline_refresh",
    job=full_pipeline_job,
    cron_schedule="0 6 * * *",    # 6 AM every day
    execution_timezone="America/New_York",
    description=(
        "Daily NYC 311 data refresh. Runs incremental ingest → Silver clean → "
        "Gold dbt build every morning at 6 AM ET."
    ),
)
