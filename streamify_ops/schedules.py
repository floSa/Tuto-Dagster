"""Définition des planifications (schedules) Dagster."""

from dagster import DefaultScheduleStatus, build_schedule_from_partitioned_job

from .jobs import daily_streamify_job

# Le cron et la partition visée (la veille) se déduisent des partitions du job.
daily_streamify_schedule = build_schedule_from_partitioned_job(
    daily_streamify_job,
    name="daily_streamify_schedule",
    default_status=DefaultScheduleStatus.STOPPED,
)
