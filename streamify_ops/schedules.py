"""Définition des planifications (schedules) Dagster."""

from datetime import datetime, timedelta, timezone

from dagster import (
    DefaultScheduleStatus,
    RunRequest,
    ScheduleEvaluationContext,
    schedule,
)

from .jobs import daily_streamify_job


@schedule(
    job=daily_streamify_job,
    cron_schedule="0 0 * * *",
    default_status=DefaultScheduleStatus.STOPPED,
    execution_timezone="UTC",
)
def daily_streamify_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    """Exécute la partition de la veille à minuit."""
    scheduled_time = context.scheduled_execution_time or datetime.now(timezone.utc)
    partition_key = (scheduled_time - timedelta(days=1)).strftime("%Y-%m-%d")
    return RunRequest(partition_key=partition_key)
