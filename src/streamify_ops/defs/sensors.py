"""Définition des sensors Dagster."""

from dagster import (
    RunFailureSensorContext,
    RunRequest,
    SensorEvaluationContext,
    run_failure_sensor,
    sensor,
)

from .jobs import daily_streamify_job
from ..settings import BASE_DATA_PATH


@sensor(job=daily_streamify_job, minimum_interval_seconds=10)
def inbox_file_sensor(context: SensorEvaluationContext):
    """Lance la partition AAAA-MM-JJ quand data/inbox/AAAA-MM-JJ.ready apparaît."""
    for marker in sorted((BASE_DATA_PATH / "inbox").glob("*.ready")):
        yield RunRequest(run_key=marker.name, partition_key=marker.stem)


# Les runs de `dg launch` n'ont pas d'emplacement de code d'origine: sans
# monitor_all_code_locations, le sensor les ignorerait.
@run_failure_sensor(minimum_interval_seconds=10, monitor_all_code_locations=True)
def alert_on_run_failure(context: RunFailureSensorContext) -> None:
    """Écrit une alerte dans data/alerts/ pour chaque run en échec."""
    alerts = BASE_DATA_PATH / "alerts"
    alerts.mkdir(parents=True, exist_ok=True)
    run = context.dagster_run
    partition = run.tags.get("dagster/partition", "sans partition")
    (alerts / f"{run.run_id}.txt").write_text(
        f"run={run.run_id}\npartition={partition}\njob={run.job_name}\n"
        f"erreur={context.failure_event.message}\n"
    )
