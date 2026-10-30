from dagster import (
    DagsterEventType,
    DagsterInstance,
    build_run_status_sensor_context,
    build_sensor_context,
    materialize,
)

from pipeline import asset_checks, assets
from streamify_ops.defs import sensors
from streamify_ops.resources import LocalFilesystemIOManager


def test_inbox_sensor_requests_one_run_per_marker(tmp_path, monkeypatch):
    monkeypatch.setattr(sensors, "BASE_DATA_PATH", tmp_path)
    (tmp_path / "inbox").mkdir()
    (tmp_path / "inbox" / "2024-04-10.ready").touch()
    (tmp_path / "inbox" / "2024-04-11.ready").touch()
    (tmp_path / "inbox" / "notes.txt").touch()

    requests = list(sensors.inbox_file_sensor(build_sensor_context()))

    assert [(r.run_key, r.partition_key) for r in requests] == [
        ("2024-04-10.ready", "2024-04-10"),
        ("2024-04-11.ready", "2024-04-11"),
    ]


def test_inbox_sensor_ignores_a_missing_folder(tmp_path, monkeypatch):
    monkeypatch.setattr(sensors, "BASE_DATA_PATH", tmp_path)

    assert list(sensors.inbox_file_sensor(build_sensor_context())) == []


def test_failure_sensor_writes_an_alert(tmp_path, monkeypatch):
    monkeypatch.setattr(sensors, "BASE_DATA_PATH", tmp_path)
    instance = DagsterInstance.ephemeral()
    result = materialize(
        [*assets, *asset_checks],
        partition_key="2024-03-15",
        instance=instance,
        resources={"io_manager": LocalFilesystemIOManager(base_path=str(tmp_path))},
        run_config={"ops": {"raw_tracks_data": {"config": {"null_track_ids": 3}}}},
        raise_on_error=False,
    )
    event = next(
        e
        for e in result.all_events
        if e.event_type == DagsterEventType.PIPELINE_FAILURE
    )
    context = build_run_status_sensor_context(
        sensor_name="alert_on_run_failure",
        dagster_instance=instance,
        dagster_run=result.dagster_run,
        dagster_event=event,
    ).for_run_failure()

    sensors.alert_on_run_failure(context)

    alert = (tmp_path / "alerts" / f"{result.run_id}.txt").read_text()
    assert "partition=2024-03-15" in alert
    assert "clean_tracks_clean_tracks_id_not_null" in alert
