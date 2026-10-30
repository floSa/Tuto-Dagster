from datetime import datetime, timezone

import pandas as pd

from dagster import (
    AssetKey,
    DefaultScheduleStatus,
    build_schedule_context,
    materialize,
)

from streamify_ops import assets, asset_checks, defs
from streamify_ops.resources import LocalFilesystemIOManager
from streamify_ops.schedules import daily_streamify_schedule
from streamify_ops.settings import FRESHNESS_FAIL, FRESHNESS_WARN


def _materialize(tmp_path, null_track_ids: int):
    return materialize(
        [*assets, *asset_checks],
        partition_key="2024-01-15",
        resources={"io_manager": LocalFilesystemIOManager(base_path=str(tmp_path))},
        run_config={
            "ops": {"raw_tracks_data": {"config": {"null_track_ids": null_track_ids}}}
        },
        raise_on_error=False,
    )


def test_quality_check_is_registered():
    check_names = {key.name for key in defs.resolve_asset_graph().asset_check_keys}

    assert check_names == {"clean_tracks_id_not_null"}


def test_assets_are_grouped_by_layer():
    graph = defs.resolve_asset_graph()

    groups = {
        key.path[-1]: graph.get(key).group_name for key in graph.get_all_asset_keys()
    }
    assert groups == {
        "raw_tracks_data": "bronze",
        "clean_tracks": "silver",
        "daily_artist_stats": "gold",
    }


def test_gold_declares_a_freshness_policy():
    node = defs.resolve_asset_graph().get(AssetKey("daily_artist_stats"))

    assert node.freshness_policy.fail_window.to_timedelta() == FRESHNESS_FAIL
    assert node.freshness_policy.warn_window.to_timedelta() == FRESHNESS_WARN


def test_check_passes_on_clean_data(tmp_path):
    result = _materialize(tmp_path, null_track_ids=0)

    assert result.success
    evaluations = result.get_asset_check_evaluations()
    assert [(e.partition, e.passed) for e in evaluations] == [("2024-01-15", True)]
    assert (tmp_path / "daily_artist_stats" / "2024-01-15.parquet").exists()


def test_materialization_reports_standard_metadata(tmp_path):
    result = _materialize(tmp_path, null_track_ids=0)

    events = result.get_asset_materialization_events()
    metadata = {
        e.asset_key.path[-1]: e.step_materialization_data.materialization.metadata
        for e in events
    }
    silver = metadata["clean_tracks"]
    assert silver["dagster/row_count"].value == len(
        pd.read_parquet(tmp_path / "clean_tracks" / "2024-01-15.parquet")
    )
    assert "dagster/uri" in silver
    columns = {c.name for c in silver["dagster/column_schema"].value.columns}
    assert {"track_id", "genre", "duration_sec"} <= columns


def test_failed_check_blocks_downstream_asset(tmp_path):
    result = _materialize(tmp_path, null_track_ids=3)

    assert not result.success
    evaluations = result.get_asset_check_evaluations()
    assert [(e.partition, e.passed) for e in evaluations] == [("2024-01-15", False)]
    materialized = {e.asset_key for e in result.get_asset_materialization_events()}
    assert AssetKey("clean_tracks") in materialized
    assert AssetKey("daily_artist_stats") not in materialized


def test_schedule_targets_previous_day():
    context = build_schedule_context(
        scheduled_execution_time=datetime(2024, 3, 2, tzinfo=timezone.utc)
    )

    tick = daily_streamify_schedule.evaluate_tick(context)

    assert [r.partition_key for r in tick.run_requests] == ["2024-03-01"]
    assert daily_streamify_schedule.cron_schedule == "0 0 * * *"
    assert daily_streamify_schedule.default_status == DefaultScheduleStatus.STOPPED
