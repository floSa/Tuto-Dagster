from datetime import datetime, timezone

from dagster import AssetKey, build_schedule_context, materialize

from streamify_ops import assets, asset_checks, defs
from streamify_ops.resources import LocalFilesystemIOManager
from streamify_ops.schedules import daily_streamify_schedule


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


def test_check_passes_on_clean_data(tmp_path):
    result = _materialize(tmp_path, null_track_ids=0)

    assert result.success
    assert [e.passed for e in result.get_asset_check_evaluations()] == [True]
    assert (tmp_path / "daily_artist_stats" / "2024-01-15.parquet").exists()


def test_failed_check_blocks_downstream_asset(tmp_path):
    result = _materialize(tmp_path, null_track_ids=3)

    assert not result.success
    assert [e.passed for e in result.get_asset_check_evaluations()] == [False]
    materialized = {e.asset_key for e in result.get_asset_materialization_events()}
    assert AssetKey("clean_tracks") in materialized
    assert AssetKey("daily_artist_stats") not in materialized


def test_schedule_targets_previous_day():
    context = build_schedule_context(
        scheduled_execution_time=datetime(2024, 3, 2, tzinfo=timezone.utc)
    )

    assert daily_streamify_schedule(context).partition_key == "2024-03-01"
