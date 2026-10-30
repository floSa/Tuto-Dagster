from datetime import datetime, timedelta, timezone

from dagster import (
    AssetKey,
    AutomationCondition,
    DagsterInstance,
    DailyPartitionsDefinition,
    Definitions,
    asset,
    evaluate_automation_conditions,
    materialize,
)

from streamify_ops import assets, asset_checks
from streamify_ops.resources import LocalFilesystemIOManager


def _evaluate(defs, instance, when, cursor=None):
    return evaluate_automation_conditions(
        defs=defs, instance=instance, evaluation_time=when, cursor=cursor
    )


def _defs(tmp_path):
    return Definitions(
        assets=assets,
        asset_checks=asset_checks,
        resources={"io_manager": LocalFilesystemIOManager(base_path=str(tmp_path))},
    )


def test_cron_requests_bronze_after_the_tick(tmp_path):
    instance = DagsterInstance.ephemeral()
    defs = _defs(tmp_path)

    before = _evaluate(defs, instance, datetime(2024, 3, 2, 0, 59, tzinfo=timezone.utc))
    assert before.total_requested == 0

    after = _evaluate(
        defs,
        instance,
        datetime(2024, 3, 2, 1, 1, tzinfo=timezone.utc),
        cursor=before.cursor,
    )
    assert after.get_requested_partitions(AssetKey("raw_tracks_data")) == {"2024-03-01"}


def test_eager_follows_an_upstream_update(tmp_path):
    instance = DagsterInstance.ephemeral()
    defs = _defs(tmp_path)
    when = datetime(2024, 3, 2, 0, 30, tzinfo=timezone.utc)

    first = _evaluate(defs, instance, when)
    assert first.total_requested == 0

    materialize(
        assets,
        selection=["raw_tracks_data"],
        partition_key="2024-03-01",
        instance=instance,
        resources={"io_manager": LocalFilesystemIOManager(base_path=str(tmp_path))},
    )

    second = _evaluate(defs, instance, when, cursor=first.cursor)
    assert second.get_requested_partitions(AssetKey("clean_tracks")) == {"2024-03-01"}
    assert second.get_requested_partitions(AssetKey("daily_artist_stats")) == {
        "2024-03-01"
    }


def test_on_missing_requests_each_new_partition_once():
    @asset(
        partitions_def=DailyPartitionsDefinition(start_date="2024-01-01"),
        automation_condition=AutomationCondition.on_missing(),
    )
    def report() -> None: ...

    instance = DagsterInstance.ephemeral()
    start = datetime(2024, 3, 2, 12, tzinfo=timezone.utc)

    first = _evaluate([report], instance, start)
    assert first.total_requested == 0

    next_day = _evaluate(
        [report], instance, start + timedelta(days=1), cursor=first.cursor
    )
    assert next_day.get_requested_partitions(AssetKey("report")) == {"2024-03-02"}

    again = _evaluate(
        [report],
        instance,
        start + timedelta(days=1, minutes=1),
        cursor=next_day.cursor,
    )
    assert again.total_requested == 0
