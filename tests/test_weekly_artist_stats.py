import pandas as pd
from dagster import materialize

from pipeline import asset_checks, assets
from streamify_ops.optional.weekly_artist_stats import weekly_artist_stats
from streamify_ops.resources import LocalFilesystemIOManager


def _io(tmp_path):
    return {"io_manager": LocalFilesystemIOManager(base_path=str(tmp_path))}


def _materialize_days(tmp_path, days):
    for day in days:
        materialize(
            [*assets, *asset_checks], partition_key=day, resources=_io(tmp_path)
        )


def _weekly(tmp_path, partition):
    return materialize(
        [*assets, weekly_artist_stats],
        selection=["weekly_artist_stats"],
        partition_key=partition,
        resources=_io(tmp_path),
    )


def test_weekly_sums_the_seven_days_window(tmp_path):
    days = [f"2024-02-{day:02d}" for day in range(1, 9)]
    _materialize_days(tmp_path, days)

    result = _weekly(tmp_path, "2024-02-08")

    assert result.success
    weekly = pd.read_parquet(tmp_path / "weekly_artist_stats" / "2024-02-08.parquet")
    window = [
        pd.read_parquet(tmp_path / "daily_artist_stats" / f"{day}.parquet")
        for day in days[1:]
    ]
    assert weekly["track_count"].sum() == sum(d["track_count"].sum() for d in window)


def test_weekly_ignores_days_not_yet_produced(tmp_path):
    _materialize_days(tmp_path, ["2024-02-07", "2024-02-08"])

    result = _weekly(tmp_path, "2024-02-08")

    assert result.success
    weekly = pd.read_parquet(tmp_path / "weekly_artist_stats" / "2024-02-08.parquet")
    window = [
        pd.read_parquet(tmp_path / "daily_artist_stats" / f"{day}.parquet")
        for day in ("2024-02-07", "2024-02-08")
    ]
    assert weekly["track_count"].sum() == sum(d["track_count"].sum() for d in window)
