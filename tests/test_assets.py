from dagster import materialize_to_memory

from streamify_ops.assets.analytics import daily_artist_stats
from streamify_ops.assets.ingestion import raw_tracks_data
from streamify_ops.assets.transformation import clean_tracks


def test_clean_tracks_asset():
    result = materialize_to_memory(
        [raw_tracks_data, clean_tracks],
        partition_key="2024-01-02",
    )

    assert result.success
    clean_df = result.output_for_node("clean_tracks")

    assert (clean_df["duration_sec"] >= 0).all()
    assert clean_df["genre"].str.isupper().all()
    assert int(clean_df["track_id"].isna().sum()) == 0


def test_daily_artist_stats_asset():
    result = materialize_to_memory(
        [raw_tracks_data, clean_tracks, daily_artist_stats],
        partition_key="2024-01-03",
    )

    assert result.success
    stats_df = result.output_for_node("daily_artist_stats")

    assert "artist_name" in stats_df.columns
    assert "track_count" in stats_df.columns
