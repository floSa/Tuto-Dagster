import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    TimeWindowPartitionMapping,
    asset,
)

from streamify_ops.defs.assets.ingestion import DAILY_PARTITIONS


@asset(
    partitions_def=DAILY_PARTITIONS,
    description="Gold: stats par artiste sur les 7 derniers jours",
    group_name="gold",
    kinds={"pandas", "parquet"},
    owners=["team:data"],
    tags={"domain": "music"},
    ins={
        "daily_artist_stats": AssetIn(
            partition_mapping=TimeWindowPartitionMapping(start_offset=-6)
        )
    },
)
def weekly_artist_stats(
    context: AssetExecutionContext, daily_artist_stats: pd.DataFrame
) -> pd.DataFrame:
    """
    Cumul glissant: chaque partition lit sa journee et les 6 precedentes.
    """
    windows = context.asset_partition_keys_for_input("daily_artist_stats")
    context.add_output_metadata({"window_start": windows[0], "window_end": windows[-1]})
    return (
        daily_artist_stats.groupby("artist_name", as_index=False)["track_count"]
        .sum()
        .sort_values("track_count", ascending=False)
    )
