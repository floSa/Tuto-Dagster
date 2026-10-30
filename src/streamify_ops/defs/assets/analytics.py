import pandas as pd
from dagster import (
    AssetExecutionContext,
    AutomationCondition,
    FreshnessPolicy,
    MetadataValue,
    TableRecord,
    asset,
)

from ...settings import FRESHNESS_FAIL, FRESHNESS_WARN
from .ingestion import DAILY_PARTITIONS


@asset(
    partitions_def=DAILY_PARTITIONS,
    description="Gold: stats par artiste",
    group_name="gold",
    kinds={"pandas", "parquet"},
    owners=["team:data"],
    tags={"domain": "music"},
    automation_condition=AutomationCondition.eager(),
    freshness_policy=FreshnessPolicy.time_window(
        fail_window=FRESHNESS_FAIL, warn_window=FRESHNESS_WARN
    ),
)
def daily_artist_stats(
    context: AssetExecutionContext, clean_tracks: pd.DataFrame
) -> pd.DataFrame:
    """
    Aggregation: compter le nombre de tracks par artiste.
    """
    stats = (
        clean_tracks.groupby("artist_name", as_index=False)
        .size()
        .rename(columns={"size": "track_count"})
        .sort_values("track_count", ascending=False)
    )

    preview = stats.head(5).to_dict(orient="records")
    context.add_output_metadata(
        {
            "preview": MetadataValue.table([TableRecord(row) for row in preview]),
        }
    )
    return stats
