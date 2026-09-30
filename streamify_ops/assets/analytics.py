import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, asset

from .ingestion import DAILY_PARTITIONS


@asset(partitions_def=DAILY_PARTITIONS, description="Gold: stats par artiste")
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
            "rows": len(stats),
            "preview": MetadataValue.json(preview),
        }
    )
    return stats
