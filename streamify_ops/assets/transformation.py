import pandas as pd
from dagster import AssetCheckResult, AssetExecutionContext, asset, asset_check

from .ingestion import DAILY_PARTITIONS


@asset(
    partitions_def=DAILY_PARTITIONS,
    description="Silver: raw -> clean",
    group_name="silver",
    kinds={"pandas", "parquet"},
    owners=["team:data"],
    tags={"domain": "music"},
)
def clean_tracks(
    context: AssetExecutionContext, raw_tracks_data: pd.DataFrame
) -> pd.DataFrame:
    """
    Nettoyage simple: normaliser les genres et filtrer les durees negatives.
    """
    # Pour scaler: remplacer les operations pandas par un job Spark ici.
    df = raw_tracks_data.copy()
    df["genre"] = df["genre"].str.upper()
    df = df[df["duration_sec"] >= 0].reset_index(drop=True)

    context.add_output_metadata({"rows": len(df)})
    return df


@asset_check(
    asset=clean_tracks,
    name="clean_tracks_id_not_null",
    blocking=True,
    partitions_def=DAILY_PARTITIONS,
)
def clean_tracks_id_not_null(clean_tracks: pd.DataFrame) -> AssetCheckResult:
    """
    Check de qualite bloquant, evalue partition par partition: aucun track_id null.
    """
    null_count = int(clean_tracks["track_id"].isna().sum())
    return AssetCheckResult(passed=null_count == 0, metadata={"null_count": null_count})
