"""Définition des jobs Dagster."""

from dagster import define_asset_job

from .assets.ingestion import DAILY_PARTITIONS

daily_streamify_job = define_asset_job(
    name="daily_streamify_job",
    selection="*",
    partitions_def=DAILY_PARTITIONS,
)
