"""Définition des jobs Dagster."""

from dagster import define_asset_job

# Le partitionnement est automatiquement inféré à partir des assets sélectionnés
daily_streamify_job = define_asset_job(
    name="daily_streamify_job",
    selection="*",
)
