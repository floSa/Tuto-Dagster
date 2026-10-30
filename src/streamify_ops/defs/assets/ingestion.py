from typing import List

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AutomationCondition,
    Config,
    DailyPartitionsDefinition,
    RetryPolicy,
    asset,
)
from faker import Faker

from ...settings import RAW_TRACKS_CRON

DAILY_PARTITIONS = DailyPartitionsDefinition(start_date="2024-01-01")


class RawTracksConfig(Config):
    """
    Options du mock: null_track_ids > 0 provoque l'echec du check qualite,
    fail_attempts > 0 simule une API indisponible pour ce nombre de tentatives.
    """

    null_track_ids: int = 0
    fail_attempts: int = 0


def _artist_pool(size: int = 20) -> List[str]:
    """
    Catalogue fixe d'artistes: un meme artiste revient plusieurs fois par jour.
    """
    faker = Faker()
    faker.seed_instance(2024)
    return [faker.name() for _ in range(size)]


ARTISTS = _artist_pool()


def _mock_tracks_api(partition_date: str, rows: int = 100) -> pd.DataFrame:
    """
    Simuler une reponse d'API musicale pour une date donnee.
    """
    faker = Faker()
    seed = int(partition_date.replace("-", ""))
    faker.seed_instance(seed)

    genres = ["pop", "rock", "jazz", "hip hop", "electronic", "classical"]

    records: List[dict] = []
    for i in range(rows):
        records.append(
            {
                "track_id": faker.uuid4(),
                "track_name": faker.sentence(nb_words=3).replace(".", ""),
                "artist_name": faker.random_element(elements=ARTISTS),
                "genre": faker.random_element(elements=genres),
                "duration_sec": faker.random_int(min=-30, max=420),
                "release_date": faker.date_this_decade().isoformat(),
                "ingestion_date": partition_date,
            }
        )

    return pd.DataFrame.from_records(records)


@asset(
    partitions_def=DAILY_PARTITIONS,
    description="Bronze: API mock -> raw DataFrame",
    group_name="bronze",
    kinds={"python", "parquet"},
    owners=["team:data"],
    tags={"domain": "music"},
    automation_condition=AutomationCondition.on_cron(RAW_TRACKS_CRON),
    retry_policy=RetryPolicy(max_retries=2, delay=5),
)
def raw_tracks_data(
    context: AssetExecutionContext, config: RawTracksConfig
) -> pd.DataFrame:
    """
    Recuperer la date de partition en cours avec context.partition_key.
    Exemple: "2024-01-15".
    """
    partition_key = context.partition_key
    if not partition_key:
        raise ValueError("partition_key est requis pour raw_tracks_data")

    if context.retry_number < config.fail_attempts:
        raise RuntimeError(f"API indisponible (tentative {context.retry_number + 1})")

    df = _mock_tracks_api(partition_key)
    df.loc[df.index[: config.null_track_ids], "track_id"] = None

    return df
