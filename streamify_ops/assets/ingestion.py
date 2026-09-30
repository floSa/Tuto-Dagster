from datetime import datetime
from typing import List

import pandas as pd
from dagster import AssetExecutionContext, DailyPartitionsDefinition, asset
from faker import Faker

DAILY_PARTITIONS = DailyPartitionsDefinition(start_date="2024-01-01")


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
                "artist_name": faker.name(),
                "genre": faker.random_element(elements=genres),
                "duration_sec": faker.random_int(min=-30, max=420),
                "release_date": faker.date_this_decade().isoformat(),
                "ingestion_date": partition_date,
            }
        )

    return pd.DataFrame.from_records(records)


@asset(partitions_def=DAILY_PARTITIONS, description="Bronze: API mock -> raw DataFrame")
def raw_tracks_data(context: AssetExecutionContext) -> pd.DataFrame:
    """
    Recuperer la date de partition en cours avec context.partition_key.
    Exemple: "2024-01-15".
    """
    partition_key = context.partition_key
    if not partition_key:
        raise ValueError("partition_key est requis pour raw_tracks_data")

    df = _mock_tracks_api(partition_key)

    context.add_output_metadata({"rows": len(df), "partition_key": partition_key})
    return df
