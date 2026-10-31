import pandas as pd
from dagster import (
    AssetExecutionContext,
    Definitions,
    EnvVar,
    MaterializeResult,
    asset,
    definitions,
)

from streamify_ops.defs.assets.ingestion import DAILY_PARTITIONS
from streamify_ops.resources import PostgresResource

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS artist_stats (
    partition_date DATE NOT NULL,
    artist_name TEXT NOT NULL,
    track_count INTEGER NOT NULL,
    PRIMARY KEY (partition_date, artist_name)
)
"""


@asset(
    partitions_def=DAILY_PARTITIONS,
    description="Gold publie dans PostgreSQL: table artist_stats",
    group_name="gold",
    kinds={"postgres"},
    owners=["team:data"],
    tags={"domain": "music"},
)
def artist_stats_table(
    context: AssetExecutionContext,
    daily_artist_stats: pd.DataFrame,
    postgres: PostgresResource,
) -> MaterializeResult:
    """
    Remplace les lignes de la partition: relancer une date ne cree pas de doublon.
    """
    partition = context.partition_key
    rows = [
        (partition, row.artist_name, int(row.track_count))
        for row in daily_artist_stats.itertuples()
    ]
    with postgres.connect() as connection, connection.cursor() as cursor:
        cursor.execute(CREATE_TABLE)
        cursor.execute(
            "DELETE FROM artist_stats WHERE partition_date = %s", (partition,)
        )
        cursor.executemany("INSERT INTO artist_stats VALUES (%s, %s, %s)", rows)
    return MaterializeResult(
        metadata={"dagster/row_count": len(rows), "dagster/table_name": "artist_stats"}
    )


# Dans un module qui contient un @definitions, Dagster ne charge que ce qu'il
# renvoie: l'asset doit y figurer, sinon il disparait sans erreur.
@definitions
def postgres_export() -> Definitions:
    return Definitions(
        assets=[artist_stats_table],
        resources={
            "postgres": PostgresResource(password=EnvVar("STREAMIFY_PG_PASSWORD"))
        },
    )
