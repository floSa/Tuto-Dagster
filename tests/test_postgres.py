import os

import psycopg
import pytest
from dagster import AssetKey, materialize

from pipeline import asset_checks, assets
from streamify_ops.optional.artist_stats_table import (
    artist_stats_table,
    postgres_export,
)
from streamify_ops.resources import LocalFilesystemIOManager, PostgresResource

PASSWORD = os.environ.get("STREAMIFY_PG_PASSWORD")
needs_postgres = pytest.mark.skipif(
    not PASSWORD, reason="STREAMIFY_PG_PASSWORD absent: PostgreSQL non disponible"
)


def _run(tmp_path, partition, postgres, selection=None):
    return materialize(
        [*assets, *asset_checks, artist_stats_table],
        selection=selection,
        partition_key=partition,
        resources={
            "io_manager": LocalFilesystemIOManager(base_path=str(tmp_path)),
            "postgres": postgres,
        },
        raise_on_error=False,
    )


@pytest.fixture
def table():
    """Base reelle, table videe avant le test."""
    with psycopg.connect(
        host="localhost",
        port=5434,
        user="streamify",
        dbname="streamify",
        password=PASSWORD,
    ) as connection:
        connection.execute("DROP TABLE IF EXISTS artist_stats")
        connection.commit()
        yield connection
        connection.rollback()
        connection.execute("DROP TABLE IF EXISTS artist_stats")
        connection.commit()


def _count(connection, partition):
    connection.rollback()
    row = connection.execute(
        "SELECT count(*), sum(track_count) FROM artist_stats WHERE partition_date = %s",
        (partition,),
    ).fetchone()
    return row


@needs_postgres
def test_publishes_gold_without_duplicates(tmp_path, table):
    postgres = PostgresResource(password=PASSWORD)

    assert _run(tmp_path, "2024-01-15", postgres).success
    first = _count(table, "2024-01-15")
    assert _run(tmp_path, "2024-01-15", postgres).success

    assert first[0] > 0
    assert _count(table, "2024-01-15") == first


@needs_postgres
def test_other_partitions_are_kept(tmp_path, table):
    postgres = PostgresResource(password=PASSWORD)

    assert _run(tmp_path, "2024-01-15", postgres).success
    first = _count(table, "2024-01-15")
    assert _run(tmp_path, "2024-01-16", postgres).success

    assert _count(table, "2024-01-15") == first
    assert _count(table, "2024-01-16")[0] > 0


def test_definitions_declare_the_asset_and_its_resource():
    defs = postgres_export()

    assert (
        AssetKey("artist_stats_table")
        in defs.resolve_asset_graph().get_all_asset_keys()
    )
    assert "postgres" in defs.resources


def test_database_outage_only_fails_the_export(tmp_path):
    unreachable = PostgresResource(port=1, password="inutile")

    result = _run(tmp_path, "2024-01-15", unreachable)

    assert not result.success
    assert (tmp_path / "daily_artist_stats" / "2024-01-15.parquet").exists()
    assert [e.step_key for e in result.all_events if e.is_step_failure] == [
        "artist_stats_table"
    ]
