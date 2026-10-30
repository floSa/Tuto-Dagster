import os
from typing import Optional

import pandas as pd
from dagster import (
    ConfigurableIOManager,
    InputContext,
    MetadataValue,
    OutputContext,
    TableColumn,
    TableSchema,
)


class LocalFilesystemIOManager(ConfigurableIOManager):
    """
    IO manager local pour separer le calcul (assets) du stockage (fichiers).
    L'asset renvoie un DataFrame, ce manager se charge de l'ecriture/lecture.
    """

    base_path: str

    def _get_path(self, asset_key_path: str, partition_key: Optional[str]) -> str:
        if partition_key:
            return os.path.join(
                self.base_path, asset_key_path, f"{partition_key}.parquet"
            )
        return os.path.join(self.base_path, f"{asset_key_path}.parquet")

    def handle_output(self, context: OutputContext, obj: pd.DataFrame) -> None:
        asset_key_path = "_".join(context.asset_key.path)
        partition_key = context.partition_key if context.has_partition_key else None
        path = self._get_path(asset_key_path, partition_key)

        os.makedirs(os.path.dirname(path), exist_ok=True)

        if not isinstance(obj, pd.DataFrame):
            raise TypeError("LocalFilesystemIOManager ne supporte que pandas.DataFrame")

        obj.to_parquet(path, index=False)
        columns = [TableColumn(name, str(obj[name].dtype)) for name in obj.columns]
        context.add_output_metadata(
            {
                "dagster/uri": MetadataValue.path(path),
                "dagster/row_count": len(obj),
                "dagster/column_schema": TableSchema(columns=columns),
            }
        )

    def load_input(self, context: InputContext) -> pd.DataFrame:
        asset_key = getattr(context, "asset_key", None)
        if asset_key is None and context.upstream_output is not None:
            asset_key = context.upstream_output.asset_key

        if asset_key is None:
            raise ValueError("asset_key introuvable pour load_input")

        asset_key_path = "_".join(asset_key.path)
        if not context.has_asset_partitions:
            return pd.read_parquet(self._get_path(asset_key_path, None))

        keys = context.asset_partition_keys
        if len(keys) == 1:
            return pd.read_parquet(self._get_path(asset_key_path, keys[0]))

        # Fenetre de plusieurs partitions: les jours pas encore produits sont ignores.
        paths = [self._get_path(asset_key_path, key) for key in keys]
        frames = [pd.read_parquet(path) for path in paths if os.path.exists(path)]
        return pd.concat(frames, ignore_index=True)
