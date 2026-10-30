from dagster import Definitions, definitions

from ..resources import LocalFilesystemIOManager
from ..settings import BASE_DATA_PATH


@definitions
def resources() -> Definitions:
    return Definitions(
        resources={
            "io_manager": LocalFilesystemIOManager(base_path=str(BASE_DATA_PATH)),
        }
    )
