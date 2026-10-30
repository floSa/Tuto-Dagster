from dagster import (
    Definitions,
    load_asset_checks_from_modules,
    load_assets_from_modules,
)

from .assets import analytics, ingestion, transformation
from .jobs import daily_streamify_job
from .resources import LocalFilesystemIOManager
from .schedules import daily_streamify_schedule
from .sensors import alert_on_run_failure, inbox_file_sensor
from .settings import BASE_DATA_PATH

assets = load_assets_from_modules([ingestion, transformation, analytics])
asset_checks = load_asset_checks_from_modules([transformation])


defs = Definitions(
    assets=assets,
    asset_checks=asset_checks,
    resources={
        "io_manager": LocalFilesystemIOManager(base_path=str(BASE_DATA_PATH)),
    },
    jobs=[daily_streamify_job],
    schedules=[daily_streamify_schedule],
    sensors=[inbox_file_sensor, alert_on_run_failure],
)
