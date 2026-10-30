"""Réglages lus dans l'environnement."""

import os
from datetime import timedelta
from pathlib import Path

BASE_DATA_PATH = Path(
    os.environ.get("STREAMIFY_DATA_DIR", Path(__file__).resolve().parents[1] / "data")
)

# Accélère le temps pour observer le cron et la fraîcheur en quelques minutes.
FAST_AUTOMATION = os.environ.get("STREAMIFY_FAST_AUTOMATION") == "1"

RAW_TRACKS_CRON = "* * * * *" if FAST_AUTOMATION else "0 1 * * *"
FRESHNESS_WARN = timedelta(minutes=2) if FAST_AUTOMATION else timedelta(hours=26)
FRESHNESS_FAIL = timedelta(minutes=4) if FAST_AUTOMATION else timedelta(hours=48)
