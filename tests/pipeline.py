from streamify_ops.defs.assets.analytics import daily_artist_stats
from streamify_ops.defs.assets.ingestion import raw_tracks_data
from streamify_ops.defs.assets.transformation import (
    clean_tracks,
    clean_tracks_id_not_null,
)

assets = [raw_tracks_data, clean_tracks, daily_artist_stats]
asset_checks = [clean_tracks_id_not_null]
