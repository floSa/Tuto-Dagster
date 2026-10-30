from streamify_ops import defs


def test_quality_check_is_registered():
    check_names = {key.name for key in defs.resolve_asset_graph().asset_check_keys}

    assert check_names == {"clean_tracks_id_not_null"}
