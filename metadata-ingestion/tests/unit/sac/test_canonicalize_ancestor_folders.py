from datahub.ingestion.source.sac.sac import _canonicalize_ancestor_folders


def test_rewrites_localized_public_root():
    # SAC localizes only the built-in root (first segment); user sub-folders are untouched.
    assert _canonicalize_ancestor_folders(
        ["Öffentlich", "Folder 1", "Folder 2"], "Public"
    ) == ["Public", "Folder 1", "Folder 2"]
    assert _canonicalize_ancestor_folders(
        ["Public", "Folder 1", "Folder 2"], "Public"
    ) == ["Public", "Folder 1", "Folder 2"]
    assert _canonicalize_ancestor_folders(["Öffentlich"], "Public") == ["Public"]
    assert _canonicalize_ancestor_folders(["Öffentlich", "Folder 1"], "Shared") == [
        "Shared",
        "Folder 1",
    ]


def test_respects_disabled_and_empty():
    assert _canonicalize_ancestor_folders(["Öffentlich", "Folder 1"], None) == [
        "Öffentlich",
        "Folder 1",
    ]
    assert _canonicalize_ancestor_folders([], "Public") == []
