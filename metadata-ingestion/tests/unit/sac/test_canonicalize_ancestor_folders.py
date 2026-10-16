from datahub.ingestion.source.sac.sac import _canonicalize_ancestor_folders

_ALIASES = ["Public", "Öffentlich"]


def test_rewrites_localized_public_root():
    # SAC localizes only the built-in root (first segment); user sub-folders are untouched.
    assert _canonicalize_ancestor_folders(
        ["Öffentlich", "Folder 1", "Folder 2"], "Public", _ALIASES
    ) == ["Public", "Folder 1", "Folder 2"]
    assert _canonicalize_ancestor_folders(
        ["Public", "Folder 1", "Folder 2"], "Public", _ALIASES
    ) == ["Public", "Folder 1", "Folder 2"]
    assert _canonicalize_ancestor_folders(["Öffentlich"], "Public", _ALIASES) == [
        "Public"
    ]
    assert _canonicalize_ancestor_folders(
        ["Öffentlich", "Folder 1"], "Shared", _ALIASES
    ) == ["Shared", "Folder 1"]


def test_matches_aliases_case_insensitively():
    assert _canonicalize_ancestor_folders(["öffentlich"], "Public", _ALIASES) == [
        "Public"
    ]


def test_leaves_unrecognized_root_untouched():
    # `isPublic eq 1` can also return a team/workspace root (e.g. "Default"); it must not
    # be merged into the public tree.
    assert _canonicalize_ancestor_folders(
        ["Default", "Folder 1"], "Public", _ALIASES
    ) == ["Default", "Folder 1"]


def test_respects_disabled_and_empty():
    assert _canonicalize_ancestor_folders(
        ["Öffentlich", "Folder 1"], None, _ALIASES
    ) == ["Öffentlich", "Folder 1"]
    assert _canonicalize_ancestor_folders([], "Public", _ALIASES) == []
