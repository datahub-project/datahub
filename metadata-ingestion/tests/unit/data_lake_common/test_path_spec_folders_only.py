import pytest

from datahub.ingestion.source.data_lake_common.path_spec import PathSpec


def test_emit_folders_only_defaults_false():
    spec = PathSpec(include="s3://bucket/media/{table}/*.csv")
    assert spec.emit_folders_only is False


def test_emit_folders_only_accepts_folder_glob():
    spec = PathSpec(include="s3://bucket/media/*/*/", emit_folders_only=True)
    assert spec.emit_folders_only is True
    # depth is defined by the wildcard levels in the glob
    assert spec.glob_include == "s3://bucket/media/*/*/"


def test_emit_folders_only_rejects_table_marker():
    with pytest.raises(ValueError, match="emit_folders_only"):
        PathSpec(include="s3://bucket/media/{table}/", emit_folders_only=True)


def test_emit_folders_only_rejects_double_star():
    with pytest.raises(ValueError, match="emit_folders_only"):
        PathSpec(
            include="s3://bucket/media/**",
            emit_folders_only=True,
            allow_double_stars=True,
        )
