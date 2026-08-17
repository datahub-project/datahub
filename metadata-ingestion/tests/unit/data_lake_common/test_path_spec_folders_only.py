import pytest

from datahub.configuration.common import AllowDenyPattern
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


@pytest.mark.parametrize(
    "kwargs",
    [
        {"table_name": "foo"},
        {"default_extension": "csv"},
        {"file_types": ["csv"]},
        {"tables_filter_pattern": AllowDenyPattern(allow=["foo"])},
    ],
)
def test_emit_folders_only_rejects_file_dataset_fields(kwargs):
    # File/dataset-selection fields have no effect in folders-only mode and are
    # rejected rather than silently ignored.
    with pytest.raises(ValueError, match="emit_folders_only"):
        PathSpec(include="s3://bucket/media/*/", emit_folders_only=True, **kwargs)


def test_emit_folders_only_allows_folder_filters():
    # exclude and include_hidden_folders DO apply to the folder walk -> must be accepted.
    spec = PathSpec(
        include="s3://bucket/media/*/",
        emit_folders_only=True,
        exclude=["s3://bucket/media/tmp/**"],
        include_hidden_folders=True,
    )
    assert spec.exclude == ["s3://bucket/media/tmp/**"]
    assert spec.include_hidden_folders is True


def test_folder_allowed_applies_hidden_and_exclude():
    spec = PathSpec(
        include="s3://bucket/media/*/",
        emit_folders_only=True,
        exclude=["s3://bucket/media/tmp/**"],
    )
    assert spec.folder_allowed("s3://bucket/media/keep")
    assert not spec.folder_allowed("s3://bucket/media/tmp/2024")  # excluded subtree
    assert not spec.folder_allowed("s3://bucket/media/_staging")  # hidden by default
