from typing import List, cast

import pytest
from pydantic import ValidationError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec
from datahub.ingestion.source.s3.config import DataLakeSourceConfig
from datahub.ingestion.source.s3.datalake_profiler_config import (
    DataLakeProfilerConfig,
)
from datahub.ingestion.source.s3.source import S3Source


def test_incorrect_config_raises_error():
    """Reject invalid path specs at config time (bad table var, exclude var, unsupported
    file type, ``**`` in include)."""
    ctx = PipelineContext(run_id="test-s3")

    # Baseline: valid config
    source: dict = {
        "path_spec": {"include": "a/b/c/d/{table}.*", "table_name": "{table}"}
    }
    s3 = S3Source.create(source, ctx)
    assert s3.source_config.platform == "file"

    # Case 1 : named variable in table name is not present in include
    source = {"path_spec": {"include": "a/b/c/d/{table}.*", "table_name": "{table1}"}}
    with pytest.raises(ValidationError, match="table_name"):
        S3Source.create(source, ctx)

    # Case 2 : named variable in exclude is not allowed
    source = {
        "path_spec": {
            "include": "a/b/c/d/{table}/*.*",
            "exclude": ["a/b/c/d/a-{exclude}/**"],
        },
    }
    with pytest.raises(ValidationError, match=r"exclude.*named variable"):
        S3Source.create(source, ctx)

    # Case 3 : unsupported file type not allowed
    source = {"path_spec": {"include": "a/b/c/d/{table}/*.hd5"}}
    with pytest.raises(ValidationError, match="file type"):
        S3Source.create(source, ctx)

    # Case 4 : ** in include not allowed
    source = {"path_spec": {"include": "a/b/c/d/**/*.*"}}
    with pytest.raises(ValidationError, match=r"\*\*"):
        S3Source.create(source, ctx)


class TestS3Config:
    def test_config_platform_inference(self):
        config_dict: dict = {
            "path_specs": [
                {
                    "include": "s3://test-bucket/data/*.parquet",
                }
            ],
        }

        config = DataLakeSourceConfig.parse_obj(config_dict)

        assert config.platform == "s3"

    def test_empty_path_specs_fails(self):
        config_dict: dict = {
            "path_specs": [],
        }

        with pytest.raises(ValueError) as exc_info:
            DataLakeSourceConfig.parse_obj(config_dict)

        assert "path_specs must not be empty" in str(exc_info.value)

    def test_mixed_platform_path_specs_fails(self):
        config_dict: dict = {
            "path_specs": [
                {"include": "s3://bucket/data/*.parquet"},
                {"include": "file:///local/path/*.csv"},
            ],
        }

        with pytest.raises(ValueError) as exc_info:
            DataLakeSourceConfig.parse_obj(config_dict)

        assert "Cannot have multiple platforms" in str(exc_info.value)

    @pytest.mark.parametrize(
        "flag", ["use_s3_bucket_tags", "use_s3_object_tags", "use_s3_content_type"]
    )
    def test_s3_only_flags_are_rejected_for_local_platform(self, flag: str) -> None:
        """S3 tag and content-type lookups have nothing to read on a local
        platform, so enabling them is a config error rather than a silent no-op."""
        with pytest.raises(ValueError, match="platform is not s3"):
            DataLakeSourceConfig.parse_obj(
                {"path_specs": [{"include": "/local/path/*.csv"}], flag: True}
            )

    def test_explicit_platform_is_not_second_guessed_from_uris(self) -> None:
        """An explicit platform wins over the one inferred from path URIs (GCS
        through the S3 API keeps s3:// includes)."""
        config = DataLakeSourceConfig.parse_obj(
            {"platform": "gcs", "path_specs": [{"include": "s3://bkt/data/*.csv"}]}
        )

        assert config.platform == "gcs"
        assert cast(List[PathSpec], config.path_specs)[0].include.startswith("s3://")

    def test_enable_schema_inference_defaults_to_true(self):
        config = DataLakeSourceConfig.parse_obj(
            {"path_specs": [{"include": "s3://bucket/data/*.parquet"}]}
        )
        assert config.enable_schema_inference is True

    def test_enable_schema_inference_can_be_disabled(self):
        config = DataLakeSourceConfig.parse_obj(
            {
                "path_specs": [{"include": "s3://bucket/data/*.parquet"}],
                "enable_schema_inference": False,
            }
        )
        assert config.enable_schema_inference is False

    def test_add_partition_columns_rejected_without_schema_inference(self):
        with pytest.raises(
            ValidationError, match="add_partition_columns_to_schema has no effect"
        ):
            DataLakeSourceConfig.parse_obj(
                {
                    "path_specs": [{"include": "s3://bucket/data/*.parquet"}],
                    "enable_schema_inference": False,
                    "add_partition_columns_to_schema": True,
                }
            )


def test_table_level_only_profiling_disables_field_metrics() -> None:
    config = DataLakeProfilerConfig(enabled=True, profile_table_level_only=True)

    assert not config.include_field_null_count
    assert not config.include_field_min_value
    assert not config.include_field_sample_values
