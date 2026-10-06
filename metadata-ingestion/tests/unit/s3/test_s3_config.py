import pytest
from pydantic import ValidationError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.s3.config import DataLakeSourceConfig
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

    def test_s3_tags_with_non_s3_platform_fails(self):
        config_dict: dict = {
            "path_specs": [
                {"include": "file:///local/path/*.csv"},
            ],
            "use_s3_bucket_tags": True,
        }

        with pytest.raises(ValueError) as exc_info:
            DataLakeSourceConfig.parse_obj(config_dict)

        error_msg = str(exc_info.value).lower()
        assert "s3 bucket tags" in error_msg and "platform is not s3" in error_msg
