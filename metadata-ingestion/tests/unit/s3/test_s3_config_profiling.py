"""Unit tests for S3 config profiling validation."""

import sys
from unittest.mock import patch

import pytest

from datahub.ingestion.source.s3.config import DataLakeSourceConfig


class TestS3ConfigProfilingValidation:
    """Tests for S3 config profiling dependency validation."""

    def test_config_without_profiling(self):
        config_dict: dict = {
            "path_specs": [
                {
                    "include": "s3://test-bucket/data/*.parquet",
                }
            ],
            "profiling": {"enabled": False},
        }

        config = DataLakeSourceConfig.parse_obj(config_dict)

        assert config is not None
        assert config.platform == "s3"
        assert config.profiling.enabled is False

    def test_config_with_profiling_when_pyspark_available(self):
        config_dict: dict = {
            "path_specs": [
                {
                    "include": "s3://test-bucket/data/*.parquet",
                }
            ],
            "profiling": {"enabled": True},
        }

        config = DataLakeSourceConfig.parse_obj(config_dict)

        assert config is not None
        assert config.profiling.enabled is True

    def test_config_with_profiling_accepts_without_pyspark(self):
        """Test that config accepts profiling even without PySpark (backward compatibility).

        Note: In the default s3 installation, PySpark is included.
        When using s3-slim/, profiling will be disabled at runtime
        with appropriate warnings, but config validation does not fail.
        """
        with patch.dict(
            sys.modules,
            {
                "pyspark": None,
                "pyspark.sql": None,
                "pyspark.sql.dataframe": None,
                "pyspark.sql.types": None,
                "pydeequ": None,
                "pydeequ.analyzers": None,
            },
        ):
            config_dict: dict = {
                "path_specs": [
                    {
                        "include": "s3://test-bucket/data/*.parquet",
                    }
                ],
                "profiling": {"enabled": True},
            }

            # Config validation should succeed - PySpark validation removed for backward compatibility
            config = DataLakeSourceConfig.parse_obj(config_dict)

            assert config is not None
            assert config.profiling.enabled is True

    def test_config_spark_settings(self):
        config_dict: dict = {
            "path_specs": [
                {
                    "include": "s3://test-bucket/data/*.parquet",
                }
            ],
            "spark_driver_memory": "8g",
            "spark_config": {
                "spark.executor.memory": "4g",
                "spark.sql.shuffle.partitions": "200",
            },
            "profiling": {"enabled": False},
        }

        config = DataLakeSourceConfig.parse_obj(config_dict)

        assert config is not None
        assert config.spark_driver_memory == "8g"
        assert config.spark_config["spark.executor.memory"] == "4g"


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


class TestS3ConfigEdgeCases:
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
