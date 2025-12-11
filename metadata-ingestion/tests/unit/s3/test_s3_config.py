# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import pytest

from datahub.ingestion.source.s3.config import DataLakeSourceConfig


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
