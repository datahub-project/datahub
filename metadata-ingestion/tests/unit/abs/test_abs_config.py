"""Unit tests for ABS DataLakeSourceConfig validation."""

import pytest

from datahub.ingestion.source.abs.config import DataLakeSourceConfig


def test_abs_config_accepts_dict_path_specs_and_infers_abs_platform():
    config_dict = {
        "path_specs": [
            {
                "include": "https://acct.blob.core.windows.net/container/{table}/*.csv",
                "file_types": ["csv"],
            }
        ],
        "azure_config": {
            "account_name": "acct",
            "container_name": "container",
            "account_key": "dummy",
        },
    }

    config = DataLakeSourceConfig.model_validate(config_dict)

    assert config.platform == "abs"
    assert len(config.path_specs) == 1
    assert config.path_specs[0].is_abs is True


def test_abs_config_accepts_dict_path_specs_and_infers_file_platform():
    config_dict = {
        "path_specs": [
            {
                "include": "/var/lib/data/{table}/*.parquet",
                "file_types": ["parquet"],
            }
        ],
    }

    config = DataLakeSourceConfig.model_validate(config_dict)

    assert config.platform == "file"
    assert config.path_specs[0].is_abs is False


def test_abs_config_rejects_abs_specific_flags_for_file_platform():
    config_dict = {
        "path_specs": [
            {
                "include": "/var/lib/data/{table}/*.csv",
                "file_types": ["csv"],
            }
        ],
        "use_abs_container_properties": True,
    }

    with pytest.raises(
        ValueError,
        match="Cannot grab abs blob/container tags when platform is not abs",
    ):
        DataLakeSourceConfig.model_validate(config_dict)
