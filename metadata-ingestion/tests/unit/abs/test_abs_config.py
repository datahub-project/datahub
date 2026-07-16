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


def test_abs_config_accepts_dict_path_specs_with_explicit_platform_match():
    """Test that config accepts dict path_specs when explicit platform matches inferred."""
    config_dict = {
        "path_specs": [
            {
                "include": "https://acct.blob.core.windows.net/container/{table}/*.csv",
                "file_types": ["csv"],
            }
        ],
        "platform": "abs",
        "azure_config": {
            "account_name": "acct",
            "container_name": "container",
            "account_key": "dummy",
        },
    }

    config = DataLakeSourceConfig.model_validate(config_dict)
    assert config.platform == "abs"
    assert config.path_specs[0].is_abs is True


def test_abs_config_rejects_dict_path_specs_with_platform_mismatch():
    """Test that config raises ValueError when explicit platform contradicts path_specs."""
    config_dict = {
        "path_specs": [
            {
                "include": "/var/lib/data/{table}/*.csv",
                "file_types": ["csv"],
            }
        ],
        "platform": "abs",
    }

    with pytest.raises(
        ValueError,
        match="All path_specs belong to file platform, but platform is set to abs",
    ):
        DataLakeSourceConfig.model_validate(config_dict)


def test_abs_config_rejects_mixed_abs_file_uris():
    """Test that config raises ValueError when path_specs contain both ABS and file URIs."""
    config_dict = {
        "path_specs": [
            {
                "include": "https://acct.blob.core.windows.net/container/{table}/*.csv",
                "file_types": ["csv"],
            },
            {
                "include": "/var/lib/data/{table}/*.parquet",
                "file_types": ["parquet"],
            },
        ],
        "azure_config": {
            "account_name": "acct",
            "container_name": "container",
            "account_key": "dummy",
        },
    }

    with pytest.raises(
        ValueError,
        match="Cannot have multiple platforms in path_specs",
    ):
        DataLakeSourceConfig.model_validate(config_dict)


def test_abs_config_rejects_empty_path_specs():
    """Test that config raises ValueError for empty path_specs array."""
    config_dict: dict = {
        "path_specs": [],
    }

    with pytest.raises(
        ValueError,
        match="path_specs must not be empty",
    ):
        DataLakeSourceConfig.model_validate(config_dict)
