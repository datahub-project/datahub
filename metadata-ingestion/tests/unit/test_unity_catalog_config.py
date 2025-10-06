from datetime import datetime, timedelta

import pytest
from freezegun import freeze_time

from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.source import UnityCatalogSource

FROZEN_TIME = datetime.fromisoformat("2023-01-01 00:00:00+00:00")


@freeze_time(FROZEN_TIME)
def test_within_thirty_days():
    config = UnityCatalogSourceConfig.parse_obj(
        {
            "token": "token",
            "workspace_url": "https://workspace_url",
            "include_usage_statistics": True,
            "include_hive_metastore": False,
            "include_tags": False,
            "start_time": FROZEN_TIME - timedelta(days=30),
        }
    )
    assert config.start_time == FROZEN_TIME - timedelta(days=30)

    with pytest.raises(
        ValueError, match="Query history is only maintained for 30 days."
    ):
        UnityCatalogSourceConfig.parse_obj(
            {
                "token": "token",
                "workspace_url": "https://workspace_url",
                "include_usage_statistics": True,
                "start_time": FROZEN_TIME - timedelta(days=31),
            }
        )


def test_profiling_requires_warehouses_id():
    config = UnityCatalogSourceConfig.parse_obj(
        {
            "token": "token",
            "workspace_url": "https://workspace_url",
            "include_hive_metastore": False,
            "profiling": {
                "enabled": True,
                "method": "ge",
                "warehouse_id": "my_warehouse_id",
            },
        }
    )
    assert config.profiling.enabled is True

    config = UnityCatalogSourceConfig.parse_obj(
        {
            "token": "token",
            "workspace_url": "https://workspace_url",
            "include_hive_metastore": False,
            "include_tags": False,
            "profiling": {"enabled": False, "method": "ge"},
        }
    )
    assert config.profiling.enabled is False

    with pytest.raises(ValueError):
        UnityCatalogSourceConfig.parse_obj(
            {
                "token": "token",
                "include_hive_metastore": False,
                "include_tags": False,
                "workspace_url": "workspace_url",
            }
        )


@freeze_time(FROZEN_TIME)
def test_workspace_url_should_start_with_https():
    with pytest.raises(ValueError, match="Workspace URL must start with http scheme"):
        UnityCatalogSourceConfig.parse_obj(
            {
                "token": "token",
                "workspace_url": "workspace_url",
                "profiling": {"enabled": True},
            }
        )


def test_global_warehouse_id_is_set_from_profiling():
    config = UnityCatalogSourceConfig.parse_obj(
        {
            "token": "token",
            "workspace_url": "https://XXXXXXXXXXXXXXXXXXXXX",
            "profiling": {
                "method": "ge",
                "enabled": True,
                "warehouse_id": "my_warehouse_id",
            },
        }
    )
    assert config.profiling.warehouse_id == "my_warehouse_id"
    assert config.warehouse_id == "my_warehouse_id"


def test_set_different_warehouse_id_from_profiling():
    with pytest.raises(
        ValueError,
        match="When `warehouse_id` is set, it must match the `warehouse_id` in `profiling`.",
    ):
        UnityCatalogSourceConfig.parse_obj(
            {
                "token": "token",
                "workspace_url": "https://XXXXXXXXXXXXXXXXXXXXX",
                "warehouse_id": "my_global_warehouse_id",
                "profiling": {
                    "method": "ge",
                    "enabled": True,
                    "warehouse_id": "my_warehouse_id",
                },
            }
        )


def test_warehouse_id_must_be_set_if_include_hive_metastore_is_true():
    """Test that include_hive_metastore is auto-disabled when warehouse_id is missing."""
    config = UnityCatalogSourceConfig.parse_obj(
        {
            "token": "token",
            "workspace_url": "https://XXXXXXXXXXXXXXXXXXXXX",
            "include_hive_metastore": True,
        }
    )
    # Should automatically disable hive_metastore when warehouse_id is missing
    assert config.include_hive_metastore is False
    assert config.warehouse_id is None


@pytest.mark.skip(
    reason="This test is making actual network calls with retries taking ~5 mins, needs to be mocked"
)
def test_warehouse_id_must_be_present_test_connection():
    """Test that connection succeeds when hive_metastore gets auto-disabled."""
    config_dict = {
        "token": "token",
        "workspace_url": "https://XXXXXXXXXXXXXXXXXXXXX",
        "include_hive_metastore": True,  # Will be auto-disabled
    }
    report = UnityCatalogSource.test_connection(config_dict)
    # Should succeed since include_hive_metastore gets auto-disabled
    assert not report.internal_failure


def test_set_profiling_warehouse_id_from_global():
    config = UnityCatalogSourceConfig.parse_obj(
        {
            "token": "token",
            "workspace_url": "https://XXXXXXXXXXXXXXXXXXXXX",
            "warehouse_id": "my_global_warehouse_id",
            "profiling": {
                "method": "ge",
                "enabled": True,
            },
        }
    )
    assert config.profiling.warehouse_id == "my_global_warehouse_id"


def test_warehouse_id_auto_disables_tags_when_missing():
    """Test that include_tags is automatically disabled when warehouse_id is missing."""
    config = UnityCatalogSourceConfig.parse_obj(
        {
            "token": "token",
            "workspace_url": "https://test.databricks.com",
            "include_hive_metastore": False,  # Disable to test tag validation specifically
            # include_tags defaults to True, warehouse_id is missing
        }
    )
    # Should automatically disable tags when warehouse_id is missing
    assert config.include_tags is False
    assert config.warehouse_id is None


def test_warehouse_id_not_required_when_tags_disabled():
    """Test that warehouse_id is not required when include_tags=False."""
    config = UnityCatalogSourceConfig.parse_obj(
        {
            "token": "token",
            "workspace_url": "https://test.databricks.com",
            "include_hive_metastore": False,
            "include_tags": False,  # Explicitly disable tags
            # warehouse_id is missing but should be allowed
        }
    )
    assert config.include_tags is False
    assert config.warehouse_id is None


def test_warehouse_id_explicit_true_auto_disables():
    """Test that explicitly setting include_tags=True gets auto-disabled when warehouse_id is missing."""
    config = UnityCatalogSourceConfig.parse_obj(
        {
            "token": "token",
            "workspace_url": "https://test.databricks.com",
            "include_hive_metastore": False,
            "include_tags": True,  # Explicitly enable tags
            # warehouse_id is missing
        }
    )
    # Should automatically disable tags even when explicitly set to True
    assert config.include_tags is False
    assert config.warehouse_id is None


def test_warehouse_id_with_tags_enabled_succeeds():
    """Test that providing warehouse_id with include_tags=True succeeds."""
    config = UnityCatalogSourceConfig.parse_obj(
        {
            "token": "token",
            "workspace_url": "https://test.databricks.com",
            "include_hive_metastore": False,
            "include_tags": True,
            "warehouse_id": "test_warehouse_123",
        }
    )
    assert config.include_tags is True
    assert config.warehouse_id == "test_warehouse_123"


def test_warehouse_id_validation_with_hive_metastore_precedence():
    """Test that both hive_metastore and tags are auto-disabled when warehouse_id is missing."""
    config = UnityCatalogSourceConfig.parse_obj(
        {
            "token": "token",
            "workspace_url": "https://test.databricks.com",
            "include_hive_metastore": True,  # Should be auto-disabled
            "include_tags": True,  # Should be auto-disabled
            # warehouse_id is missing
        }
    )
    # Both should be auto-disabled when warehouse_id is missing
    assert config.include_hive_metastore is False
    assert config.include_tags is False
    assert config.warehouse_id is None


def test_databricks_api_page_size_default():
    """Test that databricks_api_page_size defaults to 0."""
    config = UnityCatalogSourceConfig.parse_obj(
        {
            "token": "token",
            "workspace_url": "https://test.databricks.com",
            "include_hive_metastore": False,
            "include_tags": False,
        }
    )
    assert config.databricks_api_page_size == 0


def test_databricks_api_page_size_valid_values():
    """Test that databricks_api_page_size accepts valid positive integers."""
    config = UnityCatalogSourceConfig.parse_obj(
        {
            "token": "token",
            "workspace_url": "https://test.databricks.com",
            "include_hive_metastore": False,
            "include_tags": False,
            "databricks_api_page_size": 100,
        }
    )
    assert config.databricks_api_page_size == 100

    config = UnityCatalogSourceConfig.parse_obj(
        {
            "token": "token",
            "workspace_url": "https://test.databricks.com",
            "include_hive_metastore": False,
            "include_tags": False,
            "databricks_api_page_size": 1000,
        }
    )
    assert config.databricks_api_page_size == 1000


def test_databricks_api_page_size_zero_allowed():
    """Test that databricks_api_page_size allows zero (default behavior)."""
    config = UnityCatalogSourceConfig.parse_obj(
        {
            "token": "token",
            "workspace_url": "https://test.databricks.com",
            "include_hive_metastore": False,
            "include_tags": False,
            "databricks_api_page_size": 0,
        }
    )
    assert config.databricks_api_page_size == 0


def test_databricks_api_page_size_negative_invalid():
    """Test that databricks_api_page_size rejects negative values."""
    with pytest.raises(ValueError, match="Input should be greater than or equal to 0"):
        UnityCatalogSourceConfig.parse_obj(
            {
                "token": "token",
                "workspace_url": "https://test.databricks.com",
                "include_hive_metastore": False,
                "include_tags": False,
                "databricks_api_page_size": -1,
            }
        )

    with pytest.raises(ValueError, match="Input should be greater than or equal to 0"):
        UnityCatalogSourceConfig.parse_obj(
            {
                "token": "token",
                "workspace_url": "https://test.databricks.com",
                "include_hive_metastore": False,
                "include_tags": False,
                "databricks_api_page_size": -100,
            }
        )


def test_include_ml_model_default():
    """Test that include_ml_model_aliases defaults to False."""
    config = UnityCatalogSourceConfig.parse_obj(
        {
            "token": "token",
            "workspace_url": "https://test.databricks.com",
            "include_hive_metastore": False,
        }
    )
    assert config.include_ml_model_aliases is False
    assert config.ml_model_max_results == 1000


def test_include_ml_model_aliases_explicit_true():
    """Test that include_ml_model_aliases can be set to True."""
    config = UnityCatalogSourceConfig.parse_obj(
        {
            "token": "token",
            "workspace_url": "https://test.databricks.com",
            "include_hive_metastore": False,
            "include_ml_model_aliases": True,
        }
    )
    assert config.include_ml_model_aliases is True


def test_ml_model_max_results_valid_values():
    """Test that ml_model_max_results accepts valid positive integers."""
    config = UnityCatalogSourceConfig.parse_obj(
        {
            "token": "token",
            "workspace_url": "https://test.databricks.com",
            "include_hive_metastore": False,
            "ml_model_max_results": 2000,
        }
    )
    assert config.ml_model_max_results == 2000

    config = UnityCatalogSourceConfig.parse_obj(
        {
            "token": "token",
            "workspace_url": "https://test.databricks.com",
            "include_hive_metastore": False,
            "ml_model_max_results": 1,
        }
    )
    assert config.ml_model_max_results == 1


def test_ml_model_max_results_negative_invalid():
    """Test that ml_model_max_results rejects negative values."""
    with pytest.raises(ValueError):
        UnityCatalogSourceConfig.parse_obj(
            {
                "token": "token",
                "workspace_url": "https://test.databricks.com",
                "include_hive_metastore": False,
                "ml_model_max_results": -100,
            }
        )
