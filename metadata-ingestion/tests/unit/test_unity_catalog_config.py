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
            "profiling": {"enabled": False, "method": "ge"},
        }
    )
    assert config.profiling.enabled is False

    with pytest.raises(ValueError):
        UnityCatalogSourceConfig.parse_obj(
            {
                "token": "token",
                "include_hive_metastore": False,
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
    with pytest.raises(
        ValueError,
        match="When `include_hive_metastore` is set, `warehouse_id` must be set.",
    ):
        UnityCatalogSourceConfig.parse_obj(
            {
                "token": "token",
                "workspace_url": "https://XXXXXXXXXXXXXXXXXXXXX",
                "include_hive_metastore": True,
            }
        )


def test_warehouse_id_must_be_present_test_connection():
    config_dict = {
        "token": "token",
        "workspace_url": "https://XXXXXXXXXXXXXXXXXXXXX",
        "include_hive_metastore": True,
    }
    report = UnityCatalogSource.test_connection(config_dict)
    assert report.internal_failure
    print(report.internal_failure_reason)


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
