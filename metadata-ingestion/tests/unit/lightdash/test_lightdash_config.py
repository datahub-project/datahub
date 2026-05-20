"""Tests for LightdashSourceConfig — parsing, validators, warehouse-platform resolution."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from datahub.ingestion.source.lightdash.config import (
    PLATFORM_NAME,
    WAREHOUSE_TYPE_TO_PLATFORM,
    LightdashSourceConfig,
)


def _base_config(**overrides):
    cfg = {
        "connection": {
            "base_url": "https://lightdash.example.com/",
            "personal_access_token": "ldpat_test",
        },
    }
    cfg.update(overrides)
    return cfg


class TestConnectionConfig:
    def test_base_url_must_be_http(self):
        with pytest.raises(ValidationError) as excinfo:
            LightdashSourceConfig.model_validate(
                _base_config(
                    connection={
                        "base_url": "ftp://x",
                        "personal_access_token": "ldpat_x",
                    }
                )
            )
        assert "http://" in str(excinfo.value)

    def test_base_url_trailing_slash_stripped(self):
        cfg = LightdashSourceConfig.model_validate(_base_config())
        assert cfg.connection.base_url == "https://lightdash.example.com"

    def test_pat_is_secret(self):
        cfg = LightdashSourceConfig.model_validate(_base_config())
        # SecretStr round-trips through .get_secret_value(); the string repr is masked.
        assert cfg.connection.personal_access_token.get_secret_value() == "ldpat_test"
        assert "ldpat_test" not in str(cfg.connection)


class TestWarehouseResolution:
    @pytest.mark.parametrize(
        "warehouse_type",
        sorted(WAREHOUSE_TYPE_TO_PLATFORM.keys()),
    )
    def test_known_warehouse_autodetected(self, warehouse_type):
        cfg = LightdashSourceConfig.model_validate(_base_config())
        platform, auto = cfg.resolve_warehouse_platform(warehouse_type)
        assert platform == WAREHOUSE_TYPE_TO_PLATFORM[warehouse_type]
        assert auto is True

    def test_unknown_warehouse_falls_back_to_lightdash(self):
        cfg = LightdashSourceConfig.model_validate(_base_config())
        platform, auto = cfg.resolve_warehouse_platform("teradata_xyz")
        assert platform == PLATFORM_NAME
        assert auto is True

    def test_explicit_override_wins(self):
        cfg = LightdashSourceConfig.model_validate(
            _base_config(warehouse_platform="snowflake_eu")
        )
        platform, auto = cfg.resolve_warehouse_platform("clickhouse")
        assert platform == "snowflake_eu"
        assert auto is False

    def test_none_warehouse_type(self):
        cfg = LightdashSourceConfig.model_validate(_base_config())
        platform, auto = cfg.resolve_warehouse_platform(None)
        assert platform == PLATFORM_NAME
        assert auto is True


class TestDefaults:
    def test_default_env_prod(self):
        cfg = LightdashSourceConfig.model_validate(_base_config())
        assert cfg.env == "PROD"

    def test_default_patterns_allow_all(self):
        cfg = LightdashSourceConfig.model_validate(_base_config())
        for p in (
            cfg.project_pattern,
            cfg.space_pattern,
            cfg.dashboard_pattern,
            cfg.chart_pattern,
        ):
            assert p.allowed("anything")

    def test_default_feature_flags(self):
        cfg = LightdashSourceConfig.model_validate(_base_config())
        assert cfg.extract_owners is True
        assert cfg.extract_lineage is True
        assert cfg.include_organization_container is False
