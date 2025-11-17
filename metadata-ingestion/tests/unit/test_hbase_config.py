"""
Unit tests for HBase source configuration validation
"""

from typing import Any, Dict

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.hbase.hbase import HBaseSourceConfig


def _base_config() -> Dict[str, Any]:
    """Base configuration for HBase tests."""
    return {
        "host": "localhost",
        "port": 9090,
    }


class TestHBaseConfig:
    """Test configuration validation and initialization."""

    def test_valid_config(self):
        """Test that valid configuration is accepted."""
        config_dict = _base_config()
        config = HBaseSourceConfig.model_validate(config_dict)

        assert config.host == "localhost"
        assert config.port == 9090
        assert config.use_ssl is False
        assert config.timeout == 30000
        assert config.include_column_families is True
        assert config.env == "PROD"
        assert config.platform_instance is None

    def test_custom_port(self):
        """Test custom port configuration."""
        config_dict = {
            **_base_config(),
            "port": 9095,
        }
        config = HBaseSourceConfig.model_validate(config_dict)
        assert config.port == 9095

    def test_invalid_port_too_low(self):
        """Test that port below 1 is rejected."""
        config_dict = {
            **_base_config(),
            "port": 0,
        }
        with pytest.raises(ValueError, match="Port must be between 1 and 65535"):
            HBaseSourceConfig.model_validate(config_dict)

    def test_invalid_port_too_high(self):
        """Test that port above 65535 is rejected."""
        config_dict = {
            **_base_config(),
            "port": 65536,
        }
        with pytest.raises(ValueError, match="Port must be between 1 and 65535"):
            HBaseSourceConfig.model_validate(config_dict)

    def test_valid_port_edge_cases(self):
        """Test valid port edge cases (1 and 65535)."""
        # Test minimum valid port
        config_dict = {
            **_base_config(),
            "port": 1,
        }
        config = HBaseSourceConfig.model_validate(config_dict)
        assert config.port == 1

        # Test maximum valid port
        config_dict = {
            **_base_config(),
            "port": 65535,
        }
        config = HBaseSourceConfig.model_validate(config_dict)
        assert config.port == 65535

    def test_ssl_configuration(self):
        """Test SSL configuration."""
        config_dict = {
            **_base_config(),
            "use_ssl": True,
        }
        config = HBaseSourceConfig.model_validate(config_dict)
        assert config.use_ssl is True

    def test_timeout_configuration(self):
        """Test timeout configuration."""
        config_dict = {
            **_base_config(),
            "timeout": 60000,
        }
        config = HBaseSourceConfig.model_validate(config_dict)
        assert config.timeout == 60000

    def test_invalid_timeout_negative(self):
        """Test that negative timeout is rejected."""
        config_dict = {
            **_base_config(),
            "timeout": -1,
        }
        with pytest.raises(ValueError, match="Timeout must be positive"):
            HBaseSourceConfig.model_validate(config_dict)

    def test_invalid_timeout_zero(self):
        """Test that zero timeout is rejected."""
        config_dict = {
            **_base_config(),
            "timeout": 0,
        }
        with pytest.raises(ValueError, match="Timeout must be positive"):
            HBaseSourceConfig.model_validate(config_dict)

    def test_namespace_pattern_default(self):
        """Test namespace pattern defaults to allow all."""
        config_dict = _base_config()
        config = HBaseSourceConfig.model_validate(config_dict)
        assert isinstance(config.namespace_pattern, AllowDenyPattern)
        assert config.namespace_pattern.allowed("any_namespace")

    def test_namespace_pattern_custom(self):
        """Test custom namespace pattern."""
        config_dict = {
            **_base_config(),
            "namespace_pattern": {
                "allow": ["prod_.*"],
                "deny": ["prod_test"],
            },
        }
        config = HBaseSourceConfig.model_validate(config_dict)
        assert config.namespace_pattern.allowed("prod_main")
        assert not config.namespace_pattern.allowed("prod_test")
        assert not config.namespace_pattern.allowed("dev_main")

    def test_table_pattern_default(self):
        """Test table pattern defaults to allow all."""
        config_dict = _base_config()
        config = HBaseSourceConfig.model_validate(config_dict)
        assert isinstance(config.table_pattern, AllowDenyPattern)
        assert config.table_pattern.allowed("any_table")

    def test_table_pattern_custom(self):
        """Test custom table pattern."""
        config_dict = {
            **_base_config(),
            "table_pattern": {
                "allow": ["users_.*", "products_.*"],
                "deny": [".*_temp"],
            },
        }
        config = HBaseSourceConfig.model_validate(config_dict)
        assert config.table_pattern.allowed("users_main")
        assert config.table_pattern.allowed("products_catalog")
        assert not config.table_pattern.allowed("users_temp")
        assert not config.table_pattern.allowed("orders_main")

    def test_include_column_families_default(self):
        """Test include_column_families defaults to True."""
        config_dict = _base_config()
        config = HBaseSourceConfig.model_validate(config_dict)
        assert config.include_column_families is True

    def test_include_column_families_false(self):
        """Test include_column_families can be set to False."""
        config_dict = {
            **_base_config(),
            "include_column_families": False,
        }
        config = HBaseSourceConfig.model_validate(config_dict)
        assert config.include_column_families is False

    def test_env_default(self):
        """Test env defaults to PROD."""
        config_dict = _base_config()
        config = HBaseSourceConfig.model_validate(config_dict)
        assert config.env == "PROD"

    def test_env_custom(self):
        """Test custom env value."""
        config_dict = {
            **_base_config(),
            "env": "DEV",
        }
        config = HBaseSourceConfig.model_validate(config_dict)
        assert config.env == "DEV"

    def test_platform_instance_default(self):
        """Test platform_instance defaults to None."""
        config_dict = _base_config()
        config = HBaseSourceConfig.model_validate(config_dict)
        assert config.platform_instance is None

    def test_platform_instance_custom(self):
        """Test custom platform_instance value."""
        config_dict = {
            **_base_config(),
            "platform_instance": "hbase-cluster-1",
        }
        config = HBaseSourceConfig.model_validate(config_dict)
        assert config.platform_instance == "hbase-cluster-1"

    def test_full_configuration(self):
        """Test full configuration with all options."""
        config_dict = {
            "host": "hbase.example.com",
            "port": 9095,
            "use_ssl": True,
            "timeout": 45000,
            "namespace_pattern": {
                "allow": ["prod_.*"],
                "deny": ["prod_test"],
            },
            "table_pattern": {
                "allow": [".*"],
                "deny": [".*_backup"],
            },
            "include_column_families": True,
            "env": "PROD",
            "platform_instance": "hbase-prod-cluster",
        }
        config = HBaseSourceConfig.model_validate(config_dict)

        assert config.host == "hbase.example.com"
        assert config.port == 9095
        assert config.use_ssl is True
        assert config.timeout == 45000
        assert config.namespace_pattern.allowed("prod_main")
        assert not config.namespace_pattern.allowed("prod_test")
        assert config.table_pattern.allowed("users")
        assert not config.table_pattern.allowed("users_backup")
        assert config.include_column_families is True
        assert config.env == "PROD"
        assert config.platform_instance == "hbase-prod-cluster"
