"""Unit tests for Snowplow configuration classes."""

import pytest

from datahub.ingestion.source.snowplow.snowplow_config import (
    IgluConnectionConfig,
    SnowplowBDPConnectionConfig,
    SnowplowSourceConfig,
)


class TestSnowplowBDPConnectionConfig:
    """Test BDP connection configuration."""

    def test_valid_bdp_config(self):
        """Test valid BDP configuration."""
        config = SnowplowBDPConnectionConfig.model_validate(
            {
                "organization_id": "123e4567-e89b-12d3-a456-426614174000",
                "api_key_id": "test_key_id",
                "api_key": "test_secret",
            }
        )

        assert config.organization_id == "123e4567-e89b-12d3-a456-426614174000"
        assert config.api_key_id == "test_key_id"
        assert config.api_key.get_secret_value() == "test_secret"
        assert config.timeout_seconds == 60  # Default
        assert config.max_retries == 3  # Default

    def test_bdp_config_with_custom_url(self):
        """Test BDP config with custom API URL."""
        config = SnowplowBDPConnectionConfig.model_validate(
            {
                "organization_id": "test-org",
                "api_key_id": "key",
                "api_key": "secret",
                "console_api_url": "https://custom.snowplowanalytics.com/api/v1",
            }
        )

        assert config.console_api_url == "https://custom.snowplowanalytics.com/api/v1"

    def test_bdp_config_url_trailing_slash_removed(self):
        """Test that trailing slash is removed from URL."""
        config = SnowplowBDPConnectionConfig.model_validate(
            {
                "organization_id": "test-org",
                "api_key_id": "key",
                "api_key": "secret",
                "console_api_url": "https://console.snowplowanalytics.com/api/v1/",
            }
        )

        assert config.console_api_url == "https://console.snowplowanalytics.com/api/v1"

    def test_bdp_config_invalid_url(self):
        """Test that invalid URL raises error."""
        with pytest.raises(ValueError, match="must start with http"):
            SnowplowBDPConnectionConfig.model_validate(
                {
                    "organization_id": "test-org",
                    "api_key_id": "key",
                    "api_key": "secret",
                    "console_api_url": "invalid-url",
                }
            )


class TestIgluConnectionConfig:
    """Test Iglu connection configuration."""

    def test_valid_iglu_config(self):
        """Test valid Iglu configuration."""
        config = IgluConnectionConfig.model_validate(
            {
                "iglu_server_url": "https://iglu.example.com",
            }
        )

        assert config.iglu_server_url == "https://iglu.example.com"
        assert config.api_key is None  # Optional
        assert config.timeout_seconds == 30  # Default

    def test_iglu_config_with_api_key(self):
        """Test Iglu config with API key."""
        config = IgluConnectionConfig.model_validate(
            {
                "iglu_server_url": "https://iglu.example.com",
                "api_key": "123e4567-e89b-12d3-a456-426614174000",
            }
        )

        assert config.api_key is not None
        assert (
            config.api_key.get_secret_value() == "123e4567-e89b-12d3-a456-426614174000"
        )

    def test_iglu_config_url_trailing_slash_removed(self):
        """Test that trailing slash is removed."""
        config = IgluConnectionConfig.model_validate(
            {
                "iglu_server_url": "https://iglu.example.com/",
            }
        )

        assert config.iglu_server_url == "https://iglu.example.com"

    def test_iglu_config_invalid_url(self):
        """Test that invalid URL raises error."""
        with pytest.raises(ValueError, match="must start with http"):
            IgluConnectionConfig.model_validate(
                {
                    "iglu_server_url": "ftp://iglu.example.com",
                }
            )


class TestSnowplowSourceConfig:
    """Test main Snowplow source configuration."""

    def test_valid_bdp_only_config(self):
        """Test valid BDP-only configuration."""
        config = SnowplowSourceConfig.model_validate(
            {
                "bdp_connection": {
                    "organization_id": "test-org",
                    "api_key_id": "key",
                    "api_key": "secret",
                }
            }
        )

        assert config.bdp_connection is not None
        assert config.iglu_connection is None

    def test_valid_iglu_only_config(self):
        """Test valid Iglu-only configuration."""
        config = SnowplowSourceConfig.model_validate(
            {
                "iglu_connection": {
                    "iglu_server_url": "https://iglu.example.com",
                }
            }
        )

        assert config.bdp_connection is None
        assert config.iglu_connection is not None

    def test_valid_both_connections_config(self):
        """Test configuration with both BDP and Iglu."""
        config = SnowplowSourceConfig.model_validate(
            {
                "bdp_connection": {
                    "organization_id": "test-org",
                    "api_key_id": "key",
                    "api_key": "secret",
                },
                "iglu_connection": {
                    "iglu_server_url": "https://iglu.example.com",
                },
            }
        )

        assert config.bdp_connection is not None
        assert config.iglu_connection is not None

    def test_config_requires_at_least_one_connection(self):
        """Test that at least one connection type is required."""
        with pytest.raises(
            ValueError,
            match="Either bdp_connection or iglu_connection must be configured",
        ):
            SnowplowSourceConfig.model_validate({})

    def test_default_feature_flags(self):
        """Test default feature flag values."""
        config = SnowplowSourceConfig.model_validate(
            {
                "bdp_connection": {
                    "organization_id": "test-org",
                    "api_key_id": "key",
                    "api_key": "secret",
                }
            }
        )

        assert config.extract_event_specifications is True
        assert config.extract_tracking_scenarios is True
        assert config.extract_data_products is False
        assert config.include_hidden_schemas is False

    def test_custom_feature_flags(self):
        """Test custom feature flag values."""
        config = SnowplowSourceConfig.model_validate(
            {
                "bdp_connection": {
                    "organization_id": "test-org",
                    "api_key_id": "key",
                    "api_key": "secret",
                },
                "extract_event_specifications": False,
                "extract_tracking_scenarios": False,
                "include_hidden_schemas": True,
            }
        )

        assert config.extract_event_specifications is False
        assert config.extract_tracking_scenarios is False
        assert config.include_hidden_schemas is True

    def test_default_schema_types(self):
        """Test default schema types to extract."""
        config = SnowplowSourceConfig.model_validate(
            {
                "bdp_connection": {
                    "organization_id": "test-org",
                    "api_key_id": "key",
                    "api_key": "secret",
                }
            }
        )

        assert config.schema_types_to_extract == ["event", "entity"]

    def test_custom_schema_types(self):
        """Test custom schema types."""
        config = SnowplowSourceConfig.model_validate(
            {
                "bdp_connection": {
                    "organization_id": "test-org",
                    "api_key_id": "key",
                    "api_key": "secret",
                },
                "schema_types_to_extract": ["event"],
            }
        )

        assert config.schema_types_to_extract == ["event"]

    def test_invalid_schema_types(self):
        """Test that invalid schema types raise error."""
        with pytest.raises(
            ValueError, match="schema_types_to_extract must contain only"
        ):
            SnowplowSourceConfig.model_validate(
                {
                    "bdp_connection": {
                        "organization_id": "test-org",
                        "api_key_id": "key",
                        "api_key": "secret",
                    },
                    "schema_types_to_extract": ["invalid_type"],
                }
            )

    def test_empty_schema_types(self):
        """Test that empty schema types list raises error."""
        with pytest.raises(ValueError, match="schema_types_to_extract cannot be empty"):
            SnowplowSourceConfig.model_validate(
                {
                    "bdp_connection": {
                        "organization_id": "test-org",
                        "api_key_id": "key",
                        "api_key": "secret",
                    },
                    "schema_types_to_extract": [],
                }
            )

    def test_filtering_patterns_default(self):
        """Test default filtering patterns."""
        config = SnowplowSourceConfig.model_validate(
            {
                "bdp_connection": {
                    "organization_id": "test-org",
                    "api_key_id": "key",
                    "api_key": "secret",
                }
            }
        )

        # Default patterns allow all
        assert config.schema_pattern.allowed("com.example/test")
        assert config.event_spec_pattern.allowed("test_spec")
        assert config.tracking_scenario_pattern.allowed("test_scenario")

    def test_custom_filtering_patterns(self):
        """Test custom filtering patterns."""
        config = SnowplowSourceConfig.model_validate(
            {
                "bdp_connection": {
                    "organization_id": "test-org",
                    "api_key_id": "key",
                    "api_key": "secret",
                },
                "schema_pattern": {
                    "allow": ["com\\.example/.*"],
                    "deny": [".*\\.test$"],
                },
            }
        )

        assert config.schema_pattern.allowed("com.example/page_view")
        assert not config.schema_pattern.allowed("com.other/page_view")
        assert not config.schema_pattern.allowed("com.example/page_view.test")
