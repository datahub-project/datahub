"""Unit tests for Kafka SSL helper utilities."""

import ssl

from datahub.ingestion.source.kafka.kafka_ssl_helper import (
    prepare_schema_registry_config,
)


class TestPrepareSchemaRegistryConfig:
    """Tests for prepare_schema_registry_config function."""

    def test_no_ssl_ca_location(self):
        """Test config without ssl.ca.location."""
        config = {
            "url": "http://registry.example.com",
            "some.other.config": "value",
        }

        result = prepare_schema_registry_config(config)

        # Should return unchanged
        assert result == config
        assert "ssl.ca.location" not in result

    def test_ssl_ca_location_already_context(self):
        """Test that existing SSL context is not modified."""
        ca_context = ssl.create_default_context()
        config = {
            "url": "https://registry.example.com",
            "ssl.ca.location": ca_context,
        }

        result = prepare_schema_registry_config(config)

        # Should remain unchanged
        assert result["ssl.ca.location"] is ca_context
        assert isinstance(result["ssl.ca.location"], ssl.SSLContext)

    def test_ssl_ca_location_nonexistent_file(self):
        """Test handling of nonexistent CA certificate file."""
        config = {
            "url": "https://registry.example.com",
            "ssl.ca.location": "/nonexistent/path/to/ca.pem",
        }

        result = prepare_schema_registry_config(config)

        # Should keep original path (warning logged)
        assert result["ssl.ca.location"] == "/nonexistent/path/to/ca.pem"
        assert isinstance(result["ssl.ca.location"], str)

    def test_ssl_ca_location_invalid_type(self):
        """Test handling of invalid type for ssl.ca.location."""
        config = {
            "url": "https://registry.example.com",
            "ssl.ca.location": 12345,  # Invalid type
        }

        result = prepare_schema_registry_config(config)

        # Should keep original value (warning logged)
        assert result["ssl.ca.location"] == 12345

    def test_ssl_ca_location_none(self):
        """Test handling of None value for ssl.ca.location."""
        config = {
            "url": "https://registry.example.com",
            "ssl.ca.location": None,
        }

        result = prepare_schema_registry_config(config)

        # Should return unchanged
        assert result["ssl.ca.location"] is None

    def test_config_not_modified(self):
        """Test that original config dict is not modified."""
        original_config = {
            "url": "https://registry.example.com",
            "ssl.ca.location": "/path/to/ca.pem",
        }

        # Make a copy to compare
        config_copy = original_config.copy()

        result = prepare_schema_registry_config(original_config)

        # Original should be unchanged
        assert original_config == config_copy
        # Result should be different
        assert result is not original_config

    def test_other_config_options_preserved(self):
        """Test that other config options are preserved."""
        config = {
            "url": "https://registry.example.com",
            "ssl.ca.location": "/path/to/ca.pem",
            "ssl.certificate.location": "/path/to/cert.pem",
            "ssl.key.location": "/path/to/key.pem",
            "basic.auth.credentials.source": "USER_INFO",
            "basic.auth.user.info": "user:password",
        }

        result = prepare_schema_registry_config(config)

        # Other options should be preserved
        assert result["url"] == config["url"]
        assert result["ssl.certificate.location"] == config["ssl.certificate.location"]
        assert result["ssl.key.location"] == config["ssl.key.location"]
        assert (
            result["basic.auth.credentials.source"]
            == config["basic.auth.credentials.source"]
        )
        assert result["basic.auth.user.info"] == config["basic.auth.user.info"]

    def test_empty_config(self):
        """Test with empty configuration."""
        config: dict[str, str] = {}

        result = prepare_schema_registry_config(config)

        assert result == {}

    def test_ssl_ca_location_empty_string(self):
        """Test handling of empty string for ssl.ca.location."""
        config = {
            "url": "https://registry.example.com",
            "ssl.ca.location": "",
        }

        result = prepare_schema_registry_config(config)

        # Empty string should be kept as-is (falsy value)
        assert result["ssl.ca.location"] == ""
