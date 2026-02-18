"""
Tests for Kafka Connect configuration validation.

This module tests the pydantic validators that ensure proper configuration
interdependencies and provide clear error messages for invalid combinations.
"""

from typing import Any

import pytest

from datahub.ingestion.source.kafka_connect.common import KafkaConnectSourceConfig


class TestConfigurationValidation:
    """Tests for KafkaConnectSourceConfig validators."""

    def test_schema_resolver_defaults_when_enabled(self):
        """Test that schema resolver features default to True when use_schema_resolver=True."""
        config = KafkaConnectSourceConfig(
            connect_uri="http://test:8083",
            cluster_name="test",
            use_schema_resolver=True,
        )

        # Should auto-set both features to True
        assert config.use_schema_resolver is True
        assert config.schema_resolver_expand_patterns is True
        assert config.schema_resolver_finegrained_lineage is True

    def test_schema_resolver_defaults_when_disabled(self):
        """Test that schema resolver features default to False when use_schema_resolver=False."""
        config = KafkaConnectSourceConfig(
            connect_uri="http://test:8083",
            cluster_name="test",
            use_schema_resolver=False,
        )

        # Should auto-set both features to False
        assert config.use_schema_resolver is False
        assert config.schema_resolver_expand_patterns is False
        assert config.schema_resolver_finegrained_lineage is False

    def test_schema_resolver_explicit_override_when_enabled(self):
        """Test that explicit values override defaults when use_schema_resolver=True."""
        config = KafkaConnectSourceConfig(
            connect_uri="http://test:8083",
            cluster_name="test",
            use_schema_resolver=True,
            schema_resolver_expand_patterns=False,
            schema_resolver_finegrained_lineage=True,
        )

        assert config.use_schema_resolver is True
        assert config.schema_resolver_expand_patterns is False
        assert config.schema_resolver_finegrained_lineage is True

    def test_schema_resolver_explicit_override_when_disabled(self):
        """Test that explicit True values are preserved even when use_schema_resolver=False."""
        # This allows users to pre-configure features before enabling schema resolver
        config = KafkaConnectSourceConfig(
            connect_uri="http://test:8083",
            cluster_name="test",
            use_schema_resolver=False,
            schema_resolver_expand_patterns=True,
            schema_resolver_finegrained_lineage=False,
        )

        assert config.use_schema_resolver is False
        # Explicit values are preserved (though they won't take effect until use_schema_resolver=True)
        assert config.schema_resolver_expand_patterns is True
        assert config.schema_resolver_finegrained_lineage is False

    def test_kafka_api_key_without_secret_raises_error(self):
        """Test that kafka_api_key without kafka_api_secret raises error."""
        with pytest.raises(ValueError) as exc_info:
            KafkaConnectSourceConfig(
                connect_uri="http://test:8083",
                cluster_name="test",
                kafka_api_key="test-key",
                kafka_api_secret=None,
            )

        assert "kafka_api_key" in str(exc_info.value)
        assert "kafka_api_secret" in str(exc_info.value)
        assert "must be provided together" in str(exc_info.value)

    def test_kafka_api_secret_without_key_raises_error(self):
        """Test that kafka_api_secret without kafka_api_key raises error."""
        with pytest.raises(ValueError) as exc_info:
            KafkaConnectSourceConfig(
                connect_uri="http://test:8083",
                cluster_name="test",
                kafka_api_key=None,
                kafka_api_secret="test-secret",
            )

        assert "kafka_api_key" in str(exc_info.value)
        assert "kafka_api_secret" in str(exc_info.value)
        assert "must be provided together" in str(exc_info.value)

    def test_kafka_api_credentials_valid_when_both_provided(self):
        """Test that kafka API credentials work when both key and secret provided."""
        config = KafkaConnectSourceConfig(
            connect_uri="http://test:8083",
            cluster_name="test",
            kafka_api_key="test-key",
            kafka_api_secret="test-secret",
        )

        assert config.kafka_api_key == "test-key"
        assert config.kafka_api_secret is not None
        assert config.kafka_api_secret.get_secret_value() == "test-secret"

    def test_environment_id_without_cluster_id_raises_error(self):
        """Test that confluent_cloud_environment_id without cluster_id raises error."""
        with pytest.raises(ValueError) as exc_info:
            KafkaConnectSourceConfig(
                connect_uri="http://test:8083",
                cluster_name="test",
                confluent_cloud_environment_id="env-123",
                confluent_cloud_cluster_id=None,
            )

        assert "confluent_cloud_environment_id" in str(exc_info.value)
        assert "confluent_cloud_cluster_id" in str(exc_info.value)
        assert "must be provided together" in str(exc_info.value)

    def test_cluster_id_without_environment_id_raises_error(self):
        """Test that confluent_cloud_cluster_id without environment_id raises error."""
        with pytest.raises(ValueError) as exc_info:
            KafkaConnectSourceConfig(
                connect_uri="http://test:8083",
                cluster_name="test",
                confluent_cloud_environment_id=None,
                confluent_cloud_cluster_id="lkc-123",
            )

        assert "confluent_cloud_environment_id" in str(exc_info.value)
        assert "confluent_cloud_cluster_id" in str(exc_info.value)
        assert "must be provided together" in str(exc_info.value)

    def test_cloud_ids_valid_when_both_provided(self):
        """Test that Confluent Cloud IDs work when both provided."""
        config = KafkaConnectSourceConfig(
            cluster_name="test",
            confluent_cloud_environment_id="env-123",
            confluent_cloud_cluster_id="lkc-456",
        )

        assert config.confluent_cloud_environment_id == "env-123"
        assert config.confluent_cloud_cluster_id == "lkc-456"
        # URI should be auto-constructed
        assert "env-123" in config.connect_uri
        assert "lkc-456" in config.connect_uri

    def test_invalid_kafka_rest_endpoint_raises_error(self):
        """Test that invalid kafka_rest_endpoint format raises error."""
        with pytest.raises(ValueError) as exc_info:
            KafkaConnectSourceConfig(
                connect_uri="http://test:8083",
                cluster_name="test",
                kafka_rest_endpoint="invalid-endpoint",
            )

        assert "kafka_rest_endpoint" in str(exc_info.value)
        assert "HTTP" in str(exc_info.value)

    def test_valid_kafka_rest_endpoint_https(self):
        """Test that valid HTTPS kafka_rest_endpoint is accepted."""
        config = KafkaConnectSourceConfig(
            connect_uri="http://test:8083",
            cluster_name="test",
            kafka_rest_endpoint="https://pkc-12345.us-west-2.aws.confluent.cloud",
        )

        assert (
            config.kafka_rest_endpoint
            == "https://pkc-12345.us-west-2.aws.confluent.cloud"
        )

    def test_valid_kafka_rest_endpoint_http(self):
        """Test that valid HTTP kafka_rest_endpoint is accepted."""
        config = KafkaConnectSourceConfig(
            connect_uri="http://test:8083",
            cluster_name="test",
            kafka_rest_endpoint="http://localhost:8082",
        )

        assert config.kafka_rest_endpoint == "http://localhost:8082"

    def test_auto_construct_uri_from_cloud_ids(self):
        """Test automatic URI construction from Confluent Cloud IDs."""
        config = KafkaConnectSourceConfig(
            cluster_name="test",
            confluent_cloud_environment_id="env-xyz123",
            confluent_cloud_cluster_id="lkc-abc456",
        )

        expected_uri = (
            "https://api.confluent.cloud/connect/v1/"
            "environments/env-xyz123/"
            "clusters/lkc-abc456"
        )
        assert config.connect_uri == expected_uri

    def test_explicit_uri_not_overwritten_by_cloud_ids(self):
        """Test that explicit connect_uri is preserved even with Cloud IDs."""
        explicit_uri = "http://my-custom-connect:8083"
        config = KafkaConnectSourceConfig(
            connect_uri=explicit_uri,
            cluster_name="test",
            confluent_cloud_environment_id="env-123",
            confluent_cloud_cluster_id="lkc-456",
        )

        # Should keep explicit URI (but validator warns)
        assert config.connect_uri == explicit_uri

    def test_default_config_is_valid(self):
        """Test that default configuration is valid."""
        config = KafkaConnectSourceConfig(
            cluster_name="test",
        )

        # Should not raise any validation errors
        assert config.use_schema_resolver is False
        # When use_schema_resolver=False, features should default to False
        assert config.schema_resolver_expand_patterns is False
        assert config.schema_resolver_finegrained_lineage is False
        assert config.kafka_api_key is None
        assert config.kafka_api_secret is None

    def test_schema_resolver_enabled_with_all_features_disabled_warns(
        self, caplog: Any
    ) -> None:
        """Test warning when schema resolver enabled but all features disabled."""
        import logging

        with caplog.at_level(logging.WARNING):
            KafkaConnectSourceConfig(
                connect_uri="http://test:8083",
                cluster_name="test",
                use_schema_resolver=True,
                schema_resolver_expand_patterns=False,
                schema_resolver_finegrained_lineage=False,
            )

        # Should log a warning about no features enabled
        assert any(
            "Schema resolver is enabled but all features are disabled" in record.message
            for record in caplog.records
        )

    def test_multiple_validation_errors_caught_independently(self):
        """Test that each validation error is caught independently."""
        # Test kafka_api_key without secret
        with pytest.raises(ValueError, match="kafka_api_key.*kafka_api_secret"):
            KafkaConnectSourceConfig(
                connect_uri="http://test:8083",
                cluster_name="test",
                kafka_api_key="key",
            )

        # Test environment_id without cluster_id
        with pytest.raises(
            ValueError,
            match="confluent_cloud_environment_id.*confluent_cloud_cluster_id",
        ):
            KafkaConnectSourceConfig(
                connect_uri="http://test:8083",
                cluster_name="test",
                confluent_cloud_environment_id="env-123",
            )

        # Test invalid kafka_rest_endpoint format
        with pytest.raises(ValueError, match="kafka_rest_endpoint.*HTTP"):
            KafkaConnectSourceConfig(
                connect_uri="http://test:8083",
                cluster_name="test",
                kafka_rest_endpoint="not-a-url",
            )

    def test_complex_valid_configuration(self):
        """Test a complex but valid configuration with multiple features."""
        config = KafkaConnectSourceConfig(
            cluster_name="test",
            confluent_cloud_environment_id="env-123",
            confluent_cloud_cluster_id="lkc-456",
            kafka_api_key="api-key",
            kafka_api_secret="api-secret",
            kafka_rest_endpoint="https://pkc-12345.us-west-2.aws.confluent.cloud",
            use_schema_resolver=True,
            schema_resolver_expand_patterns=True,
            schema_resolver_finegrained_lineage=True,
            use_connect_topics_api=True,
        )

        # All fields should be properly set
        assert config.confluent_cloud_environment_id == "env-123"
        assert config.confluent_cloud_cluster_id == "lkc-456"
        assert config.kafka_api_key == "api-key"
        assert config.kafka_api_secret is not None
        assert config.kafka_api_secret.get_secret_value() == "api-secret"
        assert config.use_schema_resolver is True
        assert config.schema_resolver_expand_patterns is True
        assert config.schema_resolver_finegrained_lineage is True


class TestSchemaResolverAutoEnable:
    """Tests for automatic schema_resolver enabling for Confluent Cloud."""

    def test_oss_keeps_default_false(self):
        """Test that OSS Kafka Connect keeps use_schema_resolver=False by default."""
        config = KafkaConnectSourceConfig(
            cluster_name="test",
            connect_uri="http://localhost:8083",
        )

        assert config.use_schema_resolver is False
        assert config.schema_resolver_expand_patterns is False
        assert config.schema_resolver_finegrained_lineage is False

    def test_confluent_cloud_auto_enables_via_uri(self):
        """Test that Confluent Cloud auto-enables use_schema_resolver via URI detection."""
        config = KafkaConnectSourceConfig(
            cluster_name="test",
            connect_uri="https://api.confluent.cloud/connect/v1/environments/env-123/clusters/lkc-456",
        )

        # Should auto-enable schema resolver for Confluent Cloud
        assert config.use_schema_resolver is True
        assert config.schema_resolver_expand_patterns is True
        assert config.schema_resolver_finegrained_lineage is True

    def test_confluent_cloud_auto_enables_via_ids(self):
        """Test that Confluent Cloud auto-enables use_schema_resolver via environment/cluster IDs."""
        config = KafkaConnectSourceConfig(
            cluster_name="test",
            confluent_cloud_environment_id="env-123",
            confluent_cloud_cluster_id="lkc-456",
        )

        # Should auto-enable schema resolver for Confluent Cloud
        assert config.use_schema_resolver is True
        assert config.schema_resolver_expand_patterns is True
        assert config.schema_resolver_finegrained_lineage is True

    def test_confluent_cloud_respects_explicit_false(self):
        """Test that explicit use_schema_resolver=false is respected for Confluent Cloud."""
        config = KafkaConnectSourceConfig(
            cluster_name="test",
            connect_uri="https://api.confluent.cloud/connect/v1/environments/env-123/clusters/lkc-456",
            use_schema_resolver=False,
        )

        # Should respect explicit False
        assert config.use_schema_resolver is False
        assert config.schema_resolver_expand_patterns is False
        assert config.schema_resolver_finegrained_lineage is False

    def test_confluent_cloud_respects_explicit_true(self):
        """Test that explicit use_schema_resolver=true is respected for Confluent Cloud."""
        config = KafkaConnectSourceConfig(
            cluster_name="test",
            confluent_cloud_environment_id="env-123",
            confluent_cloud_cluster_id="lkc-456",
            use_schema_resolver=True,
        )

        # Should respect explicit True
        assert config.use_schema_resolver is True
        assert config.schema_resolver_expand_patterns is True
        assert config.schema_resolver_finegrained_lineage is True

    def test_confluent_cloud_auto_enable_with_sub_features_override(self):
        """Test auto-enable with explicit sub-feature overrides."""
        config = KafkaConnectSourceConfig(
            cluster_name="test",
            confluent_cloud_environment_id="env-123",
            confluent_cloud_cluster_id="lkc-456",
            # Don't set use_schema_resolver - let it auto-enable
            schema_resolver_expand_patterns=False,  # Explicit override
            schema_resolver_finegrained_lineage=True,  # Explicit override
        )

        # Should auto-enable use_schema_resolver
        assert config.use_schema_resolver is True
        # But respect explicit sub-feature overrides
        assert config.schema_resolver_expand_patterns is False
        assert config.schema_resolver_finegrained_lineage is True

    def test_oss_with_explicit_true_not_affected(self):
        """Test that OSS with explicit use_schema_resolver=true works correctly."""
        config = KafkaConnectSourceConfig(
            cluster_name="test",
            connect_uri="http://localhost:8083",
            use_schema_resolver=True,
        )

        # Explicit True should work for OSS
        assert config.use_schema_resolver is True
        assert config.schema_resolver_expand_patterns is True
        assert config.schema_resolver_finegrained_lineage is True

    def test_is_confluent_cloud_detection(self):
        """Test is_confluent_cloud() detection method."""
        # OSS by URI
        config_oss = KafkaConnectSourceConfig(
            cluster_name="test",
            connect_uri="http://localhost:8083",
        )
        assert config_oss.is_confluent_cloud() is False

        # Cloud by URI
        config_cloud_uri = KafkaConnectSourceConfig(
            cluster_name="test",
            connect_uri="https://api.confluent.cloud/connect/v1/environments/env-123/clusters/lkc-456",
        )
        assert config_cloud_uri.is_confluent_cloud() is True

        # Cloud by IDs
        config_cloud_ids = KafkaConnectSourceConfig(
            cluster_name="test",
            confluent_cloud_environment_id="env-123",
            confluent_cloud_cluster_id="lkc-456",
        )
        assert config_cloud_ids.is_confluent_cloud() is True

        # Cloud by IDs takes precedence even with non-cloud URI
        config_mixed = KafkaConnectSourceConfig(
            cluster_name="test",
            connect_uri="http://localhost:8083",
            confluent_cloud_environment_id="env-123",
            confluent_cloud_cluster_id="lkc-456",
        )
        assert config_mixed.is_confluent_cloud() is True
