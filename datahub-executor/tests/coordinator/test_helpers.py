"""Tests for coordinator helpers module."""

import os
from typing import Any, Callable
from unittest.mock import MagicMock, Mock, patch


class TestGetKafkaConsumerConfigFromEnv:
    """Tests for get_kafka_consumer_config_from_env function.

    Note: This replaces the old _build_oauth_consumer_config function with a more
    flexible dynamic configuration approach.
    """

    def test_no_kafka_properties_returns_empty(self) -> None:
        """Test that no KAFKA_PROPERTIES_* vars returns empty config."""
        from datahub_executor.coordinator.helpers import (
            get_kafka_consumer_config_from_env,
        )

        test_env: dict[str, str] = {}
        with patch.dict(os.environ, test_env, clear=False):
            result = get_kafka_consumer_config_from_env()
            # Result may contain existing env vars, just check it's a dict
            assert isinstance(result, dict)

    def test_oauthbearer_with_callback(self) -> None:
        """Test OAUTHBEARER mechanism with MSK IAM callback."""
        from datahub_executor.coordinator.helpers import (
            get_kafka_consumer_config_from_env,
        )

        test_env = {
            "KAFKA_PROPERTIES_SASL_MECHANISM": "OAUTHBEARER",
            "KAFKA_PROPERTIES_SASL_OAUTHBEARER_METHOD": "default",
            "KAFKA_PROPERTIES_OAUTH_CB": "datahub_executor.common.kafka_msk_iam:oauth_cb",
        }

        with patch.dict(os.environ, test_env, clear=False):
            result = get_kafka_consumer_config_from_env()

            assert result["sasl.mechanism"] == "OAUTHBEARER"
            assert result["sasl.oauthbearer.method"] == "default"
            assert (
                result["oauth_cb"] == "datahub_executor.common.kafka_msk_iam:oauth_cb"
            )

    def test_oauthbearer_with_custom_callback(self) -> None:
        """Test OAUTHBEARER mechanism with Event Hubs callback."""
        from datahub_executor.coordinator.helpers import (
            get_kafka_consumer_config_from_env,
        )

        test_env = {
            "KAFKA_PROPERTIES_SASL_MECHANISM": "OAUTHBEARER",
            "KAFKA_PROPERTIES_OAUTH_CB": "datahub_executor.common.kafka_eventhubs_auth:oauth_cb",
            "KAFKA_PROPERTIES_SASL_OAUTHBEARER_METHOD": "default",
        }

        with patch.dict(os.environ, test_env, clear=False):
            result = get_kafka_consumer_config_from_env()

            assert result["sasl.mechanism"] == "OAUTHBEARER"
            assert (
                result["oauth_cb"]
                == "datahub_executor.common.kafka_eventhubs_auth:oauth_cb"
            )
            assert result["sasl.oauthbearer.method"] == "default"

    def test_plain_mechanism_with_credentials(self) -> None:
        """Test PLAIN mechanism with username and password."""
        from datahub_executor.coordinator.helpers import (
            get_kafka_consumer_config_from_env,
        )

        test_env = {
            "KAFKA_PROPERTIES_SASL_MECHANISM": "PLAIN",
            "KAFKA_PROPERTIES_SASL_USERNAME": "$ConnectionString",
            "KAFKA_PROPERTIES_SASL_PASSWORD": "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123",
        }

        with patch.dict(os.environ, test_env, clear=False):
            result = get_kafka_consumer_config_from_env()

            assert result["sasl.mechanism"] == "PLAIN"
            assert result["sasl.username"] == "$ConnectionString"
            assert (
                result["sasl.password"]
                == "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123"
            )

    def test_plain_mechanism_with_username_only(self) -> None:
        """Test PLAIN mechanism with only username (password missing)."""
        from datahub_executor.coordinator.helpers import (
            get_kafka_consumer_config_from_env,
        )

        test_env = {
            "KAFKA_PROPERTIES_SASL_MECHANISM": "PLAIN",
            "KAFKA_PROPERTIES_SASL_USERNAME": "username",
        }

        with patch.dict(os.environ, test_env, clear=False):
            result = get_kafka_consumer_config_from_env()

            # Both properties are included if set, even if incomplete
            assert result["sasl.mechanism"] == "PLAIN"
            assert result["sasl.username"] == "username"
            assert "sasl.password" not in result

    def test_ssl_tls_configuration(self) -> None:
        """Test SSL/TLS with client certificates - NEW CAPABILITY."""
        from datahub_executor.coordinator.helpers import (
            get_kafka_consumer_config_from_env,
        )

        test_env = {
            "KAFKA_PROPERTIES_SECURITY_PROTOCOL": "SSL",
            "KAFKA_PROPERTIES_SSL_CA_LOCATION": "/certs/ca.pem",
            "KAFKA_PROPERTIES_SSL_CERTIFICATE_LOCATION": "/certs/client.pem",
            "KAFKA_PROPERTIES_SSL_KEY_LOCATION": "/certs/key.pem",
        }

        with patch.dict(os.environ, test_env, clear=False):
            result = get_kafka_consumer_config_from_env()

            assert result["security.protocol"] == "SSL"
            assert result["ssl.ca.location"] == "/certs/ca.pem"
            assert result["ssl.certificate.location"] == "/certs/client.pem"
            assert result["ssl.key.location"] == "/certs/key.pem"

    def test_empty_values_excluded(self) -> None:
        """Test that empty string values are excluded from configuration."""
        from datahub_executor.coordinator.helpers import (
            get_kafka_consumer_config_from_env,
        )

        test_env = {
            "KAFKA_PROPERTIES_SASL_MECHANISM": "",  # Empty
            "KAFKA_PROPERTIES_SASL_USERNAME": "username",  # Has value
        }

        with patch.dict(os.environ, test_env, clear=False):
            result = get_kafka_consumer_config_from_env()

            assert "sasl.mechanism" not in result
            assert result["sasl.username"] == "username"

    def test_underscore_to_dot_conversion(self) -> None:
        """Test that underscores in env var names are converted to dots in property names."""
        from datahub_executor.coordinator.helpers import (
            get_kafka_consumer_config_from_env,
        )

        test_env = {
            "KAFKA_PROPERTIES_MAX_POLL_INTERVAL_MS": "600000",
            "KAFKA_PROPERTIES_SESSION_TIMEOUT_MS": "45000",
            "KAFKA_PROPERTIES_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM": "https",
        }

        with patch.dict(os.environ, test_env, clear=False):
            result = get_kafka_consumer_config_from_env()

            # Underscores in env var names should be converted to dots
            assert result["max.poll.interval.ms"] == "600000"
            assert result["session.timeout.ms"] == "45000"
            assert result["ssl.endpoint.identification.algorithm"] == "https"

    def test_oauth_cb_special_case(self) -> None:
        """Test that OAUTH_CB property name is NOT converted to dots and its value (containing dots) is preserved."""
        from datahub_executor.coordinator.helpers import (
            get_kafka_consumer_config_from_env,
        )

        # The value contains dots and colons which should be preserved
        test_env = {
            "KAFKA_PROPERTIES_OAUTH_CB": "datahub_executor.common.kafka_msk_iam:oauth_cb",
            "KAFKA_PROPERTIES_SASL_MECHANISM": "OAUTHBEARER",
        }

        with patch.dict(os.environ, test_env, clear=False):
            result = get_kafka_consumer_config_from_env()

            # Property name should remain as oauth_cb (NOT oauth.cb)
            assert "oauth_cb" in result
            assert "oauth.cb" not in result
            # Value should be preserved exactly, including dots and colons
            assert (
                result["oauth_cb"] == "datahub_executor.common.kafka_msk_iam:oauth_cb"
            )
            assert result["sasl.mechanism"] == "OAUTHBEARER"


class TestStartIngestionPipeline:
    """Tests for start_ingestion_pipeline function."""

    @patch("datahub_executor.coordinator.helpers.Thread")
    @patch("datahub_executor.coordinator.helpers.Pipeline")
    @patch("datahub_executor.coordinator.helpers.IngestionAction")
    @patch("datahub_executor.coordinator.helpers.KafkaEventSource")
    @patch("datahub_executor.coordinator.helpers.load_config_file")
    @patch("datahub_executor.coordinator.helpers.asyncio.set_event_loop_policy")
    def test_start_ingestion_pipeline_with_oauth(
        self,
        mock_set_event_loop_policy: MagicMock,
        mock_load_config: MagicMock,
        mock_kafka_source: MagicMock,
        mock_ingestion_action: MagicMock,
        mock_pipeline: MagicMock,
        mock_thread: MagicMock,
    ) -> None:
        """Test starting ingestion pipeline with OAuth configuration."""
        from datahub_executor.coordinator.helpers import start_ingestion_pipeline

        # Setup
        mock_graph = Mock()
        mock_discovery = Mock()
        sighandler: list[Callable[[], Any]] = []

        mock_load_config.return_value = {
            "connection": {
                "bootstrap": "broker:9092",
                "schema_registry_url": "http://schema-registry:8081",
                "consumer_config": {
                    "group.id": "test-consumer",
                    "auto.offset.reset": "latest",
                },
            },
            "topic_routes": {
                "mcl": "MetadataChangeLog_Versioned_v1",
            },
        }

        # Set OAuth environment variables
        test_env = {
            "KAFKA_PROPERTIES_SECURITY_PROTOCOL": "SASL_SSL",
            "KAFKA_PROPERTIES_SASL_MECHANISM": "OAUTHBEARER",
            "KAFKA_PROPERTIES_OAUTH_CB": "datahub_executor.common.kafka_msk_iam:oauth_cb",
        }

        with patch.dict(os.environ, test_env, clear=False):
            # Execute
            start_ingestion_pipeline(mock_graph, mock_discovery, sighandler)

            # Verify
            mock_load_config.assert_called_once()
            mock_kafka_source.assert_called_once()

            # Check that consumer_config was updated with OAuth settings
            call_args = mock_kafka_source.call_args
            consumer_config = call_args.kwargs["config"].connection.consumer_config

            assert consumer_config["security.protocol"] == "SASL_SSL"
            assert consumer_config["sasl.mechanism"] == "OAUTHBEARER"
            # oauth_cb is loaded as a function, not kept as a string
            assert callable(consumer_config["oauth_cb"])
            assert (
                consumer_config["oauth_cb"].__module__
                == "datahub_executor.common.kafka_msk_iam"
            )
            assert consumer_config["oauth_cb"].__name__ == "oauth_cb"

            # Verify sighandlers were registered
            assert len(sighandler) == 2

    @patch("datahub_executor.coordinator.helpers.Thread")
    @patch("datahub_executor.coordinator.helpers.Pipeline")
    @patch("datahub_executor.coordinator.helpers.IngestionAction")
    @patch("datahub_executor.coordinator.helpers.KafkaEventSource")
    @patch("datahub_executor.coordinator.helpers.load_config_file")
    @patch("datahub_executor.coordinator.helpers.asyncio.set_event_loop_policy")
    def test_start_ingestion_pipeline_override_bootstrap(
        self,
        mock_set_event_loop_policy: MagicMock,
        mock_load_config: MagicMock,
        mock_kafka_source: MagicMock,
        mock_ingestion_action: MagicMock,
        mock_pipeline: MagicMock,
        mock_thread: MagicMock,
    ) -> None:
        """Test starting ingestion pipeline with bootstrap override."""
        from datahub_executor.coordinator.helpers import start_ingestion_pipeline

        # Setup
        mock_graph = Mock()
        mock_discovery = Mock()
        sighandler: list[Callable[[], Any]] = []

        mock_load_config.return_value = {
            "connection": {
                "bootstrap": "broker:9092",
                "schema_registry_url": "http://schema-registry:8081",
                "consumer_config": {},
            },
            "topic_routes": {},
        }

        # Override bootstrap via environment variable
        os.environ["KAFKA_BOOTSTRAP_SERVER"] = (
            "msk-cluster.kafka.us-east-1.amazonaws.com:9098"
        )

        # Execute
        start_ingestion_pipeline(mock_graph, mock_discovery, sighandler)

        # Verify
        call_args = mock_kafka_source.call_args
        bootstrap = call_args.kwargs["config"].connection.bootstrap

        assert bootstrap == "msk-cluster.kafka.us-east-1.amazonaws.com:9098"

        # Cleanup
        os.environ.pop("KAFKA_BOOTSTRAP_SERVER", None)

    @patch("datahub_executor.coordinator.helpers.Thread")
    @patch("datahub_executor.coordinator.helpers.Pipeline")
    @patch("datahub_executor.coordinator.helpers.IngestionAction")
    @patch("datahub_executor.coordinator.helpers.KafkaEventSource")
    @patch("datahub_executor.coordinator.helpers.load_config_file")
    @patch("datahub_executor.coordinator.helpers.asyncio.set_event_loop_policy")
    def test_start_ingestion_pipeline_plain_auth(
        self,
        mock_set_event_loop_policy: MagicMock,
        mock_load_config: MagicMock,
        mock_kafka_source: MagicMock,
        mock_ingestion_action: MagicMock,
        mock_pipeline: MagicMock,
        mock_thread: MagicMock,
    ) -> None:
        """Test starting ingestion pipeline with PLAIN authentication."""
        from datahub_executor.coordinator.helpers import start_ingestion_pipeline

        # Setup
        mock_graph = Mock()
        mock_discovery = Mock()
        sighandler: list[Callable[[], Any]] = []

        mock_load_config.return_value = {
            "connection": {
                "bootstrap": "broker:9092",
                "schema_registry_url": "http://schema-registry:8081",
                "consumer_config": {},
            },
            "topic_routes": {},
        }

        # Set PLAIN auth environment variables
        test_env = {
            "KAFKA_PROPERTIES_SECURITY_PROTOCOL": "SASL_SSL",
            "KAFKA_PROPERTIES_SASL_MECHANISM": "PLAIN",
            "KAFKA_PROPERTIES_SASL_USERNAME": "testuser",
            "KAFKA_PROPERTIES_SASL_PASSWORD": "testpassword",
        }

        with patch.dict(os.environ, test_env, clear=False):
            # Execute
            start_ingestion_pipeline(mock_graph, mock_discovery, sighandler)

            # Verify
            call_args = mock_kafka_source.call_args
            consumer_config = call_args.kwargs["config"].connection.consumer_config

            assert consumer_config["security.protocol"] == "SASL_SSL"
            assert consumer_config["sasl.mechanism"] == "PLAIN"
            assert consumer_config["sasl.username"] == "testuser"
            assert consumer_config["sasl.password"] == "testpassword"
