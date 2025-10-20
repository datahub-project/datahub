"""Tests for coordinator helpers module."""

import os
from typing import Any, Callable
from unittest.mock import MagicMock, Mock, patch


class TestBuildOAuthConsumerConfig:
    """Tests for _build_oauth_consumer_config function."""

    def teardown_method(self) -> None:
        """Clean up environment variables after each test."""
        env_vars_to_clean = [
            "KAFKA_PROPERTIES_SASL_MECHANISM",
            "KAFKA_PROPERTIES_OAUTH_CALLBACK",
            "KAFKA_PROPERTIES_SASL_OAUTHBEARER_METHOD",
            "KAFKA_PROPERTIES_SASL_USERNAME",
            "KAFKA_PROPERTIES_SASL_PASSWORD",
        ]
        for var in env_vars_to_clean:
            os.environ.pop(var, None)

    def test_no_sasl_mechanism_returns_empty(self) -> None:
        """Test that no SASL mechanism returns empty config."""
        from datahub_executor.coordinator.helpers import _build_oauth_consumer_config

        result = _build_oauth_consumer_config()
        assert result == {}

    def test_oauthbearer_with_default_callback(self) -> None:
        """Test OAUTHBEARER mechanism with default MSK IAM callback."""
        from datahub_executor.coordinator.helpers import _build_oauth_consumer_config

        os.environ["KAFKA_PROPERTIES_SASL_MECHANISM"] = "OAUTHBEARER"

        result = _build_oauth_consumer_config()

        assert result == {
            "sasl.mechanism": "OAUTHBEARER",
            "sasl.oauthbearer.method": "default",
            "oauth_cb": "datahub_executor.common.kafka_msk_iam:oauth_cb",
        }

    def test_oauthbearer_with_custom_callback(self) -> None:
        """Test OAUTHBEARER mechanism with custom callback."""
        from datahub_executor.coordinator.helpers import _build_oauth_consumer_config

        os.environ["KAFKA_PROPERTIES_SASL_MECHANISM"] = "OAUTHBEARER"
        os.environ["KAFKA_PROPERTIES_OAUTH_CALLBACK"] = (
            "datahub_executor.common.kafka_eventhubs_auth:oauth_cb"
        )

        result = _build_oauth_consumer_config()

        assert result == {
            "sasl.mechanism": "OAUTHBEARER",
            "sasl.oauthbearer.method": "default",
            "oauth_cb": "datahub_executor.common.kafka_eventhubs_auth:oauth_cb",
        }

    def test_oauthbearer_with_custom_method(self) -> None:
        """Test OAUTHBEARER mechanism with custom oauth method."""
        from datahub_executor.coordinator.helpers import _build_oauth_consumer_config

        os.environ["KAFKA_PROPERTIES_SASL_MECHANISM"] = "OAUTHBEARER"
        os.environ["KAFKA_PROPERTIES_SASL_OAUTHBEARER_METHOD"] = "oidc"

        result = _build_oauth_consumer_config()

        assert result == {
            "sasl.mechanism": "OAUTHBEARER",
            "sasl.oauthbearer.method": "oidc",
            "oauth_cb": "datahub_executor.common.kafka_msk_iam:oauth_cb",
        }

    def test_plain_mechanism_with_credentials(self) -> None:
        """Test PLAIN mechanism with username and password."""
        from datahub_executor.coordinator.helpers import _build_oauth_consumer_config

        os.environ["KAFKA_PROPERTIES_SASL_MECHANISM"] = "PLAIN"
        os.environ["KAFKA_PROPERTIES_SASL_USERNAME"] = "$ConnectionString"
        os.environ["KAFKA_PROPERTIES_SASL_PASSWORD"] = (
            "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123"
        )

        result = _build_oauth_consumer_config()

        assert result == {
            "sasl.mechanism": "PLAIN",
            "sasl.username": "$ConnectionString",
            "sasl.password": "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123",
        }

    def test_plain_mechanism_missing_username(self) -> None:
        """Test PLAIN mechanism with missing username logs warning."""
        from datahub_executor.coordinator.helpers import _build_oauth_consumer_config

        os.environ["KAFKA_PROPERTIES_SASL_MECHANISM"] = "PLAIN"
        os.environ["KAFKA_PROPERTIES_SASL_PASSWORD"] = "password"

        with patch("datahub_executor.coordinator.helpers.logger") as mock_logger:
            result = _build_oauth_consumer_config()

            mock_logger.warning.assert_called_once()
            assert result == {"sasl.mechanism": "PLAIN"}

    def test_plain_mechanism_missing_password(self) -> None:
        """Test PLAIN mechanism with missing password logs warning."""
        from datahub_executor.coordinator.helpers import _build_oauth_consumer_config

        os.environ["KAFKA_PROPERTIES_SASL_MECHANISM"] = "PLAIN"
        os.environ["KAFKA_PROPERTIES_SASL_USERNAME"] = "username"

        with patch("datahub_executor.coordinator.helpers.logger") as mock_logger:
            result = _build_oauth_consumer_config()

            mock_logger.warning.assert_called_once()
            assert result == {"sasl.mechanism": "PLAIN"}

    def test_unknown_mechanism(self) -> None:
        """Test unknown SASL mechanism logs warning."""
        from datahub_executor.coordinator.helpers import _build_oauth_consumer_config

        os.environ["KAFKA_PROPERTIES_SASL_MECHANISM"] = "SCRAM-SHA-256"

        with patch("datahub_executor.coordinator.helpers.logger") as mock_logger:
            result = _build_oauth_consumer_config()

            mock_logger.warning.assert_called_once()
            assert result == {"sasl.mechanism": "SCRAM-SHA-256"}


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
        os.environ["KAFKA_PROPERTIES_SECURITY_PROTOCOL"] = "SASL_SSL"
        os.environ["KAFKA_PROPERTIES_SASL_MECHANISM"] = "OAUTHBEARER"
        os.environ["KAFKA_PROPERTIES_OAUTH_CALLBACK"] = (
            "datahub_executor.common.kafka_msk_iam:oauth_cb"
        )

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

        # Cleanup
        os.environ.pop("KAFKA_PROPERTIES_SECURITY_PROTOCOL", None)
        os.environ.pop("KAFKA_PROPERTIES_SASL_MECHANISM", None)
        os.environ.pop("KAFKA_PROPERTIES_OAUTH_CALLBACK", None)

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
        os.environ["KAFKA_PROPERTIES_SECURITY_PROTOCOL"] = "SASL_SSL"
        os.environ["KAFKA_PROPERTIES_SASL_MECHANISM"] = "PLAIN"
        os.environ["KAFKA_PROPERTIES_SASL_USERNAME"] = "testuser"
        os.environ["KAFKA_PROPERTIES_SASL_PASSWORD"] = "testpassword"

        # Execute
        start_ingestion_pipeline(mock_graph, mock_discovery, sighandler)

        # Verify
        call_args = mock_kafka_source.call_args
        consumer_config = call_args.kwargs["config"].connection.consumer_config

        assert consumer_config["security.protocol"] == "SASL_SSL"
        assert consumer_config["sasl.mechanism"] == "PLAIN"
        assert consumer_config["sasl.username"] == "testuser"
        assert consumer_config["sasl.password"] == "testpassword"

        # Cleanup
        os.environ.pop("KAFKA_PROPERTIES_SECURITY_PROTOCOL", None)
        os.environ.pop("KAFKA_PROPERTIES_SASL_MECHANISM", None)
        os.environ.pop("KAFKA_PROPERTIES_SASL_USERNAME", None)
        os.environ.pop("KAFKA_PROPERTIES_SASL_PASSWORD", None)
