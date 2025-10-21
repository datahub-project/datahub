"""Tests for Kafka configuration environment variable handling.

These tests validate that KAFKA_PROPERTIES_* environment variables are correctly
applied to the base action configuration for different authentication mechanisms:
- PLAIN/SCRAM (username/password)
- OAUTHBEARER (MSK IAM, Azure Event Hubs, etc.)
- SSL/TLS with certificates
- Kerberos (GSSAPI)
- No authentication

The new implementation uses get_kafka_consumer_config_from_env() to dynamically
load any KAFKA_PROPERTIES_* environment variables at module initialization time.

Note: conftest.py mocks DataHub server connections to avoid requiring a running instance.
"""

import os
from unittest.mock import patch


def test_base_action_config_with_plain_sasl_mechanism() -> None:
    """Test that base_action_config correctly applies PLAIN SASL mechanism."""
    with patch.dict(
        os.environ,
        {
            "KAFKA_PROPERTIES_SASL_MECHANISM": "PLAIN",
            "KAFKA_PROPERTIES_SASL_USERNAME": "test_user",
            "KAFKA_PROPERTIES_SASL_PASSWORD": "test_password",
            "KAFKA_PROPERTIES_SECURITY_PROTOCOL": "SASL_SSL",
        },
        clear=False,
    ):
        # Re-import to get fresh config with environment variables
        import importlib

        import datahub_integrations.actions.router as router_module

        importlib.reload(router_module)
        config = router_module.base_action_config

        consumer_config = config["source"]["config"]["connection"]["consumer_config"]

        # Should have PLAIN SASL mechanism config loaded from environment
        assert consumer_config["sasl.mechanism"] == "PLAIN"
        assert consumer_config["sasl.username"] == "test_user"
        assert consumer_config["sasl.password"] == "test_password"
        assert consumer_config["security.protocol"] == "SASL_SSL"

        # Should not have OAUTHBEARER config
        assert "sasl.oauthbearer.method" not in consumer_config
        assert "oauth_cb" not in consumer_config


def test_base_action_config_with_oauthbearer_mechanism() -> None:
    """Test that base_action_config correctly applies OAUTHBEARER SASL mechanism."""
    with patch.dict(
        os.environ,
        {
            "KAFKA_PROPERTIES_SASL_MECHANISM": "OAUTHBEARER",
            "KAFKA_PROPERTIES_SASL_OAUTHBEARER_METHOD": "custom_method",
            "KAFKA_PROPERTIES_SECURITY_PROTOCOL": "SASL_SSL",
            "KAFKA_PROPERTIES_OAUTH_CB": "my_module:my_callback",
        },
        clear=False,
    ):
        # Re-import to get fresh config with environment variables
        import importlib

        import datahub_integrations.actions.router as router_module

        importlib.reload(router_module)
        config = router_module.base_action_config

        consumer_config = config["source"]["config"]["connection"]["consumer_config"]

        # Should have OAUTHBEARER config loaded from environment
        assert consumer_config["sasl.mechanism"] == "OAUTHBEARER"
        assert consumer_config["sasl.oauthbearer.method"] == "custom_method"
        assert consumer_config["oauth_cb"] == "my_module:my_callback"
        assert consumer_config["security.protocol"] == "SASL_SSL"

        # Should not have PLAIN/SCRAM config
        assert "sasl.username" not in consumer_config
        assert "sasl.password" not in consumer_config


def test_base_action_config_with_scram_mechanism() -> None:
    """Test that base_action_config correctly applies SCRAM SASL mechanism."""
    with patch.dict(
        os.environ,
        {
            "KAFKA_PROPERTIES_SASL_MECHANISM": "SCRAM-SHA-512",
            "KAFKA_PROPERTIES_SASL_USERNAME": "scram_user",
            "KAFKA_PROPERTIES_SASL_PASSWORD": "scram_password",
            "KAFKA_PROPERTIES_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
        },
        clear=False,
    ):
        # Re-import to get fresh config with environment variables
        import importlib

        import datahub_integrations.actions.router as router_module

        importlib.reload(router_module)
        config = router_module.base_action_config

        consumer_config = config["source"]["config"]["connection"]["consumer_config"]

        # Should have SCRAM SASL mechanism config loaded from environment
        assert consumer_config["sasl.mechanism"] == "SCRAM-SHA-512"
        assert consumer_config["sasl.username"] == "scram_user"
        assert consumer_config["sasl.password"] == "scram_password"
        assert consumer_config["security.protocol"] == "SASL_PLAINTEXT"

        # Should not have OAUTHBEARER config
        assert "sasl.oauthbearer.method" not in consumer_config
        assert "oauth_cb" not in consumer_config


def test_base_action_config_with_ssl_certs() -> None:
    """Test that base_action_config correctly applies SSL/TLS certificate configuration."""
    with patch.dict(
        os.environ,
        {
            "KAFKA_PROPERTIES_SECURITY_PROTOCOL": "SSL",
            "KAFKA_PROPERTIES_SSL_CA_LOCATION": "/mnt/certs/ca.pem",
            "KAFKA_PROPERTIES_SSL_CERTIFICATE_LOCATION": "/mnt/certs/cert.pem",
            "KAFKA_PROPERTIES_SSL_KEY_LOCATION": "/mnt/certs/key.pem",
        },
        clear=False,
    ):
        # Re-import to get fresh config with environment variables
        import importlib

        import datahub_integrations.actions.router as router_module

        importlib.reload(router_module)
        config = router_module.base_action_config

        consumer_config = config["source"]["config"]["connection"]["consumer_config"]

        # Should have SSL config loaded from environment
        assert consumer_config["security.protocol"] == "SSL"
        assert consumer_config["ssl.ca.location"] == "/mnt/certs/ca.pem"
        assert consumer_config["ssl.certificate.location"] == "/mnt/certs/cert.pem"
        assert consumer_config["ssl.key.location"] == "/mnt/certs/key.pem"

        # Should not have SASL config
        assert "sasl.mechanism" not in consumer_config
        assert "sasl.username" not in consumer_config
        assert "sasl.password" not in consumer_config


def test_base_action_config_with_kerberos() -> None:
    """Test that base_action_config correctly applies Kerberos configuration."""
    with patch.dict(
        os.environ,
        {
            "KAFKA_PROPERTIES_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
            "KAFKA_PROPERTIES_SASL_MECHANISM": "GSSAPI",
            "KAFKA_PROPERTIES_SASL_KERBEROS_SERVICE_NAME": "kafka",
        },
        clear=False,
    ):
        # Re-import to get fresh config with environment variables
        import importlib

        import datahub_integrations.actions.router as router_module

        importlib.reload(router_module)
        config = router_module.base_action_config

        consumer_config = config["source"]["config"]["connection"]["consumer_config"]

        # Should have Kerberos config loaded from environment
        assert consumer_config["security.protocol"] == "SASL_PLAINTEXT"
        assert consumer_config["sasl.mechanism"] == "GSSAPI"
        assert consumer_config["sasl.kerberos.service.name"] == "kafka"

        # Should not have username/password
        assert "sasl.username" not in consumer_config
        assert "sasl.password" not in consumer_config


def test_base_action_config_without_kafka_properties() -> None:
    """Test that base_action_config works when no KAFKA_PROPERTIES_* variables are set."""
    # Create a clean environment without any KAFKA_PROPERTIES_* variables
    env_vars = {
        k: v for k, v in os.environ.items() if not k.startswith("KAFKA_PROPERTIES_")
    }

    with patch.dict(os.environ, env_vars, clear=True):
        # Re-import to get fresh config without environment variables
        import importlib

        import datahub_integrations.actions.router as router_module

        importlib.reload(router_module)
        config = router_module.base_action_config

        consumer_config = config["source"]["config"]["connection"]["consumer_config"]

        # Should only have default configs
        assert "auto.offset.reset" in consumer_config
        assert "session.timeout.ms" in consumer_config
        assert "max.poll.interval.ms" in consumer_config

        # Should not have any Kafka auth properties from KAFKA_PROPERTIES_*
        assert "security.protocol" not in consumer_config
        assert "sasl.mechanism" not in consumer_config
        assert "sasl.username" not in consumer_config
        assert "sasl.password" not in consumer_config
        assert "sasl.oauthbearer.method" not in consumer_config
        assert "oauth_cb" not in consumer_config
