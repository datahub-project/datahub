"""Tests for Kafka configuration environment variable handling.

These tests validate that the OAuth environment variables are correctly
applied to the base action configuration for different authentication mechanisms:
- PLAIN/SCRAM (username/password)
- OAUTHBEARER (MSK IAM, Azure Event Hubs, etc.)
- No authentication

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

        # Should have PLAIN SASL mechanism config
        assert (
            consumer_config["sasl.mechanism"]
            == "${KAFKA_PROPERTIES_SASL_MECHANISM:-PLAIN}"
        )
        assert consumer_config["sasl.username"] == "${KAFKA_PROPERTIES_SASL_USERNAME:-}"
        assert consumer_config["sasl.password"] == "${KAFKA_PROPERTIES_SASL_PASSWORD:-}"
        assert (
            consumer_config["security.protocol"]
            == "${KAFKA_PROPERTIES_SECURITY_PROTOCOL:-PLAINTEXT}"
        )

        # Should not have OAUTHBEARER config
        assert "sasl.oauthbearer.method" not in consumer_config
        assert "oauth_cb" not in consumer_config


def test_base_action_config_with_oauthbearer_mechanism() -> None:
    """Test that base_action_config correctly applies OAUTHBEARER SASL mechanism with default MSK IAM callback."""
    with patch.dict(
        os.environ,
        {
            "KAFKA_PROPERTIES_SASL_MECHANISM": "OAUTHBEARER",
            "KAFKA_PROPERTIES_SASL_OAUTHBEARER_METHOD": "custom_method",
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

        # Should have OAUTHBEARER config
        assert (
            consumer_config["sasl.mechanism"]
            == "${KAFKA_PROPERTIES_SASL_MECHANISM:-OAUTHBEARER}"
        )
        assert (
            consumer_config["sasl.oauthbearer.method"]
            == "${KAFKA_PROPERTIES_SASL_OAUTHBEARER_METHOD:-default}"
        )
        assert (
            consumer_config["oauth_cb"]
            == "${KAFKA_PROPERTIES_OAUTH_CALLBACK:-datahub_actions.utils.kafka_msk_iam:oauth_cb}"
        )
        assert (
            consumer_config["security.protocol"]
            == "${KAFKA_PROPERTIES_SECURITY_PROTOCOL:-PLAINTEXT}"
        )

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

        # Should have SCRAM SASL mechanism config (same as PLAIN)
        assert (
            consumer_config["sasl.mechanism"]
            == "${KAFKA_PROPERTIES_SASL_MECHANISM:-PLAIN}"
        )
        assert consumer_config["sasl.username"] == "${KAFKA_PROPERTIES_SASL_USERNAME:-}"
        assert consumer_config["sasl.password"] == "${KAFKA_PROPERTIES_SASL_PASSWORD:-}"
        assert (
            consumer_config["security.protocol"]
            == "${KAFKA_PROPERTIES_SECURITY_PROTOCOL:-PLAINTEXT}"
        )

        # Should not have OAUTHBEARER config
        assert "sasl.oauthbearer.method" not in consumer_config
        assert "oauth_cb" not in consumer_config


def test_base_action_config_with_custom_oauth_callback() -> None:
    """Test that base_action_config correctly applies a custom OAuth callback."""
    with patch.dict(
        os.environ,
        {
            "KAFKA_PROPERTIES_SASL_MECHANISM": "OAUTHBEARER",
            "KAFKA_PROPERTIES_OAUTH_CALLBACK": "my_custom_module:my_oauth_cb",
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

        # Should have OAUTHBEARER config with custom callback
        assert (
            consumer_config["sasl.mechanism"]
            == "${KAFKA_PROPERTIES_SASL_MECHANISM:-OAUTHBEARER}"
        )
        assert (
            consumer_config["oauth_cb"]
            == "${KAFKA_PROPERTIES_OAUTH_CALLBACK:-datahub_actions.utils.kafka_msk_iam:oauth_cb}"
        )
        assert (
            consumer_config["security.protocol"]
            == "${KAFKA_PROPERTIES_SECURITY_PROTOCOL:-PLAINTEXT}"
        )


def test_base_action_config_with_event_hubs_oauth() -> None:
    """Test that base_action_config correctly applies Azure Event Hubs OAuth callback."""
    with patch.dict(
        os.environ,
        {
            "KAFKA_PROPERTIES_SASL_MECHANISM": "OAUTHBEARER",
            "KAFKA_PROPERTIES_OAUTH_CALLBACK": "datahub_actions.utils.kafka_eventhubs_auth:oauth_cb",
            "KAFKA_PROPERTIES_SASL_OAUTHBEARER_METHOD": "default",
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

        # Should have OAUTHBEARER config with Event Hubs callback
        assert (
            consumer_config["sasl.mechanism"]
            == "${KAFKA_PROPERTIES_SASL_MECHANISM:-OAUTHBEARER}"
        )
        assert (
            consumer_config["oauth_cb"]
            == "${KAFKA_PROPERTIES_OAUTH_CALLBACK:-datahub_actions.utils.kafka_msk_iam:oauth_cb}"
        )
        assert (
            consumer_config["sasl.oauthbearer.method"]
            == "${KAFKA_PROPERTIES_SASL_OAUTHBEARER_METHOD:-default}"
        )
        assert (
            consumer_config["security.protocol"]
            == "${KAFKA_PROPERTIES_SECURITY_PROTOCOL:-PLAINTEXT}"
        )

        # Should not have PLAIN/SCRAM config
        assert "sasl.username" not in consumer_config
        assert "sasl.password" not in consumer_config


def test_base_action_config_without_sasl_mechanism() -> None:
    """Test that base_action_config uses defaults when no SASL mechanism is set."""
    # Ensure KAFKA_PROPERTIES_SASL_MECHANISM is not set
    env_vars = os.environ.copy()
    env_vars.pop("KAFKA_PROPERTIES_SASL_MECHANISM", None)

    with patch.dict(os.environ, env_vars, clear=True):
        # Re-import to get fresh config without environment variables
        import importlib

        import datahub_integrations.actions.router as router_module

        importlib.reload(router_module)
        config = router_module.base_action_config

        consumer_config = config["source"]["config"]["connection"]["consumer_config"]

        # Should only have security.protocol
        assert (
            consumer_config["security.protocol"]
            == "${KAFKA_PROPERTIES_SECURITY_PROTOCOL:-PLAINTEXT}"
        )

        # Should not have SASL-specific config
        assert "sasl.mechanism" not in consumer_config
        assert "sasl.username" not in consumer_config
        assert "sasl.password" not in consumer_config
        assert "sasl.oauthbearer.method" not in consumer_config
        assert "oauth_cb" not in consumer_config
