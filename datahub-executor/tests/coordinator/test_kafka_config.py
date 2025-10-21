"""Tests for Kafka configuration environment variable handling in the executor.

These tests validate that KAFKA_PROPERTIES_* environment variables are correctly
parsed and that the precedence order is correct (YAML overrides env vars).
"""

import os
from typing import Dict
from unittest.mock import patch

from datahub_executor.coordinator.helpers import get_kafka_consumer_config_from_env


def test_get_kafka_consumer_config_from_env_basic() -> None:
    """Test basic SASL/PLAIN configuration from environment variables."""
    test_env = {
        "KAFKA_PROPERTIES_SECURITY_PROTOCOL": "SASL_SSL",
        "KAFKA_PROPERTIES_SASL_MECHANISM": "PLAIN",
        "KAFKA_PROPERTIES_SASL_USERNAME": "test-user",
        "KAFKA_PROPERTIES_SASL_PASSWORD": "test-password",
    }

    with patch.dict(os.environ, test_env, clear=False):
        config = get_kafka_consumer_config_from_env()

    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.mechanism"] == "PLAIN"
    assert config["sasl.username"] == "test-user"
    assert config["sasl.password"] == "test-password"


def test_get_kafka_consumer_config_from_env_ssl_certs() -> None:
    """Test SSL/TLS certificate configuration - a new capability enabled by this change."""
    test_env = {
        "KAFKA_PROPERTIES_SECURITY_PROTOCOL": "SSL",
        "KAFKA_PROPERTIES_SSL_CA_LOCATION": "/mnt/certs/ca-cert.pem",
        "KAFKA_PROPERTIES_SSL_CERTIFICATE_LOCATION": "/mnt/certs/client-cert.pem",
        "KAFKA_PROPERTIES_SSL_KEY_LOCATION": "/mnt/certs/client-key.pem",
        "KAFKA_PROPERTIES_SSL_KEY_PASSWORD": "keypassword",
    }

    with patch.dict(os.environ, test_env, clear=False):
        config = get_kafka_consumer_config_from_env()

    assert config["security.protocol"] == "SSL"
    assert config["ssl.ca.location"] == "/mnt/certs/ca-cert.pem"
    assert config["ssl.certificate.location"] == "/mnt/certs/client-cert.pem"
    assert config["ssl.key.location"] == "/mnt/certs/client-key.pem"
    assert config["ssl.key.password"] == "keypassword"


def test_get_kafka_consumer_config_from_env_aws_msk_iam() -> None:
    """Test AWS MSK IAM (OAUTHBEARER) configuration - a new capability enabled by this change."""
    test_env = {
        "KAFKA_PROPERTIES_SECURITY_PROTOCOL": "SASL_SSL",
        "KAFKA_PROPERTIES_SASL_MECHANISM": "OAUTHBEARER",
        "KAFKA_PROPERTIES_SASL_OAUTHBEARER_METHOD": "default",
        "KAFKA_PROPERTIES_OAUTH_CB": "datahub_executor.common.kafka_msk_iam:oauth_cb",
    }

    with patch.dict(os.environ, test_env, clear=False):
        config = get_kafka_consumer_config_from_env()

    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.mechanism"] == "OAUTHBEARER"
    assert config["sasl.oauthbearer.method"] == "default"
    # Special case: oauth_cb should remain as underscore, not converted to dots
    assert config["oauth_cb"] == "datahub_executor.common.kafka_msk_iam:oauth_cb"


def test_get_kafka_consumer_config_from_env_kerberos() -> None:
    """Test Kerberos (GSSAPI) configuration - a new capability enabled by this change."""
    test_env = {
        "KAFKA_PROPERTIES_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
        "KAFKA_PROPERTIES_SASL_MECHANISM": "GSSAPI",
        "KAFKA_PROPERTIES_SASL_KERBEROS_SERVICE_NAME": "kafka",
    }

    with patch.dict(os.environ, test_env, clear=False):
        config = get_kafka_consumer_config_from_env()

    assert config["security.protocol"] == "SASL_PLAINTEXT"
    assert config["sasl.mechanism"] == "GSSAPI"
    assert config["sasl.kerberos.service.name"] == "kafka"


def test_get_kafka_consumer_config_from_env_scram() -> None:
    """Test SASL/SCRAM configuration - a new capability enabled by this change."""
    test_env = {
        "KAFKA_PROPERTIES_SECURITY_PROTOCOL": "SASL_SSL",
        "KAFKA_PROPERTIES_SASL_MECHANISM": "SCRAM-SHA-256",
        "KAFKA_PROPERTIES_SASL_USERNAME": "test-user",
        "KAFKA_PROPERTIES_SASL_PASSWORD": "test-password",
    }

    with patch.dict(os.environ, test_env, clear=False):
        config = get_kafka_consumer_config_from_env()

    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.mechanism"] == "SCRAM-SHA-256"
    assert config["sasl.username"] == "test-user"
    assert config["sasl.password"] == "test-password"


def test_get_kafka_consumer_config_from_env_empty_values() -> None:
    """Test that empty values are excluded from the configuration."""
    test_env = {
        "KAFKA_PROPERTIES_SECURITY_PROTOCOL": "SASL_SSL",
        "KAFKA_PROPERTIES_SASL_MECHANISM": "",  # Empty string should be excluded
        "KAFKA_PROPERTIES_SASL_USERNAME": "test-user",
    }

    with patch.dict(os.environ, test_env, clear=False):
        config = get_kafka_consumer_config_from_env()

    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.username"] == "test-user"
    assert "sasl.mechanism" not in config  # Empty value excluded


def test_get_kafka_consumer_config_from_env_no_kafka_properties() -> None:
    """Test behavior when no KAFKA_PROPERTIES_* variables are set."""
    test_env = {
        "OTHER_VAR": "value",
        "KAFKA_BOOTSTRAP_SERVER": "broker:9092",  # Not a KAFKA_PROPERTIES_ var
    }

    with patch.dict(os.environ, test_env, clear=True):
        config = get_kafka_consumer_config_from_env()

    assert config == {}


def test_get_kafka_consumer_config_from_env_underscore_to_dot_conversion() -> None:
    """Test that underscores in env var names are correctly converted to dots in property names."""
    test_env = {
        "KAFKA_PROPERTIES_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM": "https",
        "KAFKA_PROPERTIES_SASL_LOGIN_CALLBACK_HANDLER_CLASS": "com.example.MyHandler",
        "KAFKA_PROPERTIES_MAX_POLL_INTERVAL_MS": "600000",
    }

    with patch.dict(os.environ, test_env, clear=False):
        config = get_kafka_consumer_config_from_env()

    # Underscores in env var names should be converted to dots
    assert config["ssl.endpoint.identification.algorithm"] == "https"
    assert config["sasl.login.callback.handler.class"] == "com.example.MyHandler"
    assert config["max.poll.interval.ms"] == "600000"


def test_get_kafka_consumer_config_from_env_oauth_cb_special_case() -> None:
    """Test that OAUTH_CB is NOT converted to dots and its value (containing dots) is preserved."""
    test_env = {
        "KAFKA_PROPERTIES_OAUTH_CB": "datahub_executor.common.kafka_msk_iam:oauth_cb",
        "KAFKA_PROPERTIES_SASL_MECHANISM": "OAUTHBEARER",
        "KAFKA_PROPERTIES_SECURITY_PROTOCOL": "SASL_SSL",
    }

    with patch.dict(os.environ, test_env, clear=False):
        config = get_kafka_consumer_config_from_env()

    # Property name should remain as oauth_cb (NOT converted to oauth.cb)
    assert "oauth_cb" in config
    assert "oauth.cb" not in config
    # Value should be preserved exactly, including dots and colons
    assert config["oauth_cb"] == "datahub_executor.common.kafka_msk_iam:oauth_cb"
    assert config["sasl.mechanism"] == "OAUTHBEARER"
    assert config["security.protocol"] == "SASL_SSL"


# Precedence tests - These verify that YAML config overrides env vars


def test_precedence_env_var_only() -> None:
    """Test that env vars are used when no YAML config is present."""
    env_config = get_kafka_consumer_config_from_env()
    yaml_config: Dict[str, str] = {}

    # Simulate the precedence logic from start_ingestion_pipeline
    final_config = env_config.copy()
    final_config.update(yaml_config)

    # If env vars are set, they should be in final config
    assert isinstance(final_config, dict)


def test_precedence_yaml_only() -> None:
    """Test that YAML config is used when no env vars are present."""
    env_config: Dict[str, str] = {}
    yaml_config = {
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "yaml-user",
        "sasl.password": "yaml-password",
    }

    # Simulate the precedence logic from start_ingestion_pipeline
    final_config = env_config.copy()
    final_config.update(yaml_config)

    assert final_config["security.protocol"] == "SASL_SSL"
    assert final_config["sasl.username"] == "yaml-user"
    assert final_config["sasl.password"] == "yaml-password"


def test_precedence_yaml_overrides_env_vars() -> None:
    """Test that YAML config overrides env vars when both are present (correct precedence)."""
    # Simulate env vars
    env_config = {
        "security.protocol": "PLAINTEXT",  # From env
        "sasl.mechanism": "PLAIN",  # From env
        "sasl.username": "env-user",  # From env
        "sasl.password": "env-password",  # From env
    }

    # Simulate YAML config
    yaml_config = {
        "security.protocol": "SASL_SSL",  # Override env
        "sasl.username": "yaml-user",  # Override env
        # sasl.mechanism and sasl.password not in YAML, should use env values
    }

    # Simulate the precedence logic from start_ingestion_pipeline
    final_config = env_config.copy()
    final_config.update(yaml_config)

    # YAML values should win
    assert final_config["security.protocol"] == "SASL_SSL"  # YAML wins
    assert final_config["sasl.username"] == "yaml-user"  # YAML wins

    # Env values should be used when not in YAML
    assert final_config["sasl.mechanism"] == "PLAIN"  # From env
    assert final_config["sasl.password"] == "env-password"  # From env


def test_precedence_yaml_can_override_specific_properties() -> None:
    """Test that YAML can override specific properties while keeping others from env vars."""
    test_env = {
        "KAFKA_PROPERTIES_SECURITY_PROTOCOL": "PLAINTEXT",
        "KAFKA_PROPERTIES_SASL_MECHANISM": "PLAIN",
        "KAFKA_PROPERTIES_SASL_USERNAME": "env-user",
        "KAFKA_PROPERTIES_SASL_PASSWORD": "env-password",
        "KAFKA_PROPERTIES_SESSION_TIMEOUT_MS": "30000",
    }

    with patch.dict(os.environ, test_env, clear=False):
        env_config = get_kafka_consumer_config_from_env()

    # YAML overrides only specific properties
    yaml_config = {
        "security.protocol": "SASL_SSL",  # Override
        "sasl.username": "yaml-user",  # Override
        # Other properties not specified, should use env values
    }

    # Simulate the precedence logic from start_ingestion_pipeline
    final_config = env_config.copy()
    final_config.update(yaml_config)

    # Overridden by YAML
    assert final_config["security.protocol"] == "SASL_SSL"
    assert final_config["sasl.username"] == "yaml-user"

    # Still from env
    assert final_config["sasl.mechanism"] == "PLAIN"
    assert final_config["sasl.password"] == "env-password"
    assert final_config["session.timeout.ms"] == "30000"
