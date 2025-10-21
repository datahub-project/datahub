from unittest.mock import patch

from datahub_integrations.actions.router import (
    get_config_from_details,
    get_kafka_consumer_config_from_env,
)


def test_get_config_from_details_preserves_custom_source_config() -> None:
    """Test that custom source configuration from GraphQL mutation overrides base config."""
    # Mock action details with custom datahub-stream source configuration
    action_details = {
        "config": {
            "recipe": """{
                "source": {
                    "type": "datahub-stream",
                    "config": {
                        "auto_offset_reset": "latest",
                        "connection": {
                            "bootstrap": "custom-kafka:9092",
                            "schema_registry_url": "http://custom:8081",
                            "consumer_config": {
                                "security.protocol": "SASL_SSL",
                                "sasl.mechanism": "OAUTHBEARER",
                                "sasl.oauthbearer.method": "default",
                                "oauth_cb": "datahub_actions.utils.kafka_msk_iam:oauth_cb"
                            }
                        }
                    }
                },
                "action": {
                    "type": "datahub_integrations.propagation.propagation_v2.propagation_v2_action.PropagationV2Action",
                    "config": {
                        "enabled": true
                    }
                }
            }"""
        }
    }

    action_urn = "urn:li:dataHubAction:test"
    executor_id = "default"

    # Mock the secret stores
    with patch("datahub_integrations.actions.router.secret_stores", []):
        with patch("datahub_integrations.actions.router.DataHubSecretStore"):
            with patch("datahub_integrations.actions.router.DataHubSecretStoreConfig"):
                with patch(
                    "datahub_integrations.actions.router.EnvironmentSecretStore"
                ):
                    with patch(
                        "datahub_integrations.actions.router.resolve_recipe"
                    ) as mock_resolve:
                        # Mock resolve_recipe to return parsed recipe
                        mock_resolve.return_value = {
                            "source": {
                                "type": "datahub-stream",
                                "config": {
                                    "auto_offset_reset": "latest",
                                    "connection": {
                                        "bootstrap": "custom-kafka:9092",
                                        "schema_registry_url": "http://custom:8081",
                                        "consumer_config": {
                                            "security.protocol": "SASL_SSL",
                                            "sasl.mechanism": "OAUTHBEARER",
                                            "sasl.oauthbearer.method": "default",
                                            "oauth_cb": "datahub_actions.utils.kafka_msk_iam:oauth_cb",
                                        },
                                    },
                                },
                            },
                            "action": {
                                "type": "datahub_integrations.propagation.propagation_v2.propagation_v2_action.PropagationV2Action",
                                "config": {"enabled": True},
                            },
                        }

                        recipe = get_config_from_details(
                            action_urn, action_details, executor_id
                        )

                        # Assert that custom source configuration is preserved
                        assert recipe["source"]["type"] == "datahub-stream"
                        assert (
                            recipe["source"]["config"]["connection"]["bootstrap"]
                            == "custom-kafka:9092"
                        )
                        assert (
                            recipe["source"]["config"]["connection"][
                                "schema_registry_url"
                            ]
                            == "http://custom:8081"
                        )
                        assert (
                            recipe["source"]["config"]["connection"]["consumer_config"][
                                "security.protocol"
                            ]
                            == "SASL_SSL"
                        )
                        assert (
                            recipe["source"]["config"]["connection"]["consumer_config"][
                                "sasl.mechanism"
                            ]
                            == "OAUTHBEARER"
                        )
                        assert (
                            "oauth_cb"
                            in recipe["source"]["config"]["connection"][
                                "consumer_config"
                            ]
                        )


def test_get_config_from_details_uses_base_config_as_fallback() -> None:
    """Test that base configuration is used for missing fields."""
    # Mock action details with minimal custom configuration
    action_details = {
        "config": {
            "recipe": """{
                "action": {
                    "type": "datahub_integrations.propagation.propagation_v2.propagation_v2_action.PropagationV2Action",
                    "config": {
                        "enabled": true
                    }
                }
            }"""
        }
    }

    action_urn = "urn:li:dataHubAction:test"
    executor_id = "default"

    # Mock the secret stores
    with patch("datahub_integrations.actions.router.secret_stores", []):
        with patch("datahub_integrations.actions.router.DataHubSecretStore"):
            with patch("datahub_integrations.actions.router.DataHubSecretStoreConfig"):
                with patch(
                    "datahub_integrations.actions.router.EnvironmentSecretStore"
                ):
                    with patch(
                        "datahub_integrations.actions.router.resolve_recipe"
                    ) as mock_resolve:
                        # Mock resolve_recipe to return parsed recipe with only action config
                        mock_resolve.return_value = {
                            "action": {
                                "type": "datahub_integrations.propagation.propagation_v2.propagation_v2_action.PropagationV2Action",
                                "config": {"enabled": True},
                            }
                        }

                        recipe = get_config_from_details(
                            action_urn, action_details, executor_id
                        )

                        # Assert that base source configuration is used
                        assert (
                            recipe["source"]["type"] == "kafka"
                        )  # From base_action_config
                        assert (
                            "${KAFKA_BOOTSTRAP_SERVER:-broker:29092}"
                            in recipe["source"]["config"]["connection"]["bootstrap"]
                        )
                        assert (
                            recipe["datahub"]["server"] is not None
                        )  # From base_action_config

                        # Assert that custom action configuration is preserved
                        assert (
                            recipe["action"]["type"]
                            == "datahub_integrations.propagation.propagation_v2.propagation_v2_action.PropagationV2Action"
                        )
                        assert recipe["action"]["config"]["enabled"] is True


def test_get_config_from_details_adds_consumer_group_prefix() -> None:
    """Test that consumer group prefix is added to action name when environment variable is set."""
    action_details = {
        "config": {
            "recipe": """{
                "action": {
                    "type": "test.action",
                    "config": {}
                }
            }"""
        }
    }

    action_urn = "urn:li:dataHubAction:test"
    executor_id = "default"

    with patch("datahub_integrations.actions.router.secret_stores", []):
        with patch("datahub_integrations.actions.router.DataHubSecretStore"):
            with patch("datahub_integrations.actions.router.DataHubSecretStoreConfig"):
                with patch(
                    "datahub_integrations.actions.router.EnvironmentSecretStore"
                ):
                    with patch(
                        "datahub_integrations.actions.router.resolve_recipe"
                    ) as mock_resolve:
                        mock_resolve.return_value = {
                            "action": {"type": "test.action", "config": {}}
                        }

                        # Test without consumer group prefix
                        with patch("os.getenv", return_value=None):
                            recipe = get_config_from_details(
                                action_urn, action_details, executor_id
                            )
                            assert recipe["name"] == action_urn

                        # Test with consumer group prefix
                        with patch("os.getenv", return_value="tenant1"):
                            recipe = get_config_from_details(
                                action_urn, action_details, executor_id
                            )
                            assert recipe["name"] == f"tenant1_{action_urn}"


def test_get_kafka_consumer_config_from_env_basic() -> None:
    """Test basic extraction of Kafka properties from environment variables."""
    test_env = {
        "KAFKA_PROPERTIES_SECURITY_PROTOCOL": "SASL_SSL",
        "KAFKA_PROPERTIES_SASL_MECHANISM": "PLAIN",
        "KAFKA_PROPERTIES_SASL_USERNAME": "test-user",
        "KAFKA_PROPERTIES_SASL_PASSWORD": "test-password",
        "OTHER_ENV_VAR": "should-be-ignored",
    }

    with patch("os.environ", test_env):
        config = get_kafka_consumer_config_from_env()

    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.mechanism"] == "PLAIN"
    assert config["sasl.username"] == "test-user"
    assert config["sasl.password"] == "test-password"
    assert "other.env.var" not in config
    assert len(config) == 4


def test_get_kafka_consumer_config_from_env_ssl_certs() -> None:
    """Test SSL/TLS certificate configuration - a previously unsupported authentication method."""
    test_env = {
        "KAFKA_PROPERTIES_SECURITY_PROTOCOL": "SSL",
        "KAFKA_PROPERTIES_SSL_CA_LOCATION": "/mnt/certs/ca-cert.pem",
        "KAFKA_PROPERTIES_SSL_CERTIFICATE_LOCATION": "/mnt/certs/client-cert.pem",
        "KAFKA_PROPERTIES_SSL_KEY_LOCATION": "/mnt/certs/client-key.pem",
        "KAFKA_PROPERTIES_SSL_KEY_PASSWORD": "keypassword",
    }

    with patch("os.environ", test_env):
        config = get_kafka_consumer_config_from_env()

    assert config["security.protocol"] == "SSL"
    assert config["ssl.ca.location"] == "/mnt/certs/ca-cert.pem"
    assert config["ssl.certificate.location"] == "/mnt/certs/client-cert.pem"
    assert config["ssl.key.location"] == "/mnt/certs/client-key.pem"
    assert config["ssl.key.password"] == "keypassword"
    assert len(config) == 5


def test_get_kafka_consumer_config_from_env_aws_msk_iam() -> None:
    """Test AWS MSK IAM (OAUTHBEARER) configuration - a previously unsupported authentication method."""
    test_env = {
        "KAFKA_PROPERTIES_SECURITY_PROTOCOL": "SASL_SSL",
        "KAFKA_PROPERTIES_SASL_MECHANISM": "OAUTHBEARER",
        "KAFKA_PROPERTIES_SASL_OAUTHBEARER_METHOD": "default",
        "KAFKA_PROPERTIES_OAUTH_CB": "datahub_executor.common.kafka_msk_iam:oauth_cb",
    }

    with patch("os.environ", test_env):
        config = get_kafka_consumer_config_from_env()

    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.mechanism"] == "OAUTHBEARER"
    assert config["sasl.oauthbearer.method"] == "default"
    assert config["oauth_cb"] == "datahub_executor.common.kafka_msk_iam:oauth_cb"
    assert len(config) == 4


def test_get_kafka_consumer_config_from_env_kerberos() -> None:
    """Test Kerberos (GSSAPI) configuration - a previously unsupported authentication method."""
    test_env = {
        "KAFKA_PROPERTIES_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
        "KAFKA_PROPERTIES_SASL_MECHANISM": "GSSAPI",
        "KAFKA_PROPERTIES_SASL_KERBEROS_SERVICE_NAME": "kafka",
    }

    with patch("os.environ", test_env):
        config = get_kafka_consumer_config_from_env()

    assert config["security.protocol"] == "SASL_PLAINTEXT"
    assert config["sasl.mechanism"] == "GSSAPI"
    assert config["sasl.kerberos.service.name"] == "kafka"
    assert len(config) == 3


def test_get_kafka_consumer_config_from_env_scram() -> None:
    """Test SASL/SCRAM configuration - a previously unsupported authentication method."""
    test_env = {
        "KAFKA_PROPERTIES_SECURITY_PROTOCOL": "SASL_SSL",
        "KAFKA_PROPERTIES_SASL_MECHANISM": "SCRAM-SHA-256",
        "KAFKA_PROPERTIES_SASL_USERNAME": "test-user",
        "KAFKA_PROPERTIES_SASL_PASSWORD": "test-password",
    }

    with patch("os.environ", test_env):
        config = get_kafka_consumer_config_from_env()

    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.mechanism"] == "SCRAM-SHA-256"
    assert config["sasl.username"] == "test-user"
    assert config["sasl.password"] == "test-password"
    assert len(config) == 4


def test_get_kafka_consumer_config_from_env_empty_values() -> None:
    """Test that empty environment variable values are not included in config."""
    test_env = {
        "KAFKA_PROPERTIES_SECURITY_PROTOCOL": "SASL_SSL",
        "KAFKA_PROPERTIES_SASL_MECHANISM": "",  # Empty string
        "KAFKA_PROPERTIES_SASL_USERNAME": "test-user",
    }

    with patch("os.environ", test_env):
        config = get_kafka_consumer_config_from_env()

    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.username"] == "test-user"
    assert "sasl.mechanism" not in config  # Empty value should be excluded
    assert len(config) == 2


def test_get_kafka_consumer_config_from_env_no_kafka_properties() -> None:
    """Test behavior when no KAFKA_PROPERTIES_* environment variables are set."""
    test_env = {
        "OTHER_VAR": "value",
        "KAFKA_BOOTSTRAP_SERVER": "broker:9092",  # Not KAFKA_PROPERTIES_*
    }

    with patch("os.environ", test_env):
        config = get_kafka_consumer_config_from_env()

    assert config == {}
    assert len(config) == 0


def test_get_kafka_consumer_config_from_env_underscore_to_dot_conversion() -> None:
    """Test that underscores in env var names are correctly converted to dots."""
    test_env = {
        "KAFKA_PROPERTIES_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM": "https",
        "KAFKA_PROPERTIES_SASL_LOGIN_CALLBACK_HANDLER_CLASS": "com.example.MyHandler",
        "KAFKA_PROPERTIES_MAX_POLL_INTERVAL_MS": "600000",
    }

    with patch("os.environ", test_env):
        config = get_kafka_consumer_config_from_env()

    assert config["ssl.endpoint.identification.algorithm"] == "https"
    assert config["sasl.login.callback.handler.class"] == "com.example.MyHandler"
    assert config["max.poll.interval.ms"] == "600000"
    assert len(config) == 3
