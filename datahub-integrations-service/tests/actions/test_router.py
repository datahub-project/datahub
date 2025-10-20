from unittest.mock import patch

from datahub_integrations.actions.router import get_config_from_details


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
