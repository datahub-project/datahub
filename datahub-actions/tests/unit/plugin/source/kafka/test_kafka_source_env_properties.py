# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from typing import Any, Dict
from unittest.mock import patch

import pytest

from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.source.kafka.kafka_event_source import (
    KafkaEventSource,
    KafkaEventSourceConfig,
)
from datahub_actions.utils.kafka_msk_iam import oauth_cb


@pytest.fixture(autouse=True)
def _clean_kafka_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in list(os.environ):
        if name.startswith("KAFKA_PROPERTIES_"):
            monkeypatch.delenv(name)


def _consumer_config(config_dict: Dict[str, Any]) -> Dict[str, Any]:
    config = KafkaEventSourceConfig.model_validate(config_dict)
    ctx = PipelineContext(pipeline_name="test-pipeline", graph=None)
    with (
        patch(
            "datahub_actions.plugin.source.kafka.kafka_event_source.confluent_kafka.DeserializingConsumer"
        ) as consumer_cls,
        patch(
            "datahub_actions.plugin.source.kafka.kafka_event_source.SchemaRegistryClient"
        ),
    ):
        KafkaEventSource(config, ctx)
    return consumer_cls.call_args.args[0]


def test_env_properties_are_mapped_to_consumer_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KAFKA_PROPERTIES_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_PROPERTIES_SASL_MECHANISM", "PLAIN")
    monkeypatch.setenv("KAFKA_PROPERTIES_SASL_USERNAME", "user")
    monkeypatch.setenv("KAFKA_PROPERTIES_SESSION_TIMEOUT_MS", "45000")
    monkeypatch.setenv("KAFKA_PROPERTIES_SASL_PASSWORD", "")
    monkeypatch.setenv("SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG", "ignored")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVER", "ignored:9092")

    config = _consumer_config({})

    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.mechanism"] == "PLAIN"
    assert config["sasl.username"] == "user"
    assert config["session.timeout.ms"] == "45000"
    assert "sasl.password" not in config
    assert "sasl.jaas.config" not in config
    assert "kafka.bootstrap.server" not in config


def test_recipe_consumer_config_takes_precedence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KAFKA_PROPERTIES_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_PROPERTIES_SASL_MECHANISM", "PLAIN")

    config = _consumer_config(
        {"connection": {"consumer_config": {"security.protocol": "SSL"}}}
    )

    assert config["security.protocol"] == "SSL"
    assert config["sasl.mechanism"] == "PLAIN"


def test_oauth_cb_from_env_is_resolved_to_callable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "KAFKA_PROPERTIES_OAUTH_CB", "datahub_actions.utils.kafka_msk_iam:oauth_cb"
    )

    assert _consumer_config({})["oauth_cb"] is oauth_cb


def test_recipe_oauth_cb_wins_without_resolving_env_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KAFKA_PROPERTIES_OAUTH_CB", "does.not.exist:oauth_cb")

    config = _consumer_config(
        {
            "connection": {
                "consumer_config": {
                    "oauth_cb": "datahub_actions.utils.kafka_msk_iam:oauth_cb"
                }
            }
        }
    )

    assert config["oauth_cb"] is oauth_cb


@pytest.mark.parametrize(
    "env_var",
    [
        # Java client and schema registry properties the Helm charts pass to every
        # service; librdkafka rejects them as unknown configuration properties.
        "KAFKA_PROPERTIES_SASL_JAAS_CONFIG",
        "KAFKA_PROPERTIES_SSL_TRUSTSTORE_LOCATION",
        "KAFKA_PROPERTIES_SSL_KEYSTORE_PASSWORD",
        "KAFKA_PROPERTIES_BASIC_AUTH_USER_INFO",
        # Each pipeline consumes with its own group id.
        "KAFKA_PROPERTIES_GROUP_ID",
    ],
)
def test_properties_librdkafka_cannot_use_are_skipped(
    monkeypatch: pytest.MonkeyPatch, env_var: str
) -> None:
    monkeypatch.setenv(env_var, "value")

    config = _consumer_config({})

    prop = env_var[len("KAFKA_PROPERTIES_") :].lower().replace("_", ".")
    assert config["group.id"] == "test-pipeline"
    if prop != "group.id":
        assert prop not in config


def test_env_properties_can_be_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_PROPERTIES_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("DATAHUB_ACTIONS_KAFKA_ENV_PROPERTIES_ENABLED", "false")

    assert "security.protocol" not in _consumer_config({})
