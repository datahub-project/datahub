import os
from unittest.mock import patch

import pytest

from datahub.configuration.env_vars import get_kafka_auto_register_schemas
from datahub.configuration.kafka import KafkaProducerConnectionConfig


@pytest.mark.parametrize(
    "env, expected",
    [
        ({}, True),
        ({"DATAHUB_KAFKA_AUTO_REGISTER_SCHEMAS": "true"}, True),
        ({"DATAHUB_KAFKA_AUTO_REGISTER_SCHEMAS": "false"}, False),
        ({"DATAHUB_KAFKA_AUTO_REGISTER_SCHEMAS": "FALSE"}, False),
    ],
)
def test_auto_register_schemas_env_var(env: dict, expected: bool) -> None:
    with patch.dict(os.environ, env, clear=True):
        assert get_kafka_auto_register_schemas() == expected
        assert KafkaProducerConnectionConfig().auto_register_schemas == expected


def test_auto_register_schemas_recipe_overrides_env_var() -> None:
    with patch.dict(os.environ, {"DATAHUB_KAFKA_AUTO_REGISTER_SCHEMAS": "false"}):
        config = KafkaProducerConnectionConfig.model_validate(
            {"auto_register_schemas": True}
        )
    assert config.auto_register_schemas
