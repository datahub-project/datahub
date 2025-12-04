import os
from unittest.mock import patch


class TestKafkaProducerConnectionConfig:
    def test_disable_auto_schema_registration_default(self):
        """Default is False when env var is not set."""
        with patch.dict(os.environ, {}, clear=True):
            # Note: default_factory is evaluated at class definition time
            # so we test the actual environment variable function
            from datahub.configuration.env_vars import (
                get_kafka_disable_auto_schema_registration,
            )

            assert get_kafka_disable_auto_schema_registration() is False

    def test_disable_auto_schema_registration_env_true(self):
        """Returns True when env var is 'true'."""
        with patch.dict(
            os.environ,
            {"DATAHUB_KAFKA_DISABLE_AUTO_SCHEMA_REGISTRATION": "true"},
            clear=False,
        ):
            from datahub.configuration.env_vars import (
                get_kafka_disable_auto_schema_registration,
            )

            assert get_kafka_disable_auto_schema_registration() is True

    def test_disable_auto_schema_registration_env_one(self):
        """Returns True when env var is '1'."""
        with patch.dict(
            os.environ,
            {"DATAHUB_KAFKA_DISABLE_AUTO_SCHEMA_REGISTRATION": "1"},
            clear=False,
        ):
            from datahub.configuration.env_vars import (
                get_kafka_disable_auto_schema_registration,
            )

            assert get_kafka_disable_auto_schema_registration() is True

    def test_disable_auto_schema_registration_env_false(self):
        """Returns False when env var is 'false'."""
        with patch.dict(
            os.environ,
            {"DATAHUB_KAFKA_DISABLE_AUTO_SCHEMA_REGISTRATION": "false"},
            clear=False,
        ):
            from datahub.configuration.env_vars import (
                get_kafka_disable_auto_schema_registration,
            )

            assert get_kafka_disable_auto_schema_registration() is False
