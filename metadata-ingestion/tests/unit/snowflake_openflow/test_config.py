import pytest

from datahub.ingestion.source.snowflake.snowflake_openflow_config import (
    SnowflakeOpenflowSourceConfig,
)

MINIMAL = {
    "connection": {
        "account_id": "abc12345",
        "username": "user",
        "password": "pass",
    }
}


def test_foreign_snowflake_coordinates_are_independent_of_openflow_coordinates():
    config = SnowflakeOpenflowSourceConfig.model_validate(
        {
            **MINIMAL,
            "platform_instance": "openflow_prod",
            "env": "PROD",
            "snowflake_platform_instance": "warehouse_prod",
            "snowflake_env": "DEV",
        }
    )
    identifier_config = config.get_snowflake_identifier_config()
    # The destination coordinates must come from the snowflake_* fields, never
    # from Openflow's own. Leaking Openflow's platform_instance here points
    # every lineage edge at a URN the warehouse ingestion never emitted.
    assert identifier_config.platform_instance == "warehouse_prod"
    assert identifier_config.env == "DEV"


def test_snowflake_env_defaults_to_openflow_env():
    # A user who sets only `env` almost always means both; defaulting avoids a
    # silent PROD/DEV split that produces well-formed URNs pointing nowhere.
    config = SnowflakeOpenflowSourceConfig.model_validate({**MINIMAL, "env": "DEV"})
    assert config.get_snowflake_identifier_config().env == "DEV"


def test_invalid_env_is_rejected():
    with pytest.raises(ValueError):
        SnowflakeOpenflowSourceConfig.model_validate({**MINIMAL, "env": "NOT_AN_ENV"})


def test_invalid_snowflake_env_is_rejected():
    with pytest.raises(ValueError):
        SnowflakeOpenflowSourceConfig.model_validate(
            {**MINIMAL, "snowflake_env": "NOT_AN_ENV"}
        )


def test_upstream_coordinates_are_independent_of_openflow_coordinates():
    config = SnowflakeOpenflowSourceConfig.model_validate(
        {
            **MINIMAL,
            "platform_instance": "openflow_prod",
            "env": "PROD",
            "source_platform_instance": "pg_prod",
            "source_env": "DEV",
        }
    )
    assert config.source_platform_instance == "pg_prod"
    assert config.source_env == "DEV"


def test_source_env_defaults_to_openflow_env():
    config = SnowflakeOpenflowSourceConfig.model_validate({**MINIMAL, "env": "DEV"})
    assert config.source_env == "DEV"


def test_invalid_source_env_is_rejected():
    # Matched on the validation semantics, not the exact wording: without the
    # match, an "extra key not permitted" error would also satisfy the raise and
    # the test would pass even if the field were never added.
    with pytest.raises(ValueError, match="must be one of"):
        SnowflakeOpenflowSourceConfig.model_validate(
            {**MINIMAL, "source_env": "NOT_AN_ENV"}
        )
