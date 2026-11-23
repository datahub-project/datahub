from typing import Callable, Dict

import pytest
from pydantic import BaseModel

from datahub.configuration.source_common import (
    EnvConfigMixin,
    is_env_name,
    validate_env,
)
from datahub.ingestion.source.dlt.config import DestinationPlatformConfig
from datahub.ingestion.source.kinesis.kinesis_config import DestinationPlatformDetail
from datahub.ingestion.source.odcs.odcs_config import ServerMapping
from datahub.ingestion.source.sap_datasphere.config import ConnectionPlatformConfig
from datahub.ingestion.source.unity.config import FederationConnectionDetail

# Every config model with an env field, built from just that env.
ENV_SITES: Dict[str, Callable[[str], BaseModel]] = {
    "EnvConfigMixin": lambda env: EnvConfigMixin(env=env),
    "dlt": lambda env: DestinationPlatformConfig(env=env),
    "kinesis": lambda env: DestinationPlatformDetail(env=env),
    "odcs": lambda env: ServerMapping(server="s", env=env),
    "sap_datasphere": lambda env: ConnectionPlatformConfig(platform="p", env=env),
    "unity": lambda env: FederationConnectionDetail(env=env),
}


def test_built_in_envs_are_upper_cased_as_before() -> None:
    assert validate_env("PROD") == "PROD"
    assert validate_env("prod") == "PROD"
    assert validate_env("Dev") == "DEV"


def test_custom_env_ids_are_accepted_and_upper_cased() -> None:
    assert validate_env("MY_ENV") == "MY_ENV"
    assert validate_env("my_env") == "MY_ENV"
    assert validate_env("E" + "1" * 63) == "E" + "1" * 63


@pytest.mark.parametrize("env", ["my-env", "1ENV", "_ENV", "MY ENV", "", "E" * 65])
def test_malformed_envs_are_rejected(env: str) -> None:
    with pytest.raises(ValueError, match="env must be one of"):
        validate_env(env)


def test_custom_env_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level("INFO"):
        validate_env("MY_ENV")
        validate_env("PROD")

    assert "'MY_ENV' is not a built-in environment" in caplog.text
    assert "PROD" not in caplog.text


def test_urn_env_names_are_exact() -> None:
    assert is_env_name("PROD")
    assert is_env_name("MY_ENV")
    assert not is_env_name("prod")
    assert not is_env_name("my-env")


@pytest.mark.parametrize("site", sorted(ENV_SITES))
def test_every_config_site_accepts_a_custom_env(site: str) -> None:
    config = ENV_SITES[site]("my_env")
    assert config.model_dump()["env"] == "MY_ENV"


@pytest.mark.parametrize("site", sorted(ENV_SITES))
def test_every_config_site_rejects_a_malformed_env(site: str) -> None:
    with pytest.raises(ValueError, match="env must be one of"):
        ENV_SITES[site]("my-env")
