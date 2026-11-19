from typing import Any, Dict, Type

import pytest
from pydantic import ValidationError

from datahub.configuration.common import ConfigModel
from datahub.configuration.source_common import EnvConfigMixin, PlatformDetail
from datahub.ingestion.source.dlt.config import DestinationPlatformConfig
from datahub.ingestion.source.kinesis.kinesis_config import DestinationPlatformDetail
from datahub.ingestion.source.odcs.odcs_config import ServerMapping
from datahub.ingestion.source.sap_datasphere.config import ConnectionPlatformConfig
from datahub.ingestion.source.unity.config import FederationConnectionDetail

# Every recipe-level `env` field must accept and reject the same values, or one
# block of a recipe silently writes URNs that another block can never match.
ENV_CONFIGS = [
    pytest.param(EnvConfigMixin, {}, id="EnvConfigMixin"),
    pytest.param(PlatformDetail, {}, id="PlatformDetail"),
    pytest.param(FederationConnectionDetail, {}, id="unity"),
    pytest.param(DestinationPlatformConfig, {}, id="dlt"),
    pytest.param(ServerMapping, {"server": "s"}, id="odcs"),
    pytest.param(ConnectionPlatformConfig, {"platform": "snowflake"}, id="sap"),
    pytest.param(DestinationPlatformDetail, {}, id="kinesis"),
]


@pytest.mark.parametrize("config_cls, required", ENV_CONFIGS)
def test_env_is_normalized_and_validated_everywhere(
    config_cls: Type[ConfigModel], required: Dict[str, Any]
) -> None:
    assert config_cls.model_validate({**required, "env": "prod"}).env == "PROD"  # type: ignore[attr-defined]
    with pytest.raises(ValidationError, match="env must be one of"):
        config_cls.model_validate({**required, "env": "PORD"})
