from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.airbyte.config import (
    KNOWN_SOURCE_TYPE_MAPPING,
    AirbyteDeploymentType,
    AirbyteSourceConfig,
    PlatformDetail,
)
from datahub.ingestion.source.airbyte.models import (
    AirbyteDestinationPartial,
    AirbyteSourcePartial,
)
from datahub.ingestion.source.airbyte.source import (
    AirbyteSource,
    _map_source_type_to_platform,
)


@pytest.fixture
def mock_ctx():
    ctx = MagicMock(spec=PipelineContext)
    ctx.graph = MagicMock()
    ctx.pipeline_name = "airbyte_test"
    return ctx


def test_source_type_mapping():
    assert _map_source_type_to_platform("postgres", {}) == "postgres"
    assert _map_source_type_to_platform("postgresql", {}) == "postgres"
    assert _map_source_type_to_platform("MySQL", {}) == "mysql"

    user_mapping = {"PostgreSQL": "my-postgres"}
    assert _map_source_type_to_platform("PostgreSQL", user_mapping) == "my-postgres"

    assert _map_source_type_to_platform("Custom Database", {}) == "custom-database"


def test_known_source_type_mapping_is_lowercase():
    # Keys and values are looked up case-insensitively after lowercasing
    # in `_map_source_type_to_platform`; the canonical form must match.
    for key, value in KNOWN_SOURCE_TYPE_MAPPING.items():
        assert key == key.lower()
        assert value == value.lower()


@patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
def test_per_source_platform_override(mock_create_client, mock_ctx):
    mock_create_client.return_value = MagicMock()

    config = AirbyteSourceConfig(
        deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
        host_port="http://localhost:8000",
        sources_to_platform_instance={
            "source-1": PlatformDetail(
                platform="postgres", platform_instance="prod-postgres", env="PROD"
            )
        },
    )
    source = AirbyteSource(config, mock_ctx)

    mock_source = AirbyteSourcePartial.model_validate(
        {"sourceId": "source-1", "name": "Test Source", "sourceType": "PostgreSQL"}
    )

    platform_info = source._get_platform_for_source(mock_source)

    assert platform_info.platform == "postgres"
    assert platform_info.platform_instance == "prod-postgres"
    assert platform_info.env == "PROD"


@patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
def test_per_destination_platform_override(mock_create_client, mock_ctx):
    mock_create_client.return_value = MagicMock()

    config = AirbyteSourceConfig(
        deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
        host_port="http://localhost:8000",
        destinations_to_platform_instance={
            "dest-1": PlatformDetail(
                platform="snowflake", platform_instance="prod-snowflake", env="PROD"
            )
        },
    )
    source = AirbyteSource(config, mock_ctx)

    mock_dest = AirbyteDestinationPartial.model_validate(
        {
            "destinationId": "dest-1",
            "name": "Test Destination",
            "destinationType": "Snowflake",
        }
    )

    platform_info = source._get_platform_for_destination(mock_dest)

    assert platform_info.platform == "snowflake"
    assert platform_info.platform_instance == "prod-snowflake"
    assert platform_info.env == "PROD"


@patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
def test_source_type_mapping_config(mock_create_client, mock_ctx):
    mock_create_client.return_value = MagicMock()

    config = AirbyteSourceConfig(
        deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
        host_port="http://localhost:8000",
        source_type_mapping={"Custom DB": "custom-platform"},
    )
    source = AirbyteSource(config, mock_ctx)

    mock_source = AirbyteSourcePartial.model_validate(
        {"sourceId": "source-1", "name": "Test Source", "sourceType": "Custom DB"}
    )

    assert source._get_platform_for_source(mock_source).platform == "custom-platform"


@patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
def test_platform_detail_defaults(mock_create_client, mock_ctx):
    mock_create_client.return_value = MagicMock()

    config = AirbyteSourceConfig(
        deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
        host_port="http://localhost:8000",
    )
    source = AirbyteSource(config, mock_ctx)

    mock_source = AirbyteSourcePartial.model_validate(
        {"sourceId": "source-1", "name": "Test Source", "sourceType": "postgres"}
    )

    platform_info = source._get_platform_for_source(mock_source)

    assert platform_info.platform == "postgres"
    assert platform_info.platform_instance is None
    assert platform_info.env is None
